import { BeaconService, type BeaconFeatureWatchItem } from "./beacon.ts";
import { ConcordService, type ContractPointer } from "./concord.ts";
import { ServiceUnavailable, ValidationError } from "./errors.ts";
import type { JsonObject } from "./json.ts";
import type { DeckrMessage } from "./lanes.ts";
import { abortable, bounded, immutable, StateCell, type RuntimeFailure } from "./runtime.ts";
import { defaultPayloadSchemas, type PayloadSchemas } from "./schema.ts";
import type { StateStore } from "./state.ts";
import {
  parseServiceDescriptor, ServiceMessageChannel, ServiceUseLeaseManager, ServiceViewReader, ManagedServiceViewAccess,
  serviceViewReadContextFromLease, serviceViewAuthorityFromLease, validateServiceProtocol, validateServiceProtocolSchemas, validateDeclaredServiceMessage,
  type RegisteredEndpointLane, type ServiceDescriptor, type ServiceMessageBody,
  type ServiceProtocol, type ServiceUseLease, type ServiceViewRef, type ServiceViewAuthority, type ServiceOperationReply,
} from "./services.ts";

export interface ServiceClientState {
  readonly status: "connecting" | "ready" | "reconnecting" | "failed" | "closed";
  readonly contract?: ContractPointer;
  readonly reason?: string;
  readonly failure?: RuntimeFailure;
}
export interface ManagedServiceClientOptions {
  protocol: ServiceProtocol; endpoint: RegisteredEndpointLane; beacon: BeaconService; concord: ConcordService;
  serviceUseIndex?: StateStore; stateFor?: (storeName: string) => StateStore;
  serviceId?: string; predicate?: (descriptor: ServiceDescriptor) => boolean;
  /** Restricts every race to the supplied endpoint/session pair, using current discovery on recovery. */
  descriptor?: ServiceDescriptor;
  operations?: string[]; views?: string[] | Record<string, string[]>; negotiationTimeoutMs?: number;
  schemas?: PayloadSchemas; onError?: (error: unknown) => void;
}
export interface ServiceConsumerDelivery extends ServiceViewAuthority {
  readonly message: DeckrMessage;
  readonly body: ServiceMessageBody;
  /** Replies remain attached to the original request and its exact agreement. */
  reply(value: ServiceOperationReply): Promise<void>;
}

/** Shared feature discovery and one owned service relationship; published work is never replayed. */
export class ManagedServiceClient {
  private readonly options: ManagedServiceClientOptions;
  private readonly abort = new AbortController();
  private readonly candidates = new Map<string, ServiceDescriptor>();
  private readonly admitted = new Set<string>();
  private readonly attempts = new Map<string, { manager: ServiceUseLeaseManager; task?: Promise<void> }>();
  private readonly states = new StateCell<ServiceClientState>({ status: "connecting" });
  private readonly accesses = new Set<ManagedServiceViewAccess>();
  private winner?: ServiceUseLease;
  private winnerManager?: ServiceUseLeaseManager;
  private unwatchWinner?: () => void;
  private sourceSubscriptions: (() => void)[] = [];
  private discoveryTask?: Promise<void>;
  private starting?: Promise<void>;
  private closing?: Promise<void>;
  private epoch = 0;
  private cancelRetry?: () => void;
  constructor(options: ManagedServiceClientOptions) {
    const protocol = validateServiceProtocol(options.protocol);
    validateServiceProtocolSchemas(protocol, options.schemas ?? defaultPayloadSchemas);
    const descriptor = options.descriptor && parseServiceDescriptor(options.descriptor.candidate, protocol);
    if (options.descriptor && (!descriptor || descriptor.endpoint !== options.descriptor.endpoint || descriptor.sessionId !== options.descriptor.sessionId)) throw new ValidationError("Invalid exact service descriptor");
    if (options.negotiationTimeoutMs !== undefined && (!Number.isFinite(options.negotiationTimeoutMs) || options.negotiationTimeoutMs < 0)) throw new ValidationError("Invalid negotiation timeout");
    this.options = { ...options, protocol, ...(descriptor ? { descriptor } : {}) };
  }
  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: ServiceClientState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.options.concord.scheduler });
  }
  start(): Promise<void> {
    if (this.abort.signal.aborted) return Promise.reject(new ServiceUnavailable("client_closed", "Service client is closed"));
    if (!this.starting) this.starting = abortable(this.startNow(), this.abort.signal).catch(error => {
      if (!this.abort.signal.aborted) this.fail(error);
      throw error;
    });
    return this.starting;
  }
  private async startNow(): Promise<void> {
    await Promise.all([this.options.beacon.start(), this.options.concord.start()]);
    if (this.abort.signal.aborted) return;
    const recovered = () => {
      if (!this.options.concord.contractSource.snapshot.current || !this.options.concord.tokenSource.snapshot.current) {
        if (!this.winner) {
          this.epoch++; this.admitted.clear();
          for (const attempt of this.attempts.values()) void attempt.manager.close().catch(error => this.report(error));
          this.attempts.clear();
          this.states.set({ status: "reconnecting", reason: "authority_source_stale" });
        }
      } else if (!this.winner) this.admit();
    };
    this.sourceSubscriptions = [this.options.concord.contractSource.subscribe(recovered), this.options.concord.tokenSource.subscribe(recovered)];
    this.discoveryTask = this.discover();
    // Exact negotiation starts with the supplied descriptor. Later races still
    // need current discovery, and cannot silently select a replacement session.
    if (this.options.descriptor) {
      this.candidates.set(this.options.descriptor.candidate.key, this.options.descriptor);
      this.admit();
    }
  }

  async ready(timeoutMs = 10_000, options: { signal?: AbortSignal } = {}): Promise<ServiceUseLease> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new ValidationError("Invalid readiness timeout");
    const waiting = new AbortController();
    const signal = AbortSignal.any([this.abort.signal, waiting.signal, ...(options.signal ? [options.signal] : [])]);
    try {
      return await bounded(abortable((async () => {
        await this.start();
        await this.waitFor(state => state.status === "ready" || state.status === "failed" || state.status === "closed", { signal });
        if (this.state.status === "ready" && this.winner?.agreement.valid) return this.winner;
        throw new ServiceUnavailable(this.state.failure?.code ?? "client_unavailable", this.state.failure?.message ?? "Service client is unavailable");
      })(), signal), timeoutMs, this.options.concord.scheduler);
    } finally { waiting.abort(); }
  }
  async request(name: string, params: JsonObject = {}, options: { timeoutMs?: number; signal?: AbortSignal } = {}): Promise<ServiceMessageBody> {
    const lease = await this.ready(options.timeoutMs, options);
    return new ServiceMessageChannel(this.options).request(lease, name, params, options);
  }
  async send(name: string, payload: JsonObject, identifiers?: Record<string, string>): Promise<void> {
    await new ServiceMessageChannel(this.options).send(await this.ready(), name, payload, identifiers);
  }
  /** Inbound operations remain bound to one accepted agreement, including after recovery. */
  async *messages(options: { signal?: AbortSignal; timeoutMs?: number } = {}): AsyncIterable<ServiceConsumerDelivery> {
    if (!this.options.endpoint.watch) throw new ServiceUnavailable("endpoint_unavailable", "Client endpoint cannot watch service messages");
    const lease = await this.ready(options.timeoutMs, options);
    const channel = new ServiceMessageChannel(this.options), authority = serviceViewAuthorityFromLease(lease);
    const signal = AbortSignal.any([this.abort.signal, authority.signal, ...(options.signal ? [options.signal] : [])]);
    const assertActive = async () => {
      if (signal.aborted) throw new ServiceUnavailable("context_ended", "Service message context has ended");
      await authority.assertActive();
      if (signal.aborted) throw new ServiceUnavailable("context_ended", "Service message context has ended");
    };
    for await (const delivery of this.options.endpoint.watch({ signal })) {
      let body: ServiceMessageBody;
      try {
        body = await channel.receive(lease, delivery.message, { now: () => new Date(this.options.concord.scheduler.now()) });
        await assertActive();
      } catch (error) { this.report(error); continue; }
      yield Object.freeze({ signal, assertActive, message: delivery.message, body: immutable(body),
        reply: async (value: ServiceOperationReply) => {
          const reply = { serviceNamespace: body.serviceNamespace, name: body.name, intent: body.intent, exchangePattern: body.exchangePattern,
            status: value.status ?? "ok", ...(value.result === undefined ? {} : { result: value.result }), ...(value.error === undefined ? {} : { error: value.error }) };
          validateDeclaredServiceMessage(reply, this.options.protocol, { direction: "service_to_consumer", operations: lease.terms.allowedOperations, reply: true, schemas: this.options.schemas });
          await assertActive(); await delivery.reply(reply as unknown as JsonObject);
        },
      });
    }
  }
  async read(view: ServiceViewRef): Promise<JsonObject | null> {
    if (!this.options.stateFor) throw new ServiceUnavailable("view_store_unavailable", "Client has no view store factory");
    return new ServiceViewReader({ stateFor: this.options.stateFor }).read(await this.ready(), view);
  }
  /** A view stream belongs to its accepted agreement; recovery never transfers it. */
  async *watchView(view: ServiceViewRef, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    if (!this.options.stateFor) throw new ServiceUnavailable("view_store_unavailable", "Client has no view store factory");
    const lease = await this.ready(options.timeoutMs, options);
    const access = new ManagedServiceViewAccess({ state: this.options.stateFor(view.storeName), readContext: serviceViewReadContextFromLease(lease, await lease.agreement.refresh()) });
    this.accesses.add(access);
    const close = () => { void access.close(); };
    const signal = AbortSignal.any([this.abort.signal, ...(options.signal ? [options.signal] : [])]);
    signal.addEventListener("abort", close, { once: true });
    try { if (!signal.aborted) yield* access.watch(view); }
    finally { signal.removeEventListener("abort", close); this.accesses.delete(access); await access.close(); }
  }
  private async discover(): Promise<void> {
    try {
      for await (const item of this.options.beacon.watchFeature(this.options.protocol.featureId, { signal: this.abort.signal })) {
        this.updateCandidates(item); this.admit();
      }
      if (!this.abort.signal.aborted) throw new ServiceUnavailable("discovery_ended", "Service discovery ended");
    } catch (error) {
      if (!this.abort.signal.aborted) {
        this.report(error); this.candidates.clear();
        // Discovery failure cannot end independently valid Concord authority.
        this.states.set({ ...this.state, failure: { code: "discovery_unavailable", message: String(error) } });
      }
    }
  }
  private updateCandidates(item: BeaconFeatureWatchItem): void {
    if (item.type === "snapshot") {
      this.candidates.clear();
      for (const candidate of item.candidates) {
        const descriptor = parseServiceDescriptor(candidate, this.options.protocol);
        if (descriptor) this.candidates.set(candidate.key, descriptor);
      }
    } else if (item.candidate) {
      const descriptor = parseServiceDescriptor(item.candidate, this.options.protocol);
      if (descriptor) this.candidates.set(item.key, descriptor); else this.candidates.delete(item.key);
    } else this.candidates.delete(item.key);
  }
  private admit(): void {
    if (this.abort.signal.aborted || this.state.status === "failed" || this.winner || !this.options.concord.contractSource.snapshot.current || !this.options.concord.tokenSource.snapshot.current) return;
    for (const descriptor of this.candidates.values()) {
      if (descriptor.backendStatus === "unavailable" || (this.options.serviceId && descriptor.serviceId !== this.options.serviceId)) continue;
      if (this.options.descriptor && (descriptor.endpoint !== this.options.descriptor.endpoint || descriptor.sessionId !== this.options.descriptor.sessionId)) continue;
      try { if (this.options.predicate && !this.options.predicate(descriptor)) continue; } catch (error) { this.report(error); continue; }
      const key = JSON.stringify([descriptor.endpoint, descriptor.sessionId]);
      if (this.admitted.has(key) || this.admitted.size >= 64) continue;
      this.admitted.add(key);
      const epoch = this.epoch;
      const manager = new ServiceUseLeaseManager(this.options);
      const attempt: { manager: ServiceUseLeaseManager; task?: Promise<void> } = { manager };
      this.attempts.set(key, attempt);
      const timeoutMs = this.options.negotiationTimeoutMs ?? 2000;
      attempt.task = bounded(manager.ensure(descriptor, { operations: this.options.operations, views: this.options.views, timeoutMs }), timeoutMs, this.options.concord.scheduler)
        .then(async lease => {
          if (this.winner || epoch !== this.epoch || this.abort.signal.aborted) { await manager.close(); return; }
          this.winner = lease; this.winnerManager = manager;
          this.cancelRetry?.(); this.cancelRetry = undefined;
          this.unwatchWinner = lease.agreement.onChange(() => { if (lease.agreement.closed) this.recover(); });
          // close revokes synchronously, then waits for bounded remote cleanup.
          await Promise.all([...this.attempts.values()].filter(other => other !== attempt).map(other => other.manager.close()));
          if (this.winner === lease && !lease.agreement.closed && !this.abort.signal.aborted) this.states.set({ status: "ready", contract: { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation } });
        }).catch(async error => { this.report(error); await manager.close(); })
        .finally(() => {
          if (this.attempts.get(key) === attempt) this.attempts.delete(key);
          if (!this.winner && !this.abort.signal.aborted && this.state.status !== "failed" && this.attempts.size === 0 && !this.cancelRetry) {
            this.cancelRetry = this.options.concord.scheduler.schedule(1000, () => { this.cancelRetry = undefined; this.epoch++; this.admitted.clear(); this.admit(); });
          }
        });
    }
  }
  private recover(): void {
    if (this.abort.signal.aborted) return;
    this.unwatchWinner?.(); this.unwatchWinner = undefined;
    const previous = this.winnerManager;
    this.winner = undefined; this.winnerManager = undefined;
    this.epoch++; this.admitted.clear();
    this.states.set({ status: "reconnecting" });
    void previous?.close().catch(error => this.report(error));
    this.admit();
  }
  private report(error: unknown): void { try { this.options.onError?.(error); } catch { /* Host diagnostics cannot own lifecycle. */ } }
  private fail(error: unknown): void {
    this.report(error);
    this.unwatchWinner?.(); this.unwatchWinner = undefined;
    this.states.set({ status: "failed", failure: { code: "service_client_failed", message: String(error) } });
    this.cancelRetry?.();
    for (const unsubscribe of this.sourceSubscriptions) unsubscribe(); this.sourceSubscriptions = [];
    for (const attempt of this.attempts.values()) void attempt.manager.close().catch(error => this.report(error));
    void this.winnerManager?.close().catch(error => this.report(error));
    this.winner = undefined;
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.abort.abort(); this.epoch++;
    this.cancelRetry?.(); this.unwatchWinner?.();
    for (const unsubscribe of this.sourceSubscriptions) unsubscribe();
    this.states.set({ status: "closed" }); this.states.close();
    const managers = new Set([...this.attempts.values()].map(attempt => attempt.manager));
    if (this.winnerManager) managers.add(this.winnerManager);
    const closing = [...managers].map(manager => manager.close());
    this.winner = undefined;
    this.closing = bounded(Promise.allSettled([...closing, ...[...this.accesses].map(access => access.close()), this.discoveryTask, this.starting,
      ...[...this.attempts.values()].map(attempt => attempt.task)]), 5000, this.options.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}
