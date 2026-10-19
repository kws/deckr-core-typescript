import { BeaconService, type BeaconFeatureWatchItem } from "./beacon.ts";
import { ConcordService, type ContractPointer } from "./concord.ts";
import { ServiceUnavailable } from "./errors.ts";
import type { JsonObject } from "./json.ts";
import { AsyncQueue, bounded, immutable } from "./runtime.ts";
import type { StateStore } from "./state.ts";
import {
  parseServiceDescriptor, ServiceMessageChannel, ServiceUseLeaseManager, ServiceViewReader,
  type RegisteredEndpointLane, type ServiceDescriptor, type ServiceMessageBody,
  type ServiceProtocol, type ServiceUseLease, type ServiceViewRef,
} from "./services.ts";

export interface ServiceClientState {
  readonly status: "connecting" | "ready" | "reconnecting" | "closed";
  readonly contract?: ContractPointer;
  readonly reason?: string;
}

/** One feature watch and one managed relationship, with parallel candidate negotiation. */
export class ManagedServiceClient {
  private readonly options: {
    protocol: ServiceProtocol; endpoint: RegisteredEndpointLane; beacon: BeaconService; concord: ConcordService;
    serviceUseIndex: StateStore; stateFor: (storeName: string) => StateStore;
    serviceId?: string; predicate?: (descriptor: ServiceDescriptor) => boolean;
    operations?: string[]; views?: string[] | Record<string, string[]>; negotiationTimeoutMs?: number;
  };
  private readonly abort = new AbortController();
  private readonly candidates = new Map<string, ServiceDescriptor>();
  private readonly admitted = new Set<string>();
  private readonly attempts = new Map<string, { manager: ServiceUseLeaseManager; task?: Promise<void> }>();
  private readonly subscribers = new Set<AsyncQueue<ServiceClientState>>();
  private winner?: ServiceUseLease;
  private winnerManager?: ServiceUseLeaseManager;
  private unwatchWinner?: () => void;
  private sourceSubscriptions: (() => void)[] = [];
  private discoveryTask?: Promise<void>;
  private epoch = 0;
  private cancelRetry?: () => void;
  private stateValue: ServiceClientState = Object.freeze({ status: "connecting" });
  constructor(options: ManagedServiceClient["options"]) { this.options = options; }
  get state(): ServiceClientState { return this.stateValue; }

  async start(): Promise<void> {
    if (this.abort.signal.aborted) throw new ServiceUnavailable("client_closed", "Service client is closed");
    if (this.discoveryTask) return;
    await Promise.all([this.options.beacon.start(), this.options.concord.start()]);
    if (this.abort.signal.aborted || this.discoveryTask) return;
    const recovered = () => {
      if (!this.options.concord.contractSource.snapshot.current || !this.options.concord.tokenSource.snapshot.current) {
        if (!this.winner) {
          this.epoch++; this.admitted.clear();
          for (const attempt of this.attempts.values()) void attempt.manager.close();
          this.attempts.clear();
          this.publish({ status: "reconnecting" });
        }
        return;
      }
      if (!this.winner && this.options.concord.contractSource.snapshot.current && this.options.concord.tokenSource.snapshot.current) this.admit();
    };
    this.sourceSubscriptions = [this.options.concord.contractSource.subscribe(recovered), this.options.concord.tokenSource.subscribe(recovered)];
    this.discoveryTask = this.discover();
  }

  watch(options: { signal?: AbortSignal } = {}): AsyncIterable<ServiceClientState> {
    const signal = options.signal;
    const queue = new AsyncQueue<ServiceClientState>(1, true, () => {
      this.subscribers.delete(queue); signal?.removeEventListener("abort", close);
    });
    const close = () => queue.close();
    this.subscribers.add(queue); queue.push(this.stateValue);
    signal?.addEventListener("abort", close, { once: true });
    if (signal?.aborted || this.abort.signal.aborted) close();
    return queue;
  }

  async ready(timeoutMs = 10_000): Promise<ServiceUseLease> {
    await this.start();
    if (this.state.status === "ready" && this.winner?.agreement.valid) return this.winner;
    const abort = new AbortController();
    try {
      return await bounded((async () => {
        for await (const state of this.watch({ signal: abort.signal })) {
          if (state.status === "ready" && this.winner?.agreement.valid) return this.winner;
          if (state.status === "closed") break;
        }
        throw new ServiceUnavailable("client_closed", "Service client is closed");
      })(), timeoutMs, this.options.concord.scheduler);
    } finally { abort.abort(); }
  }

  async request(name: string, params: JsonObject = {}, options: { timeoutMs?: number } = {}): Promise<ServiceMessageBody> {
    const lease = await this.ready(options.timeoutMs);
    // Dispatch once. Recovery never replays a published operation.
    return new ServiceMessageChannel({ endpoint: this.options.endpoint }).request(lease, name, params, options);
  }
  async send(name: string, payload: JsonObject): Promise<void> {
    await new ServiceMessageChannel({ endpoint: this.options.endpoint }).send(await this.ready(), name, payload);
  }
  async read(view: ServiceViewRef): Promise<JsonObject | null> {
    return new ServiceViewReader({ stateFor: this.options.stateFor }).read(await this.ready(), view);
  }

  private publish(state: ServiceClientState): void {
    this.stateValue = immutable(state);
    for (const subscriber of this.subscribers) subscriber.push(this.stateValue);
  }

  private async discover(): Promise<void> {
    try {
      for await (const item of this.options.beacon.watchFeature(this.options.protocol.featureId, { signal: this.abort.signal })) {
        this.updateCandidates(item);
        this.admit();
      }
    } catch (error) {
      if (!this.abort.signal.aborted && !this.winner) this.publish({ status: "reconnecting", reason: String(error) });
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
      if (descriptor) this.candidates.set(item.key, descriptor);
      else this.candidates.delete(item.key);
    } else this.candidates.delete(item.key);
    // Discovery loss does not alter the accepted agreement.
  }

  private admit(): void {
    if (this.abort.signal.aborted || this.winner || !this.options.concord.contractSource.snapshot.current || !this.options.concord.tokenSource.snapshot.current) return;
    for (const descriptor of this.candidates.values()) {
      if ((this.options.serviceId && descriptor.serviceId !== this.options.serviceId) || (this.options.predicate && !this.options.predicate(descriptor))) continue;
      const key = JSON.stringify([descriptor.endpoint, descriptor.sessionId]);
      if (this.admitted.has(key) || this.admitted.size >= 64) continue;
      this.admitted.add(key);
      const epoch = this.epoch;
      const manager = new ServiceUseLeaseManager({ endpoint: this.options.endpoint, concord: this.options.concord, serviceUseIndex: this.options.serviceUseIndex });
      const attempt: { manager: ServiceUseLeaseManager; task?: Promise<void> } = { manager };
      this.attempts.set(key, attempt);
      attempt.task = manager.ensure(descriptor, { operations: this.options.operations, views: this.options.views, timeoutMs: this.options.negotiationTimeoutMs ?? 2000 })
        .then(async lease => {
          if (this.winner || epoch !== this.epoch || this.abort.signal.aborted) { await manager.close(); return; }
          this.winner = lease; this.winnerManager = manager;
          this.unwatchWinner = lease.agreement.onChange(() => { if (lease.agreement.closed) this.recover(); });
          const losers = [...this.attempts.values()].filter(other => other !== attempt);
          await Promise.all(losers.map(other => other.manager.close()));
          if (this.winner === lease && !lease.agreement.closed && !this.abort.signal.aborted) this.publish({ status: "ready", contract: { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation } });
        }).catch(async () => { await manager.close(); })
        .finally(() => {
          if (this.attempts.get(key) === attempt) this.attempts.delete(key);
          if (!this.winner && !this.abort.signal.aborted && this.attempts.size === 0 && !this.cancelRetry) {
            this.cancelRetry = this.options.concord.scheduler.schedule(1000, () => {
              this.cancelRetry = undefined;
              this.epoch++; this.admitted.clear(); this.admit();
            });
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
    this.publish({ status: "reconnecting" });
    void previous?.close();
    this.admit();
  }

  async close(): Promise<void> {
    if (this.abort.signal.aborted) return;
    this.abort.abort(); this.epoch++;
    this.cancelRetry?.();
    this.unwatchWinner?.();
    for (const unsubscribe of this.sourceSubscriptions) unsubscribe();
    this.publish({ status: "closed" });
    const tasks = [...this.attempts.values()].map(attempt => attempt.task);
    const managers = new Set([...this.attempts.values()].map(attempt => attempt.manager));
    if (this.winnerManager) managers.add(this.winnerManager);
    await Promise.all([...managers].map(manager => manager.close()));
    await bounded(Promise.all(tasks), 5000, this.options.concord.scheduler).catch(() => {});
    await this.discoveryTask;
    this.winner = undefined;
    for (const subscriber of [...this.subscribers]) subscriber.close();
  }
}
