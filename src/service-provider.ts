import type { BeaconService } from "./beacon.ts";
import type { ConcordService, ContractHandle } from "./concord.ts";
import { ServiceUnavailable, ValidationError } from "./errors.ts";
import { cloneJson, type JsonObject, type JsonValue } from "./json.ts";
import { messageIsDeliverableTo } from "./lanes.ts";
import type { MessageDelivery } from "./messaging.ts";
import { abortable, bounded, immutable, StateCell, type RuntimeFailure } from "./runtime.ts";
import { defaultPayloadSchemas, type PayloadSchemas } from "./schema.ts";
import type { StateStore } from "./state.ts";
import { ServiceAdvertiser, ServiceUseAuthorizer, ServiceViewStoreWriter, validateDeclaredServiceMessage,
  validateServiceMessageBody, validateServiceEnvelope, validateServiceProtocol,
  type RegisteredEndpointLane, type ServiceProtocol, type ServiceUseScope, type ServiceUseTerms,
  type ServiceViewWriteContext, type ServiceOperationContext, type ServiceMessageBody,
  type ServiceMessageStatus, type ServiceError, type ServiceBackendStatus,
} from "./services.ts";

export interface ServiceProviderState {
  readonly status: "starting" | "ready" | "unavailable" | "failed" | "closed";
  readonly backendStatus: ServiceBackendStatus;
  readonly diagnostics: JsonObject;
  readonly failure?: RuntimeFailure;
}
export interface ServiceBackendContext {
  readonly signal: AbortSignal;
  /** Reports domain availability; unavailable immediately revokes protected work. */
  report(status: ServiceBackendStatus, diagnostics?: JsonObject): void;
}
export interface ServiceBackend {
  start(context: ServiceBackendContext): void | Promise<void>;
  /** Must be idempotent, including if start completes after cancellation. */
  close(): void | Promise<void>;
}
export interface ServiceOperationReply { status?: ServiceMessageStatus; result?: JsonValue; error?: ServiceError; }
export interface ServiceProviderContext extends ServiceOperationContext {
  viewWriter(storeName: string): Promise<ServiceViewStoreWriter>;
}
export interface ManagedServiceProviderOptions {
  protocol: ServiceProtocol; serviceId: string; endpoint: RegisteredEndpointLane;
  beacon: BeaconService; concord: ConcordService;
  acceptContract?: (scope: ServiceUseTerms) => boolean | Promise<boolean>;
  scope?: (scope: ServiceUseScope) => ServiceUseScope;
  /** Enable only when the service's profile requires one provider lifetime. */
  exclusive?: boolean;
  backend?: ServiceBackend;
  initialBackendStatus?: ServiceBackendStatus;
  handle?: (body: ServiceMessageBody, context: ServiceProviderContext) => ServiceOperationReply | void | Promise<ServiceOperationReply | void>;
  authorizeOperation?: (body: ServiceMessageBody, context: ServiceOperationContext) => boolean | Promise<boolean>;
  stateFor?: (storeName: string) => StateStore;
  schemas?: PayloadSchemas;
  onError?: (error: unknown) => void;
  /** Starts once for each accepted agreement, independently of other peers. */
  onAgreement?: (context: ServiceViewWriteContext) => void | Promise<void>;
}

/** Owns protocol and backend lifetimes while the host supplies domain behavior. */
export class ManagedServiceProvider {
  private readonly options: ManagedServiceProviderOptions;
  private readonly advertiser: ServiceAdvertiser;
  private readonly authorizer: ServiceUseAuthorizer;
  private readonly abort = new AbortController();
  private availability = new AbortController();
  private readonly states: StateCell<ServiceProviderState>;
  private readonly tasks = new Set<Promise<unknown>>();
  private readonly writers = new Map<string, ServiceViewStoreWriter>();
  private starting?: Promise<void>;
  private backendStarting?: Promise<void>;
  private listening?: Promise<void>;
  private advertising: Promise<void> = Promise.resolve();
  private bootstrapped = false;
  private closing?: Promise<void>;
  constructor(options: ManagedServiceProviderOptions) {
    options = { ...options, protocol: immutable(validateServiceProtocol(options.protocol)) };
    this.options = options;
    if (options.initialBackendStatus !== undefined && !["available", "degraded", "unavailable"].includes(options.initialBackendStatus)) throw new ValidationError("Invalid backend status");
    this.states = new StateCell<ServiceProviderState>({ status: "starting", backendStatus: options.initialBackendStatus ?? "available", diagnostics: {} });
    this.advertiser = new ServiceAdvertiser(options);
    if (options.endpoint.endpoint !== `service:${options.serviceId}`) throw new ValidationError("Provider endpoint must equal service:<serviceId>");
    if (options.handle && !options.endpoint.watch) throw new ValidationError("A managed handler requires a watch-capable services lane");
    this.authorizer = new ServiceUseAuthorizer({ ...options, onAgreement: contract => {
      if (options.onAgreement) this.track((async () => {
        await this.waitFor(state => state.status === "ready" || state.status === "failed" || state.status === "closed", { signal: this.abort.signal });
        const context = await this.viewContext(contract);
        await context.authority.assertActive();
        await options.onAgreement!(context);
      })());
    }, acceptContract: async scope => {
      if (!this.available) return false;
      const accepted = options.acceptContract ? await options.acceptContract(scope) : true;
      return this.available && accepted;
    } });
  }
  get state() { return this.states.current; }
  get ready(): boolean { return this.available && this.bootstrapped && this.state.status === "ready"; }
  private get available(): boolean { return !this.abort.signal.aborted && this.state.status !== "failed" && this.state.backendStatus !== "unavailable"; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: ServiceProviderState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.options.concord.scheduler });
  }
  start(): Promise<void> {
    if (this.abort.signal.aborted) return Promise.reject(new ServiceUnavailable("provider_closed", "Service provider is closed"));
    if (!this.starting) this.starting = abortable(this.startNow(), this.abort.signal).catch(async error => {
      if (!this.abort.signal.aborted) {
        this.states.set({ ...this.state, status: "failed", failure: { code: "provider_start_failed", message: String(error) } });
        this.availability.abort();
        await bounded(Promise.allSettled([this.authorizer.close(), this.advertiser.withdraw(), Promise.resolve().then(() => this.options.backend?.close())]), 5000, this.options.concord.scheduler).catch(() => {});
      }
      throw error;
    });
    return this.starting;
  }
  private async startNow(): Promise<void> {
    await this.authorizer.bootstrap({ exclusive: this.options.exclusive });
    this.assertOpen();
    if (this.options.backend) {
      this.backendStarting = Promise.resolve().then(() => this.options.backend!.start({ signal: this.abort.signal,
        report: (status, diagnostics) => { void this.setBackendStatus(status, diagnostics).catch(error => this.report(error)); },
      })).then(() => {});
      void this.backendStarting.finally(() => { if (this.abort.signal.aborted) this.track(Promise.resolve().then(() => this.options.backend!.close())); }).catch(() => {});
      await abortable(this.backendStarting, this.abort.signal);
    }
    this.assertOpen();
    await this.authorizer.start();
    if (this.options.handle) this.listening = this.listen();
    this.bootstrapped = true;
    await this.publish();
    this.assertOpen();
    this.states.set({ ...this.state, status: this.available ? "ready" : "unavailable" });
  }
  async setBackendStatus(status: ServiceBackendStatus, diagnostics: JsonObject = {}): Promise<void> {
    this.assertOpen();
    if (!["available", "degraded", "unavailable"].includes(status)) throw new ValidationError("Invalid backend status");
    const wasUnavailable = this.state.backendStatus === "unavailable";
    this.states.set({ status: this.bootstrapped ? status === "unavailable" ? "unavailable" : "ready" : "starting", backendStatus: status, diagnostics: immutable(cloneJson(diagnostics)) });
    if (status === "unavailable") {
      this.availability.abort();
      this.track(this.authorizer.revoke());
      for (const writer of this.writers.values()) this.track(writer.close());
      this.writers.clear();
    } else if (wasUnavailable) { this.availability = new AbortController(); this.authorizer.wake(); }
    if (this.bootstrapped) await this.publish();
  }
  private publish(): Promise<void> {
    const { backendStatus, diagnostics } = this.state;
    const operation = this.advertising.catch(() => {}).then(async () => {
      this.assertOpen();
      await this.advertiser.publish(backendStatus, { diagnostics });
      if (this.abort.signal.aborted) await this.advertiser.withdraw();
    });
    this.advertising = operation;
    return operation;
  }
  /** Explicit domain-consent reevaluation; clients recover through new agreements. */
  async reconcile(): Promise<void> { await this.authorizer.reconcileContracts(); }
  async authorizeMessage(...args: Parameters<ServiceUseAuthorizer["authorizeMessage"]>) {
    if (!this.ready) return "denied" as const;
    const decision = await this.authorizer.authorizeMessage(...args);
    if (decision !== "authorized") return decision;
    try { await this.operationContext(args[0]); return "authorized" as const; } catch { return "denied" as const; }
  }
  async viewContext(contract: ContractHandle): Promise<ServiceViewWriteContext> {
    this.assertReady();
    const context = await this.authorizer.viewContext(contract);
    const authority = this.fence(context.authority);
    await authority.assertActive();
    return { ...context, authority };
  }
  async send(contract: ContractHandle, name: string, payload: JsonObject): Promise<void> {
    this.assertReady();
    const signal = AbortSignal.any([this.abort.signal, this.availability.signal]);
    await this.authorizer.send(contract, name, payload, { signal, assertActive: async () => { this.assertReady(); } });
  }
  private fence(authority: Pick<ServiceOperationContext, "signal" | "assertActive">) {
    const signal = AbortSignal.any([authority.signal, this.abort.signal, this.availability.signal]);
    const assertActive = async () => {
      this.assertReady();
      if (signal.aborted) throw new ServiceUnavailable("context_ended", "Service context has ended");
      await authority.assertActive();
      this.assertReady();
      if (signal.aborted) throw new ServiceUnavailable("context_ended", "Service context has ended");
    };
    return { signal, assertActive };
  }
  async operationContext(message: Parameters<ServiceUseAuthorizer["operationContext"]>[0]): Promise<ServiceProviderContext> {
    this.assertReady();
    const original = await this.authorizer.operationContext(message), fenced = this.fence(original);
    const body = validateServiceMessageBody(message.body);
    const base = Object.freeze({ ...original, ...fenced });
    const assertActive = async () => {
      await fenced.assertActive();
      if (this.options.authorizeOperation && !(await this.options.authorizeOperation(body, base))) throw new ServiceUnavailable("resource_not_authorized", "Service resource policy rejected this operation");
      await fenced.assertActive();
    };
    const context: ServiceProviderContext = { ...base, assertActive,
      send: async (name, payload, identifiers, guard) => {
        const contract = await this.options.concord.getContract(original.contract);
        if (!contract) throw new ServiceUnavailable("contract_unavailable", "Service-use contract has ended");
        await this.authorizer.send(contract, name, payload, {
          signal: guard ? AbortSignal.any([fenced.signal, guard.signal]) : fenced.signal,
          assertActive: async () => { await assertActive(); await guard?.assertActive(); },
        }, identifiers);
      },
      viewContext: async () => { await assertActive(); return { ...await original.viewContext(), authority: { signal: fenced.signal, assertActive } }; },
      viewWriter: async storeName => {
        await assertActive();
        const key = JSON.stringify([original.contract, storeName, message.messageId]);
        const existing = this.writers.get(key);
        if (existing) return existing;
        if (!this.options.stateFor) throw new ServiceUnavailable("view_store_unavailable", "Provider has no view store factory");
        const viewContext = await context.viewContext();
        if (!Object.values(viewContext.views).some(view => view.storeName === storeName && view.writer === "service")) throw new ServiceUnavailable("view_not_authorized", "Store is not a service-owned view family");
        await assertActive();
        const raced = this.writers.get(key);
        if (raced) return raced;
        if (this.writers.size >= 256) throw new ServiceUnavailable("view_writer_limit", "Provider view writer limit reached");
        const writer = new ServiceViewStoreWriter({ state: this.options.stateFor(storeName), context: viewContext });
        this.writers.set(key, writer);
        const close = () => { if (this.writers.get(key) === writer) this.writers.delete(key); this.track(writer.close()); };
        fenced.signal.addEventListener("abort", close, { once: true });
        if (fenced.signal.aborted) close();
        return writer;
      },
    };
    await assertActive();
    return Object.freeze(context);
  }
  async dispatch(delivery: MessageDelivery): Promise<void> {
    const message = validateServiceEnvelope(delivery.message), body = validateServiceMessageBody(message.body);
    const context = await this.operationContext(message);
    await context.assertActive();
    if (!messageIsDeliverableTo(message, this.options.endpoint.endpoint, { endpointSessionId: this.options.endpoint.sessionId, now: new Date(this.options.concord.scheduler.now()) })) throw new ServiceUnavailable("message_expired", "Service message expired before dispatch");
    if (!this.options.handle) throw new ServiceUnavailable("handler_unavailable", "Service has no operation handler");
    const reply = await this.options.handle(immutable(body), context);
    if (body.exchangePattern === "one_way") {
      if (reply !== undefined) throw new ValidationError("One-way service handlers cannot reply");
      return;
    }
    if (!reply) throw new ServiceUnavailable("reply_missing", "Request/reply handler returned no reply");
    const response = { serviceNamespace: body.serviceNamespace, name: body.name, intent: body.intent, exchangePattern: body.exchangePattern,
      status: reply.status ?? "ok", ...(reply.result === undefined ? {} : { result: reply.result }), ...(reply.error === undefined ? {} : { error: reply.error }) };
    validateDeclaredServiceMessage(response, this.options.protocol, { direction: "consumer_to_service", operations: context.scope.allowedOperations, reply: true, schemas: this.options.schemas ?? defaultPayloadSchemas });
    await context.assertActive();
    await delivery.reply(response as unknown as JsonObject);
  }
  private async listen(): Promise<void> {
    try {
      for await (const delivery of this.options.endpoint.watch!({ signal: this.abort.signal })) {
        if (this.tasks.size >= 256) { this.report(new ServiceUnavailable("provider_busy", "Service worker limit reached")); continue; }
        this.track(this.dispatch(delivery));
      }
    } catch (error) { if (!this.abort.signal.aborted) { this.report(error); void this.close(); } }
  }
  private track(task: Promise<unknown>): void {
    const owned = task.catch(error => this.report(error));
    this.tasks.add(owned); void owned.finally(() => this.tasks.delete(owned));
  }
  private report(error: unknown): void { try { this.options.onError?.(error); } catch { /* Diagnostics do not own lifecycle. */ } }
  private assertOpen(): void {
    if (this.abort.signal.aborted) throw new ServiceUnavailable("provider_closed", "Service provider is closed");
    if (this.state.status === "failed") throw new ServiceUnavailable("provider_failed", "Service provider startup failed");
  }
  private assertReady(): void { if (!this.ready) throw new ServiceUnavailable("provider_unavailable", "Service provider is not ready"); }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.abort.abort(); this.availability.abort();
    this.states.set({ ...this.state, status: "closed" }); this.states.close();
    const closing = [this.authorizer.close(), this.advertiser.withdraw(), ...[...this.writers.values()].map(writer => writer.close())];
    this.writers.clear();
    this.closing = bounded(Promise.allSettled([...closing, this.advertising, this.listening, ...this.tasks,
      Promise.resolve().then(() => this.options.backend?.close())]), 5000, this.options.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}
