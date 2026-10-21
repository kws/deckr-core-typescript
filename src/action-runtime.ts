import { actionAvailabilityPayload, actionRuntimeProtocol, validateActionAvailabilityPayload, type ActionAvailabilityPayload } from "./actions.ts";
import { actionProviderInstanceId, actionRuntimeMessageDefinition, actionRuntimePayloadSchemas, validateActionRuntimeMessage, validateActionRuntimePayload, type ActionRuntimePayloadSchemas } from "./action-contracts.ts";
import { validateActionRuntimeReferences, type ActionRuntimeReferenceState } from "./action-references.ts";
import { ServiceUnavailable, ValidationError } from "./errors.ts";
import { isJsonObject, cloneJson, requireText, type JsonObject } from "./json.ts";
import { buildMessage, entitySubject, type DeckrMessage } from "./lanes.ts";
import { abortable, bounded, immutable } from "./runtime.ts";
import { sourceFor } from "./source.ts";
import { ManagedServiceClient, ManagedServiceProvider, ManagedServiceViewAccess, ServiceMessageChannel, ServiceViewStoreWriter,
  serviceViewAuthorityFromLease, serviceViewReadContextFromLease, serviceViewKey,
  type ManagedServiceClientOptions, type ManagedServiceProviderOptions, type ServiceProviderContext,
  type ServiceViewAuthority, type ServiceViewWriteContext, type ServiceUseLease, type ServiceViewChange,
} from "./services.ts";
import type { ContractPointer } from "./authority.ts";

export interface ActionRuntimeAuthority extends ServiceViewAuthority {
  readonly contract: ContractPointer;
  readonly peerEndpoint: string;
  readonly peerSessionId: string;
}
export type ActionReferenceResolver = (message: DeckrMessage, authority: ActionRuntimeAuthority) => ActionRuntimeReferenceState | Promise<ActionRuntimeReferenceState>;
export interface ActionRuntimeOperation extends ActionRuntimeAuthority {
  readonly message: DeckrMessage;
  readonly name: string;
  readonly payload: JsonObject;
}
export interface ActionRuntimeSendReceipt { readonly messageId: string; }
export interface ActionRuntimeContext extends ActionRuntimeOperation {
  send(name: string, payload: JsonObject, identifiers?: Record<string, string>): Promise<ActionRuntimeSendReceipt>;
  end(): Promise<void>;
}
export interface ManagedActionRuntimeProviderOptions extends Omit<ManagedServiceProviderOptions, "protocol" | "serviceId" | "handle" | "authorizeOperation" | "onAgreement" | "schemas"> {
  providerInstanceId: string; providerId: string;
  /** Returns current authorized identities and negotiated descriptors owned by the host. */
  resolveReferences: ActionReferenceResolver;
  schemas?: ActionRuntimePayloadSchemas;
  handle(operation: ActionRuntimeContext): void | Promise<void>;
  availability?: { entries: ActionAvailabilityPayload["entries"]; labels?: Record<string, string>; annotations?: JsonObject };
}
export interface ManagedActionRuntimeClientOptions extends Omit<ManagedServiceClientOptions, "protocol" | "operations" | "views" | "schemas" | "serviceId"> {
  providerInstanceId?: string;
  resolveReferences: ActionReferenceResolver;
  schemas?: ActionRuntimePayloadSchemas;
}

function wire(name: string, payload: JsonObject, options: {
  sender: string; senderSessionId: string; recipient: string; recipientSessionId: string; serviceId: string;
  contract: ContractPointer; identifiers?: Record<string, string>;
}): DeckrMessage {
  const definition = actionRuntimeMessageDefinition(name);
  return validateActionRuntimeMessage(buildMessage({ lane: "services", sender: options.sender, senderSessionId: options.senderSessionId,
    recipient: options.recipient, recipientSessionId: options.recipientSessionId, contract: options.contract,
    subject: entitySubject("service", { ...options.identifiers, serviceId: options.serviceId, namespace: "dev.deckr.action_runtime.provider", name }),
    ...(name === "binding_render_result" ? { causationId: String(payload.requestMessageId) } : {}),
    messageType: "serviceMessage", body: { serviceNamespace: "dev.deckr.action_runtime.provider", name, intent: definition.intent, exchangePattern: "one_way",
      ...(definition.intent === "event" ? { params: {}, event: payload } : { params: payload }) },
  }));
}
async function references(resolve: ActionReferenceResolver, message: DeckrMessage, authority: ActionRuntimeAuthority, schemas = actionRuntimePayloadSchemas): Promise<void> {
  await authority.assertActive();
  const state = await abortable(Promise.resolve().then(() => {
    if (authority.signal.aborted) throw new ServiceUnavailable("context_ended", "Action Runtime context has ended");
    return resolve(message, authority);
  }), authority.signal);
  await authority.assertActive();
  validateActionRuntimeReferences(message, state, schemas);
}
function operation(message: DeckrMessage, authority: ActionRuntimeAuthority): ActionRuntimeOperation {
  return Object.freeze({ ...authority, message, name: String(message.body.name), payload: immutable(cloneJson((message.body.event ?? message.body.params) as JsonObject)) });
}

interface AvailabilityPublication {
  writer: ServiceViewStoreWriter;
  context: ServiceViewWriteContext;
  pending?: Promise<void>;
  dirty: boolean;
  closed: boolean;
  retryDelay: number;
  cancelRetry?: () => void;
  stop(): void;
}

/** Managed Action Runtime wire boundary. Action behavior and identity registries stay in the host. */
export class ManagedActionRuntimeProvider {
  private readonly options: ManagedActionRuntimeProviderOptions;
  private readonly provider: ManagedServiceProvider;
  private readonly writers = new Map<string, AvailabilityPublication>();
  private readonly cleanups = new Set<Promise<void>>();
  private availabilityValue: ActionAvailabilityPayload;
  private closed = false;
  private closing?: Promise<void>;
  constructor(options: ManagedActionRuntimeProviderOptions) {
    if (typeof options.resolveReferences !== "function" || typeof options.handle !== "function") throw new ValidationError("Action Runtime requires reference resolution and behavior callbacks");
    const id = actionProviderInstanceId(options.providerInstanceId);
    if (options.endpoint.endpoint !== `service:action-runtime.${id}`) throw new ValidationError("Action Runtime endpoint does not match providerInstanceId");
    if (!options.stateFor) throw new ValidationError("Action Runtime provider requires a protected view store factory");
    this.options = { ...options, providerInstanceId: id, providerId: requireText(options.providerId, "providerId") };
    this.availabilityValue = this.availability(options.availability ?? { entries: [] });
    this.provider = new ManagedServiceProvider({ ...options, protocol: actionRuntimeProtocol(), serviceId: `action-runtime.${id}`, schemas: options.schemas ?? actionRuntimePayloadSchemas,
      onAgreement: context => this.publishFor(context),
      authorizeOperation: async (_body, context) => {
        const message = validateActionRuntimeMessage(context.message);
        this.validateProvider(message);
        await references(options.resolveReferences, message, context, options.schemas);
        return true;
      },
      handle: async (_body, context) => { await options.handle(this.context(context)); },
    });
  }
  get state() { return this.provider.state; }
  get ready() { return this.provider.ready; }
  start() { return this.provider.start(); }
  watch(...args: Parameters<ManagedServiceProvider["watch"]>) { return this.provider.watch(...args); }
  waitFor(...args: Parameters<ManagedServiceProvider["waitFor"]>) { return this.provider.waitFor(...args); }
  reconcile() { return this.provider.reconcile(); }
  setBackendStatus(...args: Parameters<ManagedServiceProvider["setBackendStatus"]>) { return this.provider.setBackendStatus(...args); }
  private availability(value: NonNullable<ManagedActionRuntimeProviderOptions["availability"]>) {
    return immutable(actionAvailabilityPayload({ ...value, providerInstanceId: this.options.providerInstanceId, providerId: this.options.providerId, sessionId: this.options.endpoint.sessionId }));
  }
  async setAvailability(value: NonNullable<ManagedActionRuntimeProviderOptions["availability"]>): Promise<void> {
    if (this.closed) throw new ServiceUnavailable("runtime_closed", "Action Runtime is closed");
    this.availabilityValue = this.availability(value);
    await Promise.all([...this.writers.values()].map(entry => this.publish(entry)));
  }
  private async publishFor(context: ServiceViewWriteContext): Promise<void> {
    const key = JSON.stringify(context.contract);
    if (this.closed || context.authority.signal.aborted) return;
    if (this.writers.has(key)) return;
    const family = Object.values(context.views).find(view => view.storeName === "deckr_action_runtime_service_view_v1");
    if (!family) throw new ValidationError("Action Runtime context has no availability view");
    const state = this.options.stateFor!(family.storeName);
    const writer = new ServiceViewStoreWriter({ state, context });
    // The writer retains this same shared source until its closure.
    const source = sourceFor(state);
    let generation = source.status.current ? source.status.generation : -1;
    const entry: AvailabilityPublication = { writer, context, dirty: false, closed: false, retryDelay: 1000, stop: () => {
      if (entry.closed) return;
      entry.closed = true; entry.cancelRetry?.(); unsubscribe();
      context.authority.signal.removeEventListener("abort", entry.stop);
      this.writers.delete(key);
      const cleanup = writer.close(); this.cleanups.add(cleanup);
      void cleanup.finally(() => this.cleanups.delete(cleanup)).catch(error => this.report(error));
    } };
    const unsubscribe = source.subscribeChanges(snapshot => {
      if (!snapshot.current) { generation = -1; return; }
      if (snapshot.generation === generation) return;
      generation = snapshot.generation;
      void this.publish(entry).catch(error => this.report(error));
    });
    this.writers.set(key, entry);
    context.authority.signal.addEventListener("abort", entry.stop, { once: true });
    if (context.authority.signal.aborted) entry.stop();
    else await this.publish(entry);
  }
  private publish(entry: AvailabilityPublication): Promise<void> {
    if (entry.closed || this.closed || entry.context.authority.signal.aborted) return Promise.resolve();
    entry.dirty = true; entry.cancelRetry?.(); entry.cancelRetry = undefined;
    if (entry.pending) return entry.pending;
    entry.pending = (async () => {
      while (entry.dirty && !entry.closed && !this.closed) {
        entry.dirty = false;
        const value = this.availabilityValue;
        await entry.context.authority.assertActive();
        await entry.writer.put({ storeName: "deckr_action_runtime_service_view_v1", key: serviceViewKey(value.serviceId, "action_availability", "current") }, value as unknown as JsonObject);
        entry.retryDelay = 1000;
      }
    })().catch(error => {
      if (!entry.closed && !this.closed && !entry.context.authority.signal.aborted && !(error instanceof ValidationError)) {
        entry.dirty = true;
        entry.cancelRetry = this.options.concord.scheduler.schedule(entry.retryDelay, () => {
          entry.cancelRetry = undefined;
          void this.publish(entry).catch(failure => this.report(failure));
        });
        entry.retryDelay = Math.min(30_000, entry.retryDelay * 2);
      }
      throw error;
    }).finally(() => {
      entry.pending = undefined;
      // A new desired value or recovery notification can arrive as work settles.
      if (entry.dirty && !entry.cancelRetry && !entry.closed && !this.closed) void this.publish(entry).catch(error => this.report(error));
    });
    return entry.pending;
  }
  private context(context: ServiceProviderContext): ActionRuntimeContext {
    return Object.freeze({ ...operation(context.message, context), end: () => context.end(),
      send: async (name: string, payload: JsonObject, identifiers?: Record<string, string>) => {
        if (actionRuntimeMessageDefinition(name).direction !== "service_to_consumer") throw new ValidationError("Runtime cannot send a controller operation");
        const message = wire(name, validateActionRuntimePayload(name, payload), { sender: this.options.endpoint.endpoint, senderSessionId: this.options.endpoint.sessionId,
          recipient: context.peerEndpoint, recipientSessionId: context.peerSessionId, serviceId: `action-runtime.${this.options.providerInstanceId}`, contract: context.contract,
          identifiers: { ...context.message.subject.identifiers, ...identifiers },
        });
        this.validateProvider(message);
        await context.send(name, payload, message.subject.identifiers, {
          signal: context.signal, assertActive: () => references(this.options.resolveReferences, message, context, this.options.schemas),
        }, { messageId: message.messageId, causationId: message.causationId });
        return Object.freeze({ messageId: message.messageId });
      },
    });
  }
  private validateProvider(message: DeckrMessage): void {
    const payload = (message.body.event ?? message.body.params) as JsonObject;
    for (const field of ["metadata", "binding", "pageSession", "actionInstance"]) {
      const value = payload[field];
      if (isJsonObject(value) && value.providerId !== this.options.providerId) throw new ValidationError("Runtime providerId mismatch");
    }
  }
  private report(error: unknown): void { try { this.options.onError?.(error); } catch { /* Host diagnostics cannot own lifecycle. */ } }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    const writes = [...this.writers.values()];
    for (const entry of writes) entry.stop();
    const closing = this.provider.close();
    this.closing = bounded(Promise.allSettled([closing, ...this.cleanups, ...writes.map(entry => entry.pending)]), 5000, this.options.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}

export interface ActionAvailabilityChange extends ServiceViewChange { availability?: ActionAvailabilityPayload; }

/** Controller-facing service client. The host supplies current routing identities. */
export class ManagedActionRuntimeClient {
  private readonly options: ManagedActionRuntimeClientOptions;
  private readonly client: ManagedServiceClient;
  private readonly abort = new AbortController();
  private readonly authoritySignals = new WeakMap<AbortSignal, AbortSignal>();
  private readonly accesses = new Set<ManagedServiceViewAccess>();
  private closing?: Promise<void>;
  constructor(options: ManagedActionRuntimeClientOptions) {
    if (typeof options.resolveReferences !== "function") throw new ValidationError("Action Runtime client requires current reference resolution");
    if (!options.endpoint.endpoint.startsWith("controller:")) throw new ValidationError("Action Runtime consumer must be a controller");
    if (options.providerInstanceId !== undefined) actionProviderInstanceId(options.providerInstanceId);
    this.options = options;
    const protocol = actionRuntimeProtocol();
    this.client = new ManagedServiceClient({ ...options, protocol, schemas: options.schemas ?? actionRuntimePayloadSchemas,
      serviceId: options.providerInstanceId === undefined ? undefined : `action-runtime.${options.providerInstanceId}`,
      operations: Object.keys(protocol.operations), views: ["action_availability"],
    });
  }
  get state() { return this.client.state; }
  start() { return this.client.start(); }
  ready(...args: Parameters<ManagedServiceClient["ready"]>) { return this.client.ready(...args); }
  watch(...args: Parameters<ManagedServiceClient["watch"]>) { return this.client.watch(...args); }
  waitFor(...args: Parameters<ManagedServiceClient["waitFor"]>) { return this.client.waitFor(...args); }
  private authority(lease: ServiceUseLease): ActionRuntimeAuthority {
    const base = serviceViewAuthorityFromLease(lease);
    let signal = this.authoritySignals.get(base.signal);
    if (!signal) { signal = AbortSignal.any([this.abort.signal, base.signal]); this.authoritySignals.set(base.signal, signal); }
    return { contract: { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation }, peerEndpoint: lease.descriptor.endpoint, peerSessionId: lease.descriptor.sessionId,
      signal, assertActive: async () => { if (signal.aborted) throw new ServiceUnavailable("context_ended", "Action Runtime context has ended"); await base.assertActive(); if (signal.aborted) throw new ServiceUnavailable("context_ended", "Action Runtime context has ended"); },
    };
  }
  async send(name: string, payload: JsonObject, identifiers?: Record<string, string>): Promise<ActionRuntimeSendReceipt> {
    if (actionRuntimeMessageDefinition(name).direction !== "consumer_to_service") throw new ValidationError("Controller cannot send a runtime operation");
    const lease = await this.ready(), authority = this.authority(lease);
    const message = wire(name, validateActionRuntimePayload(name, payload), { sender: this.options.endpoint.endpoint, senderSessionId: this.options.endpoint.sessionId,
      recipient: lease.descriptor.endpoint, recipientSessionId: lease.descriptor.sessionId, serviceId: lease.descriptor.serviceId, contract: authority.contract, identifiers });
    await new ServiceMessageChannel({ endpoint: this.options.endpoint, schemas: this.options.schemas ?? actionRuntimePayloadSchemas }).send(lease, name, payload, identifiers, {
      signal: authority.signal, assertActive: () => references(this.options.resolveReferences, message, authority, this.options.schemas),
    }, { messageId: message.messageId, causationId: message.causationId });
    return Object.freeze({ messageId: message.messageId });
  }
  async *messages(options: { signal?: AbortSignal; timeoutMs?: number } = {}): AsyncIterable<ActionRuntimeOperation> {
    for await (const delivery of this.client.messages(options)) {
      try {
        const message = validateActionRuntimeMessage(delivery.message);
        const base = { signal: delivery.signal, assertActive: () => delivery.assertActive(), contract: message.contract!, peerEndpoint: message.sender, peerSessionId: message.senderSessionId };
        const assertActive = async () => { await references(this.options.resolveReferences, message, base, this.options.schemas); };
        await assertActive(); yield operation(message, { ...base, assertActive });
      } catch (error) { try { this.options.onError?.(error); } catch { /* Diagnostics do not own the stream. */ } }
    }
  }
  async *watchAvailability(options: { signal?: AbortSignal; timeoutMs?: number } = {}): AsyncIterable<ActionAvailabilityChange> {
    if (!this.options.stateFor) throw new ServiceUnavailable("view_store_unavailable", "Action Runtime client has no protected view store factory");
    const lease = await this.ready(options.timeoutMs, options), authority = this.authority(lease);
    const access = new ManagedServiceViewAccess({ state: this.options.stateFor("deckr_action_runtime_service_view_v1"), readContext: serviceViewReadContextFromLease(lease) });
    this.accesses.add(access);
    const signal = AbortSignal.any([authority.signal, ...(options.signal ? [options.signal] : [])]);
    const close = () => { void access.close(); }; signal.addEventListener("abort", close, { once: true });
    try {
      if (signal.aborted) return;
      for await (const change of access.watch({ storeName: "deckr_action_runtime_service_view_v1", key: serviceViewKey(lease.descriptor.serviceId, "action_availability", "current") })) {
        await authority.assertActive();
        let availability: ActionAvailabilityPayload | undefined;
        if (change.entry) {
          availability = validateActionAvailabilityPayload(change.entry.value);
          if (availability.serviceSessionId !== lease.descriptor.sessionId || availability.serviceEndpoint !== lease.descriptor.endpoint) throw new ValidationError("Availability does not match the accepted runtime session");
        }
        yield immutable({ ...change, ...(availability ? { availability } : {}) });
      }
    } finally { signal.removeEventListener("abort", close); await access.close(); this.accesses.delete(access); }
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.abort.abort();
    this.closing = bounded(Promise.allSettled([this.client.close(), ...[...this.accesses].map(access => access.close())]), 5000, this.options.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}
