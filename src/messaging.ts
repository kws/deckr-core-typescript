import { randomUUID } from "node:crypto";
import { contractPointersEqual } from "./authority.ts";
import { deckrEndpointAddress, parseEndpointAddress } from "./endpoint.ts";
import { StateUnavailable, ValidationError } from "./errors.ts";
import { requireText, type JsonObject } from "./json.ts";
import { encodeKeyToken } from "./keys.ts";
import { validateLaneMessage, type LaneValidator } from "./lane-validation.ts";
import {
  buildMessage, headersFor, messageIsDeliverableTo, messageIsExpired, subjectFor,
  validateHeaderHints, validateSubjectHint, type BuildMessageInput, type DeckrMessage, type HeaderReader,
} from "./lanes.ts";
import { abortable, AsyncQueue, bounded, immutable, StateCell, systemScheduler, type RuntimeScheduler, type RuntimeState } from "./runtime.ts";

export interface TransportMessage {
  subject: string;
  data: Uint8Array;
  headers?: HeaderReader | Record<string, string>;
  reply?: string;
}
export interface MessageSubscription {
  messages: AsyncIterable<TransportMessage>;
  close(): void;
}
/** subscribe must arm delivery before it returns; the host owns the transport. */
export interface MessageTransport {
  subscribe(subject: string): MessageSubscription;
  publish(subject: string, data: Uint8Array, options?: { headers?: Record<string, string>; reply?: string }): Promise<void>;
}
export type LaneMessageInput = Omit<BuildMessageInput, "sender" | "senderSessionId" | "lane">;
export interface MessageDelivery {
  readonly message: DeckrMessage;
  reply(body: JsonObject, options?: { messageType?: string; ttlMs?: number }): Promise<void>;
}
export interface MessageRequestOptions extends LaneMessageInput {
  timeout?: number;
  signal?: AbortSignal;
  accept?: (message: DeckrMessage) => boolean;
}

/** Owns only endpoint delivery resources; closing it does not close protocol lifetimes. */
export class EndpointSession {
  readonly endpoint: string;
  readonly sessionId: string;
  readonly scheduler: RuntimeScheduler;
  readonly transport: MessageTransport;
  private readonly abort = new AbortController();
  private readonly lanes = new Map<string, EndpointLane>();
  private readonly subscriptions = new Set<MessageSubscription>();
  private readonly onError?: (error: unknown) => void;
  private readonly states = new StateCell<RuntimeState>({ status: "ready", resources: { lanes: 0, subscriptions: 0 } });
  private closing?: Promise<void>;
  constructor(options: { endpoint: string; sessionId?: string; transport: MessageTransport; scheduler?: RuntimeScheduler; onError?: (error: unknown) => void }) {
    this.endpoint = deckrEndpointAddress(options.endpoint);
    this.sessionId = requireText(options.sessionId ?? randomUUID(), "sessionId");
    this.transport = options.transport; this.scheduler = options.scheduler ?? systemScheduler; this.onError = options.onError;
  }
  get signal(): AbortSignal { return this.abort.signal; }
  get closed(): boolean { return this.signal.aborted; }
  get subscriptionCount(): number { return this.subscriptions.size; }
  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: RuntimeState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.scheduler });
  }
  private updateState(): void { if (!this.closed) this.states.set({ ...this.state, resources: { lanes: this.lanes.size, subscriptions: this.subscriptions.size } }); }
  lane(name: string, options: { validate?: LaneValidator } = {}): EndpointLane {
    this.assertOpen(); requireText(name, "lane");
    let lane = this.lanes.get(name);
    if (!lane) { lane = new EndpointLane(this, name, options.validate); this.lanes.set(name, lane); this.updateState(); }
    else if (options.validate && lane.validator !== options.validate) throw new ValidationError("Lane already has a different validator");
    return lane;
  }
  assertOpen(): void { if (this.closed) throw new StateUnavailable("Endpoint session is closed"); }
  report(error: unknown): void {
    this.states.set({ ...this.state, failure: { code: "endpoint_delivery_failed", message: String(error) } });
    try { this.onError?.(error); } catch { /* Diagnostic callbacks do not own workers. */ }
  }
  subscribe(subject: string): MessageSubscription {
    this.assertOpen();
    const original = this.transport.subscribe(subject);
    let closed = false;
    const owned = { messages: original.messages, close: () => {
      if (closed) return; closed = true;
      this.subscriptions.delete(owned); this.updateState(); original.close();
    } };
    this.subscriptions.add(owned);
    this.updateState();
    return owned;
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.abort.abort();
    const closing = [...this.lanes.values()].map(lane => lane.close());
    for (const subscription of [...this.subscriptions]) subscription.close();
    this.states.set({ status: "closed", resources: { lanes: 0, subscriptions: 0 } }); this.states.close();
    this.closing = bounded(Promise.all(closing), 5000, this.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}

export class EndpointLane {
  readonly endpoint: string;
  readonly sessionId: string;
  readonly name: string;
  readonly validator?: LaneValidator;
  private readonly session: EndpointSession;
  private readonly abort = new AbortController();
  private readonly watchers = new Set<AsyncQueue<MessageDelivery>>();
  private readonly subscriptions = new Set<MessageSubscription>();
  private readonly workers = new Set<Promise<void>>();
  constructor(session: EndpointSession, name: string, validator?: LaneValidator) {
    this.session = session; this.endpoint = session.endpoint; this.sessionId = session.sessionId; this.name = name; this.validator = validator;
    if (!["hardware_messages", "services"].includes(name) && !validator) throw new ValidationError(`No contract registered for lane ${name}`);
  }
  private assertOpen(): void { this.session.assertOpen(); if (this.abort.signal.aborted) throw new StateUnavailable("Endpoint lane is closed"); }
  private message(input: LaneMessageInput): DeckrMessage {
    this.assertOpen();
    const message = validateLaneMessage(buildMessage({ ...input, lane: this.name, sender: this.endpoint, senderSessionId: this.sessionId,
      createdAt: input.createdAt ?? new Date(this.session.scheduler.now()).toISOString(),
    }), this.validator);
    if (messageIsExpired(message, new Date(this.session.scheduler.now()))) throw new ValidationError("Cannot publish an expired message");
    return immutable(message);
  }
  private decode(raw: TransportMessage): DeckrMessage {
    const message = validateLaneMessage(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw.data)), this.validator);
    if (message.lane !== this.name) throw new ValidationError("Message belongs to another lane");
    validateSubjectHint(raw.subject, message); validateHeaderHints(raw.headers, message);
    if (!messageIsDeliverableTo(message, this.endpoint, { endpointSessionId: this.sessionId, now: new Date(this.session.scheduler.now()) })) throw new ValidationError("Message is not deliverable to this endpoint session");
    return immutable(message);
  }
  private async transmit(message: DeckrMessage, subject = subjectFor(message), reply?: string): Promise<void> {
    this.assertOpen();
    await this.session.transport.publish(subject, new TextEncoder().encode(JSON.stringify(message)), { headers: headersFor(message), ...(reply ? { reply } : {}) });
  }
  publish = async (input: LaneMessageInput): Promise<void> => { await this.transmit(this.message(input)); };

  request = async (input: MessageRequestOptions): Promise<DeckrMessage> => {
    this.assertOpen();
    const timeout = input.timeout ?? 2000;
    if (!Number.isFinite(timeout) || timeout < 0) throw new ValidationError("Request timeout must be finite and non-negative");
    const signal = AbortSignal.any([this.session.signal, this.abort.signal, ...(input.signal ? [input.signal] : [])]);
    if (signal.aborted) throw new StateUnavailable("Request cancelled");
    const request = this.message(input);
    const inbox = `_INBOX.deckr.${randomUUID()}`;
    const subscription = this.session.subscribe(inbox);
    const receiving = (async () => {
      for await (const raw of subscription.messages) {
        if (signal.aborted) throw new StateUnavailable("Request cancelled");
        try {
          const reply = this.decode(raw);
          if (reply.messageId === request.messageId || reply.inReplyTo !== request.messageId || !contractPointersEqual(reply.contract, request.contract)) continue;
          if (request.recipient.targetType === "endpoint" && reply.sender !== request.recipient.endpoint) continue;
          if (request.recipientSessionId && reply.senderSessionId !== request.recipientSessionId) continue;
          if (input.accept && !input.accept(reply)) continue;
          return reply;
        } catch (error) { this.session.report(error); }
      }
      throw new StateUnavailable("Reply subscription ended");
    })();
    try {
      // The inbox is already armed. Rejected replies never resolve this wait.
      return await abortable(bounded(Promise.all([receiving, this.transmit(request, subjectFor(request), inbox)]).then(([reply]) => reply), timeout, this.session.scheduler), signal);
    } finally { subscription.close(); }
  };

  watch(options: { signal?: AbortSignal } = {}): AsyncIterable<MessageDelivery> {
    this.assertOpen();
    if (this.watchers.size >= 256) throw new StateUnavailable("Endpoint watcher limit reached");
    const signal = options.signal;
    const close = () => queue.close();
    const queue = new AsyncQueue<MessageDelivery>(256, false, () => {
      signal?.removeEventListener("abort", close); this.watchers.delete(queue);
      if (!this.watchers.size) this.stopListening();
    });
    this.watchers.add(queue);
    signal?.addEventListener("abort", close, { once: true });
    if (signal?.aborted) close();
    else if (!this.subscriptions.size) {
      try { this.startListening(); }
      catch (error) { queue.close(error); }
    }
    return queue;
  }
  private startListening(): void {
    const { family, endpointId } = parseEndpointAddress(this.endpoint);
    const prefix = `deckr.msg.${encodeKeyToken(this.name)}`;
    const subjects = [`${prefix}.to.${encodeKeyToken(family)}.${encodeKeyToken(endpointId)}`];
    if (this.name !== "services") subjects.push(`${prefix}.broadcast.*.${encodeKeyToken(family)}`);
    try {
      for (const subject of subjects) {
        const subscription = this.session.subscribe(subject);
        this.subscriptions.add(subscription);
        const worker = this.listen(subscription).finally(() => { this.workers.delete(worker); });
        this.workers.add(worker);
      }
    } catch (error) { this.stopListening(); throw error; }
  }
  private async listen(subscription: MessageSubscription): Promise<void> {
    try {
      for await (const raw of subscription.messages) {
        if (!this.subscriptions.has(subscription) || this.session.closed || this.abort.signal.aborted) return;
        try {
          const message = this.decode(raw);
          const delivery: MessageDelivery = Object.freeze({ message, reply: async (body: JsonObject, options: { messageType?: string; ttlMs?: number } = {}) => {
            const response = this.message({ recipient: message.sender, recipientSessionId: message.senderSessionId, contract: message.contract,
              messageType: options.messageType ?? message.messageType, subject: message.subject, body,
              inReplyTo: message.messageId, ...(options.ttlMs === undefined ? {} : { ttlMs: options.ttlMs }),
            });
            await this.transmit(response, raw.reply || subjectFor(response));
          } });
          for (const watcher of this.watchers) watcher.push(delivery);
        } catch (error) { this.session.report(error); }
      }
      if (this.subscriptions.has(subscription)) throw new StateUnavailable("Lane subscription ended");
    } catch (error) {
      this.session.report(error);
      for (const watcher of [...this.watchers]) watcher.close(error);
    } finally { this.subscriptions.delete(subscription); subscription.close(); }
  }
  private stopListening(): void {
    const subscriptions = [...this.subscriptions]; this.subscriptions.clear();
    for (const subscription of subscriptions) subscription.close();
  }
  async close(): Promise<void> {
    if (this.abort.signal.aborted) return;
    this.abort.abort();
    for (const watcher of [...this.watchers]) watcher.close();
    this.stopListening();
    await bounded(Promise.all([...this.workers]), 5000, this.session.scheduler).catch(() => {});
  }
}

/** Canonical in-process transport for tests and embedded hosts. */
export class MemoryMessageTransport implements MessageTransport {
  private closed = false;
  private readonly listeners = new Map<AsyncQueue<TransportMessage>, string>();
  get subscriptionCount(): number { return this.listeners.size; }
  subscribe(subject: string): MessageSubscription {
    if (this.closed) throw new StateUnavailable("Transport is closed");
    const queue = new AsyncQueue<TransportMessage>(2048, false, () => { this.listeners.delete(queue); });
    this.listeners.set(queue, subject);
    return { messages: queue, close: () => queue.close() };
  }
  async publish(subject: string, data: Uint8Array, options: { headers?: Record<string, string>; reply?: string } = {}): Promise<void> {
    if (this.closed) throw new StateUnavailable("Transport is closed");
    for (const [queue, pattern] of this.listeners) {
      const wanted = pattern.split("."), actual = subject.split(".");
      if (wanted.length === actual.length && wanted.every((token, index) => token === "*" || token === actual[index])) {
        queue.push({ subject, data: new Uint8Array(data), ...(options.headers ? { headers: { ...options.headers } } : {}), ...(options.reply ? { reply: options.reply } : {}) });
      }
    }
  }
  close(): void { this.closed = true; for (const queue of [...this.listeners.keys()]) queue.close(); }
}
