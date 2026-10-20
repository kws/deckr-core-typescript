import { connectWithRetry, type NatsRetryOptions } from "./nats-retry.ts";
import { randomUUID } from "node:crypto";
import type { NatsConnection, Subscription } from "nats";

import { StateConflict, StateUnavailable } from "./errors.ts";
import { parseJson, requireJsonObject, type JsonObject } from "./json.ts";
import { validateHeaderHints, validateSubjectHint, type DeckrMessage } from "./lanes.ts";
import { validateLaneMessage } from "./lane-validation.ts";
import type { MessageSubscription, MessageTransport, TransportMessage } from "./messaging.ts";
import {
  PERSISTENT_STATE_STORE_POLICY,
  type StateChange,
  type StateObservation,
  type StateWatchItem,
  type StateEntry,
  type StateStore,
  type StateStorePolicy,
} from "./state.ts";

import { AsyncQueue, bounded, systemScheduler, type RuntimeScheduler } from "./runtime.ts";

export interface NatsStateStoreOptions {
  scheduler?: RuntimeScheduler;
  connection: unknown;
  bucket: string;
  policy?: StateStorePolicy;
}

const NATS_KV_CREATION_MARKER_METADATA_KEY = "deckr.kv.creation_id";
const NATS_KV_STREAM_PREFIX = "KV_";
const NATS_KV_SUBJECT_PREFIX = "$KV";

// This is the complete cross-runtime Deckr policy vector. Other JetStream
// fields are broker/client tuning or creator diagnostics and are never grounds
// for mutating or rejecting an otherwise compatible existing shared bucket.
export const NATS_KV_POLICY_VECTOR_FIELDS = Object.freeze([
  "max_age",
  "max_msgs_per_subject",
  "allow_msg_ttl",
  "subject_delete_marker_ttl",
] as const);
export const NATS_KV_NON_AUTHORITATIVE_CONFIG_FIELDS = Object.freeze([
  "allow_direct",
  "metadata",
  "storage",
] as const);

export async function connectNats(options: Record<string, unknown> & NatsRetryOptions): Promise<unknown> {
  const mod = await import("nats").catch((error) => {
    throw new StateUnavailable("Install the optional nats peer dependency to use @deckr/core/nats", {
      cause: error,
    });
  });
  return connectWithRetry(mod.connect, options);
}

export function deckrMessageFromNatsMessage(message: {
  subject: string;
  data: Uint8Array;
  headers?: unknown;
}): DeckrMessage {
  const raw = parseJson(new TextDecoder().decode(message.data));
  const deckrMessage = validateLaneMessage(raw);
  validateSubjectHint(message.subject, deckrMessage);
  validateHeaderHints(message.headers as never, deckrMessage);
  return deckrMessage;
}

/** Shares the host connection; only subscriptions belong to this adapter. */
export class NatsMessageTransport implements MessageTransport {
  private readonly connection: NatsConnection;
  private readonly subscriptions = new Set<MessageSubscription>();
  private closed = false;
  constructor(connection: unknown) { this.connection = connection as NatsConnection; }
  subscribe(subject: string): MessageSubscription {
    if (this.closed) throw new StateUnavailable("Message transport is closed");
    let subscription: Subscription | undefined;
    const queue = new AsyncQueue<TransportMessage>(2048, false, () => { subscription?.unsubscribe(); this.subscriptions.delete(owned); });
    const owned: MessageSubscription = { messages: queue, close: () => queue.close() };
    subscription = this.connection.subscribe(subject, { callback: (error, message) => {
      if (error) queue.close(error);
      else queue.push({ subject: message.subject, data: new Uint8Array(message.data), headers: message.headers, reply: message.reply });
    } });
    this.subscriptions.add(owned);
    void subscription.closed.then(() => queue.close(new StateUnavailable("NATS subscription ended")), error => queue.close(error));
    return owned;
  }
  async publish(subject: string, data: Uint8Array, options: { headers?: Record<string, string>; reply?: string } = {}): Promise<void> {
    if (this.closed) throw new StateUnavailable("Message transport is closed");
    const { headers } = await import("nats");
    if (this.closed) throw new StateUnavailable("Message transport is closed");
    const hints = headers();
    for (const [key, value] of Object.entries(options.headers ?? {})) hints.set(key, value);
    this.connection.publish(subject, data, { headers: hints, reply: options.reply });
  }
  close(): void { this.closed = true; for (const subscription of [...this.subscriptions]) subscription.close(); }
}

export class NatsStateStore implements StateStore {
  readonly name: string;
  readonly policy: StateStorePolicy;

  private readonly kv: unknown;
  private readonly connection: unknown;
  private readonly scheduler: RuntimeScheduler;
  private readonly retiredConsumers = new Map<string, unknown>();

  private constructor(
    options: NatsStateStoreOptions,
    policy: StateStorePolicy,
    kv: unknown,
  ) {
    this.name = options.bucket;
    this.policy = policy;
    this.kv = kv;
    this.connection = options.connection;
    this.scheduler = options.scheduler ?? systemScheduler;
  }

  static async open(options: NatsStateStoreOptions): Promise<NatsStateStore> {
    const policy = options.policy ?? PERSISTENT_STATE_STORE_POLICY;
    const kv = await this.openAndValidateKv(
      options.connection,
      options.bucket,
      policy,
    );
    return new NatsStateStore(options, policy, kv);
  }

  async get(key: string): Promise<StateEntry | null> {
    const kv = this.kv;
    try {
      const entry = await call(kv, "get", key);
      if (entry === null || entry === undefined) {
        return null;
      }
      const operation = (entry as { operation?: string }).operation;
      if (operation === "DEL" || operation === "PURGE") return null;
      return stateEntryFromKv(key, entry);
    } catch (error) {
      if (isMissingKey(error)) {
        return null;
      }
      throw new StateUnavailable(`Could not get state key ${JSON.stringify(key)}`, {
        cause: error,
      });
    }
  }

  async items(prefix = ""): Promise<StateEntry[]> {
    // The snapshot carries values; no keys scan followed by N exact requests.
    // Returning from the iterator awaits its finally, including consumer delete.
    let result: StateEntry[] | undefined;
    for await (const observation of this.observe(prefix)) {
      if (observation.operation !== "snapshot") throw new StateUnavailable("State listing lost currentness");
      const entries = new Map(observation.entries.map(entry => [entry.key, entry]));
      for (const change of observation.afterBarrier ?? []) {
        if (change.operation === "put" && change.entry) entries.set(change.key, change.entry);
        else entries.delete(change.key);
      }
      result = [...entries.values()].sort((a, b) => a.key.localeCompare(b.key));
      break;
    }
    await this.cleanupRetiredConsumers();
    if (result) return result;
    throw new StateUnavailable("State listing ended before its snapshot");
  }

  async put(key: string, value: JsonObject, options: { ttl?: number | null } = {}): Promise<StateEntry> {
    this.validateTtl(options.ttl ?? null);
    const kv = this.kv;
    const revision = await call(kv, "put", key, JSON.stringify(value));
    return { key, value, revision: Number(revision) };
  }

  async create(key: string, value: JsonObject, options: { ttl?: number | null } = {}): Promise<StateEntry> {
    this.validateTtl(options.ttl ?? null);
    const kv = this.kv;
    try {
      const revision = await call(kv, "create", key, JSON.stringify(value));
      return { key, value, revision: Number(revision) };
    } catch (error) {
      if (isConflict(error)) {
        throw new StateConflict(`State key ${JSON.stringify(key)} already exists`, "key_already_exists");
      }
      throw error;
    }
  }

  async update(
    key: string,
    value: JsonObject,
    options: { revision: number; ttl?: number | null },
  ): Promise<StateEntry> {
    this.validateTtl(options.ttl ?? null);
    const kv = this.kv;
    try {
      const revision = await call(kv, "update", key, JSON.stringify(value), options.revision);
      return { key, value, revision: Number(revision) };
    } catch (error) {
      if (isConflict(error)) {
        throw new StateConflict(`State key ${JSON.stringify(key)} revision changed`, "revision_changed");
      }
      throw error;
    }
  }

  async delete(key: string, options: { revision?: number | null } = {}): Promise<number> {
    if (!/^[-\w=/.]+$/.test(key)) throw new StateUnavailable("Invalid exact KV key");
    const { headers } = await import("nats");
    const hints = headers();
    hints.set("KV-Operation", "DEL");
    if (options.revision !== undefined && options.revision !== null) hints.set("Nats-Expected-Last-Subject-Sequence", String(options.revision));
    try {
      const ack = await call(callSync(this.connection, "jetstream"), "publish", `$KV.${this.name}.${key}`, new Uint8Array(), { headers: hints });
      return Number(getProperty(ack, "seq"));
    } catch (error) {
      if (isConflict(error)) throw new StateConflict(`State key ${JSON.stringify(key)} revision changed`, "revision_changed");
      throw new StateUnavailable(`Could not delete state key ${JSON.stringify(key)}`, { cause: error });
    }
  }

  async *watch(prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateWatchItem> {
    for await (const item of this.observe(prefix, options)) {
      if (item.operation === "snapshot") yield { operation: "resnapshot" };
      else if (item.operation === "stale") throw new StateUnavailable(item.reason);
      else yield item;
    }
  }

  private static async openAndValidateKv(
    connection: unknown,
    name: string,
    policy: StateStorePolicy,
  ): Promise<unknown> {
    validateNatsKvBucketName(name);
    const jetstream = callSync(connection, "jetstream");
    const views = getProperty(jetstream, "views");
    const manager = await callSync(connection, "jetstreamManager");
    const streams = getProperty(manager, "streams");
    const creationMarker = randomUUID();
    const streamName = `${NATS_KV_STREAM_PREFIX}${name}`;
    const createConfig = natsKvStreamConfig(
      name,
      streamName,
      policy,
      creationMarker,
    );
    let createError: unknown | null = null;
    let newlyCreated = false;
    try {
      await call(streams, "add", createConfig);
      newlyCreated = true;
    } catch (error) {
      // Stream creation is the atomic existence check. A failed create may mean
      // another process won the race; bind to that winner and validate its
      // complete policy read-only below.
      createError = error;
    }
    let kv: unknown;
    try {
      kv = await call(views, "kv", name, { bindOnly: true });
    } catch (bindError) {
      throw new StateUnavailable(`NATS KV bucket ${JSON.stringify(name)} is unavailable`, {
        cause:
          createError === null
            ? bindError
            : new AggregateError([createError, bindError]),
      });
    }
    let status: unknown;
    try {
      status = await call(kv, "status");
    } catch (error) {
      throw new StateUnavailable(
        `Could not inspect NATS KV bucket ${JSON.stringify(name)}`,
        {
          cause:
            createError === null
              ? error
              : new AggregateError([createError, error]),
        },
      );
    }
    await this.ensureBucketPolicy(name, policy, status, newlyCreated);
    return kv;
  }

  private static async ensureBucketPolicy(
    name: string,
    policy: StateStorePolicy,
    status: unknown,
    newlyCreated: boolean,
  ): Promise<void> {
    const expectedTtlMs =
      policy.brokerTtlSeconds === null ? 0 : policy.brokerTtlSeconds * 1000;
    const expectedMaxAgeNs = expectedTtlMs * 1_000_000;
    const expectedDeleteMarkerNs =
      policy.brokerTtlSeconds === null ? null : expectedMaxAgeNs;
    const config = streamConfigFromKvStatus(status);
    const maxAgeMatches = Number(config["max_age"] ?? 0) === expectedMaxAgeNs;
    const maxMessagesMatches = Number(config["max_msgs_per_subject"]) === 1;
    const observedAllowWriteTtl = config["allow_msg_ttl"] === true;
    const allowWriteTtlMatches =
      observedAllowWriteTtl === policy.allowWriteTtl;
    const deleteMarkerMatches =
      expectedDeleteMarkerNs === null ||
      Number(config["subject_delete_marker_ttl"] ?? 0) === expectedDeleteMarkerNs;
    const mismatches: string[] = [];
    if (!maxAgeMatches) {
      mismatches.push(
        `max_age (expected ${expectedMaxAgeNs}ns, found ${String(config["max_age"])})`,
      );
    }
    if (!maxMessagesMatches) {
      mismatches.push(
        `max_msgs_per_subject (expected 1, found ${String(config["max_msgs_per_subject"])})`,
      );
    }
    if (!allowWriteTtlMatches) {
      mismatches.push(
        `allow_msg_ttl (expected ${String(policy.allowWriteTtl)}, found ${String(config["allow_msg_ttl"])})`,
      );
    }
    if (!deleteMarkerMatches) {
      mismatches.push(
        `subject_delete_marker_ttl (expected ${expectedDeleteMarkerNs}ns, found ${String(config["subject_delete_marker_ttl"])})`,
      );
    }
    if (mismatches.length === 0) {
      return;
    }

    const streamName = String(getProperty(config, "name"));
    if (!newlyCreated) {
      throw new StateUnavailable(
        `Existing NATS KV bucket ${JSON.stringify(name)} has an incompatible Deckr KV policy: ${mismatches.join(", ")}. Deckr will not rewrite shared bucket policy; delete and recreate the development bucket/stream ${streamName} and restart.`,
      );
    }

    throw new StateUnavailable(
      `New NATS KV bucket ${JSON.stringify(name)} was created with an incompatible Deckr KV policy: ${mismatches.join(", ")}. Delete the development bucket/stream ${streamName} and restart.`,
    );
  }

  private validateTtl(ttl: number | null): void {
    if (!this.policy.allowWriteTtl && ttl !== null) {
      throw new StateUnavailable(
        `${this.policy.description} does not use write TTL; per-key TTL ${ttl} is not supported.`,
      );
    }
    if (
      ttl !== null &&
      this.policy.brokerTtlSeconds !== null &&
      Math.abs(ttl - this.policy.brokerTtlSeconds) > 0.001
    ) {
      throw new StateUnavailable(
        `current state uses broker TTL ${this.policy.brokerTtlSeconds}; per-key TTL ${ttl} is not supported.`,
      );
    }
  }

  async *observe(prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateObservation> {
    const queue = new AsyncQueue<StateObservation>(2048);
    const stop = () => queue.close();
    options.signal?.addEventListener("abort", stop, { once: true });
    let subscription: unknown;
    let consumerName: string | undefined;
    let consumers: unknown;
    let cancelHeartbeat = () => {};
    let statuses: AsyncIterator<unknown> | undefined;
    const fail = (error: unknown) => queue.close(error);
    try {
      if (options.signal?.aborted) return;
      // Outage cleanup can fail before reconnect. Retain exact owned names and
      // delete them before opening a replacement, without a consumer/key scan.
      await this.cleanupRetiredConsumers();
      const stream = callSync(this.connection, "status") as AsyncIterable<unknown>;
      statuses = stream[Symbol.asyncIterator]();
      void (async () => {
        while (true) {
          const item = await statuses!.next();
          if (item.done) return;
          const type = getProperty(item.value, "type");
          if (type === "disconnect" || type === "reconnect" || type === "error") {
            fail(new StateUnavailable(`Connection ${String(type)}`)); return;
          }
        }
      })().catch(fail);
      const status = await call(this.kv, "status");
      const streamInfo = getProperty(status, "streamInfo");
      const highWater = Number(getProperty(getProperty(streamInfo, "state"), "last_seq"));
      if (!Number.isSafeInteger(highWater) || highWater < 0) throw new StateUnavailable("Missing stream snapshot revision");
      const inbox = `_INBOX.deckr.${randomUUID()}`;
      const initial = new Map<string, StateEntry>();
      const newer: StateChange[] = [];
      let snapshotComplete = false;
      let sequence = 0;
      const barrier = () => {
        if (snapshotComplete) return;
        snapshotComplete = true;
        queue.push({ operation: "snapshot", entries: [...initial.values()], revision: highWater,
          ...(newer.length ? { afterBarrier: [...newer].sort((a, b) => a.revision! - b.revision!) } : {}) });
        initial.clear();
        newer.length = 0;
      };
      const heartbeat = () => {
        cancelHeartbeat();
        cancelHeartbeat = this.scheduler.schedule(10_000, () => fail(new StateUnavailable("Authority watch heartbeat missed")));
      };
      subscription = callSync(this.connection, "subscribe", inbox, {
        callback: (error: unknown, message: { subject: string; reply?: string; data: Uint8Array; headers?: { code?: number; get(name: string): string }; respond(): void }) => {
          if (error) { fail(error); return; }
          try {
            heartbeat();
            if (message.headers?.code === 100) {
              const lastConsumer = message.headers.get("Nats-Last-Consumer");
              if (lastConsumer && Number(lastConsumer) !== sequence) throw new StateUnavailable("Consumer continuity lost");
              if (message.reply) message.respond();
              const stalled = message.headers.get("Nats-Consumer-Stalled");
              if (stalled) callSync(this.connection, "publish", stalled);
              return;
            }
            const info = natsDeliveryInfo(message.reply ?? "");
            if (info.deliverySequence !== sequence + 1) throw new StateUnavailable("Consumer sequence gap");
            sequence = info.deliverySequence;
            const subjectPrefix = `$KV.${this.name}.`;
            if (!message.subject.startsWith(subjectPrefix)) throw new StateUnavailable("Unexpected KV subject");
            const key = message.subject.slice(subjectPrefix.length);
            if (key.startsWith(prefix)) {
              const operation = message.headers?.get("KV-Operation") ?? "PUT";
              const marker = message.headers?.get("Nats-Marker-Reason");
              const change: StateChange = operation === "DEL" || operation === "PURGE" || marker
                ? { operation: marker ? "expire" : "delete", key, revision: info.revision }
                : { operation: "put", key, revision: info.revision, entry: stateEntryFromBytes(key, info.revision, message.data) };
              if (snapshotComplete) queue.push(change);
              else if (info.revision > highWater) {
                newer.push(change);
                if (newer.length > 2000) throw new StateUnavailable("Recovery observation limit reached");
              } else if (change.entry) initial.set(key, change.entry);
              else initial.delete(key);
            }
            if (info.pending === 0) barrier();
          } catch (cause) { fail(cause); }
        },
      });
      heartbeat();
      const manager = await call(this.connection, "jetstreamManager");
      consumers = getProperty(manager, "consumers");
      consumerName = `deckr_${randomUUID().replaceAll("-", "")}`;
      const info = await call(consumers, "add", `KV_${this.name}`, {
        name: consumerName,
        deliver_subject: inbox, deliver_policy: "last_per_subject", ack_policy: "none",
        filter_subject: `$KV.${this.name}.>`, replay_policy: "instant",
        idle_heartbeat: 5_000_000_000, flow_control: true, inactive_threshold: 30_000_000_000,
      });
      consumerName = String(getProperty(info, "name"));
      if (Number(getProperty(info, "num_pending")) === 0 && sequence === 0 && Number(getProperty(getProperty(info, "delivered"), "consumer_seq")) === 0) barrier();
      yield* queue;
    } catch (error) {
      // Authority must observe continuity loss before broker cleanup can block.
      if (!options.signal?.aborted) {
        yield { operation: "stale", reason: error instanceof Error ? error.message : String(error) };
      }
      throw error;
    } finally {
      cancelHeartbeat();
      queue.close();
      options.signal?.removeEventListener("abort", stop);
      if (subscription) callSync(subscription, "unsubscribe");
      void statuses?.return?.();
      if (consumerName && consumers) {
        this.retiredConsumers.set(consumerName, consumers);
        await this.deleteOwnedConsumer(consumerName, consumers);
      }
    }
  }

  private async deleteOwnedConsumer(name: string, consumers: unknown): Promise<void> {
    try {
      await bounded(call(consumers, "delete", `KV_${this.name}`, name), 5000, this.scheduler);
      this.retiredConsumers.delete(name);
    } catch (error) {
      if (isMissingKey(error)) this.retiredConsumers.delete(name);
    }
  }

  private async cleanupRetiredConsumers(): Promise<void> {
    for (const [name, consumers] of this.retiredConsumers) await this.deleteOwnedConsumer(name, consumers);
    if (this.retiredConsumers.size) throw new StateUnavailable("Owned NATS snapshot consumer cleanup is pending");
  }
}

function validateNatsKvBucketName(name: string): void {
  if (!/^[-\w]+$/.test(name)) {
    throw new StateUnavailable(`Invalid NATS KV bucket name ${JSON.stringify(name)}`);
  }
}

function natsKvStreamConfig(
  bucket: string,
  streamName: string,
  policy: StateStorePolicy,
  creationMarker: string,
): Record<string, unknown> {
  const maxAgeNs =
    policy.brokerTtlSeconds === null
      ? 0
      : policy.brokerTtlSeconds * 1_000_000_000;
  return {
    name: streamName,
    subjects: [`${NATS_KV_SUBJECT_PREFIX}.${bucket}.>`],
    retention: "limits",
    max_consumers: -1,
    max_msgs: -1,
    max_msgs_per_subject: 1,
    max_age: maxAgeNs,
    max_bytes: -1,
    max_msg_size: -1,
    storage: "file",
    discard: "new",
    duplicate_window: Math.min(120 * 1_000_000_000, maxAgeNs || 120 * 1_000_000_000),
    deny_delete: true,
    allow_direct: true,
    num_replicas: 1,
    allow_rollup_hdrs: true,
    allow_msg_ttl: policy.allowWriteTtl,
    metadata: { [NATS_KV_CREATION_MARKER_METADATA_KEY]: creationMarker },
    ...(policy.brokerTtlSeconds === null
      ? {}
      : { subject_delete_marker_ttl: maxAgeNs }),
  };
}

function streamConfigFromKvStatus(status: unknown): Record<string, unknown> {
  const streamInfo = getProperty(status, "streamInfo");
  return getProperty(streamInfo, "config") as Record<string, unknown>;
}

function stateEntryFromKv(key: string, entry: unknown): StateEntry {
  const value = getProperty(entry, "value");
  const revision = Number(getProperty(entry, "revision"));
  const bytes =
    value instanceof Uint8Array
      ? value
      : typeof value === "string"
        ? new TextEncoder().encode(value)
        : new Uint8Array(value as ArrayBufferLike);
  return stateEntryFromBytes(String(getProperty(entry, "key") ?? key), revision, bytes);
}

function stateEntryFromBytes(key: string, revision: number, bytes: Uint8Array): StateEntry {
  const raw = new TextDecoder().decode(bytes);
  try { return { key, revision, value: requireJsonObject(parseJson(raw), "state value") }; }
  catch (error) { return { key, revision, value: {}, raw, parseError: error instanceof Error ? error.message : String(error) }; }
}

function getProperty(target: unknown, key: string): unknown {
  if (typeof target !== "object" || target === null || !(key in target)) {
    throw new StateUnavailable(`NATS object does not expose ${key}`);
  }
  return (target as Record<string, unknown>)[key];
}

function callSync(target: unknown, method: string, ...args: unknown[]): unknown {
  const fn = getProperty(target, method);
  if (typeof fn !== "function") {
    throw new StateUnavailable(`NATS object does not expose ${method}()`);
  }
  return Reflect.apply(fn, target, args);
}

async function call(target: unknown, method: string, ...args: unknown[]): Promise<unknown> {
  return await callSync(target, method, ...args);
}

function isMissingKey(error: unknown): boolean {
  return /not found|missing|no keys/i.test(String(error instanceof Error ? error.message : error));
}

function isConflict(error: unknown): boolean {
  return /wrong.*last|revision|exists|conflict/i.test(String(error instanceof Error ? error.message : error));
}

function natsDeliveryInfo(reply: string): { revision: number; deliverySequence: number; pending: number } {
  const tokens = reply.split(".");
  if (tokens.length === 9) tokens.splice(2, 0, "_", "");
  if (tokens.length < 11 || tokens[0] !== "$JS" || tokens[1] !== "ACK") throw new StateUnavailable("Invalid JetStream delivery metadata");
  const revision = Number(tokens[7]), deliverySequence = Number(tokens[8]), pending = Number(tokens[10]);
  if (![revision, deliverySequence, pending].every(Number.isSafeInteger) || revision < 1 || deliverySequence < 1 || pending < 0) throw new StateUnavailable("Invalid JetStream delivery sequence");
  return { revision, deliverySequence, pending };
}
