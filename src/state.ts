import { StateConflict, StateUnavailable } from "./errors.ts";
import { cloneJson, type JsonObject, type JsonValue } from "./json.ts";
import { AsyncQueue, systemScheduler, type RuntimeScheduler } from "./runtime.ts";

export interface StateStorePolicy {
  brokerTtlSeconds: number | null;
  allowWriteTtl: boolean;
  description: string;
}

export const DEFAULT_STATE_TTL_SECONDS = 30;
export const DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS = 5;
export const DEFAULT_STATE_RECONCILE_SECONDS = 300;
export const DEFAULT_STATE_NOTIFICATION_BATCH_SECONDS = 1;

export const TTL_STATE_STORE_POLICY: StateStorePolicy = Object.freeze({
  brokerTtlSeconds: DEFAULT_STATE_TTL_SECONDS,
  allowWriteTtl: true,
  description: "TTL current state",
});

export const PERSISTENT_STATE_STORE_POLICY: StateStorePolicy = Object.freeze({
  brokerTtlSeconds: null,
  allowWriteTtl: false,
  description: "persistent current state",
});

export interface StateEntry {
  key: string;
  value: JsonObject;
  revision: number;
  parseError?: string;
  raw?: string;
}

export type StateOperation = "put" | "delete" | "expire";

export interface StateChange {
  operation: StateOperation;
  key: string;
  entry?: StateEntry;
  revision?: number;
}

export type StateObservation = StateChange | {
  operation: "snapshot";
  entries: StateEntry[];
  /** Captured high-water barrier; replay-time writes are installed atomically below. */
  revision: number;
  afterBarrier?: StateChange[];
} | { operation: "stale"; reason: string };

export interface StateResnapshot {
  operation: "resnapshot";
}

export type StateWatchItem = StateChange | StateResnapshot;

export const MAX_MEMORY_WATCH_CHANGED_KEYS = 256;
export const MAX_MEMORY_WATCHERS = 256;

export interface StateStore {
  readonly policy?: StateStorePolicy;
  get(key: string): Promise<StateEntry | null>;
  items(prefix?: string): Promise<StateEntry[]>;
  put(key: string, value: JsonObject, options?: { ttl?: number | null }): Promise<StateEntry>;
  create(key: string, value: JsonObject, options?: { ttl?: number | null }): Promise<StateEntry>;
  update(
    key: string,
    value: JsonObject,
    options: { revision: number; ttl?: number | null },
  ): Promise<StateEntry>;
  delete(key: string, options?: { revision?: number | null }): Promise<number | void>;
  watch?(prefix?: string, options?: { signal?: AbortSignal }): AsyncIterable<StateWatchItem>;
  observe?(prefix?: string, options?: { signal?: AbortSignal }): AsyncIterable<StateObservation>;
}

export interface PrefixObservation {
  entries: StateEntry[];
  confirmedMissing: Set<string>;
}

export async function observePrefixCurrent(
  state: StateStore,
  prefix = "",
  knownKeys: Iterable<string> = [],
): Promise<PrefixObservation> {
  const observed = new Map<string, StateEntry>();
  for (const entry of await state.items(prefix)) {
    observed.set(entry.key, entry);
  }
  const confirmedMissing = new Set<string>();
  for (const key of [...new Set(knownKeys)].sort()) {
    if (!key.startsWith(prefix) || observed.has(key)) {
      continue;
    }
    const current = await state.get(key);
    if (current === null) {
      confirmedMissing.add(key);
    } else {
      observed.set(key, current);
    }
  }
  return {
    entries: [...observed.values()].sort((left, right) => left.key.localeCompare(right.key)),
    confirmedMissing,
  };
}

export class MemoryStateStore implements StateStore {
  readonly name: string;
  readonly policy: StateStorePolicy;

  private revision = 0;
  private entries = new Map<string, StateEntry>();
  private watchers = new Set<MemoryWatcher>();
  private observations = new Set<AsyncQueue<StateObservation>>();
  private expirations = new Map<string, () => void>();
  private readonly scheduler: RuntimeScheduler;

  constructor(options: { name?: string; policy?: StateStorePolicy; scheduler?: RuntimeScheduler } = {}) {
    this.name = options.name ?? "memory";
    this.policy = options.policy ?? PERSISTENT_STATE_STORE_POLICY;
    this.scheduler = options.scheduler ?? systemScheduler;
  }

  async get(key: string): Promise<StateEntry | null> {
    const entry = this.entries.get(key);
    return entry === undefined ? null : copyEntry(entry);
  }

  async items(prefix = ""): Promise<StateEntry[]> {
    return [...this.entries.values()]
      .filter((entry) => entry.key.startsWith(prefix))
      .sort((left, right) => left.key.localeCompare(right.key))
      .map(copyEntry);
  }

  async put(key: string, value: JsonObject, options: { ttl?: number | null } = {}): Promise<StateEntry> {
    this.validateTtl(options.ttl ?? null);
    const entry = this.nextEntry(key, value);
    this.entries.set(key, entry);
    this.emit({ operation: "put", key, entry: copyEntry(entry) });
    return copyEntry(entry);
  }

  async create(key: string, value: JsonObject, options: { ttl?: number | null } = {}): Promise<StateEntry> {
    this.validateTtl(options.ttl ?? null);
    if (this.entries.has(key)) {
      throw new StateConflict(`State key ${JSON.stringify(key)} already exists`, "key_already_exists");
    }
    const entry = this.nextEntry(key, value);
    this.entries.set(key, entry);
    this.emit({ operation: "put", key, entry: copyEntry(entry) });
    return copyEntry(entry);
  }

  async update(
    key: string,
    value: JsonObject,
    options: { revision: number; ttl?: number | null },
  ): Promise<StateEntry> {
    this.validateTtl(options.ttl ?? null);
    const current = this.entries.get(key);
    if (current === undefined) {
      throw new StateConflict(`State key ${key} is missing`);
    }
    if (current.revision !== options.revision) {
      throw new StateConflict(`State key ${key} revision changed`, "revision_changed");
    }
    const entry = this.nextEntry(key, value);
    this.entries.set(key, entry);
    this.emit({ operation: "put", key, entry: copyEntry(entry) });
    return copyEntry(entry);
  }

  async delete(key: string, options: { revision?: number | null } = {}): Promise<number> {
    const current = this.entries.get(key);
    if (current === undefined) {
      return this.revision;
    }
    if (options.revision !== undefined && options.revision !== null && current.revision !== options.revision) {
      throw new StateConflict(`State key ${key} revision changed`, "revision_changed");
    }
    this.entries.delete(key);
    this.expirations.get(key)?.();
    this.expirations.delete(key);
    this.emit({ operation: "delete", key, revision: ++this.revision });
    return this.revision;
  }

  watch(prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateWatchItem> {
    if (this.watchers.size >= MAX_MEMORY_WATCHERS) {
      throw new StateUnavailable(
        `Memory state store supports at most ${MAX_MEMORY_WATCHERS} simultaneous watchers`,
      );
    }
    const watcher = new MemoryWatcher(prefix, () => { this.watchers.delete(watcher); options.signal?.removeEventListener("abort", close); });
    this.watchers.add(watcher);
    const close = () => watcher.close();
    options.signal?.addEventListener("abort", close, { once: true });
    if (options.signal?.aborted) close();
    return watcher;
  }

  async *observe(prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateObservation> {
    if (this.observations.size >= MAX_MEMORY_WATCHERS) throw new StateUnavailable("Memory observation limit reached");
    const queue = new AsyncQueue<StateObservation>(2048);
    const close = () => queue.close();
    this.observations.add(queue);
    options.signal?.addEventListener("abort", close, { once: true });
    try {
      if (options.signal?.aborted) return;
      const entries = [...this.entries.values()].filter(entry => entry.key.startsWith(prefix)).map(copyEntry);
      yield { operation: "snapshot", entries, revision: this.revision };
      for await (const item of queue) {
        if (item.operation === "snapshot" || item.operation === "stale" || item.key.startsWith(prefix)) yield item;
      }
    } finally {
      options.signal?.removeEventListener("abort", close);
      this.observations.delete(queue); queue.close();
    }
  }

  private nextEntry(key: string, value: JsonObject): StateEntry {
    this.revision += 1;
    return { key, value: cloneJson(value as JsonValue) as JsonObject, revision: this.revision };
  }

  private validateTtl(ttl: number | null): void {
    if (!this.policy.allowWriteTtl && ttl !== null) {
      throw new StateUnavailable(
        `${this.policy.description} does not use write TTL; per-key TTL ${ttl} is not supported.`,
      );
    }
    if (ttl !== null && this.policy.brokerTtlSeconds !== null && Math.abs(ttl - this.policy.brokerTtlSeconds) > 0.001) {
      throw new StateUnavailable(
        `current state uses broker TTL ${this.policy.brokerTtlSeconds}; per-key TTL ${ttl} is not supported.`,
      );
    }
  }

  private emit(change: StateChange): void {
    if (change.operation === "put" && change.entry) {
      change.revision = change.entry.revision;
      this.expirations.get(change.key)?.();
      const ttl = this.policy.brokerTtlSeconds;
      if (ttl !== null) {
        const revision = change.entry.revision;
        this.expirations.set(change.key, this.scheduler.schedule(ttl * 1000, () => {
          if (this.entries.get(change.key)?.revision !== revision) return;
          this.entries.delete(change.key);
          this.expirations.delete(change.key);
          this.emit({ operation: "expire", key: change.key, revision: ++this.revision });
        }));
      }
    }
    for (const observer of this.observations) observer.push(copyChange(change));
    for (const watcher of this.watchers) {
      watcher.offer(change);
    }
  }
}

class MemoryWatcher implements AsyncIterable<StateWatchItem> {
  private pending = new Map<string, StateChange>();
  // Every watcher begins dirty. Public projections handle this marker by
  // reading their complete current state while the watcher is already armed,
  // so bootstrap cannot miss a concurrent mutation.
  private resnapshotRequired = true;
  private waiter: ((value: IteratorResult<StateWatchItem>) => void) | null = null;
  private closed = false;
  private readonly prefix: string;
  private readonly onClose: () => void;

  constructor(prefix: string, onClose: () => void) {
    this.prefix = prefix;
    this.onClose = onClose;
  }

  offer(change: StateChange): void {
    if (this.closed || !change.key.startsWith(this.prefix)) {
      return;
    }
    if (!this.resnapshotRequired) {
      this.pending.set(change.key, copyChange(change));
      if (this.pending.size > MAX_MEMORY_WATCH_CHANGED_KEYS) {
        this.pending.clear();
        this.resnapshotRequired = true;
      }
    }
    const waiter = this.waiter;
    if (waiter !== null) {
      this.waiter = null;
      waiter({ done: false, value: this.takePending() });
    }
  }

  private takePending(): StateWatchItem {
    if (this.resnapshotRequired) {
      this.resnapshotRequired = false;
      return { operation: "resnapshot" };
    }
    const first = this.pending.entries().next();
    if (first.done) {
      throw new StateUnavailable("Memory watcher woke without pending state");
    }
    const [key, change] = first.value;
    this.pending.delete(key);
    return change;
  }

  [Symbol.asyncIterator](): AsyncIterator<StateWatchItem> {
    return {
      next: () => {
        if (this.resnapshotRequired || this.pending.size > 0) {
          return Promise.resolve({ done: false, value: this.takePending() });
        }
        if (this.closed) {
          return Promise.resolve({ done: true, value: undefined });
        }
        if (this.waiter !== null) {
          return Promise.reject(
            new StateUnavailable("Memory watcher supports only one pending reader"),
          );
        }
        return new Promise((resolve) => {
          this.waiter = resolve;
        });
      },
      return: () => {
        this.close();
        return Promise.resolve({ done: true, value: undefined });
      },
    };
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.onClose();
    if (this.waiter !== null) {
      const waiter = this.waiter;
      this.waiter = null;
      waiter({ done: true, value: undefined });
    }
  }
}

function copyEntry(entry: StateEntry): StateEntry {
  return {
    ...entry,
    key: entry.key,
    value: cloneJson(entry.value as JsonValue) as JsonObject,
    revision: entry.revision,
  };
}

function copyChange(change: StateChange): StateChange {
  return {
    operation: change.operation,
    key: change.key,
    ...(change.revision === undefined ? {} : { revision: change.revision }),
    ...(change.entry === undefined ? {} : { entry: copyEntry(change.entry) }),
  };
}
