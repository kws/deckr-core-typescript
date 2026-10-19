import type { RuntimeScheduler } from "../src/runtime.ts";
import { AsyncQueue } from "../src/runtime.ts";
import { MemoryStateStore, type StateEntry, type StateObservation, type StateStore, type StateStorePolicy } from "../src/state.ts";
import { StateUnavailable } from "../src/errors.ts";
import type { JsonObject } from "../src/json.ts";

export async function flush(): Promise<void> {
  for (let index = 0; index < 30; index++) await Promise.resolve();
}

export class TestClock implements RuntimeScheduler {
  time = 0;
  private sequence = 0;
  private jobs = new Map<number, { at: number; callback: () => void }>();
  now(): number { return this.time; }
  random(): number { return 0; }
  schedule(ms: number, callback: () => void): () => void {
    const id = ++this.sequence;
    this.jobs.set(id, { at: this.time + ms, callback });
    return () => { this.jobs.delete(id); };
  }
  async advance(ms: number): Promise<void> {
    const until = this.time + ms;
    while (true) {
      await flush();
      const next = [...this.jobs].sort((a, b) => a[1].at - b[1].at)[0];
      if (!next || next[1].at > until) break;
      this.time = next[1].at; this.jobs.delete(next[0]); next[1].callback();
    }
    this.time = until; await flush();
  }
}

/** A transport that can lose watch continuity while exact reads still work. */
export class RecoverableStore implements StateStore {
  readonly policy: StateStorePolicy;
  readonly memory: MemoryStateStore;
  readonly observers = new Set<AsyncQueue<StateObservation>>();
  exactReads = 0;
  scans = 0;
  failWrites = false;
  current = true;
  private revision = 0;
  constructor(policy: StateStorePolicy) { this.policy = policy; this.memory = new MemoryStateStore({ policy }); }
  async get(key: string): Promise<StateEntry | null> { this.exactReads++; return this.memory.get(key); }
  async items(prefix = ""): Promise<StateEntry[]> { this.scans++; return this.memory.items(prefix); }
  private check(): void { if (this.failWrites) throw new StateUnavailable("Test store unavailable"); }
  private publish(entry: StateEntry): StateEntry {
    this.revision = entry.revision;
    if (this.current) for (const observer of this.observers) observer.push({ operation: "put", key: entry.key, entry, revision: entry.revision });
    return entry;
  }
  async put(key: string, value: JsonObject): Promise<StateEntry> { this.check(); return this.publish(await this.memory.put(key, value)); }
  async create(key: string, value: JsonObject): Promise<StateEntry> { this.check(); return this.publish(await this.memory.create(key, value)); }
  async update(key: string, value: JsonObject, options: { revision: number }): Promise<StateEntry> { this.check(); return this.publish(await this.memory.update(key, value, options)); }
  async delete(key: string, options: { revision?: number | null } = {}): Promise<void> {
    this.check(); await this.memory.delete(key, options); this.revision++;
    if (this.current) for (const observer of this.observers) observer.push({ operation: "delete", key, revision: this.revision });
  }
  async *observe(_prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateObservation> {
    const queue = new AsyncQueue<StateObservation>(2048);
    const close = () => queue.close();
    this.observers.add(queue);
    options.signal?.addEventListener("abort", close, { once: true });
    if (options.signal?.aborted) close();
    try {
      yield { operation: "snapshot", entries: await this.memory.items(), revision: this.revision };
      yield* queue;
    } finally { this.observers.delete(queue); options.signal?.removeEventListener("abort", close); queue.close(); }
  }
  disconnect(): void {
    this.current = false;
    for (const observer of this.observers) observer.push({ operation: "stale", reason: "consumer_recovery" });
  }
  async recover(): Promise<void> {
    this.current = true;
    const entries = await this.memory.items();
    for (const observer of this.observers) observer.push({ operation: "snapshot", entries, revision: this.revision });
  }
}
