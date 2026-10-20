import { StateUnavailable } from "./errors.ts";
import { cloneJson } from "./json.ts";
import { AsyncQueue, bounded, immutable, systemScheduler, type RuntimeScheduler } from "./runtime.ts";
import type { StateEntry, StateObservation, StateStore } from "./state.ts";

export interface SourceSnapshot {
  readonly current: boolean;
  readonly generation: number;
  readonly version: number;
  readonly revision: number;
  readonly entries: readonly StateEntry[];
  readonly reason?: string;
  /** Undefined means a complete rebuild or currentness transition. */
  readonly changedKeys?: readonly string[];
}

class SnapshotCompactionRequired extends StateUnavailable {}

/** A store's shared ordered observation cache. Exact writes never mutate it. */
export class MaterializedStateSource {
  private readonly store: StateStore;
  private readonly scheduler: RuntimeScheduler;
  private readonly abort = new AbortController();
  private state: SourceSnapshot = immutable({ current: false, generation: 0, version: 0, revision: 0, entries: [] });
  private readonly listeners = new Set<(snapshot: SourceSnapshot) => void>();
  private readonly watchers = new Set<AsyncQueue<SourceSnapshot>>();
  private readonly revisions = new Map<string, number>();
  private entries = new Map<string, StateEntry>();
  private watermark = 0;
  private task?: Promise<void>;
  private first?: Promise<void>;
  private closed = false;
  private owners = 0;
  constructor(store: StateStore, scheduler = systemScheduler) { this.store = store; this.scheduler = scheduler; }

  retain(): void {
    if (this.closed) throw new StateUnavailable("Source is closed");
    this.owners++;
  }
  async release(): Promise<void> {
    if (this.owners === 0) throw new StateUnavailable("Source has no retained owner");
    if (--this.owners === 0) await this.close();
  }

  get snapshot(): SourceSnapshot { return this.state; }
  get isClosed(): boolean { return this.closed; }
  get subscriberCount(): number { return this.listeners.size; }

  start(): Promise<void> {
    if (this.closed) return Promise.reject(new StateUnavailable("Source is closed"));
    if (this.state.current) return Promise.resolve();
    if (!this.task) {
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      this.first = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
      this.task = this.run(resolve, reject);
    }
    return this.first!;
  }

  get(key: string): StateEntry | null {
    this.requireCurrent();
    return this.entries.get(key) ?? null;
  }

  items(prefix = ""): StateEntry[] {
    this.requireCurrent();
    return this.state.entries.filter(entry => entry.key.startsWith(prefix));
  }

  requireCurrent(): void {
    if (!this.state.current || this.closed) throw new StateUnavailable(this.state.reason ?? "Source is not current");
  }

  subscribe(listener: (snapshot: SourceSnapshot) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  watch(options: { signal?: AbortSignal } = {}): AsyncIterable<SourceSnapshot> {
    if (this.watchers.size >= 256) throw new StateUnavailable("Source watcher limit reached");
    let unsubscribe = () => {};
    const queue = new AsyncQueue<SourceSnapshot>(1, true, () => {
      unsubscribe(); this.watchers.delete(queue); options.signal?.removeEventListener("abort", close);
    });
    const close = () => queue.close();
    this.watchers.add(queue);
    unsubscribe = this.subscribe(snapshot => {
      queue.push(snapshot);
      if (this.closed) queue.close();
    });
    queue.push(this.state);
    options.signal?.addEventListener("abort", close, { once: true });
    if (options.signal?.aborted || this.closed) close();
    return queue;
  }

  async observed(revision: number, key?: string): Promise<void> {
    await this.start();
    this.requireCurrent();
    const reached = () => key === undefined ? this.state.revision >= revision : (this.get(key)?.revision ?? 0) >= revision;
    if (reached()) return;
    let unsubscribe = () => {};
    try {
      await bounded(new Promise<void>((resolve, reject) => {
        unsubscribe = this.subscribe(state => {
          if (!state.current) reject(new StateUnavailable("Source lost currentness while waiting for a write"));
          else if (reached()) resolve();
        });
      }), 5000, this.scheduler);
    } finally { unsubscribe(); }
  }

  async removed(key: string, previousRevision: number): Promise<void> {
    await this.start(); this.requireCurrent();
    const removed = () => {
      const entry = this.entries.get(key);
      return !entry || entry.revision > previousRevision;
    };
    if (removed()) return;
    let unsubscribe = () => {};
    try {
      await bounded(new Promise<void>((resolve, reject) => {
        unsubscribe = this.subscribe(state => {
          if (!state.current) reject(new StateUnavailable("Source became stale during deletion"));
          else if (removed()) resolve();
        });
      }), 5000, this.scheduler);
    } finally { unsubscribe(); }
  }

  invalidate(reason: string): void {
    this.publish({ ...this.state, current: false, reason, changedKeys: undefined });
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.invalidate("source_closed");
    this.abort.abort();
    await bounded(this.task ?? Promise.resolve(), 5000, this.scheduler).catch(() => {});
    this.listeners.clear();
  }

  private publish(state: Omit<SourceSnapshot, "version"> & { version?: number }): void {
    this.state = immutable({ ...state, version: this.state.version + 1 });
    for (const listener of [...this.listeners]) listener(this.state);
  }

  private apply(observation: StateObservation): void {
    if (observation.operation === "stale") { this.invalidate(observation.reason); return; }
    if (observation.operation === "snapshot") {
      this.watermark = observation.revision;
      this.revisions.clear();
      const entries = new Map(observation.entries.map(entry => [entry.key, { ...entry, value: cloneJson(entry.value) }]));
      let revision = observation.revision;
      for (const change of [...(observation.afterBarrier ?? [])].sort((a, b) => a.revision! - b.revision!)) {
        const nextRevision = change.revision ?? change.entry?.revision;
        if (nextRevision === undefined) throw new StateUnavailable("Recovery observation has no revision");
        if (nextRevision <= Math.max(this.watermark, this.revisions.get(change.key) ?? 0, entries.get(change.key)?.revision ?? 0)) continue;
        entries.delete(change.key);
        if (change.operation === "put" && change.entry) {
          entries.set(change.key, { ...change.entry, value: cloneJson(change.entry.value) });
          this.revisions.delete(change.key);
        } else this.revisions.set(change.key, nextRevision);
        revision = Math.max(revision, nextRevision);
      }
      if (this.revisions.size >= 2000) throw new SnapshotCompactionRequired("Snapshot compaction required");
      this.entries = entries;
      this.publish({ current: true, generation: this.state.generation + 1, revision, entries: [...entries.values()] });
      return;
    }
    if (!this.state.current) throw new StateUnavailable("Change arrived without a current snapshot");
    const revision = observation.revision ?? observation.entry?.revision;
    if (revision === undefined) throw new StateUnavailable("Ordered observation has no revision");
    const existing = this.entries.get(observation.key);
    if (revision <= Math.max(this.watermark, this.revisions.get(observation.key) ?? 0, existing?.revision ?? 0)) return;
    if (observation.operation === "put") this.revisions.delete(observation.key);
    else this.revisions.set(observation.key, revision);
    this.entries.delete(observation.key);
    if (observation.operation === "put" && observation.entry) this.entries.set(observation.key, { ...observation.entry, value: cloneJson(observation.entry.value) });
    this.publish({ ...this.state, entries: [...this.entries.values()], changedKeys: [observation.key], revision: Math.max(this.state.revision, revision) });
    if (this.revisions.size >= 2000) throw new SnapshotCompactionRequired("Snapshot compaction required");
  }

  private async *observations(): AsyncIterable<StateObservation> {
    if (this.store.observe) {
      yield* this.store.observe("", { signal: this.abort.signal });
      return;
    }
    throw new StateUnavailable("Managed runtimes require an ordered materialized-source adapter");
  }

  private async run(ready: () => void, failed: (error: unknown) => void): Promise<void> {
    while (!this.closed) {
      try {
        for await (const observation of this.observations()) {
          if (this.closed) break;
          this.apply(observation);
          if (this.state.current) ready();
        }
        if (!this.closed) throw new StateUnavailable("Source watch ended");
      } catch (error) {
        if (!this.closed) this.invalidate(error instanceof Error ? error.message : String(error));
        failed(error);
        if (error instanceof SnapshotCompactionRequired) continue;
      }
      if (!this.closed) await new Promise<void>(resolve => {
        const finish = () => { cancel(); this.abort.signal.removeEventListener("abort", finish); resolve(); };
        const cancel = this.scheduler.schedule(100, finish);
        this.abort.signal.addEventListener("abort", finish, { once: true });
      });
    }
    failed(new StateUnavailable("Source closed before readiness"));
  }
}

const shared = new WeakMap<StateStore, MaterializedStateSource>();
export function sourceFor(store: StateStore, scheduler = systemScheduler): MaterializedStateSource {
  let source = shared.get(store);
  if (!source || source.isClosed) { source = new MaterializedStateSource(store, scheduler); shared.set(store, source); }
  return source;
}
