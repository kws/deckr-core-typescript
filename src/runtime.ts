import { StateUnavailable } from "./errors.ts";

/** Host-supplied time. Protocol workers never need a process-global timer. */
export interface RuntimeScheduler {
  now(): number;
  monotonicNow?(): number;
  random(): number;
  schedule(milliseconds: number, callback: () => void): () => void;
}

export const systemScheduler: RuntimeScheduler = {
  now: () => Date.now(),
  monotonicNow: () => performance.now(),
  random: () => Math.random(),
  schedule(milliseconds, callback) {
    const timer = setTimeout(callback, milliseconds);
    timer.unref();
    return () => clearTimeout(timer);
  },
};

export function monotonicNow(scheduler: RuntimeScheduler): number {
  return scheduler.monotonicNow?.() ?? scheduler.now();
}

export function bounded<T>(work: Promise<T>, milliseconds: number, scheduler = systemScheduler): Promise<T> {
  return new Promise((resolve, reject) => {
    const cancel = scheduler.schedule(milliseconds, () => reject(new StateUnavailable("Lifecycle deadline exceeded")));
    work.then(resolve, reject).finally(cancel);
  });
}

const abortSubscriptions = new WeakMap<AbortSignal, { callbacks: Set<(signal: AbortSignal) => void>; dispatch: () => void }>();

/** One native listener per source, with explicit removal when a wait ends. */
export function subscribeAbort(signals: readonly AbortSignal[], callback: (signal: AbortSignal) => void): () => void {
  const aborted = signals.find(signal => signal.aborted);
  if (aborted) { callback(aborted); return () => {}; }
  const listener = (signal: AbortSignal) => callback(signal);
  const removals = [...new Set(signals)].map(signal => {
    let entry = abortSubscriptions.get(signal);
    if (!entry) {
      const callbacks = new Set<(signal: AbortSignal) => void>();
      entry = { callbacks, dispatch: () => {
        abortSubscriptions.delete(signal);
        for (const listener of [...callbacks]) listener(signal);
        callbacks.clear();
      } };
      abortSubscriptions.set(signal, entry);
      signal.addEventListener("abort", entry.dispatch, { once: true });
    }
    const held = entry;
    held.callbacks.add(listener);
    return () => {
      held.callbacks.delete(listener);
      if (!held.callbacks.size) {
        signal.removeEventListener("abort", held.dispatch);
        if (abortSubscriptions.get(signal) === held) abortSubscriptions.delete(signal);
      }
    };
  });
  return () => { for (const remove of removals) remove(); };
}

/** Avoid per-operation AbortSignal.any registrations on long-lived parents.
 * Callers dispose a bounded operation's combined signal in finally. */
export function linkedAbortSignal(signals: readonly AbortSignal[]): { signal: AbortSignal; dispose: () => void } {
  const controller = new AbortController();
  let dispose = () => {};
  dispose = subscribeAbort(signals, source => { controller.abort(source.reason); dispose(); });
  return { signal: controller.signal, dispose };
}

/** Cancel the local wait; the owner remains responsible for late I/O cleanup. */
export function abortable<T>(work: Promise<T>, signals: AbortSignal | readonly AbortSignal[]): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let unsubscribe = () => {};
    unsubscribe = subscribeAbort(Array.isArray(signals) ? signals : [signals as AbortSignal], () => {
      unsubscribe(); reject(new StateUnavailable("Operation cancelled"));
    });
    void work.then(value => { unsubscribe(); resolve(value); }, error => { unsubscribe(); reject(error); });
  });
}

/** One worker, one held resource, and a deadline that retries cannot extend. */
export class RenewalWorker {
  private cancelTimer?: () => void;
  private running?: Promise<void>;
  private stopped = false;
  private readonly ttlSeconds: () => number;
  private readonly renew: () => Promise<void>;
  private readonly failed: (error: unknown) => void;
  private readonly scheduler: RuntimeScheduler;
  private readonly expiresAt?: () => number;
  constructor(ttlSeconds: () => number, renew: () => Promise<void>, failed: (error: unknown) => void, scheduler = systemScheduler, expiresAt?: () => number) {
    this.ttlSeconds = ttlSeconds; this.renew = renew; this.failed = failed; this.scheduler = scheduler;
    this.expiresAt = expiresAt;
  }

  start(): void {
    if (this.stopped || this.cancelTimer || this.running) return;
    const ttl = this.ttlSeconds();
    const expires = this.expiresAt?.() ?? monotonicNow(this.scheduler) + ttl * 1000;
    const interval = ttl * (0.5 + Math.max(0, Math.min(1, this.scheduler.random())) * 0.25);
    this.cancelTimer = this.scheduler.schedule(Math.max(0, expires - (ttl - interval) * 1000 - monotonicNow(this.scheduler)), () => {
      this.cancelTimer = undefined;
      if (this.stopped) return;
      const remaining = expires - monotonicNow(this.scheduler);
      if (remaining <= 0) {
        this.stop(); this.failed(new StateUnavailable("Renewal deadline exceeded")); return;
      }
      this.running = bounded(this.renew(), remaining, this.scheduler)
        .catch(error => {
          this.stop();
          this.failed(error);
        })
        .finally(() => {
          this.running = undefined;
          this.start();
        });
    });
  }

  stop(): void {
    this.stopped = true;
    this.cancelTimer?.();
    this.cancelTimer = undefined;
  }

  async close(): Promise<void> {
    this.stop();
    if (this.running) await bounded(this.running, 5000, this.scheduler).catch(() => {});
  }
}

/** Bounded iterator with explicit cancellation; snapshots can be coalesced. */
export class AsyncQueue<T> implements AsyncIterableIterator<T> {
  private pending: T[] = [];
  private waiter?: { resolve: (value: IteratorResult<T>) => void; reject: (error: unknown) => void };
  private ended = false;
  private error?: unknown;
  private readonly limit: number;
  private readonly coalesce: boolean;
  private readonly onClose?: () => void;
  constructor(limit = 256, coalesce = false, onClose?: () => void) {
    this.limit = limit; this.coalesce = coalesce; this.onClose = onClose;
  }

  push(value: T): void {
    if (this.ended) return;
    if (this.waiter) {
      const waiter = this.waiter;
      this.waiter = undefined;
      waiter.resolve({ done: false, value });
    } else {
      if (this.coalesce) this.pending = [];
      this.pending.push(value);
      if (this.pending.length > this.limit) this.close(new StateUnavailable("Observation queue overflow"));
    }
  }

  get pendingCount(): number { return this.pending.length; }
  replace(value: T): void { if (!this.ended) { this.pending = []; this.push(value); } }

  close(error?: unknown): void {
    if (this.ended) return;
    this.ended = true;
    this.error = error;
    this.pending = [];
    if (this.waiter) {
      if (error) this.waiter.reject(error);
      else this.waiter.resolve({ done: true, value: undefined });
      this.waiter = undefined;
    }
    this.onClose?.();
  }

  /** End after delivering an already queued terminal snapshot. */
  finish(): void {
    if (this.ended) return;
    this.ended = true;
    if (this.waiter) { this.waiter.resolve({ done: true, value: undefined }); this.waiter = undefined; }
    this.onClose?.();
  }

  next(): Promise<IteratorResult<T>> {
    if (this.error) return Promise.reject(this.error);
    if (this.pending.length) return Promise.resolve({ done: false, value: this.pending.shift()! });
    if (this.ended) return Promise.resolve({ done: true, value: undefined });
    if (this.waiter) return Promise.reject(new StateUnavailable("Iterator already has a pending read"));
    return new Promise((resolve, reject) => { this.waiter = { resolve, reject }; });
  }

  return(): Promise<IteratorResult<T>> {
    this.close();
    return Promise.resolve({ done: true, value: undefined });
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> { return this; }
}

export interface RuntimeFailure { readonly code: string; readonly message: string; }
/** Health and owned resource counts; this never grants protocol authority. */
export interface RuntimeState {
  readonly status: "idle" | "starting" | "ready" | "unavailable" | "closed";
  readonly failure?: RuntimeFailure;
  readonly resources: Readonly<Record<string, number>>;
}
export type VersionedState<T> = Readonly<T & { version: number }>;

/** Atomic initial observation and predicate waits over immutable, versioned state. */
export class StateCell<T extends object> {
  private value: VersionedState<T>;
  private readonly subscribers = new Set<AsyncQueue<VersionedState<T>>>();
  private ended = false;
  constructor(initial: T) { this.value = immutable({ ...initial, version: 0 }); }
  get current(): VersionedState<T> { return this.value; }
  set(value: T): void {
    if (this.ended) return;
    this.value = immutable({ ...value, version: this.value.version + 1 });
    for (const subscriber of this.subscribers) subscriber.push(this.value);
  }
  watch(options: { signal?: AbortSignal } = {}): AsyncIterable<VersionedState<T>> {
    if (this.subscribers.size >= 256) throw new StateUnavailable("State subscriber limit reached");
    const close = () => queue.close();
    const queue = new AsyncQueue<VersionedState<T>>(1, true, () => {
      this.subscribers.delete(queue); options.signal?.removeEventListener("abort", close);
    });
    this.subscribers.add(queue); queue.push(this.value);
    options.signal?.addEventListener("abort", close, { once: true });
    if (options.signal?.aborted) close();
    else if (this.ended) queue.finish();
    return queue;
  }
  async wait(predicate: (state: VersionedState<T>) => boolean, options: { signal?: AbortSignal; timeoutMs?: number; scheduler?: RuntimeScheduler } = {}): Promise<VersionedState<T>> {
    const abort = new AbortController();
    const signal = options.signal ? AbortSignal.any([options.signal, abort.signal]) : abort.signal;
    try {
      const pending = (async () => {
        for await (const state of this.watch({ signal })) if (predicate(state)) return state;
        throw new StateUnavailable("State wait ended before its predicate was satisfied");
      })();
      return await (options.timeoutMs === undefined ? pending : bounded(pending, options.timeoutMs, options.scheduler));
    } finally { abort.abort(); }
  }
  close(): void { this.ended = true; for (const subscriber of [...this.subscribers]) subscriber.finish(); }
}

export function immutable<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
}
