import { StateUnavailable } from "./errors.ts";

/** Host-supplied time. Protocol workers never need a process-global timer. */
export interface RuntimeScheduler {
  now(): number;
  random(): number;
  schedule(milliseconds: number, callback: () => void): () => void;
}

export const systemScheduler: RuntimeScheduler = {
  now: () => Date.now(),
  random: () => Math.random(),
  schedule(milliseconds, callback) {
    const timer = setTimeout(callback, milliseconds);
    timer.unref();
    return () => clearTimeout(timer);
  },
};

export function bounded<T>(work: Promise<T>, milliseconds: number, scheduler = systemScheduler): Promise<T> {
  return new Promise((resolve, reject) => {
    const cancel = scheduler.schedule(milliseconds, () => reject(new StateUnavailable("Lifecycle deadline exceeded")));
    work.then(resolve, reject).finally(cancel);
  });
}

/** Cancel the local wait; the owner remains responsible for late I/O cleanup. */
export function abortable<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(new StateUnavailable("Operation cancelled"));
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    void work.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
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
  constructor(ttlSeconds: () => number, renew: () => Promise<void>, failed: (error: unknown) => void, scheduler = systemScheduler) {
    this.ttlSeconds = ttlSeconds; this.renew = renew; this.failed = failed; this.scheduler = scheduler;
  }

  start(): void {
    if (this.stopped || this.cancelTimer || this.running) return;
    const ttl = this.ttlSeconds();
    const interval = ttl * (0.5 + Math.max(0, Math.min(1, this.scheduler.random())) * 0.25);
    this.cancelTimer = this.scheduler.schedule(interval * 1000, () => {
      this.cancelTimer = undefined;
      if (this.stopped) return;
      this.running = bounded(this.renew(), (ttl - interval) * 1000, this.scheduler)
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
