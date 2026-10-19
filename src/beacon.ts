import { randomUUID } from "node:crypto";
import { AsyncQueue, bounded, immutable, RenewalWorker, StateCell, systemScheduler, type RuntimeFailure, type RuntimeScheduler, type RuntimeState } from "./runtime.ts";
import { sourceFor, type MaterializedStateSource, type SourceSnapshot } from "./source.ts";

import { endpointAddress } from "./endpoint.ts";
import { StateConflict, StateUnavailable, ValidationError } from "./errors.ts";
import {
  cloneJson,
  parseUtcMillis,
  requireJsonObject,
  requirePositiveInteger,
  requireText,
  utcIsoNow,
  type JsonObject,
} from "./json.ts";
import { decodeKeyToken, encodeKeyToken } from "./keys.ts";
import {
  type StateChange,
  type StateEntry,
  type StateStore,
  type StateStorePolicy,
  type StateWatchItem,
} from "./state.ts";

export const BEACON_ADVERTISEMENT_SCHEMA_ID = "dev.deckr.beacon.advertisement.v1";
export const DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME = "deckr_beacon_advertisement_v1";
export const DEFAULT_BEACON_TTL_SECONDS = 300;
export const BEACON_ADVERTISEMENT_STORE_POLICY: StateStorePolicy = Object.freeze({
  brokerTtlSeconds: DEFAULT_BEACON_TTL_SECONDS,
  allowWriteTtl: true,
  description: "Beacon advertisement state",
});

export const CandidateStatus = Object.freeze({
  CANDIDATE: "candidate",
  MISSING: "missing",
  SCHEMA_INVALID: "schema_invalid",
  FEATURE_MISMATCH: "feature_mismatch",
  SESSION_MISMATCH: "session_mismatch",
  UNAVAILABLE: "unavailable",
});
export type CandidateStatus = (typeof CandidateStatus)[keyof typeof CandidateStatus];

export const BeaconFeatureEventType = Object.freeze({
  ADVERTISED: "advertised",
  UPDATED: "updated",
  WITHDRAWN: "withdrawn",
  EXPIRED: "expired",
  INVALID: "invalid",
});
export type BeaconFeatureEventType =
  (typeof BeaconFeatureEventType)[keyof typeof BeaconFeatureEventType];

export const BeaconFeatureWatchItemType = Object.freeze({
  SNAPSHOT: "snapshot",
  CHANGE: "change",
});
export type BeaconFeatureWatchItemType =
  (typeof BeaconFeatureWatchItemType)[keyof typeof BeaconFeatureWatchItemType];

export interface BeaconProtocol {
  namespace: string;
  version: string;
}

export interface AdvertisementRecord {
  schema: typeof BEACON_ADVERTISEMENT_SCHEMA_ID;
  advertisementId: string;
  featureId: string;
  advertiser: string;
  endpoint: string;
  sessionId: string;
  refreshSeq: number;
  ttlSeconds: number;
  protocol?: BeaconProtocol;
  operations: string[];
  labels: Record<string, string>;
  hints: JsonObject;
  payload?: JsonObject;
  createdAt?: string;
  updatedAt?: string;
}

export interface AdvertisementHandle {
  key: string;
  advertisementId: string;
  featureId: string;
  advertiser: string;
  endpoint: string;
  sessionId: string;
  revision: number;
  refreshSeq: number;
}

export interface Candidate {
  key: string;
  advertisement: AdvertisementRecord;
  revision: number;
  observedAt: string;
}

export interface BeaconFeatureSnapshot {
  type: typeof BeaconFeatureWatchItemType.SNAPSHOT;
  featureId: string;
  candidates: readonly Candidate[];
  current?: boolean;
  version?: number;
  generation?: number;
}

export interface BeaconFeatureChange {
  type: typeof BeaconFeatureWatchItemType.CHANGE;
  eventType: BeaconFeatureEventType;
  featureId: string;
  key: string;
  candidate?: Candidate;
  previous?: Candidate;
  reason?: string;
  change?: StateChange;
}

export type BeaconFeatureWatchItem = BeaconFeatureSnapshot | BeaconFeatureChange;

export type AdvertisementFilter = (advertisement: AdvertisementRecord) => boolean;

export function beaconAdvertisementKey(input: {
  featureId: string;
  advertisementId: string;
}): string {
  return [
    "advertisements",
    "by_feature",
    encodeKeyToken(input.featureId),
    encodeKeyToken(input.advertisementId),
  ].join(".");
}

export function parseBeaconAdvertisementKey(key: string): [string, string] | null {
  const parts = key.split(".");
  if (parts.length !== 4 || parts[0] !== "advertisements" || parts[1] !== "by_feature") {
    return null;
  }
  return [decodeKeyToken(parts[2]!), decodeKeyToken(parts[3]!)];
}

export function beaconFeaturePrefix(featureId: string): string {
  return ["advertisements", "by_feature", encodeKeyToken(featureId), ""].join(".");
}

export function validateAdvertisementRecord(value: unknown): AdvertisementRecord {
  const raw = requireJsonObject(value, "Beacon advertisement");
  if (raw.schema !== undefined && raw.schema !== BEACON_ADVERTISEMENT_SCHEMA_ID) {
    throw new ValidationError("Beacon advertisement schema is invalid");
  }
  const record: AdvertisementRecord = {
    schema: BEACON_ADVERTISEMENT_SCHEMA_ID,
    advertisementId: requireText(raw.advertisementId, "advertisementId"),
    featureId: requireText(raw.featureId, "featureId"),
    advertiser: endpointAddress(requireText(raw.advertiser, "advertiser")),
    endpoint: endpointAddress(requireText(raw.endpoint, "endpoint")),
    sessionId: requireText(raw.sessionId, "sessionId"),
    refreshSeq: requirePositiveInteger(raw.refreshSeq, "refreshSeq"),
    ttlSeconds: requirePositiveInteger(raw.ttlSeconds, "ttlSeconds"),
    operations: validateTextList(raw.operations ?? [], "Beacon operation"),
    labels: validateLabels(raw.labels ?? {}),
    hints: requireJsonObject(raw.hints ?? {}, "Beacon hints"),
  };
  if (raw.protocol !== undefined) {
    const protocol = requireJsonObject(raw.protocol, "Beacon protocol");
    record.protocol = {
      namespace: requireText(protocol.namespace, "Beacon protocol namespace"),
      version: requireText(protocol.version, "Beacon protocol version"),
    };
  }
  if (raw.payload !== undefined && raw.payload !== null) {
    record.payload = requireJsonObject(raw.payload, "Beacon payload");
  }
  if (raw.createdAt !== undefined) {
    record.createdAt = requireText(raw.createdAt, "createdAt");
  }
  if (raw.updatedAt !== undefined) {
    record.updatedAt = requireText(raw.updatedAt, "updatedAt");
  }
  beaconAdvertisementKey({
    featureId: record.featureId,
    advertisementId: record.advertisementId,
  });
  return record;
}

export class BeaconDiscovery {
  private readonly state: StateStore;
  readonly defaultTtlSeconds: number;
  get source(): MaterializedStateSource { return sourceFor(this.state, this.scheduler); }
  readonly scheduler: RuntimeScheduler;

  constructor(state: StateStore, options: { defaultTtlSeconds?: number; scheduler?: RuntimeScheduler } = {}) {
    this.state = state;
    this.defaultTtlSeconds = state.policy?.brokerTtlSeconds ?? DEFAULT_BEACON_TTL_SECONDS;
    if (options.defaultTtlSeconds !== undefined && options.defaultTtlSeconds !== this.defaultTtlSeconds) throw new ValidationError("Beacon TTL is owned by the store policy");
    this.scheduler = options.scheduler ?? systemScheduler;
    if (this.defaultTtlSeconds <= 0) {
      throw new ValidationError("defaultTtlSeconds must be greater than zero");
    }
  }

  async advertise(
    featureId: string,
    endpoint: string,
    sessionId: string,
    options: {
      advertiser?: string;
      advertisementId?: string;
      protocol?: BeaconProtocol;
      operations?: string[];
      labels?: Record<string, string>;
      hints?: JsonObject;
      payload?: JsonObject;
      ttlSeconds?: number;
    } = {},
  ): Promise<AdvertisementHandle> {
    if (options.ttlSeconds !== undefined && options.ttlSeconds !== this.defaultTtlSeconds) throw new ValidationError("Beacon TTL is owned by the store policy");
    const now = utcIsoNow();
    const record = validateAdvertisementRecord({
      schema: BEACON_ADVERTISEMENT_SCHEMA_ID,
      advertisementId: options.advertisementId ?? randomUUID(),
      featureId,
      advertiser: options.advertiser ?? endpoint,
      endpoint,
      sessionId,
      refreshSeq: 1,
      ttlSeconds: options.ttlSeconds ?? this.defaultTtlSeconds,
      protocol: options.protocol,
      operations: options.operations ?? [],
      labels: options.labels ?? {},
      hints: options.hints ?? {},
      payload: options.payload,
      createdAt: now,
      updatedAt: now,
    });
    const key = beaconAdvertisementKey({
      featureId: record.featureId,
      advertisementId: record.advertisementId,
    });
    const entry = await this.state.create(key, advertisementToJson(record), {
      ttl: record.ttlSeconds,
    });
    return advertisementHandle(key, record, entry.revision);
  }

  async refresh(
    handle: AdvertisementHandle,
    options: { hints?: JsonObject; labels?: Record<string, string>; payload?: JsonObject } = {},
  ): Promise<AdvertisementHandle> {
    const current = await this.state.get(handle.key);
    if (current === null) {
      throw new StateConflict(`Beacon advertisement ${JSON.stringify(handle.key)} is missing`);
    }
    const record = validateAdvertisementRecord(current.value);
    if (!advertisementMatchesHandle(record, handle)) {
      throw new StateConflict(`Beacon advertisement ${JSON.stringify(handle.key)} changed owner`);
    }
    const refreshed = validateAdvertisementRecord({
      ...record,
      refreshSeq: record.refreshSeq + 1,
      hints: options.hints ?? record.hints,
      labels: options.labels ?? record.labels,
      payload: options.payload ?? record.payload,
      updatedAt: utcIsoNow(),
    });
    const entry = await this.state.update(handle.key, advertisementToJson(refreshed), {
      revision: current.revision,
      ttl: refreshed.ttlSeconds,
    });
    return advertisementHandle(handle.key, refreshed, entry.revision);
  }

  async withdraw(handle: AdvertisementHandle): Promise<boolean> {
    const current = await this.state.get(handle.key);
    if (current === null) {
      return false;
    }
    const record = validateAdvertisementRecord(current.value);
    if (!advertisementMatchesHandle(record, handle)) {
      throw new StateConflict(`Beacon advertisement ${JSON.stringify(handle.key)} changed owner`);
    }
    const revision = await this.state.delete(handle.key, { revision: current.revision });
    if (this.source.snapshot.current) {
      if (typeof revision === "number") await this.source.observed(revision);
      else await this.source.removed(handle.key, current.revision);
    }
    return true;
  }

  async find(featureId: string, selector?: AdvertisementFilter): Promise<Candidate[]> {
    await this.source.start();
    const candidates: Candidate[] = [];
    for (const entry of this.source.items(beaconFeaturePrefix(featureId))) {
      const candidate = candidateFromEntry(entry);
      if (candidate === null || candidate.advertisement.featureId !== featureId) {
        continue;
      }
      if (selector !== undefined && !selector(candidate.advertisement)) {
        continue;
      }
      candidates.push(candidate);
    }
    return candidates.sort(candidateNewestSort);
  }

  async validate(
    candidate: Candidate,
    options: { currentSessions?: Record<string, string> } = {},
  ): Promise<CandidateStatus> {
    let entry: StateEntry | null;
    try {
      entry = await this.state.get(candidate.key);
    } catch (error) {
      if (error instanceof StateUnavailable) {
        return CandidateStatus.UNAVAILABLE;
      }
      throw error;
    }
    if (entry === null) {
      return CandidateStatus.MISSING;
    }
    let advertisement: AdvertisementRecord;
    try {
      advertisement = validateAdvertisementRecord(entry.value);
    } catch {
      return CandidateStatus.SCHEMA_INVALID;
    }
    if (advertisement.featureId !== candidate.advertisement.featureId) {
      return CandidateStatus.FEATURE_MISMATCH;
    }
    const currentSession = options.currentSessions?.[advertisement.advertiser];
    if (currentSession !== undefined && currentSession !== advertisement.sessionId) {
      return CandidateStatus.SESSION_MISMATCH;
    }
    return CandidateStatus.CANDIDATE;
  }

  watch(featureId: string): AsyncIterable<StateWatchItem> {
    if (this.state.watch === undefined) {
      throw new StateUnavailable("State store does not support watch");
    }
    return this.state.watch(beaconFeaturePrefix(featureId));
  }
}

export interface BeaconAdvertisementSpec {
  featureId: string;
  endpoint: string;
  sessionId: string;
  advertiser?: string;
  advertisementId?: string;
  protocol?: BeaconProtocol;
  operations?: string[];
  labels?: Record<string, string>;
  hints?: JsonObject;
  payload?: JsonObject;
  ttlSeconds?: number;
  refreshIntervalSeconds?: number;
}

export interface BeaconAdvertisementState {
  readonly status: "idle" | "publishing" | "published" | "unavailable" | "closed";
  readonly handle?: AdvertisementHandle;
  readonly failure?: RuntimeFailure;
}

export class BeaconAdvertisement {
  readonly spec: BeaconAdvertisementSpec;

  private readonly service: BeaconService;
  private handleValue: AdvertisementHandle | null = null;
  private closedValue = false;
  private worker?: RenewalWorker;
  private publishing?: Promise<AdvertisementHandle>;
  private closing?: Promise<void>;
  private failureValue?: unknown;
  private readonly states = new StateCell<BeaconAdvertisementState>({ status: "idle" });

  constructor(service: BeaconService, spec: BeaconAdvertisementSpec) {
    this.service = service;
    this.spec = {
      ...spec,
      advertisementId: spec.advertisementId ?? randomUUID(),
      endpoint: endpointAddress(spec.endpoint),
      advertiser: spec.advertiser === undefined ? undefined : endpointAddress(spec.advertiser),
      sessionId: requireText(spec.sessionId, "Beacon session id"),
      featureId: requireText(spec.featureId, "Beacon feature id"),
      refreshIntervalSeconds: spec.refreshIntervalSeconds ?? 5,
    };
    if (this.spec.refreshIntervalSeconds! <= 0) {
      throw new ValidationError("refreshIntervalSeconds must be greater than zero");
    }
  }

  get closed(): boolean {
    return this.closedValue;
  }

  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: BeaconAdvertisementState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.service.scheduler });
  }
  get failure(): unknown { return this.failureValue; }
  set failure(error: unknown) {
    this.failureValue = error;
    if (!this.closedValue) this.states.set({ status: error === undefined ? this.handleValue ? "published" : "idle" : "unavailable",
      ...(this.handleValue ? { handle: this.handleValue } : {}), ...(error === undefined ? {} : { failure: { code: "beacon_publication_failed", message: String(error) } }) });
  }

  get handle(): AdvertisementHandle | null {
    return this.handleValue;
  }

  publish(options: { payload?: JsonObject; labels?: Record<string, string>; hints?: JsonObject } = {}): Promise<AdvertisementHandle> {
    if (this.closedValue) return Promise.reject(new StateConflict("Beacon advertisement is closed"));
    const previous = this.publishing;
    const operation = (async () => {
      await previous?.catch(() => {});
      if (this.closedValue) throw new StateConflict("Beacon advertisement is closed");
      if (!this.handleValue) this.states.set({ status: "publishing" });
      Object.assign(this.spec, options);
      try {
        this.handleValue = this.handleValue
          ? await this.service.refresh(this.handleValue, options, handle => this.remember(handle))
          : await this.service.advertise(this.spec, options, handle => this.remember(handle));
      } catch (error) {
        if (!(error instanceof StateConflict)) throw error;
        this.handleValue = await this.service.advertise(this.spec, options, handle => this.remember(handle));
      }
      if (this.closedValue) {
        await this.service.withdraw(this.handleValue);
        throw new StateConflict("Beacon advertisement is closed");
      }
      this.failure = undefined;
      if (!this.worker) {
        this.worker = new RenewalWorker(() => this.service.ttlSeconds,
          async () => { await this.publish(); }, error => { this.failure = error; this.worker = undefined; }, this.service.scheduler);
        this.worker.start();
      }
      return this.handleValue;
    })().catch(error => { if (!this.closedValue) this.failure = error; throw error; });
    this.publishing = operation;
    void operation.finally(() => { if (this.publishing === operation) this.publishing = undefined; }).catch(() => {});
    return operation;
  }

  private remember(handle: AdvertisementHandle): void {
    this.handleValue = handle;
    // The exact write can complete after the owner's bounded close has returned.
    if (this.closedValue) void bounded(this.service.withdraw(handle), 5000, this.service.scheduler).catch(error => { this.failure = error; });
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closedValue = true;
    this.states.set({ status: "closed", ...(this.handleValue ? { handle: this.handleValue } : {}) }); this.states.close();
    this.worker?.stop();
    this.closing = bounded((async () => {
      await this.publishing?.catch(() => {});
      await this.worker?.close();
      const handle = this.handleValue;
      if (handle) await this.service.withdraw(handle);
    })(), 5000, this.service.scheduler).finally(() => { this.service.forgetAdvertisement(this); });
    return this.closing;
  }
}

export class BeaconService {
  private readonly discovery: BeaconDiscovery;
  private readonly advertisements = new Map<string, BeaconAdvertisement>();
  private readonly featureViews = new Map<string, BeaconFeatureView>();
  private started = false;
  private closed = false;
  private unsubscribe?: () => void;
  private readonly ownedSource: MaterializedStateSource;
  private readonly states = new StateCell<RuntimeState>({ status: "idle", resources: { advertisements: 0, featureViews: 0 } });
  private closing?: Promise<void>;
  get scheduler(): RuntimeScheduler { return this.discovery.scheduler; }
  get ttlSeconds(): number { return this.discovery.defaultTtlSeconds; }
  get source(): MaterializedStateSource { return this.ownedSource; }
  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: RuntimeState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.scheduler });
  }
  private updateState(): void {
    if (this.closed) return;
    const snapshot = this.source.snapshot;
    this.states.set({ status: !this.started ? "idle" : snapshot.current ? "ready" : "unavailable",
      resources: { advertisements: this.advertisements.size, featureViews: this.featureViews.size },
      ...(this.started && !snapshot.current ? { failure: { code: "beacon_source_unavailable", message: snapshot.reason ?? "Beacon source is not current" } } : {}) });
  }
  async start(): Promise<void> {
    if (this.closed) throw new StateUnavailable("Beacon runtime is closed");
    if (!this.started) {
      this.started = true;
      this.states.set({ ...this.state, status: "starting" });
      this.unsubscribe = this.source.subscribe(snapshot => {
        this.updateState();
        if (snapshot.current) for (const advertisement of this.advertisements.values()) {
          if (advertisement.failure && !advertisement.closed) { advertisement.failure = undefined; void advertisement.publish().catch(error => { advertisement.failure = error; }); }
        }
      });
    }
    try { await this.source.start(); this.source.requireCurrent(); this.updateState(); }
    catch (error) { this.updateState(); throw error; }
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    for (const view of this.featureViews.values()) view.close();
    const closing = [...this.advertisements.values()].map(ad => ad.close());
    this.unsubscribe?.();
    this.states.set({ status: "closed", resources: { advertisements: 0, featureViews: 0 } }); this.states.close();
    this.closing = bounded(Promise.allSettled([...closing, this.source.release()]), 5000, this.scheduler).then(() => {}, () => {});
    return this.closing;
  }

  constructor(discovery: BeaconDiscovery) {
    this.discovery = discovery;
    this.ownedSource = discovery.source;
    this.ownedSource.retain();
  }

  /** A new advertisement owner, even when endpoint, session, and intent agree. */
  createAdvertisement(spec: BeaconAdvertisementSpec): BeaconAdvertisement {
    if (this.closed) throw new StateUnavailable("Beacon runtime is closed");
    const owned = { ...spec, advertisementId: spec.advertisementId ?? randomUUID() };
    const key = advertisementCacheKey(owned);
    if (this.advertisements.has(key)) throw new StateConflict("Advertisement identity already has a local owner");
    const advertisement = new BeaconAdvertisement(this, owned);
    this.advertisements.set(key, advertisement);
    this.updateState();
    return advertisement;
  }

  /** Reuse one intent's owner. Independent lifetimes use createAdvertisement. */
  async ensureAdvertisement(spec: BeaconAdvertisementSpec): Promise<BeaconAdvertisement> {
    if (this.closed) throw new StateUnavailable("Beacon runtime is closed");
    const key = advertisementCacheKey(spec);
    const existing = this.advertisements.get(key);
    if (existing !== undefined && !existing.closed) {
      return existing;
    }
    const advertisement = new BeaconAdvertisement(this, spec);
    this.advertisements.set(key, advertisement);
    this.updateState();
    return advertisement;
  }

  async find(featureId: string, selector?: AdvertisementFilter): Promise<Candidate[]> {
    await this.start();
    return this.discovery.find(featureId, selector);
  }

  async validate(
    candidate: Candidate,
    options: { currentSessions?: Record<string, string> } = {},
  ): Promise<CandidateStatus> {
    return this.discovery.validate(candidate, options);
  }

  async *watchFeature(featureId: string, options: { signal?: AbortSignal } = {}): AsyncIterable<BeaconFeatureWatchItem> {
    await this.start();
    if (this.closed) throw new StateUnavailable("Beacon runtime is closed");
    let view = this.featureViews.get(featureId);
    if (!view) {
      view = new BeaconFeatureView(featureId, this.source, () => { this.featureViews.delete(featureId); this.updateState(); });
      this.featureViews.set(featureId, view);
      this.updateState();
    }
    yield* view.watch(options);
  }

  async advertise(
    spec: BeaconAdvertisementSpec,
    overrides: { payload?: JsonObject; labels?: Record<string, string>; hints?: JsonObject } = {},
    onCommit?: (handle: AdvertisementHandle) => void,
  ): Promise<AdvertisementHandle> {
    await this.start();
    const handle = await this.discovery.advertise(spec.featureId, spec.endpoint, spec.sessionId, {
      advertiser: spec.advertiser,
      advertisementId: spec.advertisementId,
      protocol: spec.protocol,
      operations: spec.operations,
      labels: overrides.labels ?? spec.labels,
      hints: overrides.hints ?? spec.hints,
      payload: overrides.payload ?? spec.payload,
      ttlSeconds: spec.ttlSeconds,
    });
    onCommit?.(handle);
    await this.source.observed(handle.revision);
    return handle;
  }

  async refresh(
    handle: AdvertisementHandle,
    options: { hints?: JsonObject; labels?: Record<string, string>; payload?: JsonObject } = {},
    onCommit?: (handle: AdvertisementHandle) => void,
  ): Promise<AdvertisementHandle> {
    const refreshed = await this.discovery.refresh(handle, options);
    onCommit?.(refreshed);
    if (this.started) await this.source.observed(refreshed.revision);
    return refreshed;
  }

  async withdraw(handle: AdvertisementHandle): Promise<boolean> {
    return this.discovery.withdraw(handle);
  }

  forgetAdvertisement(advertisement: BeaconAdvertisement): void {
    for (const [key, current] of this.advertisements) {
      if (current === advertisement) {
        this.advertisements.delete(key);
        this.updateState();
      }
    }
  }
}

/** One cached selection and one source subscription per active feature. */
class BeaconFeatureView {
  private readonly featureId: string;
  private readonly onClose: () => void;
  private snapshot!: BeaconFeatureSnapshot;
  private known = new Map<string, Candidate>();
  private readonly queues = new Set<AsyncQueue<BeaconFeatureWatchItem>>();
  private readonly snapshotsPending = new Set<AsyncQueue<BeaconFeatureWatchItem>>();
  private readonly unsubscribe: () => void;
  private closed = false;
  constructor(featureId: string, source: MaterializedStateSource, onClose: () => void) {
    this.featureId = featureId; this.onClose = onClose;
    this.update(source.snapshot);
    this.unsubscribe = source.subscribe(snapshot => this.update(snapshot));
  }
  private update(source: SourceSnapshot): void {
    if (this.closed) return;
    const candidates = source.current ? source.entries.filter(entry => entry.key.startsWith(beaconFeaturePrefix(this.featureId)))
      .map(candidateFromEntry).filter((candidate): candidate is Candidate => candidate !== null).sort(candidateNewestSort) : [];
    const next = new Map(candidates.map(candidate => [candidate.key, candidate]));
    const changed = [...new Set([...this.known.keys(), ...next.keys()])].filter(key => this.known.get(key)?.revision !== next.get(key)?.revision);
    const reset = !this.snapshot?.current || !source.current || this.snapshot.generation !== source.generation || changed.length > 256;
    this.snapshot = immutable({ type: BeaconFeatureWatchItemType.SNAPSHOT, featureId: this.featureId, candidates,
      current: source.current, version: source.version, generation: source.generation });
    const changes = changed.map(key => {
      const candidate = next.get(key), previous = this.known.get(key);
      return immutable({ type: BeaconFeatureWatchItemType.CHANGE, featureId: this.featureId, key,
        eventType: !candidate ? BeaconFeatureEventType.WITHDRAWN : previous ? BeaconFeatureEventType.UPDATED : BeaconFeatureEventType.ADVERTISED,
        ...(candidate ? { candidate } : {}), ...(previous ? { previous } : {}) });
    });
    for (const queue of this.queues) {
      if (!queue.pendingCount) this.snapshotsPending.delete(queue);
      if (reset || this.snapshotsPending.has(queue) || queue.pendingCount + changes.length > 256) {
        queue.replace(this.snapshot); this.snapshotsPending.add(queue);
      } else for (const item of changes) queue.push(item);
    }
    this.known = next;
  }
  watch(options: { signal?: AbortSignal }): AsyncIterable<BeaconFeatureWatchItem> {
    if (this.closed) throw new StateUnavailable("Beacon feature view is closed");
    if (this.queues.size >= 256) throw new StateUnavailable("Beacon feature subscriber limit reached");
    const close = () => queue.close();
    const queue = new AsyncQueue<BeaconFeatureWatchItem>(256, false, () => {
      this.queues.delete(queue); this.snapshotsPending.delete(queue); options.signal?.removeEventListener("abort", close);
      if (!this.queues.size) this.close();
    });
    this.queues.add(queue); queue.push(this.snapshot); this.snapshotsPending.add(queue);
    options.signal?.addEventListener("abort", close, { once: true });
    if (options.signal?.aborted) close();
    return queue;
  }
  close(): void {
    if (this.closed) return;
    this.closed = true; this.unsubscribe(); this.onClose();
    for (const queue of [...this.queues]) queue.close();
  }
}

export function candidateFromEntry(entry: StateEntry): Candidate | null {
  let advertisement: AdvertisementRecord;
  try {
    advertisement = validateAdvertisementRecord(entry.value);
  } catch {
    return null;
  }
  const parsed = parseBeaconAdvertisementKey(entry.key);
  if (parsed === null) {
    return null;
  }
  const [featureId, advertisementId] = parsed;
  if (
    featureId !== advertisement.featureId ||
    advertisementId !== advertisement.advertisementId
  ) {
    return null;
  }
  return {
    key: entry.key,
    advertisement,
    revision: entry.revision,
    observedAt: utcIsoNow(),
  };
}

async function* beaconFeatureWatchItems(
  featureId: string,
  changes: AsyncIterable<StateWatchItem>,
  snapshot: () => Promise<Candidate[]>,
): AsyncIterable<BeaconFeatureWatchItem> {
  const known = new Map<string, Candidate>();
  for await (const change of changes) {
    if (change.operation === "resnapshot") {
      const candidates = await snapshot();
      known.clear();
      for (const candidate of candidates) {
        known.set(candidate.key, candidate);
      }
      yield {
        type: BeaconFeatureWatchItemType.SNAPSHOT,
        featureId,
        candidates,
      };
      continue;
    }
    const item = beaconFeatureChange(featureId, change, known);
    if (item !== null) {
      yield item;
    }
  }
}

function beaconFeatureChange(
  featureId: string,
  change: StateChange,
  known: Map<string, Candidate>,
): BeaconFeatureChange | null {
  if (change.operation === "put" && change.entry !== undefined) {
    const previous = known.get(change.key);
    const candidate = candidateFromEntry(change.entry);
    if (candidate === null || candidate.advertisement.featureId !== featureId) {
      known.delete(change.key);
      return {
        type: BeaconFeatureWatchItemType.CHANGE,
        eventType: BeaconFeatureEventType.INVALID,
        featureId,
        key: change.key,
        ...(previous === undefined ? {} : { previous }),
        reason: "invalid_advertisement",
        change,
      };
    }
    known.set(change.key, candidate);
    return {
      type: BeaconFeatureWatchItemType.CHANGE,
      eventType:
        previous === undefined
          ? BeaconFeatureEventType.ADVERTISED
          : BeaconFeatureEventType.UPDATED,
      featureId,
      key: change.key,
      candidate,
      ...(previous === undefined ? {} : { previous }),
      change,
    };
  }
  if (change.operation === "delete" || change.operation === "expire") {
    const previous = known.get(change.key);
    known.delete(change.key);
    return {
      type: BeaconFeatureWatchItemType.CHANGE,
      eventType:
        change.operation === "expire"
          ? BeaconFeatureEventType.EXPIRED
          : BeaconFeatureEventType.WITHDRAWN,
      featureId,
      key: change.key,
      ...(previous === undefined ? {} : { previous }),
      change,
    };
  }
  return null;
}

function advertisementHandle(
  key: string,
  record: AdvertisementRecord,
  revision: number,
): AdvertisementHandle {
  return immutable({
    key,
    advertisementId: record.advertisementId,
    featureId: record.featureId,
    advertiser: record.advertiser,
    endpoint: record.endpoint,
    sessionId: record.sessionId,
    revision,
    refreshSeq: record.refreshSeq,
  });
}

function advertisementMatchesHandle(record: AdvertisementRecord, handle: AdvertisementHandle): boolean {
  return (
    record.advertisementId === handle.advertisementId &&
    record.featureId === handle.featureId &&
    record.advertiser === handle.advertiser &&
    record.endpoint === handle.endpoint &&
    record.sessionId === handle.sessionId
  );
}

function candidateNewestSort(left: Candidate, right: Candidate): number {
  return (
    right.revision - left.revision ||
    parseUtcMillis(right.advertisement.updatedAt) - parseUtcMillis(left.advertisement.updatedAt) ||
    right.advertisement.refreshSeq - left.advertisement.refreshSeq ||
    left.advertisement.advertisementId.localeCompare(right.advertisement.advertisementId)
  );
}

function advertisementCacheKey(spec: BeaconAdvertisementSpec): string {
  return JSON.stringify({
    featureId: spec.featureId,
    endpoint: endpointAddress(spec.endpoint),
    advertiser: spec.advertiser === undefined ? endpointAddress(spec.endpoint) : endpointAddress(spec.advertiser),
    sessionId: spec.sessionId,
    advertisementId: spec.advertisementId ?? null,
    operations: spec.operations ?? [],
  });
}

function validateTextList(value: unknown, fieldName: string): string[] {
  if (!Array.isArray(value)) {
    throw new ValidationError(`${fieldName} list must be an array`);
  }
  return value.map((item) => requireText(item, fieldName));
}

function validateLabels(value: unknown): Record<string, string> {
  const raw = requireJsonObject(value, "Beacon labels");
  const out: Record<string, string> = {};
  for (const [key, item] of Object.entries(raw)) {
    out[requireText(key, "Beacon label key")] = requireText(item, "Beacon label value");
  }
  return out;
}

export function advertisementToJson(record: AdvertisementRecord): JsonObject {
  return cloneJson(record as unknown as JsonObject);
}
