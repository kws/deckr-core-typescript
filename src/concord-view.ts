import {
  concordContractKey, concordParticipantTokenKey, evaluateContract, parseConcordContractKey,
  parseConcordParticipantTokenKey, validateContractRecord,
  type ContractHandle, type ContractPointer, type ContractRecord, type ContractState, type ContractValidity,
} from "./concord.ts";
import { StateUnavailable } from "./errors.ts";
import { canonicalJson } from "./json.ts";
import { AsyncQueue, immutable } from "./runtime.ts";
import type { MaterializedStateSource, SourceSnapshot } from "./source.ts";
import type { StateEntry } from "./state.ts";

export interface ConcordFilters {
  profile?: string; participant?: string; state?: ContractState; contractId?: string; pointer?: ContractPointer;
}
export interface ConcordMember { contract: ContractHandle; validity: ContractValidity; }
export interface ConcordViewSnapshot {
  version: number; current: boolean; contracts: readonly ConcordMember[];
  changed: readonly ContractPointer[]; resnapshotRequired: boolean;
}
interface IndexedContract {
  entry: StateEntry; handle: ContractHandle; record?: ContractRecord;
  tags: string[]; validity: Map<string, ContractValidity>;
}
interface Subscriber {
  filters: ConcordFilters; queue: AsyncQueue<ConcordViewSnapshot>; keys: Set<string>; pending?: ConcordViewSnapshot;
}

/** One index per pair of shared sources. Exact operations never populate it. */
export class ConcordView {
  private readonly contracts: MaterializedStateSource;
  private readonly tokens: MaterializedStateSource;
  private readonly records = new Map<string, IndexedContract>();
  private readonly indexes = new Map<string, Set<string>>();
  private readonly subscribers = new Set<Subscriber>();
  private readonly listeners = new Set<(changed: ReadonlySet<string> | undefined) => void>();
  private readonly unsubscribe: (() => void)[];
  private owners = 0;
  private version = 0;
  private current = false;
  private generations = "";
  private closed = false;
  private counters = { contractParses: 0, validityEvaluations: 0, candidateVisits: 0 };
  constructor(contracts: MaterializedStateSource, tokens: MaterializedStateSource) {
    this.contracts = contracts; this.tokens = tokens;
    this.unsubscribe = [contracts.subscribe(snapshot => this.update(true, snapshot)), tokens.subscribe(snapshot => this.update(false, snapshot))];
    this.update(true, contracts.snapshot);
  }
  retain(): void { this.owners++; }
  release(): void {
    if (--this.owners !== 0) return;
    this.closed = true;
    for (const unsubscribe of this.unsubscribe) unsubscribe();
    for (const sub of this.subscribers) sub.queue.close();
    this.listeners.clear();
  }
  get isClosed(): boolean { return this.closed; }
  get stats() { return { ...this.counters, indexedContracts: this.records.size, subscribers: this.subscribers.size }; }
  subscribe(listener: (changed: ReadonlySet<string> | undefined) => void): () => void {
    this.listeners.add(listener); return () => { this.listeners.delete(listener); };
  }
  private index(key: string, entry?: StateEntry): void {
    const old = this.records.get(key);
    if (old) for (const tag of old.tags) {
      const set = this.indexes.get(tag)!; set.delete(key); if (!set.size) this.indexes.delete(tag);
    }
    this.records.delete(key);
    const pointer = parseConcordContractKey(key);
    if (!entry || !pointer) return;
    let record: ContractRecord | undefined;
    this.counters.contractParses++;
    try {
      if (!entry.parseError) {
        const parsed = validateContractRecord(entry.value);
        if (concordContractKey(parsed) === key) record = parsed;
      }
    } catch { /* Keep invalid observations with their canonical key identity. */ }
    const participants = record?.participants ?? (Array.isArray(entry.value.participants) ? entry.value.participants.filter((value): value is string => typeof value === "string") : []);
    const profile = record?.profile ?? (typeof entry.value.profile === "string" ? entry.value.profile : undefined);
    const state = record?.state ?? (entry.value.state === "cancelled" ? "cancelled" : "open");
    const handle: ContractHandle = immutable({ key, contractId: pointer[0], generation: pointer[1], revision: entry.revision,
      participants, attachedParticipants: record?.attachedParticipants ?? [], state,
      ...(profile ? { profile } : {}), ...(record?.termsHash ? { termsHash: record.termsHash } : {}),
    });
    const tags = ["id:" + pointer[0], "state:" + state, ...participants.map(value => "participant:" + value), ...(profile ? ["profile:" + profile] : [])];
    this.records.set(key, { entry, handle, record, tags, validity: new Map() });
    for (const tag of tags) {
      let set = this.indexes.get(tag); if (!set) this.indexes.set(tag, set = new Set()); set.add(key);
    }
  }
  private update(contractSource: boolean, snapshot: SourceSnapshot): void {
    const changed = new Set<string>();
    if (contractSource) {
      if (snapshot.changedKeys) {
        for (const key of snapshot.changedKeys) {
          if (!parseConcordContractKey(key)) continue;
          this.index(key, snapshot.current ? this.contracts.get(key) ?? undefined : undefined); changed.add(key);
        }
      } else if (snapshot.current) {
        for (const key of this.records.keys()) changed.add(key);
        this.records.clear(); this.indexes.clear();
        for (const entry of snapshot.entries) { this.index(entry.key, entry); if (parseConcordContractKey(entry.key)) changed.add(entry.key); }
      }
    } else {
      for (const key of snapshot.changedKeys ?? snapshot.entries.map(entry => entry.key)) {
        const pointer = parseConcordParticipantTokenKey(key);
        if (pointer) changed.add(concordContractKey({ contractId: pointer[0], generation: pointer[1] }));
      }
    }
    const current = this.contracts.snapshot.current && this.tokens.snapshot.current;
    const generations = `${this.contracts.snapshot.generation}:${this.tokens.snapshot.generation}`;
    const reset = current !== this.current || generations !== this.generations || snapshot.changedKeys === undefined;
    this.current = current; this.generations = generations;
    if (!changed.size && !reset) return;
    this.version++;
    for (const key of reset ? this.records.keys() : changed) this.records.get(key)?.validity.clear();
    // Authority owners observe discontinuity synchronously, before public watches coalesce.
    for (const listener of this.listeners) listener(reset ? undefined : changed);
    for (const sub of this.subscribers) {
      if (!reset && ![...changed].some(key => sub.keys.has(key) || (this.records.has(key) && this.matches(this.records.get(key)!, sub.filters)))) continue;
      const keys = this.select(sub.filters);
      const relevant = [...changed].filter(key => sub.keys.has(key) || keys.has(key));
      const pending = sub.queue.pendingCount ? sub.pending : undefined;
      const pointers = new Map((pending?.changed ?? []).map(pointer => [concordContractKey(pointer), pointer]));
      for (const key of relevant) {
        const pointer = parseConcordContractKey(key)!;
        pointers.set(key, { contractId: pointer[0], generation: pointer[1] });
      }
      const resnapshotRequired = reset || pending?.resnapshotRequired === true || pointers.size > 256;
      const state = this.snapshot(keys, resnapshotRequired ? [] : [...pointers.values()], resnapshotRequired);
      sub.keys = keys; sub.pending = state; sub.queue.replace(state);
    }
  }
  private matches(item: IndexedContract, filters: ConcordFilters): boolean {
    return (!filters.profile || item.handle.profile === filters.profile) && (!filters.participant || item.handle.participants.includes(filters.participant)) &&
      (!filters.state || item.handle.state === filters.state) && (!filters.contractId || item.handle.contractId === filters.contractId) &&
      (!filters.pointer || item.handle.key === concordContractKey(filters.pointer));
  }
  private select(filters: ConcordFilters): Set<string> {
    const sets: ReadonlySet<string>[] = [];
    for (const [name, value] of [["profile", filters.profile], ["participant", filters.participant], ["state", filters.state], ["id", filters.contractId]]) {
      if (value !== undefined) sets.push(this.indexes.get(`${name}:${value}`) ?? new Set());
    }
    if (filters.pointer) sets.push(new Set([concordContractKey(filters.pointer)]));
    const candidates = sets.length ? sets.reduce((a, b) => a.size <= b.size ? a : b) : this.records.keys();
    const result = new Set<string>();
    for (const key of candidates) {
      this.counters.candidateVisits++;
      const item = this.records.get(key); if (item && this.matches(item, filters)) result.add(key);
    }
    return result;
  }
  handles(filters: ConcordFilters): ContractHandle[] {
    if (!this.current) throw new StateUnavailable("Concord view is not current");
    return [...this.select(filters)].sort().flatMap(key => { const item = this.records.get(key)!; return item.record ? [item.handle] : []; });
  }
  bootstrapCandidates(filters: ConcordFilters): ContractHandle[] {
    if (!this.current) throw new StateUnavailable("Concord view is not current");
    // Retain malformed matching ownership evidence: omitting it is not proof
    // that an endpoint is free to publish a new proposal.
    return [...this.select(filters)].sort().map(key => this.records.get(key)!.handle);
  }
  record(contract: ContractHandle): ContractRecord | null {
    if (!this.current) throw new StateUnavailable("Concord view is not current");
    const record = this.records.get(contract.key)?.record;
    return record?.contractId === contract.contractId && record.generation === contract.generation ? record : null;
  }
  validate(contract: ContractHandle, sessions: Record<string, string> = {}): ContractValidity {
    if (!this.current) return evaluateContract({ pointer: contract, contract: null, tokens: {}, current: false });
    const item = this.records.get(contract.key);
    if (!item) return evaluateContract({ pointer: contract, contract: null, tokens: {} });
    const cacheKey = canonicalJson({ contractId: contract.contractId, generation: contract.generation, sessions });
    let value = item.validity.get(cacheKey);
    if (!value) {
      const tokens: Record<string, StateEntry | null> = {};
      for (const participant of item.handle.participants) {
        const key = concordParticipantTokenKey({ ...item.handle, participant }); tokens[key] = this.tokens.get(key);
      }
      this.counters.validityEvaluations++;
      value = evaluateContract({ pointer: contract, contract: item.entry, tokens, currentSessions: sessions });
      if (item.validity.size >= 256) item.validity.delete(item.validity.keys().next().value!);
      item.validity.set(cacheKey, value);
    }
    return value;
  }
  private snapshot(keys: Set<string>, changed: ContractPointer[], resnapshotRequired: boolean): ConcordViewSnapshot {
    return immutable({ version: this.version, current: this.current, changed, resnapshotRequired,
      contracts: [...keys].sort().map(key => ({ contract: this.records.get(key)!.handle, validity: this.validate(this.records.get(key)!.handle) })),
    });
  }
  watch(filters: ConcordFilters, options: { signal?: AbortSignal } = {}): AsyncIterable<ConcordViewSnapshot> {
    if (this.closed) throw new StateUnavailable("Concord view is closed");
    if (this.subscribers.size >= 256) throw new StateUnavailable("Concord view subscriber limit reached");
    const close = () => queue.close();
    const queue = new AsyncQueue<ConcordViewSnapshot>(1, true, () => { this.subscribers.delete(sub); options.signal?.removeEventListener("abort", close); });
    const keys = this.select(filters);
    const sub: Subscriber = { filters: { ...filters }, queue, keys };
    this.subscribers.add(sub);
    const initial = this.snapshot(keys, [], false); sub.pending = initial; queue.push(initial);
    options.signal?.addEventListener("abort", close, { once: true }); if (options.signal?.aborted) close();
    return queue;
  }
}

const shared = new WeakMap<MaterializedStateSource, WeakMap<MaterializedStateSource, ConcordView>>();
export function concordViewFor(contracts: MaterializedStateSource, tokens: MaterializedStateSource): ConcordView {
  let byTokens = shared.get(contracts); if (!byTokens) shared.set(contracts, byTokens = new WeakMap());
  let view = byTokens.get(tokens); if (!view || view.isClosed) byTokens.set(tokens, view = new ConcordView(contracts, tokens));
  view.retain(); return view;
}
