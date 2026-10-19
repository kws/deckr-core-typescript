import { ConcordCoordinator, ContractState, ContractValidityStatus,
  CONCORD_REAPER_STALE_CONTRACT_REASON, CONCORD_STALE_OBSERVATION_SCHEMA_ID,
  DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS, DEFAULT_CONCORD_REAPER_STALE_GRACE_SECONDS,
  concordContractKey, concordContractPrefix, concordContractsPrefix, concordParticipantTokenKey, concordStaleObservationKey,
  parseConcordContractKey, validateContractRecord, validateParticipantTokenRecord, validateStaleObservationRecord,
  type ConcordService, type ConcordReaperScanResult, type ContractRecord, type ContractHandle, type ParticipantTokenRecord,
} from "./concord.ts";
import { StateConflict, StateUnavailable, ValidationError } from "./errors.ts";
import { cloneJson, requirePositiveInteger, requireText, utcIsoNow, type JsonObject } from "./json.ts";
import { encodeKeyToken } from "./keys.ts";
import { bounded, immutable, StateCell, systemScheduler, type RuntimeScheduler, type RuntimeState } from "./runtime.ts";
import type { StateEntry, StateStore } from "./state.ts";
import { closedObject, timestamp } from "./validation.ts";

export const CONCORD_TOKEN_CLEANUP_SCHEMA_ID = "dev.deckr.concord.token-cleanup.v1";
export interface ConcordTokenCleanupRecord {
  schema: typeof CONCORD_TOKEN_CLEANUP_SCHEMA_ID;
  contractId: string; generation: number; contractRevision: number; cleanupStartedAt: string; termsHash?: string;
}
export function concordTokenCleanupKey(pointer: { contractId: string; generation: number }): string {
  return `cleanup.${encodeKeyToken(requireText(pointer.contractId, "contractId"))}.${requirePositiveInteger(pointer.generation, "generation")}`;
}
export function validateTokenCleanupRecord(value: unknown): ConcordTokenCleanupRecord {
  const raw = closedObject(value, ["schema", "contractId", "generation", "contractRevision", "cleanupStartedAt", "termsHash"], "Concord token cleanup");
  if (raw.schema !== undefined && raw.schema !== CONCORD_TOKEN_CLEANUP_SCHEMA_ID) throw new ValidationError("Invalid Concord cleanup schema");
  if (!Number.isSafeInteger(raw.contractRevision) || Number(raw.contractRevision) < 0) throw new ValidationError("Invalid cleanup contract revision");
  return { schema: CONCORD_TOKEN_CLEANUP_SCHEMA_ID, contractId: requireText(raw.contractId, "contractId"), generation: requirePositiveInteger(raw.generation, "generation"),
    contractRevision: Number(raw.contractRevision), cleanupStartedAt: timestamp(raw.cleanupStartedAt, "cleanupStartedAt"),
    ...(raw.termsHash == null ? {} : { termsHash: requireText(raw.termsHash, "termsHash") }) };
}

export interface ConcordMaintenanceAudit {
  readonly event: "delete_contract" | "delete_conflict" | "cleanup_pending" | "cleanup_complete" | "scan_failure";
  readonly contractId?: string;
  readonly generation?: number;
  /** Metadata only: full terms are deliberately excluded. */
  readonly contract?: JsonObject;
  readonly validationStatus?: ContractValidityStatus;
  readonly listedTokenKeyCount?: number;
  readonly deletedTokenKeyCount?: number;
  readonly tokens?: readonly { key: string; revision: number; participant: string; sessionId: string; tokenId: string; refreshSeq: number; termsHash?: string }[];
  readonly reason?: string;
}
export interface ConcordReaperOptions {
  contractState: StateStore; tokenState: StateStore; maintenanceState: StateStore;
  staleGraceSeconds?: number; cancelledRetentionSeconds?: number;
  clock?: () => string; scheduler?: RuntimeScheduler;
  /** Must complete before contract deletion. Default: structured console logging. */
  audit?: (event: ConcordMaintenanceAudit) => void | Promise<void>;
}

/** Optional, host-scheduled exact-store capability. Construction opens no watches. */
export class ConcordReaperService {
  private readonly options: ConcordReaperOptions;
  private readonly coordinator: ConcordCoordinator;
  private readonly states = new StateCell<RuntimeState>({ status: "idle", resources: { cleanupPending: 0 } });
  private readonly scheduler: RuntimeScheduler;
  private readonly staleGrace: number;
  private readonly retention: number;
  private scanning?: Promise<ConcordReaperScanResult>;
  private closing?: Promise<void>;
  private closed = false;
  constructor(options: ConcordReaperOptions);
  /** @deprecated Pass only the dedicated stores. The participation runtime is unused. */
  constructor(concord: ConcordService, options: ConcordReaperOptions);
  constructor(first: ConcordReaperOptions | ConcordService, supplied?: ConcordReaperOptions) {
    this.options = supplied ?? first as ConcordReaperOptions;
    this.scheduler = this.options.scheduler ?? systemScheduler;
    this.staleGrace = this.options.staleGraceSeconds ?? DEFAULT_CONCORD_REAPER_STALE_GRACE_SECONDS;
    this.retention = this.options.cancelledRetentionSeconds ?? DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS;
    if (![this.staleGrace, this.retention].every(value => Number.isFinite(value) && value >= 0)) throw new ValidationError("Maintenance intervals must be finite and non-negative");
    this.coordinator = new ConcordCoordinator(this.options.contractState, this.options.tokenState);
  }
  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: RuntimeState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.scheduler });
  }
  private assertOpen(): void { if (this.closed) throw new StateUnavailable("Concord maintenance is closed"); }
  private async audit(event: ConcordMaintenanceAudit): Promise<void> {
    await (this.options.audit ?? (value => { console.info("Concord maintenance", value); }))(immutable(event));
  }
  scanOnce(): Promise<ConcordReaperScanResult> {
    if (this.closed) return Promise.reject(new StateUnavailable("Concord maintenance is closed"));
    if (this.scanning) return this.scanning;
    this.states.set({ ...this.state, status: "starting" });
    this.scanning = this.scan().then(result => {
      this.states.set({ status: "ready", resources: { cleanupPending: result.cleanupPendingCount ?? 0 } }); return result;
    }, error => {
      this.states.set({ ...this.state, status: "unavailable", failure: { code: "concord_maintenance_failed", message: String(error) } }); throw error;
    }).finally(() => { this.scanning = undefined; });
    return this.scanning;
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true; this.states.set({ ...this.state, status: "closed" }); this.states.close();
    this.closing = bounded<unknown>(this.scanning ?? Promise.resolve(), 5000, this.scheduler).then(() => {}, () => {});
    return this.closing;
  }
  private async scan(): Promise<ConcordReaperScanResult> {
    const counts: ConcordReaperScanResult = { scannedContractCount: 0, staleObservationCount: 0, staleObservationsCreated: 0, staleObservationsCleared: 0,
      contractsCancelled: 0, contractsDeleted: 0, tokenKeysDeleted: 0, cleanupPendingCount: 0 };
    const now = timestamp((this.options.clock ?? utcIsoNow)(), "maintenance time");
    for (const entry of await this.options.contractState.items(concordContractsPrefix())) {
      this.assertOpen();
      let parsed: [string, number] | null;
      try {
        parsed = parseConcordContractKey(entry.key);
        if (parsed && (!Number.isSafeInteger(parsed[1]) || parsed[1] < 1 || concordContractKey({ contractId: parsed[0], generation: parsed[1] }) !== entry.key)) throw new ValidationError("Noncanonical Concord contract key");
      } catch (error) {
        await this.audit({ event: "scan_failure", validationStatus: ContractValidityStatus.INVALID_CONTRACT, reason: String(error) });
        continue;
      }
      if (!parsed) continue;
      counts.scannedContractCount++;
      const [contractId, generation] = parsed, pointer = { contractId, generation };
      let record: ContractRecord;
      try {
        if (entry.parseError) throw new ValidationError("Malformed contract");
        record = validateContractRecord(entry.value);
        if (record.contractId !== contractId || record.generation !== generation) throw new ValidationError("Contract identity mismatch");
      } catch (error) {
        // Concord §12.2 forbids mutation of malformed or mismatched records.
        // Keep the stale evidence for later scans and surface the conflict;
        // cancellation must never invent a valid identity or repair terms.
        const observed = await this.observeStale(pointer, ContractValidityStatus.INVALID_CONTRACT, entry.revision, now);
        counts.staleObservationsCreated += Number(observed.created);
        await this.audit({ event: "scan_failure", ...pointer, validationStatus: ContractValidityStatus.INVALID_CONTRACT, reason: String(error) });
        continue;
      }
      const handle = { ...record, key: entry.key, revision: entry.revision } as ContractHandle;
      if (record.state === ContractState.CANCELLED) {
        counts.staleObservationsCleared += await this.clearStale(pointer);
        if (record.cancelledAt && (Date.parse(now) - Date.parse(record.cancelledAt)) / 1000 >= this.retention) {
          if (await this.deleteRetained(handle, now)) counts.contractsDeleted++;
        }
        continue;
      }
      const validity = await this.coordinator.validate(handle);
      const stale = validity.status === ContractValidityStatus.NOT_YET_FULFILLED ? Object.keys(validity.tokens).length === 0
        : [ContractValidityStatus.MISSING_TOKEN, ContractValidityStatus.INVALID_TOKEN, ContractValidityStatus.SESSION_MISMATCH,
          ContractValidityStatus.TERMS_HASH_MISMATCH, ContractValidityStatus.GENERATION_MISMATCH, ContractValidityStatus.INVALID_CONTRACT].includes(validity.status as "missing_token");
      if (stale) {
        const observed = await this.observeStale(pointer, validity.status, entry.revision, now);
        counts.staleObservationsCreated += Number(observed.created);
        if ((Date.parse(now) - Date.parse(observed.firstObservedStaleAt)) / 1000 >= this.staleGrace) {
          // Recheck exact evidence before cancellation; pending participation may have advanced.
          const latest = await this.coordinator.validate(handle);
          if (latest.status === validity.status && (latest.status !== ContractValidityStatus.NOT_YET_FULFILLED || Object.keys(latest.tokens).length === 0)) {
            this.assertOpen();
            try {
              if (await this.coordinator.maintenanceCancel(handle, { now, reason: CONCORD_REAPER_STALE_CONTRACT_REASON })) counts.contractsCancelled++;
              counts.staleObservationsCleared += await this.clearStale(pointer);
            } catch (error) {
              if (!(error instanceof StateConflict)) throw error;
              await this.audit({ event: "scan_failure", ...pointer, reason: String(error) });
            }
          }
        }
      } else if (validity.status !== ContractValidityStatus.UNAVAILABLE) counts.staleObservationsCleared += await this.clearStale(pointer);
    }
    // Markers are independent of contract enumeration, including after a restart.
    for (const entry of await this.options.maintenanceState.items("cleanup.")) {
      this.assertOpen();
      const result = await this.cleanup(entry);
      counts.tokenKeysDeleted += result.deleted;
      counts.cleanupPendingCount! += Number(!result.complete);
    }
    for (const entry of await this.options.maintenanceState.items("stale.")) {
      this.assertOpen();
      try {
        const record = validateStaleObservationRecord(entry.value);
        if (entry.key !== concordStaleObservationKey(record)) continue;
        if (!(await this.options.contractState.get(concordContractKey(record)))) counts.staleObservationsCleared += Number(await this.guardedDelete(this.options.maintenanceState, entry));
      } catch (error) { if (!(error instanceof ValidationError)) throw error; }
    }
    counts.staleObservationCount = (await this.options.maintenanceState.items("stale.")).length;
    return counts;
  }
  private async observeStale(pointer: { contractId: string; generation: number }, status: ContractValidityStatus, contractRevision: number, now: string) {
    const store = this.options.maintenanceState, key = concordStaleObservationKey(pointer), current = await store.get(key);
    if (current) {
      const record = validateStaleObservationRecord(current.value);
      if (record.contractId !== pointer.contractId || record.generation !== pointer.generation) throw new ValidationError("Stale observation identity mismatch");
      return { created: false, firstObservedStaleAt: record.firstObservedStaleAt };
    }
    this.assertOpen();
    try { await store.create(key, { schema: CONCORD_STALE_OBSERVATION_SCHEMA_ID, ...pointer, status, contractRevision, firstObservedStaleAt: now }); }
    catch (error) { if (!(error instanceof StateConflict)) throw error; return { created: false, firstObservedStaleAt: now }; }
    return { created: true, firstObservedStaleAt: now };
  }
  private async clearStale(pointer: { contractId: string; generation: number }): Promise<number> {
    const entry = await this.options.maintenanceState.get(concordStaleObservationKey(pointer));
    return entry ? Number(await this.guardedDelete(this.options.maintenanceState, entry)) : 0;
  }
  private async guardedDelete(store: StateStore, entry: StateEntry): Promise<boolean> {
    this.assertOpen();
    try { await store.delete(entry.key, { revision: entry.revision }); return true; }
    catch (error) { if (error instanceof StateConflict) return false; throw error; }
  }
  private marker(entry: StateEntry): ConcordTokenCleanupRecord {
    if (entry.parseError) throw new ValidationError("Malformed cleanup marker");
    const marker = validateTokenCleanupRecord(entry.value);
    if (entry.key !== concordTokenCleanupKey(marker)) throw new ValidationError("Cleanup marker key mismatch");
    return marker;
  }
  private tokens(marker: ConcordTokenCleanupRecord, entries: StateEntry[], participants?: readonly string[]): ParticipantTokenRecord[] {
    const prefix = concordContractPrefix(marker);
    return entries.map(entry => {
      if (!entry.key.startsWith(prefix) || entry.parseError) throw new ValidationError("Cleanup token is outside its exact prefix or malformed");
      const token = validateParticipantTokenRecord(entry.value);
      if (entry.key !== concordParticipantTokenKey(token) || token.contractId !== marker.contractId || token.generation !== marker.generation ||
        (token.termsHash ?? null) !== (marker.termsHash ?? null) || (participants && !participants.includes(token.participant))) throw new ValidationError("Cleanup token identity mismatch");
      return token;
    });
  }
  private async deleteRetained(handle: ContractHandle, now: string): Promise<boolean> {
    const store = this.options.contractState, entry = await store.get(handle.key);
    if (!entry) return false;
    const record = validateContractRecord(entry.value);
    if (entry.key !== concordContractKey(record) || record.contractId !== handle.contractId || record.generation !== handle.generation || record.state !== ContractState.CANCELLED ||
      !record.cancelledAt || (Date.parse(now) - Date.parse(record.cancelledAt)) / 1000 < this.retention) return false;
    const marker: ConcordTokenCleanupRecord = { schema: CONCORD_TOKEN_CLEANUP_SCHEMA_ID, contractId: record.contractId, generation: record.generation,
      contractRevision: entry.revision, cleanupStartedAt: now, ...(record.termsHash === undefined ? {} : { termsHash: record.termsHash }) };
    try {
      const entries = await this.options.tokenState.items(concordContractPrefix(record)), tokens = this.tokens(marker, entries, record.participants);
      const key = concordTokenCleanupKey(marker), existing = await this.options.maintenanceState.get(key);
      if (existing) {
        const prior = this.marker(existing);
        if (prior.contractId !== marker.contractId || prior.generation !== marker.generation || prior.contractRevision > marker.contractRevision || (prior.termsHash ?? null) !== (marker.termsHash ?? null)) throw new ValidationError("Cleanup marker disagrees with retained contract");
      }
      const metadata = cloneJson(record as unknown as JsonObject); delete metadata.terms;
      await this.audit({ event: "delete_contract", contractId: record.contractId, generation: record.generation, contract: metadata,
        validationStatus: ContractValidityStatus.CANCELLED, listedTokenKeyCount: entries.length,
        tokens: tokens.map((token, index) => ({ key: entries[index]!.key, revision: entries[index]!.revision, participant: token.participant,
          sessionId: token.sessionId, tokenId: token.tokenId, refreshSeq: token.refreshSeq, ...(token.termsHash === undefined ? {} : { termsHash: token.termsHash }) })) });
      this.assertOpen();
      // Updating a prior attempt's revision is guarded, after revalidating identity.
      if (!existing) await this.options.maintenanceState.create(key, marker as unknown as JsonObject);
      else if (this.marker(existing).contractRevision !== marker.contractRevision) await this.options.maintenanceState.update(key, marker as unknown as JsonObject, { revision: existing.revision });
      this.assertOpen();
      await store.delete(entry.key, { revision: entry.revision });
      return true;
    } catch (error) {
      await this.audit({ event: error instanceof StateConflict ? "delete_conflict" : "cleanup_pending", contractId: marker.contractId, generation: marker.generation, reason: String(error) });
      return false;
    }
  }
  private async cleanup(entry: StateEntry): Promise<{ complete: boolean; deleted: number }> {
    let deleted = 0;
    let marker: ConcordTokenCleanupRecord | undefined;
    try {
      marker = this.marker(entry);
      if (await this.options.contractState.get(concordContractKey(marker))) return { complete: false, deleted };
      for (let pass = 0; pass < 8; pass++) {
        this.assertOpen();
        const entries = await this.options.tokenState.items(concordContractPrefix(marker));
        this.tokens(marker, entries);
        for (const token of entries) if (await this.guardedDelete(this.options.tokenState, token)) deleted++;
        // Verify absence with an exact rescan, even after an initially empty listing.
        const remaining = await this.options.tokenState.items(concordContractPrefix(marker));
        this.tokens(marker, remaining);
        if (remaining.length) continue;
        if (await this.guardedDelete(this.options.maintenanceState, entry)) {
          await this.audit({ event: "cleanup_complete", contractId: marker.contractId, generation: marker.generation, deletedTokenKeyCount: deleted });
          return { complete: true, deleted };
        }
        break;
      }
      await this.audit({ event: "cleanup_pending", contractId: marker.contractId, generation: marker.generation, deletedTokenKeyCount: deleted, reason: "Cleanup conflict retry limit reached" });
    } catch (error) {
      await this.audit({ event: "cleanup_pending", ...(marker ? { contractId: marker.contractId, generation: marker.generation } : {}), deletedTokenKeyCount: deleted, reason: String(error) });
    }
    return { complete: false, deleted };
  }
}
