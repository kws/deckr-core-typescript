import { closedObject, timestamp } from "./validation.ts";
import { concordViewFor, type ConcordView, type ConcordFilters, type ConcordViewSnapshot } from "./concord-view.ts";
export type { ConcordFilters, ConcordMember, ConcordViewSnapshot } from "./concord-view.ts";
import { randomUUID } from "node:crypto";
import { monotonicNow, AsyncQueue, bounded, immutable, RenewalWorker, StateCell, systemScheduler, type RuntimeScheduler, type RuntimeState } from "./runtime.ts";
import { sourceFor, type MaterializedStateSource } from "./source.ts";

import { validateContractPointer, type ContractPointer } from "./authority.ts";
import { endpointAddress } from "./endpoint.ts";
import { StateConflict, StateUnavailable, ValidationError } from "./errors.ts";
import {
  canonicalJsonHash,
  JsonNumber,
  cloneJson,
  requireJsonObject,
  requirePositiveInteger,
  requireText,
  utcIsoNow,
  type JsonObject,
  type JsonValue,
} from "./json.ts";
import { decodeKeyToken, encodeKeyToken } from "./keys.ts";
import {
  PERSISTENT_STATE_STORE_POLICY,
  type StateEntry,
  type StateStore,
  type StateStorePolicy,
  type StateWatchItem,
} from "./state.ts";

export { canonicalJson, canonicalJsonBytes, canonicalJsonHash } from "./json.ts";
export { validateContractPointer, type ContractPointer } from "./authority.ts";

export const CONCORD_CONTRACT_SCHEMA_ID = "dev.deckr.concord.contract.v1";
export const CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID =
  "dev.deckr.concord.participant-token.v1";
export const CONCORD_STALE_OBSERVATION_SCHEMA_ID =
  "dev.deckr.concord.stale-observation.v1";

export const DEFAULT_CONCORD_CONTRACT_STORE_NAME = "deckr_concord_contract_v1";
export const DEFAULT_CONCORD_TOKEN_STORE_NAME = "deckr_concord_token_v1";
export const DEFAULT_CONCORD_MAINTENANCE_STORE_NAME = "deckr_concord_maintenance_v1";
export const DEFAULT_CONCORD_TOKEN_TTL_SECONDS = 120;
export const DEFAULT_CONCORD_REAPER_STALE_GRACE_SECONDS = 900;
export const DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS = 3600;
export const DEFAULT_CONCORD_REAPER_SCAN_INTERVAL_SECONDS = 60;
export const CONCORD_MAINTENANCE_ACTOR = "concord:maintenance";
export const CONCORD_REAPER_STALE_CONTRACT_REASON = "concord_reaper_stale_contract";
export const CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON =
  "concord_managed_lost_participant_token";
export const CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON =
  "concord_agreement_lost_participant_token";
export const ACTION_PROVIDER_SESSION_PROFILE_ID =
  "dev.deckr.action_runtime.provider.service_use.v1";

export const CONCORD_CONTRACT_STORE_POLICY: StateStorePolicy =
  PERSISTENT_STATE_STORE_POLICY;
export const CONCORD_MAINTENANCE_STORE_POLICY: StateStorePolicy =
  PERSISTENT_STATE_STORE_POLICY;
export const CONCORD_TOKEN_STORE_POLICY: StateStorePolicy = Object.freeze({
  brokerTtlSeconds: DEFAULT_CONCORD_TOKEN_TTL_SECONDS,
  allowWriteTtl: true,
  description: "Concord participant token state",
});

export const ContractState = Object.freeze({
  OPEN: "open",
  CANCELLED: "cancelled",
});
export type ContractState = (typeof ContractState)[keyof typeof ContractState];

export const ContractValidityStatus = Object.freeze({
  VALID: "valid",
  NOT_YET_FULFILLED: "not_yet_fulfilled",
  CANCELLED: "cancelled",
  MISSING_CONTRACT: "missing_contract",
  INVALID_CONTRACT: "invalid_contract",
  INVALID_TOKEN: "invalid_token",
  MISSING_TOKEN: "missing_token",
  GENERATION_MISMATCH: "generation_mismatch",
  SESSION_MISMATCH: "session_mismatch",
  TERMS_HASH_MISMATCH: "terms_hash_mismatch",
  UNAVAILABLE: "unavailable",
});
export type ContractValidityStatus =
  (typeof ContractValidityStatus)[keyof typeof ContractValidityStatus];

export const ConcordManagedContractEventType = Object.freeze({
  VALID: "valid",
  PENDING: "pending",
  INVALID: "invalid",
  CANCELLED: "cancelled",
  RELEASED: "released",
});
export type ConcordManagedContractEventType =
  (typeof ConcordManagedContractEventType)[keyof typeof ConcordManagedContractEventType];

export interface TokenObservation {
  generation: number;
  refreshSeq?: number;
  revision?: number;
  tokenHash?: string;
}

export interface ConcordStaleObservationRecord {
  schema: typeof CONCORD_STALE_OBSERVATION_SCHEMA_ID;
  contractId: string;
  generation: number;
  firstObservedStaleAt: string;
  status: ContractValidityStatus;
  reason?: string;
  contractRevision?: number;
}

export interface ContractRecord {
  schema: typeof CONCORD_CONTRACT_SCHEMA_ID;
  contractId: string;
  generation: number;
  participants: string[];
  attachedParticipants: string[];
  state: ContractState;
  profile?: string;
  termsHash?: string;
  terms?: JsonObject;
  createdBy?: string;
  participantSessions: Record<string, string>;
  createdAt?: string;
  cancelledBy?: string;
  cancelledAt?: string;
  cancelRevision?: number;
  cancelReason?: string;
  supersedes?: ContractPointer;
}

export interface ParticipantTokenRecord {
  schema: typeof CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID;
  contractId: string;
  generation: number;
  participant: string;
  sessionId: string;
  tokenId: string;
  refreshSeq: number;
  ttlSeconds: number;
  termsHash?: string;
  contractHash?: string;
  observed: Record<string, TokenObservation>;
}

export interface ContractHandle {
  key: string;
  contractId: string;
  generation: number;
  participants: string[];
  attachedParticipants: string[];
  revision: number;
  state: ContractState;
  profile?: string;
  termsHash?: string;
}

export interface ParticipantHandle {
  key: string;
  contractId: string;
  generation: number;
  participant: string;
  sessionId: string;
  tokenId: string;
  revision: number;
  refreshSeq: number;
  ttlSeconds: number;
  termsHash?: string;
}

export interface ContractValidity {
  status: ContractValidityStatus;
  contract?: ContractRecord;
  tokens: Record<string, ParticipantHandle>;
  reason?: string;
  reasonCode?: string;
  valid: boolean;
}

export interface ConcordMaintenanceDeletionResult {
  deleted: boolean;
  deletedTokenKeyCount: number;
}

export interface ConcordReaperScanResult {
  scannedContractCount: number;
  staleObservationCount: number;
  staleObservationsCreated: number;
  staleObservationsCleared: number;
  contractsCancelled: number;
  contractsDeleted: number;
  tokenKeysDeleted: number;
  cleanupPendingCount?: number;
}

export interface ConcordManagedContract {
  contract: ContractHandle;
  record: ContractRecord;
  validity: ContractValidity;
  token?: ParticipantHandle;
}

export interface ConcordManagedContractEvent {
  eventType: ConcordManagedContractEventType;
  contract: ContractHandle;
  record?: ContractRecord;
  validity?: ContractValidity;
  token?: ParticipantHandle;
  reason?: string;
}

export function concordContractKey(input: {
  contractId: string;
  generation: number;
}): string {
  return ["contracts", encodeKeyToken(input.contractId), String(input.generation), "meta"].join(".");
}

export function parseConcordContractKey(key: string): [string, number] | null {
  const parts = key.split(".");
  if (parts.length !== 4 || parts[0] !== "contracts" || parts[3] !== "meta") {
    return null;
  }
  const generation = Number(parts[2]);
  if (!Number.isInteger(generation)) {
    return null;
  }
  return [decodeKeyToken(parts[1]!), generation];
}

export function concordParticipantTokenKey(input: {
  contractId: string;
  generation: number;
  participant: string;
}): string {
  return [
    "contracts",
    encodeKeyToken(input.contractId),
    String(input.generation),
    "participants",
    encodeKeyToken(endpointAddress(input.participant)),
  ].join(".");
}

export function parseConcordParticipantTokenKey(key: string): [string, number, string] | null {
  const parts = key.split(".");
  if (parts.length !== 5 || parts[0] !== "contracts" || parts[3] !== "participants") {
    return null;
  }
  const generation = Number(parts[2]);
  if (!Number.isInteger(generation)) {
    return null;
  }
  return [decodeKeyToken(parts[1]!), generation, endpointAddress(decodeKeyToken(parts[4]!))];
}

export function concordContractPrefix(input: { contractId: string; generation: number }): string {
  return ["contracts", encodeKeyToken(input.contractId), String(input.generation), ""].join(".");
}

export function concordContractsPrefix(): string {
  return "contracts.";
}

export function concordContractIdPrefix(input: { contractId: string }): string {
  return ["contracts", encodeKeyToken(input.contractId), ""].join(".");
}

export function concordStaleObservationKey(input: {
  contractId: string;
  generation: number;
}): string {
  return ["stale", encodeKeyToken(input.contractId), String(input.generation)].join(".");
}

export function validateContractRecord(value: unknown): ContractRecord {
  const raw = closedObject(value, ["schema", "contractId", "generation", "participants", "attachedParticipants", "participantSessions", "state", "profile", "terms", "termsHash", "createdBy", "createdAt", "cancelledBy", "cancelledAt", "cancelRevision", "cancelReason", "supersedes"], "Concord contract");
  if (raw.schema !== undefined && raw.schema !== CONCORD_CONTRACT_SCHEMA_ID) {
    throw new ValidationError("Concord contract schema is invalid");
  }
  const participants = validateEndpointList(raw.participants, "participants");
  if (participants.length === 0) {
    throw new ValidationError("Concord contracts require at least one participant");
  }
  requireCanonicalUnique(participants, "Concord contract participants");
  const attachedParticipants = validateEndpointList(
    raw.attachedParticipants,
    "attachedParticipants",
  );
  requireCanonicalUnique(attachedParticipants, "Concord attached participants");
  const participantSet = new Set(participants);
  for (const participant of attachedParticipants) {
    if (!participantSet.has(participant)) {
      throw new ValidationError("attachedParticipants must be a subset of participants");
    }
  }
  const participantSessions: Record<string, string> = {};
  if (raw.participantSessions !== undefined) {
    const sessions = requireJsonObject(raw.participantSessions, "participantSessions");
    for (const [participant, session] of Object.entries(sessions)) {
      const parsedParticipant = endpointAddress(participant);
      if (parsedParticipant !== participant || !participantSet.has(parsedParticipant)) {
        throw new ValidationError("participantSessions keys must be named participants");
      }
      participantSessions[parsedParticipant] = requireText(
        session,
        "participantSessions session",
      );
    }
  }
  const state = raw.state === undefined ? ContractState.OPEN : requireText(raw.state, "state");
  if (state !== ContractState.OPEN && state !== ContractState.CANCELLED) {
    throw new ValidationError("Concord contract state must be open or cancelled");
  }
  const record: ContractRecord = {
    schema: CONCORD_CONTRACT_SCHEMA_ID,
    contractId: requireText(raw.contractId, "contractId"),
    generation: requirePositiveInteger(raw.generation, "generation"),
    participants,
    attachedParticipants,
    participantSessions,
    state,
  };
  setOptionalText(record, "profile", raw.profile, "profile");
  setOptionalText(record, "termsHash", raw.termsHash, "termsHash");
  if (raw.terms !== undefined && raw.terms !== null) {
    record.terms = requireJsonObject(raw.terms, "terms");
    if (record.termsHash === undefined) {
      throw new ValidationError("Concord contract terms require termsHash");
    }
    if (record.termsHash !== canonicalJsonHash(record.terms as JsonValue)) {
      throw new ValidationError("Concord contract termsHash does not match terms");
    }
    const termsProfile = record.terms.profile;
    if (record.profile !== undefined && termsProfile !== undefined && termsProfile !== record.profile) {
      throw new ValidationError("Concord contract profile must match terms.profile");
    }
  }
  setOptionalEndpoint(record, "createdBy", raw.createdBy);
  if (raw.createdAt != null) record.createdAt = timestamp(raw.createdAt, "createdAt");
  if (raw.cancelledBy != null) {
    record.cancelledBy =
      raw.cancelledBy === CONCORD_MAINTENANCE_ACTOR
        ? CONCORD_MAINTENANCE_ACTOR
        : endpointAddress(requireText(raw.cancelledBy, "cancelledBy"));
  }
  if (raw.cancelledAt != null) record.cancelledAt = timestamp(raw.cancelledAt, "cancelledAt");
  if (raw.cancelRevision != null) {
    record.cancelRevision = nonNegativeInteger(raw.cancelRevision, "cancelRevision");
  }
  setOptionalText(record, "cancelReason", raw.cancelReason, "cancelReason");
  if (raw.supersedes !== undefined && raw.supersedes !== null) {
    record.supersedes = validateContractPointer(raw.supersedes);
  }
  return record;
}

export function validateParticipantTokenRecord(value: unknown): ParticipantTokenRecord {
  const raw = closedObject(value, ["schema", "contractId", "generation", "participant", "sessionId", "tokenId", "refreshSeq", "ttlSeconds", "termsHash", "contractHash", "observed"], "Concord participant token");
  if (raw.schema !== undefined && raw.schema !== CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID) {
    throw new ValidationError("Concord participant token schema is invalid");
  }
  const observedRaw = requireJsonObject(raw.observed === undefined ? {} : raw.observed, "observed");
  const observed: Record<string, TokenObservation> = {};
  for (const [key, item] of Object.entries(observedRaw)) {
    endpointAddress(key);
    const observation = closedObject(item, ["generation", "refreshSeq", "revision", "tokenHash"], "token observation");
    const record: TokenObservation = {
      generation: requirePositiveInteger(observation.generation, "token observation generation"),
    };
    if (observation.refreshSeq != null) {
      record.refreshSeq = nonNegativeInteger(observation.refreshSeq, "refreshSeq");
    }
    if (observation.revision != null) {
      record.revision = nonNegativeInteger(observation.revision, "revision");
    }
    if (observation.tokenHash != null) {
      if (typeof observation.tokenHash !== "string") throw new ValidationError("tokenHash must be a string");
      record.tokenHash = observation.tokenHash;
    }
    observed[key] = record;
  }
  const record: ParticipantTokenRecord = {
    schema: CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID,
    contractId: requireText(raw.contractId, "contractId"),
    generation: requirePositiveInteger(raw.generation, "generation"),
    participant: endpointAddress(requireText(raw.participant, "participant")),
    sessionId: requireText(raw.sessionId, "sessionId"),
    tokenId: requireText(raw.tokenId, "tokenId"),
    refreshSeq: requirePositiveInteger(raw.refreshSeq, "refreshSeq"),
    ttlSeconds: requirePositiveInteger(raw.ttlSeconds, "ttlSeconds"),
    observed,
  };
  setOptionalText(record, "termsHash", raw.termsHash, "termsHash");
  setOptionalText(record, "contractHash", raw.contractHash, "contractHash");
  return record;
}

export function validateStaleObservationRecord(value: unknown): ConcordStaleObservationRecord {
  const raw = closedObject(value, ["schema", "contractId", "generation", "firstObservedStaleAt", "status", "reason", "contractRevision"], "Concord stale observation");
  if (raw.schema !== undefined && raw.schema !== CONCORD_STALE_OBSERVATION_SCHEMA_ID) {
    throw new ValidationError("Concord stale observation schema is invalid");
  }
  const status = requireText(raw.status, "status") as ContractValidityStatus;
  if (!Object.values(ContractValidityStatus).includes(status)) {
    throw new ValidationError("invalid Concord stale observation status");
  }
  const record: ConcordStaleObservationRecord = {
    schema: CONCORD_STALE_OBSERVATION_SCHEMA_ID,
    contractId: requireText(raw.contractId, "contractId"),
    generation: requirePositiveInteger(raw.generation, "generation"),
    firstObservedStaleAt: timestamp(raw.firstObservedStaleAt, "firstObservedStaleAt"),
    status,
  };
  setOptionalText(record, "reason", raw.reason, "reason");
  if (raw.contractRevision != null) {
    record.contractRevision = nonNegativeInteger(raw.contractRevision, "contractRevision");
  }
  return record;
}

export interface ContractObservations {
  pointer: ContractPointer;
  contract: StateEntry | null;
  tokens: Readonly<Record<string, StateEntry | null>>;
  currentSessions?: Readonly<Record<string, string>>;
  current?: boolean;
}

/** Pure evaluation shared by exact reads and ordered cached observations. */
export function evaluateContract(observations: ContractObservations): ContractValidity {
  const { pointer, contract: entry } = observations;
  const tokens: Record<string, ParticipantHandle> = {};
  let record: ContractRecord | undefined;
  const result = (status: ContractValidityStatus, reasonCode?: string, reason?: string): ContractValidity =>
    immutable({ status, valid: status === ContractValidityStatus.VALID, tokens: { ...tokens },
      ...(record ? { contract: record } : {}), ...(reasonCode ? { reasonCode } : {}), ...(reason ? { reason } : {}) });
  if (observations.current === false) return result(ContractValidityStatus.UNAVAILABLE, "source_unavailable");
  if (!entry) return result(ContractValidityStatus.MISSING_CONTRACT, "contract_missing");
  if (entry.key !== concordContractKey(pointer)) return result(ContractValidityStatus.INVALID_CONTRACT, "contract_key_mismatch");
  if (entry.parseError) return result(ContractValidityStatus.INVALID_CONTRACT, "contract_malformed");
  try { record = validateContractRecord(cloneJson(entry.value)); }
  catch { return result(ContractValidityStatus.INVALID_CONTRACT, "contract_malformed"); }
  if (record.contractId !== pointer.contractId || record.generation !== pointer.generation) return result(ContractValidityStatus.INVALID_CONTRACT, "contract_pointer_mismatch");
  if (record.state === ContractState.CANCELLED) return result(ContractValidityStatus.CANCELLED, "contract_cancelled");
  let pending: string | undefined;
  for (const participant of record.participants) {
    const key = concordParticipantTokenKey({ ...pointer, participant });
    const entry = observations.tokens[key];
    if (!entry) {
      if (record.attachedParticipants.includes(participant)) return result(ContractValidityStatus.MISSING_TOKEN, "token_missing", participant);
      pending ??= participant; continue;
    }
    if (entry.key !== key) return result(ContractValidityStatus.INVALID_TOKEN, "token_key_mismatch", participant);
    if (entry.parseError) return result(ContractValidityStatus.INVALID_TOKEN, "token_malformed", participant);
    let token: ParticipantTokenRecord;
    try { token = validateParticipantTokenRecord(cloneJson(entry.value)); }
    catch { return result(ContractValidityStatus.INVALID_TOKEN, "token_malformed", participant); }
    const status = tokenValidityStatus(token, record, participant, observations.currentSessions);
    if (status) {
      const reason = token.contractId !== record.contractId ? "token_contract_mismatch"
        : token.participant !== participant ? "token_participant_mismatch"
        : status === ContractValidityStatus.GENERATION_MISMATCH ? "token_generation_mismatch"
        : status === ContractValidityStatus.TERMS_HASH_MISMATCH ? "token_terms_hash_mismatch" : "token_session_mismatch";
      return result(status, reason, participant);
    }
    tokens[participant] = participantHandle(key, token, entry.revision);
    if (!record.attachedParticipants.includes(participant)) pending ??= participant;
  }
  return pending ? result(ContractValidityStatus.NOT_YET_FULFILLED, "participant_not_attached", pending) : result(ContractValidityStatus.VALID);
}

export class ConcordAttachmentError extends StateUnavailable {
  readonly token: ParticipantHandle;
  constructor(token: ParticipantHandle, cause: unknown) {
    super("Participant attachment did not complete", { cause });
    this.token = token;
  }
}

function mutationContract(entry: StateEntry, pointer?: ContractPointer): ContractRecord {
  let record: ContractRecord;
  try {
    if (entry.parseError) throw new ValidationError(entry.parseError);
    record = validateContractRecord(entry.value);
  } catch { throw new StateConflict("Malformed Concord contract", "contract_invalid"); }
  if (entry.key !== concordContractKey(record) || (pointer && (record.contractId !== pointer.contractId || record.generation !== pointer.generation))) throw new StateConflict("Concord contract changed identity", "contract_identity_mismatch");
  return record;
}

function mutationToken(entry: StateEntry, handle?: ParticipantHandle): ParticipantTokenRecord {
  let token: ParticipantTokenRecord;
  try {
    if (entry.parseError) throw new ValidationError(entry.parseError);
    token = validateParticipantTokenRecord(entry.value);
  } catch { throw new StateConflict("Malformed Concord participant token", "token_invalid"); }
  if (entry.key !== concordParticipantTokenKey(token) || (handle && !tokenMatchesHandle(token, handle))) throw new StateConflict("Concord participant token changed owner", "token_identity_mismatch");
  return token;
}

export class ConcordCoordinator {
  private readonly contractState: StateStore;
  private readonly tokenState: StateStore;
  private readonly tokenTtlSeconds: number;

  constructor(
    contractState: StateStore,
    tokenState: StateStore,
    options: { tokenTtlSeconds?: number } = {},
  ) {
    this.contractState = contractState;
    this.tokenState = tokenState;
    this.tokenTtlSeconds = tokenState.policy?.brokerTtlSeconds ?? DEFAULT_CONCORD_TOKEN_TTL_SECONDS;
    if (options.tokenTtlSeconds !== undefined && options.tokenTtlSeconds !== this.tokenTtlSeconds) throw new ValidationError("Token TTL is owned by the store policy");
    if (this.tokenTtlSeconds <= 0) {
      throw new ValidationError("tokenTtlSeconds must be greater than zero");
    }
  }

  async createContract(
    participants: string[],
    options: {
      contractId?: string;
      generation?: number;
      profile?: string;
      terms?: JsonObject;
      createdBy?: string;
      supersedes?: ContractPointer;
      participantSessions?: Record<string, string>;
    } = {},
  ): Promise<ContractHandle> {
    const parsedParticipants = participants.map(endpointAddress).sort();
    requireCanonicalUnique(parsedParticipants, "Concord contract participants");
    if (parsedParticipants.length === 0) {
      throw new ValidationError("Concord contracts require at least one participant");
    }
    const terms = options.terms === undefined ? undefined : cloneJson(options.terms);
    const termsHash = terms === undefined ? undefined : canonicalJsonHash(terms as JsonValue);
    const now = utcIsoNow();
    const record = validateContractRecord({
      schema: CONCORD_CONTRACT_SCHEMA_ID,
      contractId: options.contractId ?? randomUUID(),
      generation: options.generation ?? 1,
      participants: parsedParticipants,
      attachedParticipants: [],
      state: ContractState.OPEN,
      profile: options.profile,
      terms,
      termsHash,
      createdBy: options.createdBy,
      participantSessions: options.participantSessions ?? {},
      createdAt: now,
      supersedes: options.supersedes,
    });
    const key = concordContractKey({
      contractId: record.contractId,
      generation: record.generation,
    });
    const entry = await this.contractState.create(key, recordToJson(record));
    return contractHandle(key, record, entry.revision);
  }

  async getContract(pointer: ContractPointer): Promise<ContractHandle | null> {
    const key = concordContractKey(pointer);
    const entry = await this.contractState.get(key);
    if (entry === null) {
      return null;
    }
    const record = validateContractRecord(entry.value);
    if (record.contractId !== pointer.contractId || record.generation !== pointer.generation) {
      return null;
    }
    return contractHandle(key, record, entry.revision);
  }

  async contractRecord(contract: ContractHandle): Promise<ContractRecord | null> {
    const entry = await this.contractState.get(contract.key);
    if (entry === null) {
      return null;
    }
    const record = validateContractRecord(entry.value);
    if (record.contractId !== contract.contractId || record.generation !== contract.generation) {
      return null;
    }
    return record;
  }

  async contracts(
    profile?: string,
    options: { contractId?: string; participant?: string; state?: ContractState } = {},
  ): Promise<ContractHandle[]> {
    const participant =
      options.participant === undefined ? undefined : endpointAddress(options.participant);
    const prefix =
      options.contractId === undefined
        ? concordContractsPrefix()
        : concordContractIdPrefix({ contractId: requireText(options.contractId, "contractId") });
    const contracts: ContractHandle[] = [];
    for (const entry of await this.contractState.items(prefix)) {
      const parsed = parseConcordContractKey(entry.key);
      if (parsed === null) {
        continue;
      }
      const [contractId, generation] = parsed;
      if (options.contractId !== undefined && contractId !== options.contractId) {
        continue;
      }
      let record: ContractRecord;
      try {
        record = validateContractRecord(entry.value);
      } catch {
        continue;
      }
      if (profile !== undefined && record.profile !== profile) {
        continue;
      }
      if (participant !== undefined && !record.participants.includes(participant)) {
        continue;
      }
      if (options.state !== undefined && record.state !== options.state) {
        continue;
      }
      if (record.contractId !== contractId || record.generation !== generation) {
        continue;
      }
      contracts.push(contractHandle(entry.key, record, entry.revision));
    }
    return contracts.sort((left, right) => left.key.localeCompare(right.key));
  }

  watchContracts(): AsyncIterable<StateWatchItem> {
    if (this.contractState.watch === undefined) {
      throw new StateUnavailable("Contract state store does not support watch");
    }
    return this.contractState.watch(concordContractsPrefix());
  }

  async attach(
    contract: ContractHandle,
    participant: string,
    sessionId: string,
    options: { tokenId?: string; ttlSeconds?: number } = {},
  ): Promise<ParticipantHandle> {
    const current = await this.contractState.get(contract.key);
    if (current === null) {
      throw new StateConflict(`Concord contract ${JSON.stringify(contract.key)} is missing`);
    }
    const record = mutationContract(current);
    if (record.contractId !== contract.contractId || record.generation !== contract.generation || current.key !== concordContractKey(record)) throw new StateConflict("Concord contract changed identity");
    if (record.state === ContractState.CANCELLED) {
      throw new StateConflict(`Concord contract ${JSON.stringify(contract.key)} is cancelled`);
    }
    const parsedParticipant = endpointAddress(participant);
    if (!record.participants.includes(parsedParticipant)) {
      throw new ValidationError("participant is not named by the Concord contract");
    }
    const expectedSession = record.participantSessions[parsedParticipant];
    if (expectedSession !== undefined && expectedSession !== sessionId) {
      throw new StateConflict("participant session does not match the contract session fence");
    }
    const key = concordParticipantTokenKey({
      contractId: record.contractId,
      generation: record.generation,
      participant: parsedParticipant,
    });
    if (record.attachedParticipants.includes(parsedParticipant)) {
      const tokenEntry = await this.tokenState.get(key);
      if (tokenEntry !== null) {
        const existing = mutationToken(tokenEntry);
        if (tokenMatchesAttachRequest(existing, record, parsedParticipant, sessionId, options.tokenId)) {
          return participantHandle(key, existing, tokenEntry.revision);
        }
      }
      throw new StateConflict("Concord participant is already attached");
    }
    const ttlSeconds = this.tokenTtlSeconds;
    if (options.ttlSeconds !== undefined && options.ttlSeconds !== ttlSeconds) throw new ValidationError("Token TTL is owned by the store policy");
    const token = validateParticipantTokenRecord({
      schema: CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID,
      contractId: record.contractId,
      generation: record.generation,
      participant: parsedParticipant,
      sessionId,
      tokenId: options.tokenId ?? randomUUID(),
      refreshSeq: 1,
      ttlSeconds,
      termsHash: record.termsHash,
      observed: {},
    });
    let entry: StateEntry;
    let attachedToken = token;
    try {
      entry = await this.tokenState.create(key, recordToJson(token), {
        ttl: token.ttlSeconds,
      });
    } catch (error) {
      if (!(error instanceof StateConflict)) {
        // A publish may commit before its acknowledgement is lost. Recover
        // only the exact prepared token, never adopt another lifetime's token.
        const recovered = await this.tokenState.get(key).catch(() => null);
        if (!recovered || canonicalJsonHash(recovered.value) !== canonicalJsonHash(recordToJson(token))) throw error;
        entry = recovered;
      } else {
        const tokenEntry = await this.tokenState.get(key);
        if (tokenEntry === null) {
          throw new StateConflict("Concord participant token changed during attach");
        }
        const existing = mutationToken(tokenEntry);
        if (!tokenMatchesAttachRequest(existing, record, parsedParticipant, sessionId, options.tokenId)) {
          throw new StateConflict("Concord participant token already exists");
        }
        attachedToken = existing;
        entry = tokenEntry;
      }
    }
    const handle = participantHandle(key, attachedToken, entry.revision);
    try { await this.markParticipantAttached(contract.key, parsedParticipant); }
    catch (error) {
      await bounded(this.withdraw(handle), 5000).catch(() => {});
      throw new ConcordAttachmentError(handle, error);
    }
    return handle;
  }

  async refresh(handle: ParticipantHandle): Promise<ParticipantHandle> {
    const contractEntry = await this.contractState.get(
      concordContractKey({ contractId: handle.contractId, generation: handle.generation }),
    );
    if (contractEntry === null) {
      throw new StateConflict("Concord contract is missing");
    }
    const contract = mutationContract(contractEntry, handle);
    if (contract.state === ContractState.CANCELLED) {
      throw new StateConflict("Concord contract is cancelled");
    }
    const tokenEntry = await this.tokenState.get(handle.key);
    if (tokenEntry === null) {
      throw new StateConflict("Concord participant token is missing");
    }
    const token = mutationToken(tokenEntry);
    if (!tokenMatchesHandle(token, handle)) {
      throw new StateConflict("Concord participant token changed owner");
    }
    const refreshed = validateParticipantTokenRecord({
      ...token,
      refreshSeq: token.refreshSeq + 1,
    });
    try {
      const entry = await this.tokenState.update(handle.key, recordToJson(refreshed), {
        revision: tokenEntry.revision,
        ttl: refreshed.ttlSeconds,
      });
      return participantHandle(handle.key, refreshed, entry.revision);
    } catch (error) {
      if (!(error instanceof StateConflict) || error.code !== "revision_changed") {
        throw error;
      }
      const latestEntry = await this.tokenState.get(handle.key);
      if (latestEntry === null) {
        throw new StateConflict("Concord participant token is missing");
      }
      const latest = mutationToken(latestEntry);
      if (!tokenMatchesHandle(latest, handle)) {
        throw new StateConflict("Concord participant token changed owner");
      }
      return participantHandle(handle.key, latest, latestEntry.revision);
    }
  }

  async cancel(
    contract: ContractHandle,
    participant: string,
    options: { reason?: string } = {},
  ): Promise<boolean> {
    const current = await this.contractState.get(contract.key);
    if (current === null) {
      return false;
    }
    const record = mutationContract(current);
    if (record.contractId !== contract.contractId || record.generation !== contract.generation || current.key !== concordContractKey(record)) throw new StateConflict("Concord contract changed identity");
    if (record.state === ContractState.CANCELLED) {
      return false;
    }
    const parsedParticipant = endpointAddress(participant);
    if (!record.participants.includes(parsedParticipant)) {
      throw new ValidationError("participant is not named by the Concord contract");
    }
    const cancelled = validateContractRecord({
      ...record,
      state: ContractState.CANCELLED,
      cancelledBy: parsedParticipant,
      cancelledAt: utcIsoNow(),
      cancelRevision: current.revision,
      cancelReason: options.reason,
    });
    await this.contractState.update(contract.key, recordToJson(cancelled), {
      revision: current.revision,
    });
    return true;
  }

  async maintenanceCancel(
    contract: ContractHandle,
    options: { reason?: string; now?: string } = {},
  ): Promise<boolean> {
    const current = await this.contractState.get(contract.key);
    if (current === null) {
      return false;
    }
    if (current.revision !== contract.revision) throw new StateConflict("Concord contract changed during maintenance", "revision_changed");
    const record = mutationContract(current);
    if (record.contractId !== contract.contractId || record.generation !== contract.generation) {
      throw new StateConflict(`Concord contract ${JSON.stringify(contract.key)} changed identity`);
    }
    if (record.state === ContractState.CANCELLED) {
      return false;
    }
    const cancelled = validateContractRecord({
      ...record,
      state: ContractState.CANCELLED,
      cancelledBy: CONCORD_MAINTENANCE_ACTOR,
      cancelledAt: options.now ?? utcIsoNow(),
      cancelRevision: current.revision,
      cancelReason: options.reason ?? CONCORD_REAPER_STALE_CONTRACT_REASON,
    });
    await this.contractState.update(contract.key, recordToJson(cancelled), {
      revision: current.revision,
    });
    return true;
  }

  async validate(
    contract: ContractHandle,
    options: { currentSessions?: Record<string, string> } = {},
  ): Promise<ContractValidity> {
    const sessions = { ...options.currentSessions };
    try {
      const entry = await this.contractState.get(contract.key);
      const tokens: Record<string, StateEntry | null> = {};
      if (entry) {
        let record: ContractRecord | undefined;
        try { record = validateContractRecord(entry.value); } catch { /* evaluated below */ }
        for (const participant of record?.participants ?? []) {
          const key = concordParticipantTokenKey({ ...contract, participant });
          tokens[key] = await this.tokenState.get(key);
        }
      }
      return evaluateContract({ pointer: contract, contract: entry, tokens, currentSessions: sessions });
    } catch (error) {
      if (error instanceof StateUnavailable) return { ...validity(ContractValidityStatus.UNAVAILABLE), reasonCode: "source_unavailable" };
      throw error;
    }
  }

  async withdraw(handle: ParticipantHandle): Promise<number | void> {
    const entry = await this.tokenState.get(handle.key);
    if (!entry) return;
    const token = mutationToken(entry, handle);
    if (!tokenMatchesHandle(token, handle) || handle.key !== concordParticipantTokenKey(handle)) {
      throw new StateConflict("Concord participant token changed owner");
    }
    return this.tokenState.delete(handle.key, { revision: entry.revision });
  }

  tokenStore(): StateStore {
    return this.tokenState;
  }

  contractStore(): StateStore {
    return this.contractState;
  }

  private async markParticipantAttached(contractKey: string, participant: string): Promise<void> {
    for (let attempt = 0; attempt < 8; attempt++) {
      const current = await this.contractState.get(contractKey);
      if (current === null) {
        throw new StateConflict(`Concord contract ${JSON.stringify(contractKey)} is missing`);
      }
      const record = mutationContract(current);
      if (record.state === ContractState.CANCELLED) {
        throw new StateConflict(`Concord contract ${JSON.stringify(contractKey)} is cancelled`);
      }
      if (!record.participants.includes(participant)) {
        throw new StateConflict("participant is not named by the Concord contract");
      }
      if (record.attachedParticipants.includes(participant)) {
        return;
      }
      const attachedParticipants = [...record.attachedParticipants, participant].sort();
      const updated = validateContractRecord({ ...record, attachedParticipants });
      try {
        await this.contractState.update(contractKey, recordToJson(updated), {
          revision: current.revision,
        });
        return;
      } catch (error) {
        if (error instanceof StateConflict && error.code === "revision_changed" && attempt < 7) {
          const source = sourceFor(this.contractState);
          source.retain();
          try {
            await source.start();
            await source.observed(current.revision + 1, contractKey);
          } finally { await source.release(); }
          continue;
        }
        throw error;
      }
    }
    throw new StateConflict("Concord attachment retry limit reached");
  }
}

export class ConcordParticipantLease {
  readonly contract: ContractHandle;
  readonly participant: string;
  readonly sessionId: string;
  private readonly service: ConcordService;
  private readonly ownsProposal: boolean;
  private tokenValue: ParticipantHandle | null = null;
  private readonly tokenId = randomUUID();
  private attachStarted = false;
  private expiresAt = Number.POSITIVE_INFINITY;
  private closedValue = false;
  private worker?: RenewalWorker;
  private renewal?: Promise<ParticipantHandle>;
  private cleanup?: Promise<void>;
  private readonly abort = new AbortController();
  get signal(): AbortSignal { return this.abort.signal; }

  constructor(service: ConcordService, options: { contract: ContractHandle; participant: string; sessionId: string; ownsProposal?: boolean }) {
    this.service = service;
    this.ownsProposal = options.ownsProposal ?? false;
    this.contract = immutable(options.contract);
    this.participant = endpointAddress(options.participant);
    this.sessionId = requireText(options.sessionId, "Concord session id");
  }
  get token(): ParticipantHandle | null {
    if (!this.closedValue && monotonicNow(this.service.scheduler) >= this.expiresAt) this.terminate();
    return this.closedValue ? null : this.tokenValue;
  }
  get hasOwnedToken(): boolean { return this.tokenValue !== null; }
  get closed(): boolean { return this.closedValue; }
  adopt(token: ParticipantHandle): void {
    if (!this.tokenValue || !participantHandleMatches(this.tokenValue, token)) throw new ValidationError("participant token does not match local handle");
    if (this.closed) throw new ValidationError("participant lease is closed");
    this.tokenValue = token;
  }

  async attachOrRefresh(): Promise<ParticipantHandle> {
    if (monotonicNow(this.service.scheduler) >= this.expiresAt) this.terminate();
    if (this.closed) throw new StateConflict("Concord participant lease is closed");
    if (this.renewal) return this.renewal;
    this.renewal = (async () => {
      try {
        const started = monotonicNow(this.service.scheduler);
        const previousExpiry = this.expiresAt;
        if (!this.tokenValue) this.attachStarted = true;
        const token = this.tokenValue
          ? await this.service.refreshToken(this.tokenValue)
          : await this.service.attach(this.contract, this.participant, this.sessionId, { tokenId: this.tokenId });
        this.tokenValue = token;
        if (monotonicNow(this.service.scheduler) >= Math.min(previousExpiry, started + token.ttlSeconds * 1000)) this.terminate();
        if (this.closed) { await this.service.withdrawToken(token); throw new StateConflict("Concord participant lease is closed"); }
        this.expiresAt = started + token.ttlSeconds * 1000;
        this.service.holdLease(this);
        if (this.closed) throw new StateConflict("Concord participant lease is closed");
        if (!this.worker) {
          this.worker = new RenewalWorker(() => this.tokenValue!.ttlSeconds,
            async () => { await this.attachOrRefresh(); }, () => { this.terminate(); }, this.service.scheduler, () => this.expiresAt);
          this.worker.start();
        }
        return token;
      } catch (error) {
        if (error instanceof ConcordAttachmentError) this.tokenValue = error.token;
        else if (error instanceof StateConflict && !this.tokenValue) this.attachStarted = false;
        this.terminate(); throw error;
      }
    })().finally(() => { this.renewal = undefined; });
    return this.renewal;
  }

  terminate(): void {
    if (this.closedValue) return;
    this.closedValue = true;
    this.abort.abort();
    this.worker?.stop();
    this.service.releaseLease(this);
    // Revoke locally first. Cleanup failure cannot restore authority or delay it.
    const cleanup: Promise<unknown>[] = [];
    if (this.ownsProposal || this.tokenValue) cleanup.push(this.service.cancelContract(this.contract, this.participant, { reason: "local_authority_terminated" }));
    if (this.tokenValue) cleanup.push(this.service.withdrawToken(this.tokenValue));
    else if (this.attachStarted) cleanup.push(this.service.withdrawPreparedToken(this.contract, this.participant, this.sessionId, this.tokenId));
    this.cleanup = bounded(Promise.allSettled(cleanup), 5000, this.service.scheduler).then(() => {}, () => {});
    this.service.trackCleanup(this.cleanup);
  }

  async close(): Promise<void> {
    this.terminate();
    await this.worker?.close();
    await this.cleanup;
  }
}

export interface ConcordAgreementSpec {
  profile?: string;
  participants: string[];
  localParticipant: string;
  localSessionId: string;
  terms?: JsonObject;
  supersedes?: ContractPointer;
  participantSessions?: Record<string, string>;
  currentSessions?: Record<string, string> | (() => Record<string, string> | Promise<Record<string, string>>);
  refreshIntervalSeconds?: number;
  createdBy?: string;
}

export class ConcordAgreement {
  readonly spec: ConcordAgreementSpec;
  readonly contract: ContractHandle;
  private readonly service: ConcordService;
  readonly lease: ConcordParticipantLease;
  private validityValue: ContractValidity;
  private closedValue = false;
  private readonly listeners = new Set<() => void>();

  constructor(service: ConcordService, options: { spec: ConcordAgreementSpec; contract: ContractHandle; lease: ConcordParticipantLease; validity: ContractValidity }) {
    this.service = service; this.spec = immutable(options.spec); this.contract = immutable(options.contract);
    this.lease = options.lease; this.validityValue = immutable(options.validity);
  }
  get validity(): ContractValidity { return this.validityValue; }
  get valid(): boolean { return !this.closed && this.localToken !== null && this.validityValue.valid; }
  get localToken(): ParticipantHandle | null { return this.lease.token; }
  get signal(): AbortSignal { return this.lease.signal; }
  get closed(): boolean { return this.closedValue || this.lease.closed; }
  onChange(listener: () => void): () => void {
    if (this.listeners.size >= 256) throw new StateUnavailable("Agreement subscriber limit reached");
    this.listeners.add(listener); return () => { this.listeners.delete(listener); };
  }
  async *watch(options: { signal?: AbortSignal } = {}): AsyncIterable<{ closed: boolean; validity: ContractValidity }> {
    const queue = new AsyncQueue<{ closed: boolean; validity: ContractValidity }>(1, true);
    const publish = () => queue.push(Object.freeze({ closed: this.closed, validity: this.validity }));
    const unsubscribe = this.onChange(publish);
    const close = () => queue.close();
    options.signal?.addEventListener("abort", close, { once: true });
    publish();
    try {
      if (options.signal?.aborted) return;
      for await (const state of queue) { yield state; if (state.closed) return; }
    } finally { unsubscribe(); queue.close(); options.signal?.removeEventListener("abort", close); }
  }
  private notify(): void { for (const listener of [...this.listeners]) listener(); }

  observe(value: ContractValidity): void {
    if (this.closedValue) return;
    this.validityValue = immutable(value);
    if (agreementSuccessorStatus(value.status) || value.status === ContractValidityStatus.UNAVAILABLE || this.lease.closed) this.terminate(value);
    else this.notify();
  }

  terminate(value: ContractValidity = { status: ContractValidityStatus.CANCELLED, tokens: {}, valid: false, reasonCode: "token_local_authority_lost" }): void {
    if (this.closedValue) return;
    this.closedValue = true;
    this.validityValue = immutable({ ...value, valid: false });
    this.lease.terminate();
    this.service.forgetAgreement(this);
    this.notify();
  }

  async refresh(): Promise<ContractValidity> {
    if (this.closed) return this.validityValue.valid
      ? { ...this.validityValue, valid: false, status: ContractValidityStatus.CANCELLED }
      : this.validityValue;
    try { await this.service.refreshAgreement(this); }
    catch (error) {
      this.terminate({ status: ContractValidityStatus.UNAVAILABLE, tokens: {}, valid: false, reasonCode: "source_unavailable" });
      throw error;
    }
    return this.validity;
  }

  async cancel(reason?: string): Promise<boolean> {
    this.terminate();
    const result = await bounded(this.service.cancelContract(this.contract, this.spec.localParticipant, { reason }), 5000, this.service.scheduler).catch(() => false);
    await this.lease.close();
    return result;
  }
  async close(): Promise<void> { this.terminate(); await this.lease.close(); }
}

export class ConcordService {
  private readonly coordinator: ConcordCoordinator;
  readonly scheduler: RuntimeScheduler;
  readonly contractSource: MaterializedStateSource;
  readonly tokenSource: MaterializedStateSource;
  private readonly view: ConcordView;
  private readonly leasesByContract = new Map<string, Set<ConcordParticipantLease>>();
  private readonly agreementsByContract = new Map<string, Set<ConcordAgreement>>();
  private readonly retiredOwners = new Map<string, Set<string>>();
  private readonly agreements = new Set<ConcordAgreement>();
  private readonly leases = new Set<ConcordParticipantLease>();
  private readonly cleanups = new Set<Promise<void>>();
  private readonly unsubscribe: (() => void)[] = [];
  private started = false;
  private closed = false;
  private closing?: Promise<void>;
  private readonly states = new StateCell<RuntimeState>({ status: "idle", resources: { agreements: 0, participants: 0, cleanups: 0 } });

  constructor(coordinator: ConcordCoordinator, options: { scheduler?: RuntimeScheduler } = {}) {
    this.coordinator = coordinator;
    this.scheduler = options.scheduler ?? systemScheduler;
    this.contractSource = sourceFor(coordinator.contractStore(), this.scheduler);
    this.tokenSource = sourceFor(coordinator.tokenStore(), this.scheduler);
    this.contractSource.retain(); this.tokenSource.retain();
    this.view = concordViewFor(this.contractSource, this.tokenSource);
  }

  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: RuntimeState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.scheduler });
  }
  private updateState(): void {
    if (this.closed) return;
    const stale = [this.contractSource.snapshot, this.tokenSource.snapshot].find(snapshot => !snapshot.current);
    this.states.set({ status: !this.started ? "idle" : stale ? "unavailable" : "ready",
      resources: { agreements: this.agreements.size, participants: this.leases.size, cleanups: this.cleanups.size },
      ...(this.started && stale ? { failure: { code: "concord_source_unavailable", message: stale.reason ?? "Concord sources are not current" } } : {}) });
  }

  async start(): Promise<void> {
    if (this.closed) throw new StateUnavailable("Concord runtime is closed");
    if (!this.started) {
      this.started = true;
      this.states.set({ ...this.state, status: "starting" });
      this.unsubscribe.push(this.view.subscribe(changed => this.observeAuthority(changed)));
    }
    try {
      await Promise.all([this.contractSource.start(), this.tokenSource.start()]);
      this.contractSource.requireCurrent(); this.tokenSource.requireCurrent(); this.updateState();
    } catch (error) { this.updateState(); throw error; }
  }

  private cachedValidity(contract: ContractHandle, sessions: Record<string, string> = {}): ContractValidity {
    return this.view.validate(contract, sessions);
  }

  get viewStats() { return this.view.stats; }

  private observeAuthority(changed?: ReadonlySet<string>): void {
    const leases = changed ? [...changed].flatMap(key => [...(this.leasesByContract.get(key) ?? [])]) : [...this.leases];
    for (const lease of leases) {
      const current = this.cachedValidity(lease.contract, { [lease.participant]: lease.sessionId });
      const observed = current.tokens[lease.participant];
      if (agreementSuccessorStatus(current.status) || current.status === ContractValidityStatus.UNAVAILABLE ||
        (observed && lease.token && !participantHandleMatches(lease.token, observed))) lease.terminate();
    }
    const agreements = changed ? [...changed].flatMap(key => [...(this.agreementsByContract.get(key) ?? [])]) : [...this.agreements];
    for (const agreement of agreements) agreement.observe(this.cachedValidity(agreement.contract, {
      ...(typeof agreement.spec.currentSessions === "object" ? agreement.spec.currentSessions : {}),
      [agreement.spec.localParticipant]: agreement.spec.localSessionId,
    }));
    this.updateState();
  }

  holdLease(lease: ConcordParticipantLease): void {
    if (this.closed) lease.terminate();
    else {
      this.leases.add(lease);
      let set = this.leasesByContract.get(lease.contract.key);
      if (!set) this.leasesByContract.set(lease.contract.key, set = new Set());
      set.add(lease);
    }
    this.updateState();
  }
  trackCleanup(cleanup: Promise<void>): void {
    this.cleanups.add(cleanup);
    this.updateState();
    void cleanup.finally(() => { this.cleanups.delete(cleanup); this.updateState(); }).catch(() => {});
  }
  releaseLease(lease: ConcordParticipantLease): void {
    if (lease.hasOwnedToken) {
      let retired = this.retiredOwners.get(lease.contract.key);
      if (!retired) this.retiredOwners.set(lease.contract.key, retired = new Set());
      retired.add(lease.participant);
    }
    this.leases.delete(lease);
    const set = this.leasesByContract.get(lease.contract.key); set?.delete(lease);
    if (!set?.size) this.leasesByContract.delete(lease.contract.key);
    for (const agreement of [...this.agreements]) if (agreement.lease === lease) agreement.terminate();
    this.updateState();
  }
  forgetAgreement(agreement: ConcordAgreement): void {
    this.agreements.delete(agreement);
    const set = this.agreementsByContract.get(agreement.contract.key); set?.delete(agreement);
    if (!set?.size) this.agreementsByContract.delete(agreement.contract.key);
    this.updateState();
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    // Capture both before termination unregisters leases. Revoke every owner
    // synchronously, then join their cleanup under one shutdown deadline.
    const agreements = [...this.agreements];
    const leases = [...this.leases];
    const closing = [...agreements.map(agreement => agreement.close()), ...leases.map(lease => lease.close())];
    for (const unsubscribe of this.unsubscribe) unsubscribe();
    this.view.release();
    this.states.set({ status: "closed", resources: { agreements: 0, participants: 0, cleanups: this.cleanups.size } }); this.states.close();
    this.closing = bounded(Promise.allSettled([...closing, ...this.cleanups, this.contractSource.release(), this.tokenSource.release()]), 5000, this.scheduler).then(() => {}, () => {});
    return this.closing;
  }

  async ensureAgreement(spec: ConcordAgreementSpec, options: { signal?: AbortSignal; onCreated?: (agreement: ConcordAgreement) => void } = {}): Promise<ConcordAgreement> {
    await this.start();
    if (options.signal?.aborted) throw new StateUnavailable("Agreement owner is closed");
    const normalized = normalizeAgreementSpec(spec);
    const [contract, initialValidity] = await this.selectOrCreateAgreementContract(normalized);
    const lease = new ConcordParticipantLease(this, { contract, participant: normalized.localParticipant, sessionId: normalized.localSessionId, ownsProposal: true });
    const agreement = new ConcordAgreement(this, { spec: normalized, contract, lease, validity: initialValidity });
    const terminate = () => agreement.terminate();
    options.signal?.addEventListener("abort", terminate, { once: true });
    agreement.signal.addEventListener("abort", () => options.signal?.removeEventListener("abort", terminate), { once: true });
    this.agreements.add(agreement);
    let owners = this.agreementsByContract.get(contract.key);
    if (!owners) this.agreementsByContract.set(contract.key, owners = new Set());
    owners.add(agreement);
    this.updateState();
    try {
      options.onCreated?.(agreement);
      if (this.closed || options.signal?.aborted) throw new StateUnavailable("Agreement owner is closed");
      await agreement.refresh();
      if (this.closed) { await agreement.close(); throw new StateUnavailable("Concord runtime is closed"); }
      return agreement;
    } catch (error) { await agreement.close(); throw error; }
  }

  participantManager(options: {
    participant: string;
    sessionId: string;
    acceptContract: (contract: ContractHandle, record: ContractRecord) => boolean | Promise<boolean>;
    currentSessions?: (contract: ContractHandle) => Record<string, string> | Promise<Record<string, string>>;
    profile?: string;
    refreshIntervalSeconds?: number;
    reconcileIntervalSeconds?: number;
    contractSortKey?: (contract: ContractHandle) => string | number | [number, string];
    cancelTerminalStatuses?: ContractValidityStatus[];
    onError?: (error: unknown) => void;
    onValid?: (managed: ConcordManagedContract) => void;
  }): ConcordParticipantManager {
    return new ConcordParticipantManager({ concord: this, ...options });
  }

  async createContract(
    participants: string[],
    options: Parameters<ConcordCoordinator["createContract"]>[1] = {},
  ): Promise<ContractHandle> {
    await this.start();
    const handle = await this.coordinator.createContract(participants, options);
    await this.contractSource.observed(handle.revision);
    return handle;
  }

  async getContract(pointer: ContractPointer): Promise<ContractHandle | null> {
    return this.coordinator.getContract(pointer);
  }

  async contractRecord(contract: ContractHandle): Promise<ContractRecord | null> {
    await this.start();
    return this.view.record(contract);
  }

  async contracts(profile?: string, options: { contractId?: string; participant?: string; state?: ContractState; pointer?: ContractPointer } = {}): Promise<ContractHandle[]> {
    await this.start();
    return this.view.handles({ ...options, profile });
  }

  async *watchContracts(options: ConcordFilters & { signal?: AbortSignal } = {}): AsyncIterable<ConcordViewSnapshot> {
    await this.start();
    yield* this.view.watch(options, options);
  }

  validateExact(contract: ContractHandle, options: { currentSessions?: Record<string, string> } = {}): Promise<ContractValidity> {
    return this.coordinator.validate(contract, options);
  }

  async cancelContract(
    contract: ContractHandle,
    participant: string,
    options: { reason?: string } = {},
  ): Promise<boolean> {
    const cancelled = await this.coordinator.cancel(contract, participant, options);
    if (cancelled && this.contractSource.snapshot.current) {
      const entry = await this.coordinator.contractStore().get(contract.key);
      if (entry) await this.contractSource.observed(entry.revision);
    }
    if (cancelled) this.retiredOwners.delete(contract.key);
    return cancelled;
  }

  /** @deprecated Maintenance uses a dedicated ConcordReaperService capability. */
  async maintenanceCancelContract(
    contract: ContractHandle,
    options: { reason?: string; now?: string } = {},
  ): Promise<boolean> {
    throw new ValidationError("Maintenance requires a dedicated ConcordReaperService store capability");
  }

  async validate(
    contract: ContractHandle,
    options: { currentSessions?: Record<string, string> } = {},
  ): Promise<ContractValidity> {
    try { await this.start(); }
    catch (error) {
      if (error instanceof StateUnavailable) return evaluateContract({ pointer: contract, contract: null, tokens: {}, current: false });
      throw error;
    }
    return this.cachedValidity(contract, { ...options.currentSessions });
  }

  async attach(
    contract: ContractHandle,
    participant: string,
    sessionId: string,
    options: { tokenId?: string } = {},
  ): Promise<ParticipantHandle> {
    await this.start();
    const handle = await this.coordinator.attach(contract, participant, sessionId, options);
    await this.tokenSource.observed(handle.revision);
    const entry = await this.coordinator.contractStore().get(contract.key);
    if (entry) await this.contractSource.observed(entry.revision);
    return handle;
  }

  async refreshToken(handle: ParticipantHandle): Promise<ParticipantHandle> {
    const refreshed = await this.coordinator.refresh(handle);
    if (this.tokenSource.snapshot.current) await this.tokenSource.observed(refreshed.revision);
    return refreshed;
  }

  async withdrawToken(handle: ParticipantHandle): Promise<void> {
    const revision = await this.coordinator.withdraw(handle);
    if (this.tokenSource.snapshot.current) {
      if (typeof revision === "number") await this.tokenSource.observed(revision);
      else await this.tokenSource.removed(handle.key, handle.revision);
    }
  }

  async withdrawPreparedToken(contract: ContractHandle, participant: string, sessionId: string, tokenId: string): Promise<void> {
    const key = concordParticipantTokenKey({ ...contract, participant });
    const entry = await this.coordinator.tokenStore().get(key);
    if (!entry) return;
    const token = mutationToken(entry);
    if (token.tokenId !== tokenId || token.sessionId !== sessionId || token.contractId !== contract.contractId || token.generation !== contract.generation || token.participant !== participant || token.termsHash !== contract.termsHash) return;
    await this.withdrawToken(participantHandle(key, token, entry.revision));
  }

  participantLease(options: {
    contract: ContractHandle;
    participant: string;
    sessionId: string;
  }): ConcordParticipantLease {
    return new ConcordParticipantLease(this, options);
  }

  async refreshAgreement(agreement: ConcordAgreement): Promise<ContractValidity> {
    if (agreement.closed) {
      throw new StateConflict("Concord agreement is closed");
    }
    const sessions = await agreementCurrentSessions(agreement.spec);
    const observe = (value: ContractValidity): ContractValidity => { agreement.observe(value); return value; };
    let current = this.cachedValidity(agreement.contract, sessions);
    if (current.status === ContractValidityStatus.UNAVAILABLE) {
      return observe(current);
    }
    if (agreementSuccessorStatus(current.status)) {
      return observe(current);
    }
    const existing = current.tokens[agreement.spec.localParticipant];
    if (existing !== undefined && existing.sessionId !== agreement.spec.localSessionId) {
      return observe(validity(ContractValidityStatus.SESSION_MISMATCH, {
        contract: current.contract,
        tokens: current.tokens,
        reason: agreement.spec.localParticipant,
      }));
    }
    if (existing !== undefined) {
      if (agreement.localToken === null) {
        await agreement.cancel(CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON);
        return this.validate(agreement.contract, { currentSessions: sessions });
      }
      try {
        agreement.lease.adopt(existing);
      } catch (error) {
        if (!(error instanceof ValidationError)) {
          throw error;
        }
        await agreement.cancel(CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON);
        return this.validate(agreement.contract, { currentSessions: sessions });
      }
    }
    if (agreement.localToken === null) {
      await agreement.lease.attachOrRefresh();
      current = this.cachedValidity(agreement.contract, sessions);
    }
    // Publish from the latest ordered snapshot in this synchronous turn. An
    // earlier read must never overwrite a newer watch observation after await.
    return observe(current);
  }

  coordinatorForTesting(): ConcordCoordinator {
    return this.coordinator;
  }

  private async selectOrCreateAgreementContract(
    spec: ConcordAgreementSpec,
  ): Promise<[ContractHandle, ContractValidity]> {
    await this.bootstrap(spec.localParticipant, spec.localSessionId, { profile: spec.profile });
    const sessions = await agreementCurrentSessions(spec);
    const contract = await this.createContract(spec.participants, {
      generation: 1,
      profile: spec.profile,
      terms: spec.terms,
      createdBy: spec.createdBy ?? spec.localParticipant,
      supersedes: spec.supersedes,
      participantSessions: {
        ...(spec.participantSessions ?? {}),
        [spec.localParticipant]: spec.localSessionId,
      },
    });
    return [contract, await this.validate(contract, { currentSessions: sessions })];
  }

  /** Resolve proven lost ownership before proposals, readiness or advertising. */
  async bootstrap(participant: string, sessionId: string, options: { profile?: string; exclusive?: boolean } = {}): Promise<void> {
    await this.start();
    await bounded((async () => {
      for (const contract of this.view.bootstrapCandidates({ profile: options.profile, participant, state: ContractState.OPEN })) {
        const current = await this.validateExact(contract);
        if (current.status === ContractValidityStatus.UNAVAILABLE || current.status === ContractValidityStatus.INVALID_CONTRACT) throw new StateUnavailable("Bootstrap authority is unresolved");
        const lostHere = this.retiredOwners.get(contract.key)?.has(participant);
        if (!lostHere && (current.valid || current.status === ContractValidityStatus.NOT_YET_FULFILLED)) {
          const token = current.tokens[participant];
          const assertedSession = current.contract?.participantSessions[participant];
          if (options.exclusive && (token || (assertedSession && assertedSession !== sessionId))) {
            throw new StateConflict("Another lifetime owns participation", "participant_owned");
          }
          continue;
        }
        if (current.status === ContractValidityStatus.CANCELLED || current.status === ContractValidityStatus.MISSING_CONTRACT) {
          this.retiredOwners.delete(contract.key); continue;
        }
        await this.cancelContract(contract, participant, { reason: "bootstrap_authority_lost" });
        const confirmed = await this.validateExact(contract);
        if (confirmed.status !== ContractValidityStatus.CANCELLED && confirmed.status !== ContractValidityStatus.MISSING_CONTRACT) {
          throw new StateUnavailable("Bootstrap cleanup is incomplete");
        }
      }
    })(), 5000, this.scheduler);
  }
}

export class ConcordParticipantManager {
  readonly participant: string;
  readonly sessionId: string;
  readonly profile?: string;

  private readonly concord: ConcordService;
  private readonly acceptContract: (contract: ContractHandle, record: ContractRecord) => boolean | Promise<boolean>;
  private readonly currentSessions?: (contract: ContractHandle) => Record<string, string> | Promise<Record<string, string>>;
  private readonly cancelTerminalStatuses: Set<ContractValidityStatus>;
  private readonly reconcileIntervalSeconds: number;
  private readonly contractSortKey?: (contract: ContractHandle) => string | number | [number, string];
  private readonly onError?: (error: unknown) => void;
  private readonly onValid?: (managed: ConcordManagedContract) => void;
  private readonly announced = new Set<string>();
  private readonly announcing = new Set<string>();
  private readonly announceDirty = new Set<string>();
  private unsubscribeTokens?: () => void;
  private readonly states = new StateCell<RuntimeState>({ status: "idle", resources: { participants: 0, reconciliations: 0 } });
  private closing?: Promise<void>;
  private managed = new Map<string, ConcordManagedContract>();
  private leases = new Map<string, ConcordParticipantLease>();
  private readonly retired = new Set<string>();
  private closed = false;
  private selectionPending = false;
  private selectionTask?: Promise<void>;
  private readonly pointerWork = new Map<string, { contract: ContractHandle; pending: boolean; task: Promise<void> }>();
  private watchAbort: AbortController | null = null;
  private watchTask?: Promise<void>;

  constructor(options: {
    concord: ConcordService;
    participant: string;
    sessionId: string;
    acceptContract: (contract: ContractHandle, record: ContractRecord) => boolean | Promise<boolean>;
    currentSessions?: (contract: ContractHandle) => Record<string, string> | Promise<Record<string, string>>;
    profile?: string;
    refreshIntervalSeconds?: number;
    reconcileIntervalSeconds?: number;
    contractSortKey?: (contract: ContractHandle) => string | number | [number, string];
    cancelTerminalStatuses?: ContractValidityStatus[];
    onError?: (error: unknown) => void;
    /** Delivered once per locally owned valid agreement. Handlers must not block. */
    onValid?: (managed: ConcordManagedContract) => void;
  }) {
    this.concord = options.concord;
    this.participant = endpointAddress(options.participant);
    this.sessionId = requireText(options.sessionId, "Concord session id");
    this.acceptContract = options.acceptContract;
    this.currentSessions = options.currentSessions;
    this.profile = options.profile;
    this.cancelTerminalStatuses = new Set(options.cancelTerminalStatuses ?? []);
    this.reconcileIntervalSeconds = positiveInterval(options.reconcileIntervalSeconds ?? 300, "reconcileIntervalSeconds");
    this.contractSortKey = options.contractSortKey;
    this.onError = options.onError;
    this.onValid = options.onValid;
  }

  managedContracts(): ConcordManagedContract[] {
    return immutable([...this.managed.values()].sort((left, right) =>
      left.contract.key.localeCompare(right.contract.key),
    ));
  }

  get state() { return this.states.current; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: RuntimeState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.concord.scheduler });
  }
  private updateState(error?: unknown): void {
    if (this.closed) return;
    const current = this.concord.contractSource.snapshot.current && this.concord.tokenSource.snapshot.current;
    this.states.set({ status: !this.watchAbort ? "idle" : current ? "ready" : "unavailable",
      resources: { participants: [...this.leases.values()].filter(lease => !lease.closed).length, reconciliations: this.pointerWork.size },
      ...(error === undefined ? !current && this.watchAbort ? { failure: { code: "concord_source_unavailable", message: "Participant sources are not current" } } : {}
        : { failure: { code: "participant_reconciliation_failed", message: String(error) } }) });
  }

  managedContract(contract: ContractHandle): ConcordManagedContract | null {
    return this.managed.get(contract.key) ?? null;
  }

  start(): void {
    if (this.closed || this.watchAbort !== null) {
      return;
    }
    this.scheduleReconcile();
    this.startWatchLoop();
    this.unsubscribeTokens = this.concord.tokenSource.subscribe(() => {
      for (const managed of this.managed.values()) this.announceValid(managed);
      this.updateState();
    });
    this.updateState();
  }

  /** Explicit domain-policy wakeup; independent pointers continue in parallel. */
  wake(): void { this.scheduleReconcile(); }

  aclose(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    this.unsubscribeTokens?.();
    this.watchAbort?.abort();
    this.watchAbort = null;
    const closing = [...this.leases.values()].map(lease => lease.close());
    this.states.set({ status: "closed", resources: { participants: 0, reconciliations: 0 } }); this.states.close();
    this.managed.clear();
    this.leases.clear();
    this.closing = bounded(Promise.allSettled([this.watchTask, this.selectionTask, ...[...this.pointerWork.values()].map(work => work.task), ...closing]), 5000, this.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }

  async release(contract: ContractHandle | string): Promise<void> {
    const key = typeof contract === "string" ? contract : contract.key;
    this.retired.add(key);
    await this.releaseInternal(key);
  }

  async releaseAll(): Promise<void> {
    const keys = new Set([...this.leases.keys(), ...this.pointerWork.keys(), ...this.managed.keys()]);
    await Promise.all([...keys].map(key => this.release(key)));
  }

  async reconcile(): Promise<ConcordManagedContract[]> {
    await this.select();
    await Promise.all([...this.pointerWork.values()].map(work => work.task));
    return this.managedContracts();
  }

  /** Wait only for this pointer; a blocked policy for another agreement is unrelated. */
  async reconcileOne(contract: ContractHandle): Promise<ConcordManagedContract | null> {
    if (this.closed) return null;
    await this.enqueue(contract);
    return this.managedContract(contract);
  }

  private async selectNow(): Promise<void> {
    const contracts = this.sortedContracts(await this.concord.contracts(this.profile, {
      participant: this.participant,
      state: ContractState.OPEN,
    }));
    if (this.closed) return;
    const present = new Set(contracts.map(contract => contract.key));
    // Revocation is synchronous in lease.close; remote cleanup is independent.
    for (const key of this.leases.keys()) if (!present.has(key)) void this.releaseInternal(key).catch(error => this.reportError(error));
    for (const contract of contracts) void this.enqueue(contract).catch(error => this.reportError(error));
  }

  private enqueue(contract: ContractHandle): Promise<void> {
    const existing = this.pointerWork.get(contract.key);
    if (existing) { existing.contract = contract; existing.pending = true; return existing.task; }
    const work = { contract, pending: true, task: Promise.resolve() };
    this.pointerWork.set(contract.key, work);
    this.updateState();
    work.task = (async () => {
      while (work.pending && !this.closed) {
        work.pending = false;
        const managed = await this.reconcileContract(work.contract);
        if (!this.closed && managed) { this.managed.set(contract.key, managed); this.announceValid(managed); }
        else this.managed.delete(contract.key);
      }
    })().finally(() => { if (this.pointerWork.get(contract.key) === work) this.pointerWork.delete(contract.key); this.updateState(); });
    return work.task;
  }

  private announceValid(managed: ConcordManagedContract): void {
    const key = managed.contract.key;
    if (!this.onValid || this.closed || this.announced.has(key)) return;
    if (this.announcing.has(key)) { this.announceDirty.add(key); return; }
    this.announcing.add(key);
    void this.concord.validate(managed.contract).then(validity => {
      const lease = this.leases.get(key);
      if (!this.closed && lease && !lease.closed && validity.valid && !this.announced.has(key)) {
        this.announced.add(key);
        this.onValid!(immutable({ ...managed, validity }));
      }
    }).catch(error => this.reportError(error)).finally(() => {
      this.announcing.delete(key);
      if (this.announceDirty.delete(key)) {
        const latest = this.managed.get(key);
        if (latest) this.announceValid(latest);
      }
    });
  }

  async validate(
    contract: ContractHandle,
    options: { currentSessions?: Record<string, string> } = {},
  ): Promise<ContractValidity> {
    const sessions = { ...(options.currentSessions ?? {}), [this.participant]: this.sessionId };
    return this.concord.validate(contract, { currentSessions: sessions });
  }

  authority(contract: ContractHandle): { signal: AbortSignal; check: () => Promise<ContractValidity> } {
    const lease = this.leases.get(contract.key);
    if (!lease || lease.closed || !lease.token) throw new StateUnavailable("No locally owned participant authority");
    return { signal: lease.signal, check: async () => {
      if (lease.closed) throw new StateUnavailable("Participant authority terminated");
      const current = await this.validate(contract);
      const held = lease.token;
      if (lease.closed || !held || !current.valid || !current.tokens[this.participant] || !participantHandleMatches(held, current.tokens[this.participant]!)) {
        lease.terminate(); throw new StateUnavailable("Participant authority terminated");
      }
      return current;
    } };
  }

  async cancel(contract: ContractHandle, options: { reason?: string } = {}): Promise<boolean> {
    return this.concord.cancelContract(contract, this.participant, options);
  }

  private scheduleReconcile(): void {
    void this.select().catch(error => this.reportError(error));
  }

  private select(): Promise<void> {
    if (this.closed) return Promise.resolve();
    this.selectionPending = true;
    if (this.selectionTask) return this.selectionTask;
    this.selectionTask = (async () => {
      while (this.selectionPending && !this.closed) {
        this.selectionPending = false;
        await this.selectNow();
      }
    })()
      .finally(() => {
        this.selectionTask = undefined;
        if (this.selectionPending && !this.closed) this.scheduleReconcile();
      });
    return this.selectionTask;
  }

  private startWatchLoop(): void {
    const abort = new AbortController();
    this.watchAbort = abort;
    this.watchTask = this.watchLoop(abort.signal).catch((error: unknown) => {
      if (!abort.signal.aborted) {
        this.reportError(error);
      }
    });
  }

  private async watchLoop(signal: AbortSignal): Promise<void> {
    while (!this.closed && !signal.aborted) {
      try {
        for await (const _change of this.concord.watchContracts({ signal, profile: this.profile, participant: this.participant })) {
          if (this.closed || signal.aborted) {
            return;
          }
          this.updateState();
          this.scheduleReconcile();
        }
        return;
      } catch (error) {
        if (this.closed || signal.aborted) {
          return;
        }
        this.reportError(error);
        await sleep(Math.min(this.reconcileIntervalSeconds, 5), signal, this.concord.scheduler);
      }
    }
  }

  private sortedContracts(contracts: ContractHandle[]): ContractHandle[] {
    if (this.contractSortKey === undefined) {
      return contracts;
    }
    return [...contracts].sort((left, right) =>
      compareSortKey(this.contractSortKey!(left), this.contractSortKey!(right)),
    );
  }

  private async reconcileContract(contract: ContractHandle): Promise<ConcordManagedContract | null> {
    if (this.retired.has(contract.key)) return null;
    if (!contract.participants.includes(this.participant)) {
      await this.releaseInternal(contract.key);
      return null;
    }
    const record = await this.concord.contractRecord(contract);
    if (record === null) {
      await this.releaseInternal(contract.key);
      return null;
    }
    if (this.profile !== undefined && record.profile !== this.profile) {
      await this.releaseInternal(contract.key);
      return null;
    }
    if (record.state === ContractState.CANCELLED) {
      await this.releaseInternal(contract.key);
      return null;
    }
    let lease = this.leases.get(contract.key);
    const observed = await this.concord.validate(contract);
    const storedToken = observed.tokens[this.participant];
    if (!lease?.token && (storedToken || (record.participantSessions[this.participant] && record.participantSessions[this.participant] !== this.sessionId))) {
      // An empty local registry cannot establish lost ownership, even if the
      // stored session happens to equal ours. Do not adopt or cancel it.
      this.reportError(new StateConflict("Participant already belongs to another lifetime", "participant_owned"));
      return null;
    }
    if (!(await this.acceptContract(contract, record))) {
      await this.releaseInternal(contract.key);
      return null;
    }
    if (this.closed || this.retired.has(contract.key)) return null;
    const sessions = {
      ...(this.currentSessions === undefined ? {} : await this.currentSessions(contract)),
      [this.participant]: this.sessionId,
    };
    let contractValidity = await this.concord.validate(contract, { currentSessions: sessions });
    const existing = contractValidity.tokens[this.participant];
    if (this.closed || this.retired.has(contract.key)) return null;
    if (terminalManagedStatus(contractValidity.status) || contractValidity.status === ContractValidityStatus.UNAVAILABLE) {
      if (this.cancelTerminalStatuses.has(contractValidity.status)) {
        const confirmed = lease?.token ? contractValidity : await this.concord.validateExact(contract);
        if (terminalManagedStatus(confirmed.status)) await this.cancel(contract, { reason: `concord_managed_${confirmed.status}` });
      }
      await this.releaseInternal(contract.key);
      return null;
    }
    if (lease === undefined) {
      lease = this.concord.participantLease({
        contract,
        participant: this.participant,
        sessionId: this.sessionId,
      });
      this.leases.set(contract.key, lease);
    }
    if (existing !== undefined) {
      if (existing.sessionId !== this.sessionId) {
        await this.releaseInternal(contract.key);
        return null;
      }
      if (lease.token === null) {
        await this.cancelAndReleaseLostParticipantToken(contract);
        return null;
      }
      try {
        lease.adopt(existing);
      } catch (error) {
        if (!(error instanceof ValidationError)) {
          throw error;
        }
        await this.cancelAndReleaseLostParticipantToken(contract);
        return null;
      }
    }
    let token = lease.token;
    if (token === null) {
      try {
        token = await lease.attachOrRefresh();
      } catch {
        await this.releaseInternal(contract.key);
        return null;
      }
      contractValidity = await this.concord.validate(contract, { currentSessions: sessions });
    }
    if (this.closed || this.retired.has(contract.key)) { await this.releaseInternal(contract.key); return null; }
    const managed: ConcordManagedContract = {
      contract,
      record: contractValidity.contract ?? record,
      validity: contractValidity,
      token,
    };
    return immutable(managed);
  }

  private async releaseInternal(key: string): Promise<void> {
    this.managed.delete(key);
    const lease = this.leases.get(key);
    if (lease !== undefined) {
      this.leases.delete(key);
      await lease.close();
    }
  }

  private async cancelAndReleaseLostParticipantToken(contract: ContractHandle): Promise<void> {
    try {
      await this.cancel(contract, { reason: CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON });
    } catch (error) {
      if (!(error instanceof StateConflict || error instanceof StateUnavailable || error instanceof ValidationError)) {
        throw error;
      }
    }
    await this.releaseInternal(contract.key);
  }

  private reportError(error: unknown): void {
    this.updateState(error);
    try { this.onError?.(error); } catch { /* Diagnostics cannot stop reconciliation. */ }
  }
}

export const STALE_OPEN_CONTRACT_STATUSES = new Set<ContractValidityStatus>([
  ContractValidityStatus.NOT_YET_FULFILLED,
  ContractValidityStatus.MISSING_TOKEN,
  ContractValidityStatus.INVALID_TOKEN,
  ContractValidityStatus.SESSION_MISMATCH,
  ContractValidityStatus.TERMS_HASH_MISMATCH,
  ContractValidityStatus.GENERATION_MISMATCH,
  ContractValidityStatus.INVALID_CONTRACT,
]);

export { ConcordReaperService, CONCORD_TOKEN_CLEANUP_SCHEMA_ID, concordTokenCleanupKey, validateTokenCleanupRecord } from "./concord-maintenance.ts";
export type { ConcordMaintenanceAudit, ConcordReaperOptions, ConcordTokenCleanupRecord } from "./concord-maintenance.ts";

function contractHandle(key: string, record: ContractRecord, revision: number): ContractHandle {
  return immutable({
    key,
    contractId: record.contractId,
    generation: record.generation,
    participants: [...record.participants],
    attachedParticipants: [...record.attachedParticipants],
    revision,
    state: record.state,
    ...(record.profile === undefined ? {} : { profile: record.profile }),
    ...(record.termsHash === undefined ? {} : { termsHash: record.termsHash }),
  });
}

function participantHandle(
  key: string,
  record: ParticipantTokenRecord,
  revision: number,
): ParticipantHandle {
  return immutable({
    key,
    contractId: record.contractId,
    generation: record.generation,
    participant: record.participant,
    sessionId: record.sessionId,
    tokenId: record.tokenId,
    revision,
    refreshSeq: record.refreshSeq,
    ttlSeconds: record.ttlSeconds,
    ...(record.termsHash === undefined ? {} : { termsHash: record.termsHash }),
  });
}

function validity(
  status: ContractValidityStatus,
  options: {
    contract?: ContractRecord;
    tokens?: Record<string, ParticipantHandle>;
    reason?: string;
  } = {},
): ContractValidity {
  return {
    status,
    ...(options.contract === undefined ? {} : { contract: options.contract }),
    tokens: options.tokens ?? {},
    ...(options.reason === undefined ? {} : { reason: options.reason }),
    valid: status === ContractValidityStatus.VALID,
  };
}

function validateEndpointList(value: unknown, fieldName: string): string[] {
  if (!Array.isArray(value)) {
    throw new ValidationError(`${fieldName} must be an array`);
  }
  return value.map((item) => endpointAddress(requireText(item, fieldName)));
}

function requireCanonicalUnique(values: string[], fieldName: string): void {
  if (new Set(values).size !== values.length) {
    throw new ValidationError(`${fieldName} must be unique`);
  }
  const sorted = [...values].sort();
  if (sorted.some((value, index) => value !== values[index])) {
    throw new ValidationError(`${fieldName} must be canonicalized`);
  }
}

function nonNegativeInteger(value: unknown, fieldName: string): number {
  const number = value instanceof JsonNumber ? Number(value) : value;
  if (!Number.isSafeInteger(number) || Number(number) < 0) {
    throw new ValidationError(`${fieldName} must be non-negative`);
  }
  return Number(value);
}

function setOptionalText(
  target: object,
  key: string,
  value: unknown,
  fieldName: string,
): void {
  if (value !== undefined && value !== null) {
    (target as Record<string, unknown>)[key] = requireText(value, fieldName);
  }
}

function setOptionalEndpoint(
  target: object,
  key: string,
  value: unknown,
): void {
  if (value !== undefined && value !== null) {
    (target as Record<string, unknown>)[key] = endpointAddress(requireText(value, key));
  }
}

function tokenMatchesAttachRequest(
  token: ParticipantTokenRecord,
  record: ContractRecord,
  participant: string,
  sessionId: string,
  tokenId: string | undefined,
): boolean {
  return (
    token.contractId === record.contractId &&
    token.generation === record.generation &&
    token.participant === participant &&
    token.sessionId === sessionId &&
    tokenId !== undefined &&
    token.tokenId === tokenId &&
    token.termsHash === record.termsHash
  );
}

function tokenMatchesHandle(token: ParticipantTokenRecord, handle: ParticipantHandle): boolean {
  return (
    token.contractId === handle.contractId &&
    token.generation === handle.generation &&
    token.participant === handle.participant &&
    token.sessionId === handle.sessionId &&
    token.tokenId === handle.tokenId &&
    token.termsHash === handle.termsHash
  );
}

function participantHandleMatches(current: ParticipantHandle, updated: ParticipantHandle): boolean {
  return (
    updated.contractId === current.contractId &&
    updated.generation === current.generation &&
    updated.participant === current.participant &&
    updated.sessionId === current.sessionId &&
    updated.tokenId === current.tokenId &&
    updated.termsHash === current.termsHash
  );
}

function tokenValidityStatus(
  token: ParticipantTokenRecord,
  contract: ContractRecord,
  participant: string,
  currentSessions: Record<string, string> | undefined,
): ContractValidityStatus | null {
  if (token.contractId !== contract.contractId) {
    return ContractValidityStatus.INVALID_TOKEN;
  }
  if (token.generation !== contract.generation) {
    return ContractValidityStatus.GENERATION_MISMATCH;
  }
  if (token.participant !== participant) {
    return ContractValidityStatus.INVALID_TOKEN;
  }
  if (contract.termsHash !== undefined && token.termsHash !== contract.termsHash) {
    return ContractValidityStatus.TERMS_HASH_MISMATCH;
  }
  const currentSession = currentSessions?.[participant];
  const frozenSession = contract.participantSessions[participant];
  if (
    (frozenSession !== undefined && token.sessionId !== frozenSession) ||
    (currentSession !== undefined && token.sessionId !== currentSession)
  ) {
    return ContractValidityStatus.SESSION_MISMATCH;
  }
  return null;
}

function normalizeAgreementSpec(spec: ConcordAgreementSpec): ConcordAgreementSpec {
  const participants = spec.participants.map(endpointAddress).sort();
  requireCanonicalUnique(participants, "Concord agreement participants");
  const localParticipant = endpointAddress(spec.localParticipant);
  if (!participants.includes(localParticipant)) {
    throw new ValidationError("localParticipant must be named by participants");
  }
  return {
    ...spec,
    currentSessions: typeof spec.currentSessions === "object" ? { ...spec.currentSessions } : spec.currentSessions,
    participants,
    localParticipant,
    localSessionId: requireText(spec.localSessionId, "Concord agreement session id"),
    participantSessions: Object.fromEntries(
      Object.entries(spec.participantSessions ?? {}).map(([participant, session]) => [
        endpointAddress(participant),
        requireText(session, "Concord participant session id"),
      ]),
    ),
    createdBy: spec.createdBy === undefined ? localParticipant : endpointAddress(spec.createdBy),
    terms: spec.terms === undefined ? undefined : cloneJson(spec.terms),
    supersedes: spec.supersedes === undefined ? undefined : validateContractPointer(spec.supersedes),
  };
}

async function agreementCurrentSessions(spec: ConcordAgreementSpec): Promise<Record<string, string>> {
  const sessions =
    spec.currentSessions === undefined
      ? {}
      : typeof spec.currentSessions === "function"
        ? await spec.currentSessions()
        : spec.currentSessions;
  return { ...sessions, [spec.localParticipant]: spec.localSessionId };
}

function agreementSuccessorStatus(status: ContractValidityStatus): boolean {
  const statuses: ContractValidityStatus[] = [
    ContractValidityStatus.CANCELLED,
    ContractValidityStatus.MISSING_CONTRACT,
    ContractValidityStatus.INVALID_CONTRACT,
    ContractValidityStatus.INVALID_TOKEN,
    ContractValidityStatus.MISSING_TOKEN,
    ContractValidityStatus.GENERATION_MISMATCH,
    ContractValidityStatus.SESSION_MISMATCH,
    ContractValidityStatus.TERMS_HASH_MISMATCH,
  ];
  return statuses.includes(status);
}

function terminalManagedStatus(status: ContractValidityStatus): boolean {
  return agreementSuccessorStatus(status);
}

function positiveInterval(value: number, fieldName: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new ValidationError(`${fieldName} must be greater than zero`);
  }
  return value;
}

function compareSortKey(
  left: string | number | [number, string],
  right: string | number | [number, string],
): number {
  if (Array.isArray(left) && Array.isArray(right)) {
    const numeric = left[0] - right[0];
    return numeric === 0 ? left[1].localeCompare(right[1]) : numeric;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  return String(left).localeCompare(String(right));
}

async function sleep(seconds: number, signal: AbortSignal, scheduler: RuntimeScheduler): Promise<void> {
  if (signal.aborted) return;
  await new Promise<void>(resolve => {
    const finish = () => { cancel(); signal.removeEventListener("abort", finish); resolve(); };
    const cancel = scheduler.schedule(seconds * 1000, finish);
    signal.addEventListener("abort", finish, { once: true });
  });
}


function recordToJson<T>(record: T): JsonObject {
  return cloneJson(record as unknown as JsonObject);
}
