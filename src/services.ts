import { AsyncQueue, bounded, immutable } from "./runtime.ts";
import { sourceFor, type MaterializedStateSource } from "./source.ts";
import {
  BeaconAdvertisement,
  BeaconService,
  type Candidate,
  type BeaconAdvertisementSpec,
} from "./beacon.ts";
import {
  ConcordAgreement,
  ConcordService,
  ContractState,
  ContractValidityStatus,
  canonicalJsonHash,
  type ContractHandle,
  type ContractRecord,
  type ContractValidity,
  type ConcordAgreementSpec,
} from "./concord.ts";
import { validateContractPointer, type ContractPointer } from "./authority.ts";
import { endpointAddress, endpointTarget, serviceAddress } from "./endpoint.ts";
import { ServiceUnavailable, StateConflict, StateUnavailable, ValidationError } from "./errors.ts";
import {
  cloneJson,
  requireJsonObject,
  requireText,
  type JsonObject,
  type JsonValue,
} from "./json.ts";
import { encodeKeyToken } from "./keys.ts";
import {
  entitySubject,
  type DeckrMessage,
  type EntitySubject,
} from "./lanes.ts";
import type { StateEntry, StateStore } from "./state.ts";

export const DEFAULT_SERVICE_CONTRACT_RECONCILE_SECONDS = 300;
export const DEFAULT_SERVICE_ADVERTISEMENT_REFRESH_SECONDS = 5;
export const DEFAULT_SERVICE_TOKEN_REFRESH_SECONDS = 5;
export const SERVICE_USE_INDEX_SCHEMA_ID = "dev.deckr.service-use-index.v1";
export const DEFAULT_SERVICE_USE_INDEX_STORE_NAME = "deckr_service_use_index_v1";

export const ServiceBackendStatus = Object.freeze({
  AVAILABLE: "available",
  DEGRADED: "degraded",
  UNAVAILABLE: "unavailable",
});
export type ServiceBackendStatus =
  (typeof ServiceBackendStatus)[keyof typeof ServiceBackendStatus];

export const AuthorizationDecision = Object.freeze({
  AUTHORIZED: "authorized",
  DENIED: "denied",
  NOT_APPLICABLE: "not_applicable",
});
export type AuthorizationDecision =
  (typeof AuthorizationDecision)[keyof typeof AuthorizationDecision];

export const ServiceExchangePattern = Object.freeze({
  ONE_WAY: "one_way",
  REQUEST_REPLY: "request_reply",
});
export type ServiceExchangePattern =
  (typeof ServiceExchangePattern)[keyof typeof ServiceExchangePattern];

export const ServiceMessageDirection = Object.freeze({
  CONSUMER_TO_SERVICE: "consumer_to_service",
  SERVICE_TO_CONSUMER: "service_to_consumer",
  BIDIRECTIONAL: "bidirectional",
});
export type ServiceMessageDirection =
  (typeof ServiceMessageDirection)[keyof typeof ServiceMessageDirection];

export const ServiceMessageIntent = Object.freeze({
  COMMAND: "command",
  QUERY: "query",
  EVENT: "event",
  NOTIFICATION: "notification",
});
export type ServiceMessageIntent =
  (typeof ServiceMessageIntent)[keyof typeof ServiceMessageIntent];

export const ServiceMessageStatus = Object.freeze({
  OK: "ok",
  REJECTED: "rejected",
  UNAVAILABLE: "unavailable",
  ERROR: "error",
});
export type ServiceMessageStatus =
  (typeof ServiceMessageStatus)[keyof typeof ServiceMessageStatus];

export const ServiceViewWriter = Object.freeze({
  SERVICE: "service",
  CONSUMER: "consumer",
});
export type ServiceViewWriter =
  (typeof ServiceViewWriter)[keyof typeof ServiceViewWriter];

export const SERVICE_MESSAGE = "serviceMessage";
const SERVICE_JSON_SCHEMA_CONTRACT_KEYS = new Set([
  "$ref",
  "allOf",
  "anyOf",
  "const",
  "enum",
  "items",
  "oneOf",
  "properties",
  "type",
]);

export interface ServiceOperationDefinition {
  description?: string;
}

export interface ServicePayloadSchema {
  schemaId: string;
  schema: JsonObject;
}

export interface ServiceMessageDefinition {
  operation?: string;
  intent: ServiceMessageIntent;
  exchangePattern: ServiceExchangePattern;
  direction: ServiceMessageDirection;
  paramsSchema?: ServicePayloadSchema;
  resultSchema?: ServicePayloadSchema;
  eventSchema?: ServicePayloadSchema;
  errorSchema?: ServicePayloadSchema;
}

export interface ServiceViewFamily {
  storeName: string;
  keyPrefix: string;
  writer: ServiceViewWriter;
}

export interface ServiceViewFamilyDeclaration {
  storeName: string;
  writer: ServiceViewWriter;
  /** Omit in a shared protocol to bind a concrete prefix for each service. */
  keyPrefix?: string;
}

export interface ServiceViewRef {
  storeName: string;
  key: string;
}

export interface ServiceProtocol {
  namespace: string;
  featureId: string;
  advertisementProfile: string;
  useProfile: string;
  operations: Record<string, ServiceOperationDefinition>;
  messages: Record<string, ServiceMessageDefinition>;
  viewFamilies: Record<string, ServiceViewFamilyDeclaration>;
}

export interface ServiceAdvertisementPayload {
  profile: string;
  serviceId: string;
  serviceEndpoint: string;
  serviceNamespace: string;
  sessionId: string;
  serviceUseProfile: string;
  backendStatus: ServiceBackendStatus;
  supportedOperations: string[];
  supportedMessages: Record<string, ServiceMessageDefinition>;
  views: Record<string, ServiceViewFamily>;
  diagnostics: JsonObject;
}

export interface ServiceUseTerms {
  profile: string;
  serviceUseScopeId: string;
  serviceId: string;
  serviceEndpoint: string;
  serviceNamespace: string;
  serviceSessionId: string;
  clientEndpoint: string;
  allowedOperations: string[];
  allowedViews: Record<string, string[]>;
}

export interface ServiceUseScopeIndexRecord {
  schema: typeof SERVICE_USE_INDEX_SCHEMA_ID;
  scopeId: string;
  contract: ContractPointer;
  termsHash: string;
  serviceEndpoint: string;
  serviceSessionId: string;
  clientEndpoint: string;
  clientSessionId: string;
  state: "candidate";
  createdAt: string;
  updatedAt: string;
  supersedes?: ContractPointer;
}

export interface ServiceDescriptor {
  candidate: Candidate;
  serviceId: string;
  namespace: string;
  endpoint: string;
  sessionId: string;
  advertisementProfile: string;
  useProfile: string;
  supportedOperations: Set<string>;
  supportedMessages: Record<string, ServiceMessageDefinition>;
  views: Record<string, ServiceViewFamily>;
  backendStatus: ServiceBackendStatus;
  diagnostics: JsonObject;
}

export interface ServiceUseLease {
  agreement: ConcordAgreement;
  descriptor: ServiceDescriptor;
  terms: ServiceUseTerms;
}

export interface ServiceMessageBody {
  serviceNamespace: string;
  name: string;
  intent: ServiceMessageIntent;
  exchangePattern: ServiceExchangePattern;
  params?: JsonObject;
  event?: JsonValue;
  status?: ServiceMessageStatus;
  result?: JsonValue;
  error?: ServiceError;
}

export interface ServiceError {
  code: string;
  message: string;
  diagnostics: JsonObject;
}

export interface ServiceViewAuthority {
  readonly signal: AbortSignal;
  assertActive(): Promise<void>;
}

export interface ServiceViewWriteContext {
  authority: ServiceViewAuthority;
  writer: ServiceViewWriter;
  serviceId: string;
  serviceNamespace: string;
  serviceEndpoint: string;
  serviceSessionId: string;
  consumerEndpoint: string;
  consumerSessionId: string;
  contract: ContractPointer;
  views: Record<string, ServiceViewFamily>;
}

export interface ServiceViewReadContext {
  authority: ServiceViewAuthority;
  reader: ServiceViewWriter;
  serviceId: string;
  serviceNamespace: string;
  serviceEndpoint: string;
  serviceSessionId: string;
  consumerEndpoint: string;
  consumerSessionId: string;
  contract: ContractPointer;
  views: Record<string, ServiceViewFamily>;
}

export interface ServiceViewEntry {
  storeName: string;
  storageKey: string;
  key: string;
  value: JsonObject;
  revision: number;
  serviceId: string;
  serviceNamespace: string;
  serviceEndpoint: string;
  serviceSessionId: string;
  consumerEndpoint: string;
  consumerSessionId: string;
  writer: ServiceViewWriter;
  contract: ContractPointer;
}

export interface ServiceViewChange {
  operation: "put" | "delete" | "expire" | "snapshot";
  storeName: string;
  key: string;
  storageKey: string;
  revision: number;
  entry?: ServiceViewEntry;
  current?: boolean;
  version?: number;
  generation?: number;
}

export interface RegisteredEndpointLane {
  endpoint: string;
  sessionId: string;
  publish?(options: {
    recipient: string; recipientSessionId?: string; subject: EntitySubject; messageType: string;
    body: JsonObject; contract: ContractPointer;
  }): Promise<void>;
  request?(options: {
    recipient: string;
    recipientSessionId?: string;
    subject: EntitySubject;
    messageType: string;
    body: JsonObject;
    contract?: ContractPointer;
    timeout?: number;
    accept?: (message: DeckrMessage) => boolean;
  }): Promise<DeckrMessage>;
}

export function validateServiceProtocol(input: ServiceProtocol): ServiceProtocol {
  const operations = validateServiceOperations(input.operations);
  const messages = validateServiceMessages(input.messages);
  validateServiceMessageOperations(operations, messages);
  const viewFamilies: Record<string, ServiceViewFamilyDeclaration> = {};
  for (const [name, family] of Object.entries(input.viewFamilies)) {
    viewFamilies[requireText(name, "service view family")] = {
      storeName: requireText(family.storeName, "service view storeName"),
      ...(family.keyPrefix === undefined ? {} : { keyPrefix: requireText(family.keyPrefix, "service view keyPrefix") }),
      writer: validateServiceViewWriter(family.writer),
    };
  }
  return {
    namespace: requireText(input.namespace, "service namespace"),
    featureId: requireText(input.featureId, "service feature id"),
    advertisementProfile: requireText(
      input.advertisementProfile,
      "service advertisement profile",
    ),
    useProfile: requireText(input.useProfile, "service use profile"),
    operations,
    messages,
    viewFamilies,
  };
}

export function serviceAdvertisementPayload(
  protocol: ServiceProtocol,
  input: {
    serviceId: string;
    sessionId: string;
    backendStatus: ServiceBackendStatus;
    diagnostics?: JsonObject;
  },
): ServiceAdvertisementPayload {
  const serviceId = requireText(input.serviceId, "service id");
  return validateServiceAdvertisementPayload({
    profile: protocol.advertisementProfile,
    serviceId,
    serviceEndpoint: serviceAddress(serviceId),
    serviceNamespace: protocol.namespace,
    sessionId: input.sessionId,
    serviceUseProfile: protocol.useProfile,
    backendStatus: input.backendStatus,
    supportedOperations: Object.keys(protocol.operations),
    supportedMessages: protocol.messages,
    views: serviceProtocolViews(protocol, serviceId),
    diagnostics: input.diagnostics ?? {},
  });
}

export function validateServiceAdvertisementPayload(value: unknown): ServiceAdvertisementPayload {
  const raw = requireJsonObject(value, "service advertisement payload");
  const backendStatus = requireText(raw.backendStatus, "backendStatus") as ServiceBackendStatus;
  if (!Object.values(ServiceBackendStatus).includes(backendStatus)) {
    throw new ValidationError("invalid service backendStatus");
  }
  const serviceId = requireText(raw.serviceId, "serviceId");
  const serviceEndpoint = endpointAddress(requireText(raw.serviceEndpoint, "serviceEndpoint"));
  if (serviceEndpoint !== serviceAddress(serviceId)) {
    throw new ValidationError("serviceEndpoint must equal service:<serviceId>");
  }
  const supportedOperations = validateTextArray(raw.supportedOperations, "supportedOperations");
  const supportedMessages = validateServiceMessages(raw.supportedMessages);
  const viewsRaw = requireJsonObject(raw.views, "views");
  const views: Record<string, ServiceViewFamily> = {};
  for (const [name, item] of Object.entries(viewsRaw)) {
    const family = requireJsonObject(item, "service view family");
    views[requireText(name, "service view family name")] = {
      storeName: requireText(family.storeName, "storeName"),
      keyPrefix: requireText(family.keyPrefix, "keyPrefix"),
      writer: validateServiceViewWriter(family.writer),
    };
  }
  return {
    profile: requireText(raw.profile, "profile"),
    serviceId,
    serviceEndpoint,
    serviceNamespace: requireText(raw.serviceNamespace, "serviceNamespace"),
    sessionId: requireText(raw.sessionId, "sessionId"),
    serviceUseProfile: requireText(raw.serviceUseProfile, "serviceUseProfile"),
    backendStatus,
    supportedOperations,
    supportedMessages,
    views,
    diagnostics: requireJsonObject(raw.diagnostics ?? {}, "diagnostics"),
  };
}

export function validateServiceUseTerms(value: unknown): ServiceUseTerms {
  const raw = requireJsonObject(value, "service use terms");
  const serviceId = requireText(raw.serviceId, "serviceId");
  const serviceEndpoint = endpointAddress(requireText(raw.serviceEndpoint, "serviceEndpoint"));
  if (serviceEndpoint !== serviceAddress(serviceId)) {
    throw new ValidationError("serviceEndpoint must equal service:<serviceId>");
  }
  const allowedViewsRaw = requireJsonObject(raw.allowedViews ?? {}, "allowedViews");
  const allowedViews: Record<string, string[]> = {};
  for (const [family, prefixes] of Object.entries(allowedViewsRaw)) {
    allowedViews[requireText(family, "service view family")] = validateTextArray(
      prefixes,
      "service view prefix",
    );
  }
  return {
    profile: requireText(raw.profile, "profile"),
    serviceUseScopeId: requireText(raw.serviceUseScopeId, "serviceUseScopeId"),
    serviceId,
    serviceEndpoint,
    serviceNamespace: requireText(raw.serviceNamespace, "serviceNamespace"),
    serviceSessionId: requireText(raw.serviceSessionId, "serviceSessionId"),
    clientEndpoint: endpointAddress(requireText(raw.clientEndpoint, "clientEndpoint")),
    allowedOperations: validateTextArray(raw.allowedOperations ?? [], "allowedOperations"),
    allowedViews,
  };
}

export function validateServiceUseScopeIndexRecord(value: unknown): ServiceUseScopeIndexRecord {
  const raw = requireJsonObject(value, "service-use scope index record");
  const schema = requireText(raw.schema, "schema");
  if (schema !== SERVICE_USE_INDEX_SCHEMA_ID) {
    throw new ValidationError(`service-use scope index schema must be ${SERVICE_USE_INDEX_SCHEMA_ID}`);
  }
  const state = requireText(raw.state, "state");
  if (state !== "candidate") {
    throw new ValidationError("service-use scope index state must be candidate");
  }
  const record: ServiceUseScopeIndexRecord = {
    schema: SERVICE_USE_INDEX_SCHEMA_ID,
    scopeId: requireText(raw.scopeId, "scopeId"),
    contract: validateContractPointer(raw.contract),
    termsHash: requireText(raw.termsHash, "termsHash"),
    serviceEndpoint: endpointAddress(requireText(raw.serviceEndpoint, "serviceEndpoint")),
    serviceSessionId: requireText(raw.serviceSessionId, "serviceSessionId"),
    clientEndpoint: endpointAddress(requireText(raw.clientEndpoint, "clientEndpoint")),
    clientSessionId: requireText(raw.clientSessionId, "clientSessionId"),
    state,
    createdAt: requireText(raw.createdAt, "createdAt"),
    updatedAt: requireText(raw.updatedAt, "updatedAt"),
  };
  if (raw.supersedes !== undefined) {
    record.supersedes = validateContractPointer(raw.supersedes);
  }
  return record;
}

export class ServiceAdvertiser {
  readonly protocol: ServiceProtocol;
  readonly serviceId: string;
  readonly endpoint: RegisteredEndpointLane;

  private readonly beacon: BeaconService;
  private readonly refreshIntervalSeconds: number;
  private advertiser: BeaconAdvertisement | null = null;
  private advertisementHandle: Awaited<ReturnType<BeaconAdvertisement["publish"]>> | null = null;
  private backendStatusValue: ServiceBackendStatus = ServiceBackendStatus.UNAVAILABLE;
  private backendDiagnosticsValue: JsonObject = {};

  constructor(options: {
    protocol: ServiceProtocol;
    serviceId: string;
    endpoint: RegisteredEndpointLane;
    beacon: BeaconService;
    refreshIntervalSeconds?: number;
  }) {
    this.protocol = validateServiceProtocol(options.protocol);
    this.serviceId = requireText(options.serviceId, "service id");
    this.endpoint = {
      endpoint: endpointAddress(options.endpoint.endpoint),
      sessionId: requireText(options.endpoint.sessionId, "endpoint session id"),
      request: options.endpoint.request,
      publish: options.endpoint.publish,
    };
    this.beacon = options.beacon;
    this.refreshIntervalSeconds =
      options.refreshIntervalSeconds ?? DEFAULT_SERVICE_ADVERTISEMENT_REFRESH_SECONDS;
  }

  get advertisement() {
    return this.advertisementHandle;
  }

  get backendStatus(): ServiceBackendStatus {
    return this.backendStatusValue;
  }

  get backendDiagnostics(): JsonObject {
    return this.backendDiagnosticsValue;
  }

  async publish(
    status: ServiceBackendStatus,
    options: { diagnostics?: JsonObject } = {},
  ): Promise<void> {
    this.backendStatusValue = status;
    this.backendDiagnosticsValue = cloneJson(options.diagnostics ?? {});
    const payload = serviceAdvertisementPayload(this.protocol, {
      serviceId: this.serviceId,
      sessionId: this.endpoint.sessionId,
      backendStatus: status,
      diagnostics: this.backendDiagnosticsValue,
    });
    if (this.advertiser === null || this.advertiser.closed) {
      this.advertiser = await this.beacon.ensureAdvertisement({
        featureId: this.protocol.featureId,
        endpoint: this.endpoint.endpoint,
        sessionId: this.endpoint.sessionId,
        payload: payload as unknown as JsonObject,
        operations: Object.keys(this.protocol.operations),
        refreshIntervalSeconds: this.refreshIntervalSeconds,
      } satisfies BeaconAdvertisementSpec);
    }
    this.advertisementHandle = await this.advertiser.publish({
      payload: payload as unknown as JsonObject,
    });
  }

  async withdraw(): Promise<void> {
    if (this.advertiser !== null) {
      await this.advertiser.close();
    }
    this.advertiser = null;
    this.advertisementHandle = null;
  }
}

export class ServiceUseLeaseManager {
  private readonly endpoint: RegisteredEndpointLane;
  private readonly concord: ConcordService;
  private readonly serviceUseIndex: StateStore;
  private readonly refreshIntervalSeconds: number;
  private readonly leases = new Map<string, ServiceUseLease>();
  private closed = false;

  constructor(options: {
    endpoint: RegisteredEndpointLane;
    concord: ConcordService;
    serviceUseIndex: StateStore;
    refreshIntervalSeconds?: number;
  }) {
    this.endpoint = {
      endpoint: endpointAddress(options.endpoint.endpoint),
      sessionId: requireText(options.endpoint.sessionId, "endpoint session id"),
      request: options.endpoint.request,
      publish: options.endpoint.publish,
    };
    this.concord = options.concord;
    this.serviceUseIndex = options.serviceUseIndex;
    this.refreshIntervalSeconds =
      options.refreshIntervalSeconds ?? DEFAULT_SERVICE_TOKEN_REFRESH_SECONDS;
  }

  async close(): Promise<void> {
    this.closed = true;
    const leases = [...this.leases.values()];
    this.leases.clear();
    await Promise.all(leases.map(lease => lease.agreement.cancel("service lease manager closed")));
  }

  async ensure(
    descriptor: ServiceDescriptor,
    options: {
      operations?: string[];
      views?: string[] | Record<string, string[]>;
      timeoutMs?: number;
    } = {},
  ): Promise<ServiceUseLease> {
    if (this.closed) {
      throw new ServiceUnavailable("client_closed", "The service lease manager is closed");
    }
    const terms = serviceUseTerms(descriptor, this.endpoint.endpoint, {
      operations: options.operations ?? [],
      views: options.views ?? [],
    });
    const key = leaseKey(this.endpoint, descriptor, terms);
    let lease = this.leases.get(key);
    if (lease === undefined || lease.agreement.closed || JSON.stringify(lease.terms) !== JSON.stringify(terms)) {
      if (lease !== undefined) {
        await lease.agreement.cancel("service terms changed");
      }
      lease = await this.createOrReuseLease(descriptor, terms);
      if (this.closed) { await lease.agreement.close(); throw new ServiceUnavailable("client_closed", "The service lease manager is closed"); }
      this.leases.set(key, lease);
    }
    return this.validLease(lease, options.timeoutMs ?? 2000);
  }

  async cached(options: {
    serviceId: string;
    namespace: string;
    operations?: string[];
    views?: string[] | Record<string, string[]>;
    timeoutMs?: number;
  }): Promise<ServiceUseLease | null> {
    for (const lease of [...this.leases.values()].sort((left, right) =>
      serviceDescriptorSortKey(right.descriptor).localeCompare(
        serviceDescriptorSortKey(left.descriptor),
      ),
    )) {
      if (
        lease.descriptor.serviceId !== options.serviceId ||
        lease.descriptor.namespace !== options.namespace ||
        !leaseCoversScope(lease, {
          operations: options.operations ?? [],
          views: options.views ?? [],
        })
      ) {
        continue;
      }
      try {
        return await this.validLease(lease, options.timeoutMs ?? 0);
      } catch (error) {
        if (error instanceof ServiceUnavailable) {
          this.leases.delete(leaseKey(this.endpoint, lease.descriptor, lease.terms));
          continue;
        }
        throw error;
      }
    }
    return null;
  }

  private async createOrReuseLease(
    descriptor: ServiceDescriptor,
    terms: ServiceUseTerms,
  ): Promise<ServiceUseLease> {
    const key = serviceUseScopeIndexKey(terms.serviceUseScopeId);
    const currentSessions = {
      [this.endpoint.endpoint]: this.endpoint.sessionId,
      [descriptor.endpoint]: descriptor.sessionId,
    };
    for (let attempt = 0; attempt < 8; attempt++) {
      if (this.closed) throw new ServiceUnavailable("client_closed", "Service client is closed");
      const entry = await this.serviceUseIndex.get(key);
      const indexed = serviceUseIndexRecordFromEntry(entry, terms.serviceUseScopeId);
      // The index is a proposal hint. It cannot transfer ownership of a local token.
      const supersedes = indexed?.contract;
      const agreement = await this.concord.ensureAgreement({
        profile: descriptor.useProfile,
        participants: [descriptor.endpoint, this.endpoint.endpoint],
        localParticipant: this.endpoint.endpoint,
        localSessionId: this.endpoint.sessionId,
        terms: terms as unknown as JsonObject,
        supersedes,
        participantSessions: {
          [descriptor.endpoint]: descriptor.sessionId,
        },
        currentSessions,
        refreshIntervalSeconds: this.refreshIntervalSeconds,
      } satisfies ConcordAgreementSpec);
      if (this.closed) { await agreement.close(); throw new ServiceUnavailable("client_closed", "Service client is closed"); }
      const record = serviceUseIndexRecord({
        terms,
        contract: agreement.contract,
        descriptor,
        clientSessionId: this.endpoint.sessionId,
        supersedes,
        previous: indexed,
      });
      try {
        if (entry === null) {
          await this.serviceUseIndex.create(key, record as unknown as JsonObject);
        } else {
          await this.serviceUseIndex.update(key, record as unknown as JsonObject, { revision: entry.revision });
        }
      } catch (error) {
        await agreement.close();
        if (!(error instanceof StateConflict)) {
          throw error;
        }
        continue;
      }
      if (supersedes !== undefined) {
        await this.cancelPointerQuietly(supersedes);
      }
      return Object.freeze({ agreement, descriptor: immutable(descriptor), terms: immutable(terms) });
    }
    throw new ServiceUnavailable("service_use_index_conflict", "Service-use scope index retry limit reached");
  }

  private async cancelPointerQuietly(pointer: ContractPointer): Promise<void> {
    try {
      const contract = await this.concord.getContract(pointer);
      if (contract !== null) {
        await bounded(this.concord.cancelContract(contract, this.endpoint.endpoint, {
          reason: "service_use_scope_replaced",
        }), 5000, this.concord.scheduler);
      }
    } catch (error) {
      if (!(error instanceof StateConflict || error instanceof StateUnavailable || error instanceof ValidationError)) {
        throw error;
      }
    }
  }

  private async validLease(lease: ServiceUseLease, timeoutMs: number): Promise<ServiceUseLease> {
    const deadline = this.concord.scheduler.now() + Math.max(timeoutMs, 0);
    while (true) {
      if (this.closed) throw new ServiceUnavailable("client_closed", "Service client is closed");
      let validity;
      try {
        validity = await lease.agreement.refresh();
      } catch (error) {
        if (error instanceof StateConflict) {
          throw new ServiceUnavailable("contract_invalid_token", "Service-use contract could not be refreshed", {
            status: ContractValidityStatus.INVALID_TOKEN,
            reason: error.message,
          });
        }
        throw error;
      }
      if (validity.valid) {
        return lease;
      }
      if (validity.status !== ContractValidityStatus.NOT_YET_FULFILLED) {
        throw new ServiceUnavailable(
          `contract_${validity.status}`,
          "Service-use contract is not valid",
          { status: validity.status, reason: validity.reason ?? "" },
        );
      }
      if (this.concord.scheduler.now() >= deadline) {
        throw new ServiceUnavailable(
          "service_contract_pending",
          "Service-use contract is pending the service token",
          { status: validity.status },
        );
      }
      await new Promise<void>(resolve => {
        let unsubscribe = () => {};
        const finish = () => { cancel(); unsubscribe(); resolve(); };
        const cancel = this.concord.scheduler.schedule(Math.max(0, deadline - this.concord.scheduler.now()), finish);
        unsubscribe = lease.agreement.onChange(finish);
        if (lease.agreement.valid || lease.agreement.closed) finish();
      });
    }
  }
}

export class ServiceUseAuthorizer {
  readonly protocol: ServiceProtocol;
  readonly serviceId: string;
  readonly endpoint: RegisteredEndpointLane;

  private readonly manager;

  constructor(options: {
    protocol: ServiceProtocol;
    serviceId: string;
    endpoint: RegisteredEndpointLane;
    concord: ConcordService;
    acceptContract?: (terms: ServiceUseTerms) => boolean | Promise<boolean>;
  }) {
    this.protocol = validateServiceProtocol(options.protocol);
    this.serviceId = requireText(options.serviceId, "service id");
    this.endpoint = {
      endpoint: endpointAddress(options.endpoint.endpoint),
      sessionId: requireText(options.endpoint.sessionId, "endpoint session id"),
      request: options.endpoint.request,
      publish: options.endpoint.publish,
    };
    this.manager = options.concord.participantManager({
      participant: this.endpoint.endpoint,
      sessionId: this.endpoint.sessionId,
      profile: this.protocol.useProfile,
      cancelTerminalStatuses: [
        ContractValidityStatus.CANCELLED,
        ContractValidityStatus.MISSING_CONTRACT,
        ContractValidityStatus.INVALID_CONTRACT,
        ContractValidityStatus.MISSING_TOKEN,
        ContractValidityStatus.INVALID_TOKEN,
        ContractValidityStatus.GENERATION_MISMATCH,
        ContractValidityStatus.SESSION_MISMATCH,
        ContractValidityStatus.TERMS_HASH_MISMATCH,
      ],
      acceptContract: async (contract, record) => {
        const terms = this.matchingTermsRecord(contract, record);
        return terms !== null && (options.acceptContract ? await options.acceptContract(terms) : true);
      },
      currentSessions: () => ({ [this.endpoint.endpoint]: this.endpoint.sessionId }),
    });
  }

  async start(): Promise<void> { await this.reconcileContracts(); this.manager.start(); }
  async close(): Promise<void> { await this.manager.aclose(); }

  async reconcileContracts(): Promise<void> {
    await this.manager.reconcile();
  }

  async send(contract: ContractHandle, name: string, payload: JsonObject): Promise<void> {
    const managed = this.manager.managedContract(contract);
    const terms = managed && this.matchingTermsRecord(contract, managed.record);
    const definition = this.protocol.messages[name];
    if (!terms || !definition || !definition.operation || !terms.allowedOperations.includes(definition.operation) ||
      definition.exchangePattern !== ServiceExchangePattern.ONE_WAY ||
      (definition.direction !== ServiceMessageDirection.SERVICE_TO_CONSUMER && definition.direction !== ServiceMessageDirection.BIDIRECTIONAL)) throw new ServiceUnavailable("operation_not_authorized", "Service cannot send this operation");
    const current = await this.manager.authority(contract).check();
    if (!this.endpoint.publish) throw new ServiceUnavailable("service_endpoint_unavailable", "Service endpoint cannot publish");
    await this.endpoint.publish({ recipient: terms.clientEndpoint, recipientSessionId: current.tokens[terms.clientEndpoint]!.sessionId,
      contract: { contractId: contract.contractId, generation: contract.generation }, messageType: SERVICE_MESSAGE,
      subject: entitySubject("service", { serviceId: this.serviceId, namespace: this.protocol.namespace, name }),
      body: { serviceNamespace: this.protocol.namespace, name, intent: definition.intent, exchangePattern: definition.exchangePattern,
        [definition.intent === ServiceMessageIntent.EVENT ? "event" : "params"]: payload } });
  }

  async viewContext(contract: ContractHandle): Promise<ServiceViewWriteContext> {
    await this.reconcileContracts();
    const managed = this.manager.managedContract(contract);
    const terms = managed && this.matchingTermsRecord(contract, managed.record);
    if (!terms) throw new ServiceUnavailable("contract_unavailable", "No managed service-use agreement");
    const authority = this.manager.authority(contract);
    const current = await authority.check();
    return {
      authority: { signal: authority.signal, assertActive: async () => { await authority.check(); } },
      writer: ServiceViewWriter.SERVICE, serviceId: this.serviceId, serviceNamespace: this.protocol.namespace,
      serviceEndpoint: this.endpoint.endpoint, serviceSessionId: this.endpoint.sessionId,
      consumerEndpoint: terms.clientEndpoint, consumerSessionId: current.tokens[terms.clientEndpoint]!.sessionId,
      contract: { contractId: contract.contractId, generation: contract.generation },
      views: scopedViewFamilies(serviceProtocolViews(this.protocol, this.serviceId), terms.allowedViews),
    };
  }

  async matchingTerms(contract: ContractHandle): Promise<ServiceUseTerms | null> {
    const managed = this.manager.managedContract(contract);
    if (managed === null) {
      return null;
    }
    return this.matchingTermsRecord(contract, managed.record);
  }

  async authorizeMessage(
    message: DeckrMessage,
    body: ServiceMessageBody = validateServiceMessageBody(message.body),
  ): Promise<AuthorizationDecision> {
    if (message.messageType !== SERVICE_MESSAGE) {
      return AuthorizationDecision.NOT_APPLICABLE;
    }
    if (
      !messageAppliesToService(message, body, {
        serviceId: this.serviceId,
        namespace: this.protocol.namespace,
        endpoint: this.endpoint.endpoint,
      })
    ) {
      return AuthorizationDecision.NOT_APPLICABLE;
    }
    const definition = this.protocol.messages[body.name];
    if (
      definition === undefined ||
      body.intent !== definition.intent ||
      body.exchangePattern !== definition.exchangePattern ||
      (
        definition.direction !== ServiceMessageDirection.CONSUMER_TO_SERVICE &&
        definition.direction !== ServiceMessageDirection.BIDIRECTIONAL
      ) ||
      definition.operation === undefined
    ) {
      return AuthorizationDecision.DENIED;
    }
    if (message.contract === undefined) {
      return AuthorizationDecision.DENIED;
    }
    await this.reconcileContracts();
    const managed = this.manager.managedContracts().find(
      (item) =>
        item.contract.contractId === message.contract!.contractId &&
        item.contract.generation === message.contract!.generation,
    );
    if (managed === undefined) {
      return AuthorizationDecision.DENIED;
    }
    const terms = this.matchingTermsRecord(managed.contract, managed.record);
    if (terms === null || terms.clientEndpoint !== message.sender) {
      return AuthorizationDecision.DENIED;
    }
    if (!terms.allowedOperations.includes(definition.operation)) {
      return AuthorizationDecision.DENIED;
    }
    const validity = await this.manager.validate(managed.contract, {
      currentSessions: {
        [terms.serviceEndpoint]: this.endpoint.sessionId,
        [terms.clientEndpoint]: message.senderSessionId,
      },
    });
    if (validity.status === ContractValidityStatus.VALID) {
      return AuthorizationDecision.AUTHORIZED;
    }
    return AuthorizationDecision.DENIED;
  }

  private matchingTermsRecord(
    contract: ContractHandle,
    record: ContractRecord,
  ): ServiceUseTerms | null {
    if (record.state !== ContractState.OPEN || record.terms === undefined) {
      return null;
    }
    let terms: ServiceUseTerms;
    try {
      terms = validateServiceUseTerms(record.terms);
    } catch {
      return null;
    }
    if (!serviceUseParticipantsMatch(record, terms)) {
      return null;
    }
    if (
      terms.profile !== this.protocol.useProfile ||
      terms.serviceNamespace !== this.protocol.namespace ||
      terms.serviceId !== this.serviceId ||
      terms.serviceEndpoint !== this.endpoint.endpoint ||
      terms.serviceSessionId !== this.endpoint.sessionId
    ) {
      return null;
    }
    if (!terms.allowedOperations.every(operation => operation in this.protocol.operations)) return null;
    for (const [name, prefixes] of Object.entries(terms.allowedViews)) {
      const family = serviceProtocolViews(this.protocol, this.serviceId)[name];
      if (!family || !prefixes.every(prefix => prefix.startsWith(family.keyPrefix))) return null;
    }
    return terms;
  }
}

function serviceUseParticipantsMatch(record: ContractRecord, terms: ServiceUseTerms): boolean {
  const expected = [terms.clientEndpoint, terms.serviceEndpoint].sort();
  return (
    record.participants.length === expected.length &&
    record.participants.every((participant, index) => participant === expected[index])
  );
}

export class ServiceMessageChannel {
  private readonly endpoint: RegisteredEndpointLane;

  constructor(options: { endpoint: RegisteredEndpointLane }) {
    this.endpoint = options.endpoint;
  }

  async send(lease: ServiceUseLease, name: string, payload: JsonObject): Promise<void> {
    const definition = lease.descriptor.supportedMessages[name];
    const validity = await lease.agreement.refresh();
    const provider = this.endpoint.endpoint === lease.descriptor.endpoint;
    const expected = provider ? ServiceMessageDirection.SERVICE_TO_CONSUMER : ServiceMessageDirection.CONSUMER_TO_SERVICE;
    if (!validity.valid || !lease.agreement.valid || lease.agreement.spec.localParticipant !== this.endpoint.endpoint || validity.tokens[this.endpoint.endpoint]?.sessionId !== this.endpoint.sessionId ||
      !definition || definition.exchangePattern !== ServiceExchangePattern.ONE_WAY || !definition.operation ||
      !lease.terms.allowedOperations.includes(definition.operation) ||
      (definition.direction !== expected && definition.direction !== ServiceMessageDirection.BIDIRECTIONAL)) throw new ServiceUnavailable("operation_not_authorized", "Service message has no live authorized context");
    if (!this.endpoint.publish) throw new ServiceUnavailable("client_endpoint_unavailable", "Endpoint does not support one-way delivery");
    const recipient = provider ? lease.terms.clientEndpoint : lease.descriptor.endpoint;
    await this.endpoint.publish({ recipient, recipientSessionId: validity.tokens[recipient]?.sessionId,
      subject: entitySubject("service", { serviceId: lease.descriptor.serviceId, namespace: lease.descriptor.namespace, name }),
      messageType: SERVICE_MESSAGE, contract: { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation },
      body: { serviceNamespace: lease.descriptor.namespace, name, intent: definition.intent, exchangePattern: definition.exchangePattern,
        [definition.intent === ServiceMessageIntent.EVENT ? "event" : "params"]: payload } });
  }

  async request(
    lease: ServiceUseLease,
    name: string,
    params: JsonObject = {},
    options: { timeoutMs?: number } = {},
  ): Promise<ServiceMessageBody> {
    name = requireText(name, "service message name");
    const definition = lease.descriptor.supportedMessages[name];
    if (
      definition === undefined ||
      definition.exchangePattern !== ServiceExchangePattern.REQUEST_REPLY
    ) {
      return rejectedMessage(lease.descriptor.namespace, name, {
        code: "message_not_supported",
        message: `Service does not advertise request/reply message ${JSON.stringify(name)}`,
      });
    }
    if (
      definition.operation === undefined ||
      !lease.terms.allowedOperations.includes(definition.operation)
    ) {
      return rejectedMessage(lease.descriptor.namespace, name, {
        code: "operation_not_authorized",
        message: `Service-use lease does not authorize message ${JSON.stringify(name)}`,
      });
    }
    try {
      const validity = await lease.agreement.refresh();
      if (!validity.valid || !lease.agreement.valid) throw new ServiceUnavailable("contract_unavailable", "Service-use authority has ended");
      const local = validity.tokens[this.endpoint.endpoint];
      if (lease.agreement.spec.localParticipant !== this.endpoint.endpoint || this.endpoint.endpoint !== lease.terms.clientEndpoint || local?.sessionId !== this.endpoint.sessionId ||
        (definition.direction !== ServiceMessageDirection.CONSUMER_TO_SERVICE && definition.direction !== ServiceMessageDirection.BIDIRECTIONAL)) {
        throw new ServiceUnavailable("operation_not_authorized", "Message is outside the endpoint's managed context");
      }
    } catch (error) {
      return unavailableMessage(lease.descriptor.namespace, name, {
        code: error instanceof ServiceUnavailable ? error.code : "contract_unavailable",
        message: error instanceof Error ? error.message : String(error),
      });
    }
    if (this.endpoint.request === undefined) {
      return unavailableMessage(lease.descriptor.namespace, name, {
        code: "client_endpoint_unavailable",
        message: "The services endpoint is unavailable",
      });
    }
    const reply = await this.endpoint.request({
      recipient: lease.descriptor.endpoint,
      recipientSessionId: lease.descriptor.sessionId,
      subject: entitySubject("service", {
        serviceId: lease.descriptor.serviceId,
        namespace: lease.descriptor.namespace,
        name,
      }),
      messageType: SERVICE_MESSAGE,
      body: {
        serviceNamespace: lease.descriptor.namespace,
        name,
        intent: definition.intent,
        exchangePattern: definition.exchangePattern,
        params,
      },
      contract: {
        contractId: lease.agreement.contract.contractId,
        generation: lease.agreement.contract.generation,
      },
      timeout: options.timeoutMs,
      accept: message => message.sender === lease.descriptor.endpoint && message.senderSessionId === lease.descriptor.sessionId &&
        message.contract?.contractId === lease.agreement.contract.contractId && message.contract.generation === lease.agreement.contract.generation &&
        message.body.serviceNamespace === lease.descriptor.namespace && message.body.name === name,
    });
    const validity = await lease.agreement.refresh();
    if (!validity.valid || !lease.agreement.valid) return unavailableMessage(lease.descriptor.namespace, name, { code: "contract_unavailable", message: "Authority ended before reply delivery" });
    return validateServiceMessageBody(reply.body);
  }
}

export class ServiceViewReader {
  private readonly stateFor: (storeName: string) => StateStore;

  constructor(options: { stateFor: (storeName: string) => StateStore }) {
    this.stateFor = options.stateFor;
  }

  async read(lease: ServiceUseLease, view: ServiceViewRef): Promise<JsonObject | null> {
    if (!viewRefAuthorized(lease, view)) {
      throw new ValidationError(`Service-use lease does not authorize view ${JSON.stringify(view.key)}`);
    }
    const validity = await lease.agreement.refresh();
    if (!validity.valid) {
      return null;
    }
    const access = new ManagedServiceViewAccess({
      state: this.stateFor(view.storeName),
      readContext: serviceViewReadContextFromLease(lease, validity),
    });
    try { return (await access.read(view))?.value ?? null; } finally { await access.close(); }
  }
}

function copyViewContext<T extends ServiceViewReadContext | ServiceViewWriteContext>(context: T): T {
  const copy = { ...context };
  copy.contract = Object.freeze({ ...context.contract });
  copy.views = immutable(Object.fromEntries(Object.entries(context.views).map(([name, family]) => [name, { ...family }])));
  Object.freeze(copy);
  return copy;
}

export class ManagedServiceViewAccess {
  private readonly state: StateStore;
  private readonly source: MaterializedStateSource;
  private readonly readContext?: ServiceViewReadContext;
  private readonly writeContext?: ServiceViewWriteContext;
  private started = false;
  private readonly abort = new AbortController();
  constructor(options: { state: StateStore; readContext?: ServiceViewReadContext; writeContext?: ServiceViewWriteContext }) {
    this.state = options.state; this.source = sourceFor(options.state);
    this.readContext = options.readContext && copyViewContext(options.readContext);
    this.writeContext = options.writeContext && copyViewContext(options.writeContext);
  }
  private async start(): Promise<void> {
    if (this.abort.signal.aborted) throw new StateUnavailable("View access is closed");
    if (!this.started) { this.started = true; this.source.retain(); }
    await this.source.start(); this.source.requireCurrent();
  }
  async close(): Promise<void> {
    if (this.abort.signal.aborted) return;
    this.abort.abort();
    if (this.started) await this.source.release();
  }
  private async authorize(context: ServiceViewReadContext | ServiceViewWriteContext): Promise<void> {
    if (!context.authority || context.authority.signal.aborted) throw new ServiceUnavailable("contract_unavailable", "Protected view authority has ended");
    await context.authority.assertActive();
    if (context.authority.signal.aborted) throw new ServiceUnavailable("contract_unavailable", "Protected view authority has ended");
  }
  async read(view: ServiceViewRef): Promise<ServiceViewEntry | null> {
    const context = this.requireReadContext(); assertReadAuthorized(context, view);
    await this.authorize(context); await this.start();
    const entry = this.source.get(serviceViewStorageKey(view, context.contract));
    await this.authorize(context);
    return entry ? serviceViewEntryForRead(entry, view, context) : null;
  }
  async *watch(view: ServiceViewRef): AsyncIterable<ServiceViewChange> {
    const context = this.requireReadContext(); assertReadAuthorized(context, view);
    await this.authorize(context); await this.start();
    const signal = AbortSignal.any([this.abort.signal, context.authority.signal]);
    const storageKey = serviceViewStorageKey(view, context.contract);
    let visible: ServiceViewEntry | undefined;
    let generation = -1;
    for await (const snapshot of this.source.watch({ signal })) {
      if (signal.aborted) return;
      await this.authorize(context);
      const state = { storeName: view.storeName, key: view.key, storageKey,
        revision: snapshot.revision, current: snapshot.current, version: snapshot.version, generation: snapshot.generation };
      if (!snapshot.current) {
        visible = undefined; generation = -1;
        yield immutable({ ...state, operation: "snapshot" });
        continue;
      }
      const raw = snapshot.current ? snapshot.entries.find(entry => entry.key === storageKey) : undefined;
      const entry = raw ? serviceViewEntryForRead(raw, view, context) ?? undefined : undefined;
      if (entry && entry.revision !== visible?.revision) {
        visible = entry;
        yield immutable({ ...state, operation: "put", revision: entry.revision, entry });
      } else if (!entry && visible) {
        visible = undefined;
        yield immutable({ ...state, operation: "delete" });
      } else if (generation !== snapshot.generation) {
        yield immutable({ ...state, operation: "snapshot", ...(entry ? { entry } : {}) });
      }
      generation = snapshot.generation;
    }
  }
  async create(view: ServiceViewRef, payload: JsonObject, options: { ttl?: number | null } = {}): Promise<ServiceViewEntry> {
    return this.write(view, payload, (key, value) => this.state.create(key, value, options));
  }
  async put(view: ServiceViewRef, payload: JsonObject, options: { revision?: number; ttl?: number | null } = {}): Promise<ServiceViewEntry> {
    return this.write(view, payload, (key, value) => options.revision === undefined
      ? this.state.put(key, value, { ttl: options.ttl }) : this.state.update(key, value, { revision: options.revision, ttl: options.ttl }));
  }
  async update(view: ServiceViewRef, payload: JsonObject, options: { revision: number; ttl?: number | null }): Promise<ServiceViewEntry> {
    return this.write(view, payload, (key, value) => this.state.update(key, value, options));
  }
  private async write(view: ServiceViewRef, payload: JsonObject, mutate: (key: string, value: JsonObject) => Promise<StateEntry>): Promise<ServiceViewEntry> {
    const context = this.requireWriteContext(); assertWriteAuthorized(context, view);
    await this.authorize(context); await this.start();
    const entry = await mutate(serviceViewStorageKey(view, context.contract), fencedServiceViewPayload(view, payload, context));
    await this.source.observed(entry.revision); await this.authorize(context);
    return serviceViewEntryFromStateEntry(entry, view.storeName);
  }
  async delete(view: ServiceViewRef, options: { revision?: number | null } = {}): Promise<void> {
    const context = this.requireWriteContext(); assertWriteAuthorized(context, view);
    await this.authorize(context); await this.start();
    const key = serviceViewStorageKey(view, context.contract);
    const entry = await this.state.get(key);
    if (!entry) return;
    const revision = await this.state.delete(key, { revision: options.revision ?? entry.revision });
    if (typeof revision === "number") await this.source.observed(revision);
    else await this.source.removed(key, entry.revision);
  }
  /** Cleanup uses a retained revision and identity, independently of live authority. */
  async withdraw(view: ServiceViewRef, revision: number): Promise<void> {
    const context = this.requireWriteContext();
    const key = serviceViewStorageKey(view, context.contract);
    const entry = await this.state.get(key);
    if (!entry || entry.revision !== revision) return;
    const read = { ...context, reader: context.writer === ServiceViewWriter.SERVICE ? ServiceViewWriter.CONSUMER : ServiceViewWriter.SERVICE };
    if (!serviceViewEntryForRead(entry, view, read)) return;
    await this.state.delete(key, { revision });
  }
  private requireReadContext(): ServiceViewReadContext {
    if (!this.readContext) throw new ValidationError("managed service view access is write-only");
    return this.readContext;
  }
  private requireWriteContext(): ServiceViewWriteContext {
    if (!this.writeContext) throw new ValidationError("managed service view access is read-only");
    return this.writeContext;
  }
}

export class ServiceViewStoreWriter {
  private readonly access: ManagedServiceViewAccess;
  private readonly context: ServiceViewWriteContext;
  private readonly revisions = new Map<string, { view: ServiceViewRef; revision: number }>();

  constructor(options: {
    state: StateStore;
    context: ServiceViewWriteContext;
  }) {
    this.context = options.context;
    this.access = new ManagedServiceViewAccess({
      state: options.state,
      writeContext: options.context,
    });
  }

  async create(
    view: ServiceViewRef,
    payload: JsonObject,
    options: { ttl?: number | null } = {},
  ): Promise<ServiceViewEntry> {
    const entry = await this.access.create(view, payload, options);
    this.revisions.set(entry.storageKey, { view, revision: entry.revision });
    return entry;
  }

  async put(view: ServiceViewRef, payload: JsonObject): Promise<ServiceViewEntry> {
    const entry = await this.access.put(view, payload);
    this.revisions.set(entry.storageKey, { view, revision: entry.revision });
    return entry;
  }

  async update(
    view: ServiceViewRef,
    payload: JsonObject,
    options: { revision: number; ttl?: number | null },
  ): Promise<ServiceViewEntry> {
    const entry = await this.access.update(view, payload, options);
    this.revisions.set(entry.storageKey, { view, revision: entry.revision });
    return entry;
  }

  async delete(view: ServiceViewRef, options: { revision?: number | null } = {}): Promise<void> {
    await this.access.delete(view, options);
    this.revisions.delete(serviceViewStorageKey(view, this.context.contract));
  }

  async close(): Promise<void> { await this.withdraw(); await this.access.close(); }

  async withdraw(): Promise<void> {
    for (const [storageKey, item] of [...this.revisions]) {
      try {
        await bounded(this.access.withdraw(item.view, item.revision), 5000);
      } catch (error) {
        if (!(error instanceof StateConflict || error instanceof StateUnavailable)) {
          throw error;
        }
      }
      this.revisions.delete(storageKey);
    }
  }
}

export function serviceViewKey(serviceId: string, family: string, ...tokens: string[]): string {
  return ["views", encodeKeyToken(serviceId), encodeKeyToken(family), ...tokens.map(encodeKeyToken)].join(".");
}

export function serviceViewPrefix(serviceId: string, family: string): string {
  return `${serviceViewKey(serviceId, family)}.`;
}

/** Bind a protocol's view declarations to one concrete provider identity. */
export function serviceProtocolViews(protocol: ServiceProtocol, serviceId: string): Record<string, ServiceViewFamily> {
  return Object.fromEntries(Object.entries(protocol.viewFamilies).map(([name, family]) => [name, {
    ...family, keyPrefix: family.keyPrefix ?? serviceViewPrefix(serviceId, name),
  }]));
}

export function serviceViewStorageKey(
  view: ServiceViewRef | string,
  contract: ContractPointer,
): string {
  const key = typeof view === "string"
    ? requireText(view, "service view key")
    : requireText(view.key, "service view key");
  const pointer = validateContractPointer(contract);
  return `${key}.contract.${encodeKeyToken(pointer.contractId)}.${pointer.generation}`;
}

export function serviceViewAuthorityFromLease(lease: ServiceUseLease): ServiceViewAuthority {
  return { signal: lease.agreement.signal, async assertActive() {
    const validity = await lease.agreement.refresh();
    if (!validity.valid || !lease.agreement.valid) throw new ServiceUnavailable("contract_unavailable", "Protected view requires live service-use authority");
  } };
}

function scopedViewFamilies(views: Record<string, ServiceViewFamily>, allowed: Record<string, string[]>): Record<string, ServiceViewFamily> {
  return Object.fromEntries(Object.entries(allowed).flatMap(([name, prefixes]) => {
    const family = views[name];
    if (!family) return [];
    return prefixes.filter(prefix => prefix.startsWith(family.keyPrefix)).map((prefix, index) => [`${name}:${index}`, { ...family, keyPrefix: prefix }]);
  }));
}

export function serviceViewReadContextFromLease(
  lease: ServiceUseLease,
  validity: ContractValidity = lease.agreement.validity,
): ServiceViewReadContext {
  if (!validity.valid) {
    throw new ValidationError("service-use lease is not valid");
  }
  const consumerToken = validity.tokens[lease.terms.clientEndpoint] ?? lease.agreement.localToken;
  if (consumerToken === null || consumerToken === undefined) {
    throw new ValidationError("service-use lease requires a consumer participant token");
  }
  return {
    authority: serviceViewAuthorityFromLease(lease),
    reader: ServiceViewWriter.CONSUMER,
    serviceId: lease.descriptor.serviceId,
    serviceNamespace: lease.descriptor.namespace,
    serviceEndpoint: lease.descriptor.endpoint,
    serviceSessionId: lease.descriptor.sessionId,
    consumerEndpoint: lease.terms.clientEndpoint,
    consumerSessionId: consumerToken.sessionId,
    contract: {
      contractId: lease.agreement.contract.contractId,
      generation: lease.agreement.contract.generation,
    },
    views: scopedViewFamilies(lease.descriptor.views, lease.terms.allowedViews),
  };
}

export function serviceViewWriteContextFromLease(
  lease: ServiceUseLease,
  validity: ContractValidity = lease.agreement.validity,
): ServiceViewWriteContext {
  const readContext = serviceViewReadContextFromLease(lease, validity);
  return {
    authority: readContext.authority,
    writer: ServiceViewWriter.CONSUMER,
    serviceId: readContext.serviceId,
    serviceNamespace: readContext.serviceNamespace,
    serviceEndpoint: readContext.serviceEndpoint,
    serviceSessionId: readContext.serviceSessionId,
    consumerEndpoint: readContext.consumerEndpoint,
    consumerSessionId: readContext.consumerSessionId,
    contract: readContext.contract,
    views: readContext.views,
  };
}

export function serviceUseScopeIndexKey(scopeId: string): string {
  return `scopes.${encodeKeyToken(requireText(scopeId, "service-use scope id"))}`;
}

function serviceUseIndexRecordFromEntry(
  entry: StateEntry | null,
  scopeId: string,
): ServiceUseScopeIndexRecord | null {
  if (entry === null) {
    return null;
  }
  let record: ServiceUseScopeIndexRecord;
  try {
    record = validateServiceUseScopeIndexRecord(entry.value);
  } catch {
    return null;
  }
  if (record.scopeId !== scopeId) {
    return null;
  }
  return record;
}

function serviceUseIndexMatchesCurrent(
  record: ServiceUseScopeIndexRecord,
  terms: ServiceUseTerms,
  currentSessions: Record<string, string>,
): boolean {
  return (
    record.termsHash === canonicalJsonHash(terms as unknown as JsonValue) &&
    record.serviceEndpoint === terms.serviceEndpoint &&
    record.serviceSessionId === terms.serviceSessionId &&
    record.clientEndpoint === terms.clientEndpoint &&
    record.clientSessionId === currentSessions[terms.clientEndpoint]
  );
}

function serviceUseIndexRecord(options: {
  terms: ServiceUseTerms;
  contract: ContractHandle;
  descriptor: ServiceDescriptor;
  clientSessionId: string;
  supersedes?: ContractPointer;
  previous: ServiceUseScopeIndexRecord | null;
}): ServiceUseScopeIndexRecord {
  const now = new Date().toISOString();
  return {
    schema: SERVICE_USE_INDEX_SCHEMA_ID,
    scopeId: options.terms.serviceUseScopeId,
    contract: {
      contractId: options.contract.contractId,
      generation: options.contract.generation,
    },
    termsHash: canonicalJsonHash(options.terms as unknown as JsonValue),
    serviceEndpoint: options.terms.serviceEndpoint,
    serviceSessionId: options.descriptor.sessionId,
    clientEndpoint: options.terms.clientEndpoint,
    clientSessionId: options.clientSessionId,
    state: "candidate",
    createdAt: options.previous?.createdAt ?? now,
    updatedAt: now,
    ...(options.supersedes === undefined ? {} : { supersedes: options.supersedes }),
  };
}

function serviceUseContractMatchesTerms(
  contract: ContractHandle,
  record: ContractRecord,
  terms: ServiceUseTerms,
): boolean {
  if (record.state !== ContractState.OPEN) {
    return false;
  }
  if (contract.contractId !== record.contractId || contract.generation !== record.generation) {
    return false;
  }
  if (record.profile !== terms.profile) {
    return false;
  }
  if (record.termsHash !== canonicalJsonHash(terms as unknown as JsonValue)) {
    return false;
  }
  if (JSON.stringify(record.terms) !== JSON.stringify(terms)) {
    return false;
  }
  const expected = [terms.clientEndpoint, terms.serviceEndpoint].sort();
  return (
    record.participants.length === expected.length &&
    record.participants.every((participant, index) => participant === expected[index])
  );
}

export function parseServiceDescriptor(
  candidate: Candidate,
  protocol: ServiceProtocol,
): ServiceDescriptor | null {
  const normalized = validateServiceProtocol(protocol);
  const advertisement = candidate.advertisement;
  if (advertisement.featureId !== normalized.featureId || advertisement.payload === undefined) {
    return null;
  }
  let payload: ServiceAdvertisementPayload;
  try {
    payload = validateServiceAdvertisementPayload(advertisement.payload);
  } catch {
    return null;
  }
  if (
    payload.profile !== normalized.advertisementProfile ||
    payload.serviceNamespace !== normalized.namespace ||
    payload.serviceUseProfile !== normalized.useProfile ||
    payload.serviceEndpoint !== advertisement.endpoint ||
    payload.sessionId !== advertisement.sessionId
  ) {
    return null;
  }
  if (!payload.supportedOperations.every((item) => normalized.operations[item] !== undefined)) {
    return null;
  }
  for (const [name, definition] of Object.entries(payload.supportedMessages)) {
    const expected = normalized.messages[name];
    if (
      expected === undefined ||
      !serviceMessageDefinitionsEqual(definition, expected)
    ) {
      return null;
    }
  }
  const expectedViews = serviceProtocolViews(normalized, payload.serviceId);
  if (Object.keys(payload.views).length !== Object.keys(expectedViews).length ||
    Object.entries(expectedViews).some(([name, family]) => {
      const advertised = payload.views[name];
      return !advertised || advertised.storeName !== family.storeName || advertised.writer !== family.writer || advertised.keyPrefix !== family.keyPrefix;
    })) {
    return null;
  }
  return {
    candidate,
    serviceId: payload.serviceId,
    namespace: payload.serviceNamespace,
    endpoint: payload.serviceEndpoint,
    sessionId: payload.sessionId,
    advertisementProfile: payload.profile,
    useProfile: payload.serviceUseProfile,
    supportedOperations: new Set(payload.supportedOperations),
    supportedMessages: payload.supportedMessages,
    views: payload.views,
    backendStatus: payload.backendStatus,
    diagnostics: payload.diagnostics,
  };
}

export function serviceUseTerms(
  descriptor: ServiceDescriptor,
  clientEndpoint: string,
  options: {
    operations?: string[];
    views?: string[] | Record<string, string[]>;
  } = {},
): ServiceUseTerms {
  const allowedOperations = [...normalizeOperations(descriptor, options.operations ?? [])].sort();
  const allowedViews = normalizeViewScope(descriptor, options.views ?? []);
  const identity = {
    profile: descriptor.useProfile,
    serviceId: descriptor.serviceId,
    serviceEndpoint: descriptor.endpoint,
    serviceNamespace: descriptor.namespace,
    serviceSessionId: descriptor.sessionId,
    clientEndpoint: endpointAddress(clientEndpoint),
    allowedOperations,
    allowedViews,
  };
  const digest = canonicalJsonHash(identity as unknown as JsonValue)
    .replace("sha256:", "")
    .slice(0, 32);
  return validateServiceUseTerms({
    ...identity,
    serviceUseScopeId: `service-use-scope:${digest}`,
  });
}

export function serviceDescriptorSortKey(descriptor: ServiceDescriptor): string {
  const ad = descriptor.candidate.advertisement;
  return `${ad.updatedAt ?? ad.createdAt ?? ""}:${String(ad.refreshSeq).padStart(12, "0")}:${descriptor.candidate.key}`;
}

export function newestServiceDescriptor(
  descriptors: Iterable<ServiceDescriptor>,
): ServiceDescriptor | null {
  let newest: ServiceDescriptor | null = null;
  for (const descriptor of descriptors) {
    if (newest === null || serviceDescriptorSortKey(descriptor) > serviceDescriptorSortKey(newest)) {
      newest = descriptor;
    }
  }
  return newest;
}

export function validateServiceMessageBody(value: unknown): ServiceMessageBody {
  const raw = requireJsonObject(value, "service message body");
  const body: ServiceMessageBody = {
    serviceNamespace: requireText(raw.serviceNamespace, "serviceNamespace"),
    name: requireText(raw.name, "name"),
    intent: validateServiceMessageIntent(raw.intent),
    exchangePattern: validateServiceExchangePattern(raw.exchangePattern),
  };
  if (raw.params !== undefined) {
    body.params = requireJsonObject(raw.params, "params");
  }
  if (raw.event !== undefined) {
    body.event = raw.event as JsonValue;
  }
  if (raw.status !== undefined) {
    body.status = validateServiceMessageStatus(raw.status);
  }
  if (raw.result !== undefined) {
    body.result = raw.result as JsonValue;
  }
  if (raw.error !== undefined) {
    const error = requireJsonObject(raw.error, "service error");
    body.error = {
      code: requireText(error.code, "error code"),
      message: requireText(error.message, "error message"),
      diagnostics: requireJsonObject(error.diagnostics ?? {}, "error diagnostics"),
    };
  }
  return body;
}

function validateServiceOperations(value: unknown): Record<string, ServiceOperationDefinition> {
  const raw = requireJsonObject(value, "service operations");
  const operations: Record<string, ServiceOperationDefinition> = {};
  for (const [name, item] of Object.entries(raw)) {
    const operationName = requireText(name, "service operation name");
    const operation = requireJsonObject(item, "service operation");
    const description = operation.description === undefined
      ? undefined
      : requireText(operation.description, "service operation description");
    operations[operationName] = description === undefined ? {} : { description };
  }
  return operations;
}

function validateServiceMessages(value: unknown): Record<string, ServiceMessageDefinition> {
  const raw = requireJsonObject(value, "service messages");
  const messages: Record<string, ServiceMessageDefinition> = {};
  for (const [name, item] of Object.entries(raw)) {
    const messageName = requireText(name, "service message name");
    const message = requireJsonObject(item, "service message");
    const operation = message.operation === undefined
      ? undefined
      : requireText(message.operation, "service message operation");
    const definition: ServiceMessageDefinition = {
      ...(operation === undefined ? {} : { operation }),
      intent: validateServiceMessageIntent(message.intent),
      exchangePattern: validateServiceExchangePattern(message.exchangePattern),
      direction: validateServiceMessageDirection(message.direction),
    };
    if (message.paramsSchema !== undefined) {
      definition.paramsSchema = validateServicePayloadSchema(message.paramsSchema, "paramsSchema");
    }
    if (message.resultSchema !== undefined) {
      definition.resultSchema = validateServicePayloadSchema(message.resultSchema, "resultSchema");
    }
    if (message.eventSchema !== undefined) {
      definition.eventSchema = validateServicePayloadSchema(message.eventSchema, "eventSchema");
    }
    if (message.errorSchema !== undefined) {
      definition.errorSchema = validateServicePayloadSchema(message.errorSchema, "errorSchema");
    }
    messages[messageName] = definition;
  }
  return messages;
}

function validateServiceMessageOperations(
  operations: Record<string, ServiceOperationDefinition>,
  messages: Record<string, ServiceMessageDefinition>,
): void {
  for (const [name, definition] of Object.entries(messages)) {
    if (definition.operation !== undefined) {
      if (operations[definition.operation] === undefined) {
        throw new ValidationError(
          `service message ${JSON.stringify(name)} references unknown operation ${JSON.stringify(definition.operation)}`,
        );
      }
      continue;
    }
    if (
      definition.direction === ServiceMessageDirection.CONSUMER_TO_SERVICE ||
      definition.direction === ServiceMessageDirection.BIDIRECTIONAL ||
      definition.intent === ServiceMessageIntent.COMMAND ||
      definition.intent === ServiceMessageIntent.QUERY
    ) {
      throw new ValidationError(
        `service message ${JSON.stringify(name)} requires a declared operation`,
      );
    }
  }
}

function validateServicePayloadSchema(
  value: unknown,
  fieldName: string,
): ServicePayloadSchema {
  const raw = requireJsonObject(value, fieldName);
  const schemaId = requireText(raw.schemaId, `${fieldName}.schemaId`);
  const schema = requireJsonObject(raw.schema, `${fieldName}.schema`);
  if (Object.keys(schema).length === 0) {
    throw new ValidationError(`${fieldName}.schema must not be empty`);
  }
  if (!Object.keys(schema).some((key) => SERVICE_JSON_SCHEMA_CONTRACT_KEYS.has(key))) {
    throw new ValidationError(`${fieldName}.schema must include a JSON Schema contract keyword`);
  }
  requireJsonWireSafe(schema, `${fieldName}.schema`);
  return { schemaId, schema };
}

function serviceMessageDefinitionsEqual(
  left: ServiceMessageDefinition,
  right: ServiceMessageDefinition,
): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function requireJsonWireSafe(value: unknown, fieldName: string): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new ValidationError(`${fieldName} must not contain NaN or Infinity`);
    }
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      requireJsonWireSafe(item, fieldName);
    }
    return;
  }
  if (typeof value === "object") {
    for (const item of Object.values(value as Record<string, unknown>)) {
      requireJsonWireSafe(item, fieldName);
    }
    return;
  }
  throw new ValidationError(`${fieldName} contains unsupported JSON value type`);
}

function validateServiceExchangePattern(value: unknown): ServiceExchangePattern {
  const pattern = requireText(value, "exchangePattern") as ServiceExchangePattern;
  if (!Object.values(ServiceExchangePattern).includes(pattern)) {
    throw new ValidationError("invalid service exchangePattern");
  }
  return pattern;
}

function validateServiceMessageDirection(value: unknown): ServiceMessageDirection {
  const direction = requireText(value, "direction") as ServiceMessageDirection;
  if (!Object.values(ServiceMessageDirection).includes(direction)) {
    throw new ValidationError("invalid service message direction");
  }
  return direction;
}

function validateServiceMessageIntent(value: unknown): ServiceMessageIntent {
  const intent = requireText(value, "intent") as ServiceMessageIntent;
  if (!Object.values(ServiceMessageIntent).includes(intent)) {
    throw new ValidationError("invalid service message intent");
  }
  return intent;
}

function validateServiceMessageStatus(value: unknown): ServiceMessageStatus {
  const status = requireText(value, "status") as ServiceMessageStatus;
  if (!Object.values(ServiceMessageStatus).includes(status)) {
    throw new ValidationError("invalid service message status");
  }
  return status;
}

function validateServiceViewWriter(value: unknown): ServiceViewWriter {
  const writer = requireText(value, "writer") as ServiceViewWriter;
  if (!Object.values(ServiceViewWriter).includes(writer)) {
    throw new ValidationError("invalid service view writer");
  }
  return writer;
}

function validateTextArray(value: unknown, fieldName: string): string[] {
  if (!Array.isArray(value)) {
    throw new ValidationError(`${fieldName} must be an array`);
  }
  return value.map((item) => requireText(item, fieldName));
}

function normalizeOperations(descriptor: ServiceDescriptor, operations: string[]): Set<string> {
  const normalized = new Set(operations.map((item) => requireText(item, "service operation")));
  for (const operation of normalized) {
    if (!descriptor.supportedOperations.has(operation)) {
      throw new ValidationError(
        `Service ${JSON.stringify(descriptor.serviceId)} does not advertise operation ${JSON.stringify(operation)}`,
      );
    }
  }
  return normalized;
}

function normalizeViewScope(
  descriptor: ServiceDescriptor,
  views: string[] | Record<string, string[]>,
): Record<string, string[]> {
  const requested: Record<string, string[] | null> = {};
  if (Array.isArray(views)) {
    for (const family of views) {
      requested[requireText(family, "service view family")] = null;
    }
  } else {
    for (const [family, prefixes] of Object.entries(views)) {
      requested[requireText(family, "service view family")] = prefixes.map((prefix) =>
        requireText(prefix, "service view prefix"),
      );
    }
  }
  const result: Record<string, string[]> = {};
  for (const [family, prefixes] of Object.entries(requested)) {
    const viewFamily = descriptor.views[family];
    if (viewFamily === undefined) {
      throw new ValidationError(
        `Service ${JSON.stringify(descriptor.serviceId)} does not advertise view family ${JSON.stringify(family)}`,
      );
    }
    const normalized = [...new Set(prefixes ?? [viewFamily.keyPrefix])].sort();
    for (const prefix of normalized) {
      if (!prefix.startsWith(viewFamily.keyPrefix)) {
        throw new ValidationError(
          `View prefix ${JSON.stringify(prefix)} is outside service view family ${JSON.stringify(family)}`,
        );
      }
    }
    result[family] = normalized;
  }
  return result;
}

function viewRefAuthorized(lease: ServiceUseLease, view: ServiceViewRef): boolean {
  for (const [family, prefixes] of Object.entries(lease.terms.allowedViews)) {
    const viewFamily = lease.descriptor.views[family];
    if (viewFamily === undefined || view.storeName !== viewFamily.storeName) {
      continue;
    }
    if (prefixes.some((prefix) => view.key.startsWith(prefix))) {
      return true;
    }
  }
  return false;
}

function assertReadAuthorized(context: ServiceViewReadContext, view: ServiceViewRef): void {
  const family = serviceViewFamilyFor(context.views, view);
  if (family === null) {
    throw new ValidationError(`Service read context does not authorize view ${JSON.stringify(view.key)}`);
  }
  if (family.writer === context.reader) {
    throw new ValidationError(
      `Service view ${JSON.stringify(view.key)} is written by ${context.reader}`,
    );
  }
}

function assertWriteAuthorized(context: ServiceViewWriteContext, view: ServiceViewRef): void {
  const family = serviceViewFamilyFor(context.views, view);
  if (family === null) {
    throw new ValidationError(`Service write context does not authorize view ${JSON.stringify(view.key)}`);
  }
  if (family.writer !== context.writer) {
    throw new ValidationError(
      `Service view ${JSON.stringify(view.key)} writer is ${family.writer}, not ${context.writer}`,
    );
  }
}

function serviceViewFamilyFor(
  views: Record<string, ServiceViewFamily>,
  view: ServiceViewRef,
): ServiceViewFamily | null {
  for (const family of Object.values(views)) {
    if (view.storeName === family.storeName && view.key.startsWith(family.keyPrefix)) {
      return family;
    }
  }
  return null;
}

function fencedServiceViewPayload(
  view: ServiceViewRef,
  payload: JsonObject,
  context: ServiceViewWriteContext,
): JsonObject {
  return {
    ...(cloneJson(payload as JsonValue) as JsonObject),
    viewKey: view.key,
    serviceId: context.serviceId,
    serviceNamespace: context.serviceNamespace,
    serviceEndpoint: context.serviceEndpoint,
    serviceSessionId: context.serviceSessionId,
    consumerEndpoint: context.consumerEndpoint,
    consumerSessionId: context.consumerSessionId,
    writer: context.writer,
    contractId: context.contract.contractId,
    generation: context.contract.generation,
  };
}

function serviceViewEntryFromStateEntry(
  entry: StateEntry,
  storeName: string,
): ServiceViewEntry {
  const value = requireJsonObject(entry.value, "service view value");
  const contract = validateContractPointer({
    contractId: value.contractId,
    generation: value.generation,
  });
  return {
    storeName,
    storageKey: entry.key,
    key: requireText(value.viewKey, "viewKey"),
    value,
    revision: entry.revision,
    serviceId: requireText(value.serviceId, "serviceId"),
    serviceNamespace: requireText(value.serviceNamespace, "serviceNamespace"),
    serviceEndpoint: endpointAddress(requireText(value.serviceEndpoint, "serviceEndpoint")),
    serviceSessionId: requireText(value.serviceSessionId, "serviceSessionId"),
    consumerEndpoint: endpointAddress(requireText(value.consumerEndpoint, "consumerEndpoint")),
    consumerSessionId: requireText(value.consumerSessionId, "consumerSessionId"),
    writer: validateServiceViewWriter(value.writer),
    contract,
  };
}

function serviceViewEntryForRead(
  entry: StateEntry,
  view: ServiceViewRef,
  context: ServiceViewReadContext,
): ServiceViewEntry | null {
  let parsed: ServiceViewEntry;
  try {
    parsed = serviceViewEntryFromStateEntry(entry, view.storeName);
  } catch {
    return null;
  }
  const family = serviceViewFamilyFor(context.views, view);
  if (family === null) {
    return null;
  }
  if (
    parsed.key !== view.key ||
    parsed.storageKey !== serviceViewStorageKey(view, context.contract) ||
    parsed.serviceId !== context.serviceId ||
    parsed.serviceNamespace !== context.serviceNamespace ||
    parsed.serviceEndpoint !== context.serviceEndpoint ||
    parsed.serviceSessionId !== context.serviceSessionId ||
    parsed.consumerEndpoint !== context.consumerEndpoint ||
    parsed.consumerSessionId !== context.consumerSessionId ||
    parsed.contract.contractId !== context.contract.contractId ||
    parsed.contract.generation !== context.contract.generation ||
    parsed.writer !== family.writer ||
    parsed.writer === context.reader
  ) {
    return null;
  }
  return parsed;
}

function leaseCoversScope(
  lease: ServiceUseLease,
  options: { operations: string[]; views: string[] | Record<string, string[]> },
): boolean {
  try {
    const requestedOperations = normalizeOperations(lease.descriptor, options.operations);
    const requestedViews = normalizeViewScope(lease.descriptor, options.views);
    for (const operation of requestedOperations) {
      if (!lease.terms.allowedOperations.includes(operation)) {
        return false;
      }
    }
    for (const [family, prefixes] of Object.entries(requestedViews)) {
      const allowed = lease.terms.allowedViews[family] ?? [];
      if (!prefixes.every((prefix) => allowed.some((allowedPrefix) => prefix.startsWith(allowedPrefix)))) {
        return false;
      }
    }
    return true;
  } catch {
    return false;
  }
}

function leaseKey(
  endpoint: RegisteredEndpointLane,
  descriptor: ServiceDescriptor,
  terms: ServiceUseTerms,
): string {
  return JSON.stringify([
    endpoint.endpoint,
    endpoint.sessionId,
    descriptor.serviceId,
    descriptor.namespace,
    descriptor.endpoint,
    descriptor.sessionId,
    descriptor.useProfile,
    terms.serviceUseScopeId,
  ]);
}

function messageAppliesToService(
  message: DeckrMessage,
  body: ServiceMessageBody,
  options: { serviceId: string; namespace: string; endpoint: string },
): boolean {
  if (body.serviceNamespace !== options.namespace) {
    return false;
  }
  if (message.subject.kind !== "service") {
    return false;
  }
  if (
    message.subject.identifiers.serviceId !== options.serviceId ||
    message.subject.identifiers.namespace !== options.namespace
  ) {
    return false;
  }
  if (message.subject.identifiers.name !== undefined && message.subject.identifiers.name !== body.name) {
    return false;
  }
  if (message.recipient.targetType === "endpoint") {
    return message.recipient.endpoint === options.endpoint;
  }
  return JSON.stringify(message.recipient) === JSON.stringify(endpointTarget(options.endpoint));
}

function rejectedMessage(
  serviceNamespace: string,
  name: string,
  options: { code: string; message: string; diagnostics?: JsonObject },
): ServiceMessageBody {
  return {
    serviceNamespace,
    name,
    intent: ServiceMessageIntent.COMMAND,
    exchangePattern: ServiceExchangePattern.REQUEST_REPLY,
    status: ServiceMessageStatus.REJECTED,
    error: {
      code: options.code,
      message: options.message,
      diagnostics: options.diagnostics ?? {},
    },
  };
}

function unavailableMessage(
  serviceNamespace: string,
  name: string,
  options: { code: string; message: string; diagnostics?: JsonObject },
): ServiceMessageBody {
  return {
    serviceNamespace,
    name,
    intent: ServiceMessageIntent.COMMAND,
    exchangePattern: ServiceExchangePattern.REQUEST_REPLY,
    status: ServiceMessageStatus.UNAVAILABLE,
    error: {
      code: options.code,
      message: options.message,
      diagnostics: options.diagnostics ?? {},
    },
  };
}

export { ManagedServiceClient, type ServiceClientState } from "./service-client.ts";

export { ManagedServiceProvider } from "./service-provider.ts";
