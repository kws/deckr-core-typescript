import { randomUUID } from "node:crypto";
import { validateActionContract } from "./action-contracts.ts";
import { serviceAdvertisementPayload, validateServiceAdvertisementPayload, validateServiceUseTerms, serviceUseTerms, parseServiceDescriptor, serviceViewPrefix, type ServiceAdvertisementPayload, type ServiceUseTerms, type ServiceProtocol, type ServiceMessageDefinition } from "./services.ts";
import type { AdvertisementRecord } from "./beacon.ts";
import {
  actionProviderAddress,
  controllerAddress,
  endpointAddress,
  endpointTarget,
  parseEndpointAddress,
  type BroadcastTarget,
  type MessageTarget,
} from "./endpoint.ts";
import { ValidationError } from "./errors.ts";
import {
  cloneJson,
  isSafeJsonInteger,
  requireJsonObject,
  requireText,
  type JsonObject,
  type JsonValue,
} from "./json.ts";
import { decodeKeyToken, encodeKeyToken } from "./keys.ts";
import {
  entitySubject,
  messageTargetsEndpoint,
  type DeckrMessage,
  type EntitySubject,
} from "./lanes.ts";

export const ACTIONS_PROFILE_ID = "dev.deckr.action_runtime.provider.advertisement.v1";
export const ACTION_PROVIDER_SESSION_PROFILE_ID =
  "dev.deckr.action_runtime.provider.service_use.v1";
export const ACTIONS_FEATURE_ID = "dev.deckr.action_runtime.provider";

export const ACTION_INSTANCE_CREATED = "action_instance_created";
export const ACTION_INSTANCE_DESTROYED = "action_instance_destroyed";
export const BINDING_ATTACHED = "binding_attached";
export const BINDING_DETACHED = "binding_detached";
export const PAGE_SESSION_OPENED = "page_session_opened";
export const PAGE_SESSION_CLOSED = "page_session_closed";
export const ACTION_LIFECYCLE_REJECTED = "action_lifecycle_rejected";
export const CAPABILITY_INPUT = "capability_input";
export const BINDING_OUTPUT = "binding_output";
export const BINDING_RENDER = "binding_render";
export const BINDING_RENDER_RESULT = "binding_render_result";
export const BINDING_OVERLAY = "binding_overlay";
export const BINDING_OVERLAY_CLEAR = "binding_overlay_clear";
export const SETTINGS_REQUEST = "settingsRequest";
export const SETTINGS_SNAPSHOT = "settingsSnapshot";
export const OPEN_PAGE = "open_page";
export const UPDATE_PAGE = "updatePage";
export const REPLACE_PAGE = "replace_page";
export const CLOSE_PAGE = "close_page";
export const ACTION_EXTENSION = "actionExtension";

export interface CapabilityRequirementSelector {
  capabilityId?: string | null;
  family?: string | null;
  type?: string | null;
  direction?: "input" | "output" | "state" | "command" | null;
  eventTypes?: string[];
  commandTypes?: string[];
}

export interface CapabilityRequirement {
  name: string;
  required?: boolean;
  preferences: CapabilityRequirementSelector[];
  eventTypes?: string[];
  commandTypes?: string[];
  views?: Array<"raw" | "native" | "projected" | "derived" | "extension">;
}

export interface ProfileCapacity {
  totalInstances?: number | null;
  claimedInstances?: number;
  availableInstances?: number | null;
}

export interface DynamicPageRoleDescriptor {
  roleId: string;
  cardinality?: "single" | "collection";
  optional?: boolean;
  min?: number;
  preferred?: number;
  max?: number;
  requirements: CapabilityRequirement[];
  layout?: JsonObject;
}

export interface DynamicPageTemplateDescriptor {
  templateId: string;
  roles: DynamicPageRoleDescriptor[];
}

export interface ActionBeaconDescriptor {
  actionId: string;
  name?: string | null;
  providerId?: string | null;
  controllers?: string[] | null;
  requirements?: CapabilityRequirement[] | null;
  dynamicPageTemplates?: DynamicPageTemplateDescriptor[] | null;
  propertyInspectorPath?: string | null;
  manifestDefaults?: JsonObject | null;
  settingsSchema?: JsonObject | null;
  providerSettingsSchema?: JsonObject | null;
  capacity?: ProfileCapacity | null;
  /** @deprecated Application-owned metadata; not part of the Action Runtime wire descriptor. */
  hints?: JsonObject;
}

export interface RegisteredAction {
  actionId: string;
  name?: string;
  providerId?: string;
  controllers?: string[];
  requirements?: CapabilityRequirement[];
  dynamicPageTemplates?: DynamicPageTemplateDescriptor[];
  propertyInspectorPath?: string;
  manifestDefaults?: JsonObject;
  settingsSchema?: JsonObject;
  providerSettingsSchema?: JsonObject;
  capacity?: ProfileCapacity;
  hints?: JsonObject;
}

export interface DeviceRef {
  managerId: string;
  deviceId: string;
  fingerprint?: string;
}

export interface ControlRef {
  deviceRef: DeviceRef;
  controlId: string;
}

export interface CapabilityRef {
  deviceRef?: DeviceRef;
  controlId?: string;
  capabilityId: string;
}

export interface MatchedCapability {
  requirementName: string;
  roleId?: string;
  capability: CapabilityRef;
  family: string;
  type: string;
  direction: "input" | "output" | "state" | "command";
  eventTypes?: string[];
  commandTypes?: string[];
  provenance?: "native" | "projection" | "derivation" | "extension";
  source?: CapabilityRef;
}

export interface BindingMetadata {
  providerInstanceId: string;
  providerId: string;
  actionId: string;
  actionInstanceId: string;
  configId: string;
  contextId: string;
  bindingId: string;
  pageSessionId?: string;
  deviceRef: DeviceRef;
  controlRef: ControlRef;
  roleId?: string;
  itemKey?: string;
  handler?: string;
  matchedCapabilities?: MatchedCapability[];
  outputGeneration?: number;
}

export interface ActionInstanceMetadata {
  providerInstanceId: string;
  providerId: string;
  actionId: string;
  actionInstanceId: string;
  configId: string;
  contextId: string;
}

export interface PageSessionMetadata {
  generation: number;
  providerInstanceId: string;
  providerId: string;
  actionInstanceId: string;
  configId: string;
  pageId: string;
  pageSessionId: string;
  contextId: string;
  templateId?: string;
  ownerBindingId?: string;
  bindings?: BindingMetadata[];
}

export type ActionLifecycleRejectionTargetKind =
  | "action_instance"
  | "binding"
  | "page_session";

export type ActionLifecycleRejectionReason =
  | "action_not_available"
  | "provider_not_ready"
  | "invalid_settings"
  | "unsupported_capability"
  | "resource_unavailable"
  | "permission_denied"
  | "stale_lifecycle"
  | "internal_error";

export interface ActionLifecycleRejectedBody {
  targetKind: ActionLifecycleRejectionTargetKind;
  actionInstance?: ActionInstanceMetadata;
  binding?: BindingMetadata;
  pageSession?: PageSessionMetadata;
  reason: ActionLifecycleRejectionReason;
  message?: string;
  retryable?: boolean;
  details?: JsonObject;
}

export interface CapabilityInputEvent {
  capability: CapabilityRef;
  eventType: string;
  value?: JsonValue;
  sequence?: number;
  occurredAt: string;
  producer?: string;
  source?: CapabilityRef;
  view?: "raw" | "native" | "projected" | "derived" | "extension";
}

export type SettingsScope = "action_provider_instance" | "action_instance";

export interface SettingsTargetRef {
  scope: SettingsScope;
  controllerId: string;
  configId: string;
  providerInstanceId: string;
  providerId: string;
  actionId?: string;
  actionInstanceId?: string;
  stableId?: string;
}

export interface SettingsSchemaMetadata {
  schemaId?: string;
  schema?: JsonObject;
  stale?: boolean;
}

export type SettingsProvenance = string;

export interface SettingsSnapshot {
  target: SettingsTargetRef;
  settings: JsonObject;
  provenance?: SettingsProvenance[];
  schemaMetadata?: SettingsSchemaMetadata;
}

export interface SettingsTargetDescription {
  target: SettingsTargetRef;
  providerInstanceId: string;
  providerId: string;
  actionId?: string;
  label?: string;
  placement?: JsonObject;
  schemaMetadata?: SettingsSchemaMetadata;
  provenance?: SettingsProvenance[];
}

export type ActionsBeaconPayload = ServiceAdvertisementPayload;
export type ActionProviderSessionTerms = ServiceUseTerms;

export function actionRuntimeProtocol(providerInstanceId?: string): ServiceProtocol {
  const serviceId = providerInstanceId === undefined ? undefined : `action-runtime.${requireText(providerInstanceId, "providerInstanceId")}`;
  const rows: [string, "consumer_to_service" | "service_to_consumer", "event" | "command", string][] = [
    [ACTION_INSTANCE_CREATED, "consumer_to_service", "event", "action-instance-lifecycle-body"],
    [ACTION_INSTANCE_DESTROYED, "consumer_to_service", "event", "action-instance-lifecycle-body"],
    [ACTION_LIFECYCLE_REJECTED, "service_to_consumer", "event", "action-lifecycle-rejected-body"],
    [BINDING_ATTACHED, "consumer_to_service", "event", "binding-attached-body"],
    [BINDING_DETACHED, "consumer_to_service", "event", "binding-detached-body"],
    [BINDING_OUTPUT, "service_to_consumer", "command", "binding-output-body"],
    [BINDING_RENDER, "service_to_consumer", "command", "binding-render-body"],
    [BINDING_RENDER_RESULT, "consumer_to_service", "event", "binding-render-result-body"],
    [BINDING_OVERLAY, "service_to_consumer", "command", "binding-overlay-body"],
    [BINDING_OVERLAY_CLEAR, "service_to_consumer", "command", "binding-overlay-clear-body"],
    [CAPABILITY_INPUT, "consumer_to_service", "event", "capability-input-body"],
    [CLOSE_PAGE, "service_to_consumer", "command", "close-page-body"],
    [OPEN_PAGE, "service_to_consumer", "command", "open-page-body"],
    [PAGE_SESSION_CLOSED, "consumer_to_service", "event", "page-session-lifecycle-body"],
    [PAGE_SESSION_OPENED, "consumer_to_service", "event", "page-session-lifecycle-body"],
    [REPLACE_PAGE, "service_to_consumer", "command", "replace-page-body"],
  ];
  const messages: Record<string, ServiceMessageDefinition> = {};
  for (const [name, direction, intent, schema] of rows) {
    const schemaId = `https://kws.github.io/deckr/contract/v1/schemas/deckr/action-runtime/${schema}.v1.schema.json`;
    messages[name] = { operation: name, direction, intent, exchangePattern: "one_way",
      [intent === "event" ? "eventSchema" : "paramsSchema"]: { schemaId, schema: { $ref: schemaId } } };
  }
  return { namespace: ACTIONS_FEATURE_ID, featureId: ACTIONS_FEATURE_ID, advertisementProfile: ACTIONS_PROFILE_ID,
    useProfile: ACTION_PROVIDER_SESSION_PROFILE_ID, operations: Object.fromEntries(rows.map(([name]) => [name, {}])), messages,
    viewFamilies: { action_availability: { storeName: "deckr_action_runtime_service_view_v1", writer: "service", ...(serviceId ? { keyPrefix: serviceViewPrefix(serviceId, "action_availability") } : {}) } } };
}

export interface ActionAvailabilityPayload {
  providerInstanceId: string; providerId: string; serviceId: string; serviceEndpoint: string; serviceSessionId: string;
  labels: Record<string, string>; annotations: JsonObject;
  entries: { actionId: string; status: "available" | "unavailable" | "probing"; descriptor?: ActionBeaconDescriptor; reason?: string }[];
}

/** Build the payload published in the negotiated action_availability view. */
export function actionAvailabilityPayload(input: {
  providerInstanceId: string; providerId: string; sessionId: string;
  labels?: Record<string, string>; annotations?: JsonObject;
  entries: ActionAvailabilityPayload["entries"];
}): ActionAvailabilityPayload {
  return validateActionAvailabilityPayload({
    ...input, serviceId: `action-runtime.${input.providerInstanceId}`,
    serviceEndpoint: actionProviderAddress(input.providerInstanceId), serviceSessionId: input.sessionId,
  });
}

export function validateActionAvailabilityPayload(value: unknown): ActionAvailabilityPayload {
  validateActionContract("availability-payload", value);
  const raw = requireJsonObject(value, "Action Runtime availability");
  const providerInstanceId = requireText(raw.providerInstanceId, "providerInstanceId");
  const serviceId = `action-runtime.${providerInstanceId}`;
  if (raw.serviceId !== serviceId || raw.serviceEndpoint !== actionProviderAddress(providerInstanceId)) throw new ValidationError("Action Runtime service identity mismatch");
  const entries = raw.entries ?? [];
  if (!Array.isArray(entries)) throw new ValidationError("availability entries must be an array");
  const providerId = requireText(raw.providerId, "providerId");
  return { providerInstanceId, providerId, serviceId, serviceEndpoint: actionProviderAddress(providerInstanceId),
    serviceSessionId: requireText(raw.serviceSessionId, "serviceSessionId"), labels: validateStringRecord(raw.labels ?? {}, "labels"), annotations: requireJsonObject(raw.annotations ?? {}, "annotations"),
    entries: entries.map(value => {
      const entry = requireJsonObject(value, "availability entry");
      const actionId = requireText(entry.actionId, "actionId");
      if (entry.status !== "available" && entry.status !== "unavailable" && entry.status !== "probing") throw new ValidationError("Invalid availability status");
      const descriptor = entry.descriptor == null ? undefined : validateActionBeaconDescriptor(entry.descriptor);
      if (entry.status === "available" && !descriptor) throw new ValidationError("Available action requires a descriptor");
      if (descriptor && (descriptor.actionId !== actionId || (descriptor.providerId && descriptor.providerId !== providerId))) throw new ValidationError("Availability descriptor identity mismatch");
      return { actionId, status: entry.status, ...(descriptor ? { descriptor } : {}), ...(entry.reason == null ? {} : { reason: requireText(entry.reason, "reason") }) };
    }) };
}

export function actionBeaconDescriptor(action: RegisteredAction): ActionBeaconDescriptor {
  return validateActionBeaconDescriptor({
    actionId: action.actionId,
    ...(action.name === undefined ? {} : { name: action.name }),
    ...(action.providerId === undefined ? {} : { providerId: action.providerId }),
    ...(action.controllers === undefined ? {} : { controllers: [...action.controllers] }),
    ...(action.requirements === undefined
      ? {}
      : { requirements: cloneJson(action.requirements as unknown as JsonValue) }),
    ...(action.propertyInspectorPath === undefined
      ? {}
      : { propertyInspectorPath: action.propertyInspectorPath }),
    ...(action.manifestDefaults === undefined
      ? {}
      : { manifestDefaults: cloneJson(action.manifestDefaults) }),
    ...(action.settingsSchema === undefined
      ? {}
      : { settingsSchema: cloneJson(action.settingsSchema) }),
    ...(action.providerSettingsSchema === undefined
      ? {}
      : { providerSettingsSchema: cloneJson(action.providerSettingsSchema) }),
  });
}

export function actionCatalogMap(
  actions: Iterable<RegisteredAction>,
): Record<string, ActionBeaconDescriptor> {
  return Object.fromEntries(
    [...actions]
      .filter((action) => action.actionId.trim().length > 0)
      .sort((left, right) => left.actionId.localeCompare(right.actionId))
      .map((action) => [action.actionId, actionBeaconDescriptor(action)]),
  );
}

/** Standard service advertisement; publish action descriptors through actionAvailabilityPayload. */
export function actionsBeaconPayload(input: {
  providerInstanceId: string;
  providerId: string;
  sessionId: string;
  labels?: Record<string, string>;
  annotations?: JsonObject;
  /** @deprecated Accepted for source compatibility; descriptors belong in protected availability views. */
  actions?: Iterable<RegisteredAction> | Record<string, ActionBeaconDescriptor>;
}): ActionsBeaconPayload {
  const providerInstanceId = requireText(input.providerInstanceId, "providerInstanceId");
  return serviceAdvertisementPayload(actionRuntimeProtocol(providerInstanceId), {
    serviceId: `action-runtime.${providerInstanceId}`, sessionId: input.sessionId, backendStatus: "available",
    diagnostics: { providerId: input.providerId },
  });
}

export function validateActionBeaconDescriptor(value: unknown): ActionBeaconDescriptor {
  return validateActionContract("action-descriptor", value) as unknown as ActionBeaconDescriptor;
}

export function validateActionsBeaconPayload(value: unknown): ActionsBeaconPayload {
  const payload = validateServiceAdvertisementPayload(value);
  if (payload.profile !== ACTIONS_PROFILE_ID || payload.serviceNamespace !== ACTIONS_FEATURE_ID ||
    payload.serviceUseProfile !== ACTION_PROVIDER_SESSION_PROFILE_ID || !payload.serviceId.startsWith("action-runtime.") ||
    payload.serviceEndpoint !== `service:${payload.serviceId}`) throw new ValidationError("Invalid Action Runtime advertisement");
  return payload;
}

export function actionsPayloadFromAdvertisement(advertisement: AdvertisementRecord): ActionsBeaconPayload {
  if (advertisement.featureId !== ACTIONS_FEATURE_ID) throw new ValidationError("Invalid Action Runtime feature");
  const payload = validateActionsBeaconPayload(advertisement.payload);
  if (payload.sessionId !== advertisement.sessionId || payload.serviceEndpoint !== advertisement.endpoint) throw new ValidationError("Action Runtime advertisement fence mismatch");
  return payload;
}

/** Compatibility entry point: now builds the standard service-use profile. */
export function actionProviderSessionTerms(input: { sessionId: string; controllerEndpoint: string; providerEndpoint: string; providerInstanceId: string; providerId: string }): ActionProviderSessionTerms {
  if (input.providerEndpoint !== actionProviderAddress(input.providerInstanceId)) throw new ValidationError("Expected Action Runtime service endpoint");
  const protocol = actionRuntimeProtocol(input.providerInstanceId);
  const payload = actionsBeaconPayload({ ...input, actions: [] });
  const descriptor = parseServiceDescriptor({ key: "", revision: 0, observedAt: new Date().toISOString(), advertisement: {
    schema: "dev.deckr.beacon.advertisement.v1", advertisementId: randomUUID(), featureId: protocol.featureId,
    advertiser: input.providerEndpoint, endpoint: input.providerEndpoint, sessionId: input.sessionId,
    refreshSeq: 1, ttlSeconds: 300, operations: [], labels: {}, hints: {}, payload: payload as unknown as JsonObject,
  } }, protocol);
  if (!descriptor) throw new ValidationError("Invalid Action Runtime descriptor");
  return serviceUseTerms(descriptor, input.controllerEndpoint, { operations: Object.keys(protocol.operations), views: ["action_availability"] });
}
export function validateActionProviderSessionTerms(value: unknown): ActionProviderSessionTerms {
  const terms = validateServiceUseTerms(value);
  if (terms.profile !== ACTION_PROVIDER_SESSION_PROFILE_ID || terms.serviceNamespace !== ACTIONS_FEATURE_ID || !terms.serviceId.startsWith("action-runtime.")) throw new ValidationError("Invalid Action Runtime service-use terms");
  return terms;
}
/** @deprecated Proposal lifetimes allocate IDs. This helper returns a new opaque ID. */
export function actionProviderSessionContractId(controllerEndpoint: string, providerEndpoint: string): string {
  endpointAddress(controllerEndpoint); endpointAddress(providerEndpoint);
  return randomUUID();
}
/** @deprecated Action Runtime discovery uses Beacon, not broadcast messages. */
export function actionProvidersBroadcast(): BroadcastTarget {
  throw new ValidationError("Use Action Runtime Beacon discovery and service-use negotiation");
}

export function controllersBroadcast(): BroadcastTarget {
  return {
    targetType: "broadcast",
    scope: "controllers",
    endpointFamily: "controller",
  };
}

export interface ContextSubjectOptions {
  providerInstanceId?: string;
  providerId?: string;
  configId?: string;
  actionInstanceId?: string;
  bindingId?: string;
  pageSessionId?: string;
}

export function contextSubject(
  contextId: string,
  options: ContextSubjectOptions = {},
): EntitySubject {
  return entitySubject("context", {
    contextId: requireText(contextId, "contextId"),
    ...(options.providerInstanceId === undefined
      ? {}
      : { providerInstanceId: requireText(options.providerInstanceId, "providerInstanceId") }),
    ...(options.providerId === undefined
      ? {}
      : { providerId: requireText(options.providerId, "providerId") }),
    ...(options.configId === undefined
      ? {}
      : { configId: requireText(options.configId, "configId") }),
    ...(options.actionInstanceId === undefined
      ? {}
      : { actionInstanceId: requireText(options.actionInstanceId, "actionInstanceId") }),
    ...(options.bindingId === undefined
      ? {}
      : { bindingId: requireText(options.bindingId, "bindingId") }),
    ...(options.pageSessionId === undefined
      ? {}
      : { pageSessionId: requireText(options.pageSessionId, "pageSessionId") }),
  });
}

export function actionProviderInstanceSubject(input: {
  providerInstanceId: string;
  providerId: string;
}): EntitySubject {
  return entitySubject("action_provider_instance", {
    providerInstanceId: requireText(input.providerInstanceId, "providerInstanceId"),
    providerId: requireText(input.providerId, "providerId"),
  });
}

export function subjectContextId(subject: EntitySubject): string | undefined {
  return optionalTextValue(subject.identifiers.contextId, "contextId");
}

export function subjectDeviceId(subject: EntitySubject): string | undefined {
  return optionalTextValue(subject.identifiers.deviceId, "deviceId");
}

export function actionBody(message: DeckrMessage): JsonObject {
  if (message.lane !== "services" || message.messageType !== "serviceMessage" || message.body.serviceNamespace !== ACTIONS_FEATURE_ID) throw new ValidationError("Expected Action Runtime serviceMessage");
  return requireJsonObject(message.body.event ?? message.body.params ?? {}, "Action Runtime payload");
}

export function actionMessageForProvider(
  message: DeckrMessage,
  providerInstanceId: string,
): boolean {
  return messageTargetsEndpoint(message, actionProviderAddress(providerInstanceId));
}

export function actionMessageForController(
  message: DeckrMessage,
  controllerId?: string,
): boolean {
  if (controllerId !== undefined) {
    return messageTargetsEndpoint(message, controllerAddress(controllerId));
  }
  if (message.recipient.targetType === "endpoint") {
    return parseEndpointAddress(message.recipient.endpoint).family === "controller";
  }
  return message.recipient.endpointFamily === "controller";
}

export function settingsTargetKey(target: SettingsTargetRef): string {
  const validated = validateSettingsTargetRef(target);
  const parts = [
    "settings",
    "target",
    encodeKeyToken(validated.scope),
    encodeKeyToken(validated.controllerId),
    encodeKeyToken(validated.configId),
    encodeKeyToken(validated.providerInstanceId),
    encodeKeyToken(validated.providerId),
  ];
  if (validated.scope === "action_instance") {
    parts.push(
      encodeKeyToken(validated.actionId ?? ""),
      encodeKeyToken(validated.actionInstanceId ?? ""),
      validated.stableId === undefined ? "0" : "1",
    );
    if (validated.stableId !== undefined) {
      parts.push(encodeKeyToken(validated.stableId));
    }
  }
  return parts.join(".");
}

export function parseSettingsTargetKey(key: string): SettingsTargetRef | null {
  const parts = key.split(".");
  if (parts.length < 7 || parts[0] !== "settings" || parts[1] !== "target") {
    return null;
  }
  try {
    const scope = decodeKeyToken(parts[2] ?? "");
    const controllerId = decodeKeyToken(parts[3] ?? "");
    const configId = decodeKeyToken(parts[4] ?? "");
    const providerInstanceId = decodeKeyToken(parts[5] ?? "");
    const providerId = decodeKeyToken(parts[6] ?? "");
    if (scope === "action_provider_instance" && parts.length === 7) {
      return validateSettingsTargetRef({
        scope,
        controllerId,
        configId,
        providerInstanceId,
        providerId,
      });
    }
    if (scope !== "action_instance" || ![10, 11].includes(parts.length)) {
      return null;
    }
    const stableFlag = parts[9];
    if (stableFlag === "0" && parts.length !== 10) {
      return null;
    }
    if (stableFlag === "1" && parts.length !== 11) {
      return null;
    }
    if (stableFlag !== "0" && stableFlag !== "1") {
      return null;
    }
    return validateSettingsTargetRef({
      scope,
      controllerId,
      configId,
      providerInstanceId,
      providerId,
      actionId: decodeKeyToken(parts[7] ?? ""),
      actionInstanceId: decodeKeyToken(parts[8] ?? ""),
      ...(stableFlag === "1" ? { stableId: decodeKeyToken(parts[10] ?? "") } : {}),
    });
  } catch {
    return null;
  }
}

export function settingsTargetPayload(target: SettingsTargetRef): JsonObject {
  return validateSettingsTargetRef(target) as unknown as JsonObject;
}

export function validateDeviceRef(value: unknown): DeviceRef {
  validateActionContract("device-ref", value);
  const raw = requireJsonObject(value, "deviceRef");
  const ref: DeviceRef = {
    managerId: requireText(raw.managerId, "managerId"),
    deviceId: requireText(raw.deviceId, "deviceId"),
  };
  setOptionalTextValue(ref, "fingerprint", raw.fingerprint, "fingerprint");
  return ref;
}

export function validateControlRef(value: unknown): ControlRef {
  validateActionContract("control-ref", value);
  const raw = requireJsonObject(value, "controlRef");
  return {
    deviceRef: validateDeviceRef(raw.deviceRef),
    controlId: requireText(raw.controlId, "controlId"),
  };
}

export function validateCapabilityRef(value: unknown): CapabilityRef {
  validateActionContract("capability-ref", value);
  const raw = requireJsonObject(value, "capabilityRef");
  const ref: CapabilityRef = {
    capabilityId: requireText(raw.capabilityId, "capabilityId"),
  };
  if (raw.deviceRef !== undefined && raw.deviceRef !== null) {
    ref.deviceRef = validateDeviceRef(raw.deviceRef);
  }
  setOptionalTextValue(ref, "controlId", raw.controlId, "controlId");
  return ref;
}

export function validateMatchedCapability(value: unknown): MatchedCapability {
  validateActionContract("matched-capability", value);
  const raw = requireJsonObject(value, "matchedCapability");
  const direction = requireText(raw.direction, "matched capability direction");
  if (!isCapabilityDirection(direction)) {
    throw new ValidationError("matched capability direction is invalid");
  }
  const match: MatchedCapability = {
    requirementName: requireText(raw.requirementName, "requirementName"),
    capability: validateCapabilityRef(raw.capability),
    family: requireText(raw.family, "matched capability family"),
    type: requireText(raw.type, "matched capability type"),
    direction,
  };
  setOptionalTextValue(match, "requirementName", raw.requirementName, "requirementName");
  setOptionalTextValue(match, "roleId", raw.roleId, "roleId");
  setOptionalTextListValue(match, "eventTypes", raw.eventTypes, "eventTypes");
  setOptionalTextListValue(match, "commandTypes", raw.commandTypes, "commandTypes");
  if (raw.provenance !== undefined && raw.provenance !== null) {
    const provenance = requireText(raw.provenance, "provenance");
    if (!["native", "projection", "derivation", "extension"].includes(provenance)) {
      throw new ValidationError("matched capability provenance is invalid");
    }
    match.provenance = provenance as MatchedCapability["provenance"];
  }
  if (raw.source !== undefined && raw.source !== null) {
    match.source = validateCapabilityRef(raw.source);
  }
  return match;
}

export function validateBindingMetadata(value: unknown): BindingMetadata {
  validateActionContract("binding-metadata", value);
  const raw = requireJsonObject(value, "binding metadata");
  const binding: BindingMetadata = {
    providerInstanceId: requireText(raw.providerInstanceId, "providerInstanceId"),
    providerId: requireText(raw.providerId, "providerId"),
    actionId: requireText(raw.actionId, "actionId"),
    actionInstanceId: requireText(raw.actionInstanceId, "actionInstanceId"),
    configId: requireText(raw.configId, "configId"),
    contextId: requireText(raw.contextId, "contextId"),
    bindingId: requireText(raw.bindingId, "bindingId"),
    deviceRef: validateDeviceRef(raw.deviceRef),
    controlRef: validateControlRef(raw.controlRef),
  };
  setOptionalTextValue(binding, "pageSessionId", raw.pageSessionId, "pageSessionId");
  setOptionalTextValue(binding, "roleId", raw.roleId, "roleId");
  setOptionalTextValue(binding, "itemKey", raw.itemKey, "itemKey");
  setOptionalTextValue(binding, "handler", raw.handler, "handler");
  if (raw.matchedCapabilities !== undefined && raw.matchedCapabilities !== null) {
    if (!Array.isArray(raw.matchedCapabilities)) {
      throw new ValidationError("matchedCapabilities must be an array");
    }
    binding.matchedCapabilities = raw.matchedCapabilities.map(validateMatchedCapability);
  } else {
    binding.matchedCapabilities = [];
  }
  binding.outputGeneration =
    raw.outputGeneration === undefined || raw.outputGeneration === null
      ? 0
      : requireNonNegativeInteger(raw.outputGeneration, "outputGeneration");
  return binding;
}

export function validateActionInstanceMetadata(value: unknown): ActionInstanceMetadata {
  validateActionContract("action-instance-metadata", value);
  const raw = requireJsonObject(value, "action instance metadata");
  return {
    providerInstanceId: requireText(raw.providerInstanceId, "providerInstanceId"),
    providerId: requireText(raw.providerId, "providerId"),
    actionId: requireText(raw.actionId, "actionId"),
    actionInstanceId: requireText(raw.actionInstanceId, "actionInstanceId"),
    configId: requireText(raw.configId, "configId"),
    contextId: requireText(raw.contextId, "contextId"),
  };
}

export function validatePageSessionMetadata(value: unknown): PageSessionMetadata {
  validateActionContract("page-session-metadata", value);
  const raw = requireJsonObject(value, "page session metadata");
  const session: PageSessionMetadata = {
    generation: requireNonNegativeInteger(raw.generation, "generation"),
    providerInstanceId: requireText(raw.providerInstanceId, "providerInstanceId"),
    providerId: requireText(raw.providerId, "providerId"),
    actionInstanceId: requireText(raw.actionInstanceId, "actionInstanceId"),
    configId: requireText(raw.configId, "configId"),
    pageId: requireText(raw.pageId, "pageId"),
    pageSessionId: requireText(raw.pageSessionId, "pageSessionId"),
    contextId: requireText(raw.contextId, "contextId"),
  };
  setOptionalTextValue(session, "templateId", raw.templateId, "templateId");
  setOptionalTextValue(session, "ownerBindingId", raw.ownerBindingId, "ownerBindingId");
  if (raw.bindings !== undefined && raw.bindings !== null) {
    if (!Array.isArray(raw.bindings)) {
      throw new ValidationError("page session bindings must be an array");
    }
    session.bindings = raw.bindings.map(validateBindingMetadata);
  } else {
    session.bindings = [];
  }
  return session;
}

export function validateActionLifecycleRejectedBody(
  value: unknown,
): ActionLifecycleRejectedBody {
  validateActionContract("action-lifecycle-rejected-body", value);
  const raw = requireJsonObject(value, "action lifecycle rejection");
  const targetKind = requireText(raw.targetKind, "targetKind");
  if (!isActionLifecycleRejectionTargetKind(targetKind)) {
    throw new ValidationError("action lifecycle rejection targetKind is invalid");
  }
  const reason = requireText(raw.reason, "reason");
  if (!isActionLifecycleRejectionReason(reason)) {
    throw new ValidationError("action lifecycle rejection reason is invalid");
  }
  const body: ActionLifecycleRejectedBody = {
    targetKind,
    reason,
    retryable: raw.retryable === undefined ? false : raw.retryable === true,
    details:
      raw.details === undefined || raw.details === null
        ? {}
        : requireJsonObject(raw.details, "details"),
  };
  if (raw.retryable !== undefined && typeof raw.retryable !== "boolean") {
    throw new ValidationError("retryable must be boolean");
  }
  setOptionalTextValue(body, "message", raw.message, "message");
  if (raw.actionInstance !== undefined && raw.actionInstance !== null) {
    body.actionInstance = validateActionInstanceMetadata(raw.actionInstance);
  }
  if (raw.binding !== undefined && raw.binding !== null) {
    body.binding = validateBindingMetadata(raw.binding);
  }
  if (raw.pageSession !== undefined && raw.pageSession !== null) {
    body.pageSession = validatePageSessionMetadata(raw.pageSession);
  }
  const present = [
    body.actionInstance === undefined ? undefined : "action_instance",
    body.binding === undefined ? undefined : "binding",
    body.pageSession === undefined ? undefined : "page_session",
  ].filter((item) => item !== undefined);
  if (present.length !== 1 || present[0] !== targetKind) {
    throw new ValidationError(
      "action lifecycle rejection requires exactly one target matching targetKind",
    );
  }
  body.details = cloneJson(body.details);
  return body;
}

export function validateCapabilityInputEvent(value: unknown): CapabilityInputEvent {
  validateActionContract("capability-input-event", value);
  const raw = requireJsonObject(value, "capability input event");
  const event: CapabilityInputEvent = {
    capability: validateCapabilityRef(raw.capability),
    eventType: requireText(raw.eventType, "eventType"),
    occurredAt: requireText(raw.occurredAt, "occurredAt"),
  };
  if (raw.value !== undefined) {
    event.value = cloneJson(raw.value as JsonValue);
  }
  if (raw.sequence !== undefined && raw.sequence !== null) {
    event.sequence = requireNonNegativeInteger(raw.sequence, "sequence");
  }
  setOptionalTextValue(event, "producer", raw.producer, "producer");
  if (raw.source !== undefined && raw.source !== null) {
    event.source = validateCapabilityRef(raw.source);
  }
  if (raw.view !== undefined && raw.view !== null) {
    const view = requireText(raw.view, "view");
    if (!["raw", "native", "projected", "derived", "extension"].includes(view)) {
      throw new ValidationError("capability input view is invalid");
    }
    event.view = view as CapabilityInputEvent["view"];
  }
  return event;
}

export function validateSettingsTargetRef(value: unknown): SettingsTargetRef {
  validateActionContract("settings-target-ref", value);
  const raw = requireJsonObject(value, "settings target");
  const scope = requireText(raw.scope, "settings target scope");
  if (scope !== "action_provider_instance" && scope !== "action_instance") {
    throw new ValidationError("settings target scope is invalid");
  }
  const target: SettingsTargetRef = {
    scope,
    controllerId: requireText(raw.controllerId, "controllerId"),
    configId: requireText(raw.configId, "configId"),
    providerInstanceId: requireText(raw.providerInstanceId, "providerInstanceId"),
    providerId: requireText(raw.providerId, "providerId"),
  };
  const actionId = optionalTextValue(raw.actionId, "actionId");
  const actionInstanceId = optionalTextValue(raw.actionInstanceId, "actionInstanceId");
  const stableId = optionalTextValue(raw.stableId, "stableId");
  if (scope === "action_provider_instance") {
    if (actionId !== undefined || actionInstanceId !== undefined || stableId !== undefined) {
      throw new ValidationError(
        "action provider instance settings target must not include action ids",
      );
    }
    return target;
  }
  if (actionId === undefined || actionInstanceId === undefined) {
    throw new ValidationError("action instance settings target requires actionId and actionInstanceId");
  }
  target.actionId = actionId;
  target.actionInstanceId = actionInstanceId;
  if (stableId !== undefined) {
    target.stableId = stableId;
  }
  return target;
}

export function validateSettingsSchemaMetadata(value: unknown): SettingsSchemaMetadata {
  validateActionContract("settings-schema-metadata", value);
  const raw = requireJsonObject(value, "settings schema metadata");
  const metadata: SettingsSchemaMetadata = {};
  if (raw.schemaId != null) metadata.schemaId = raw.schemaId as string;
  if (raw.schema !== undefined && raw.schema !== null) {
    metadata.schema = requireJsonObject(raw.schema, "schema");
  }
  if (raw.stale !== undefined) {
    if (typeof raw.stale !== "boolean") {
      throw new ValidationError("settings schema metadata stale must be boolean");
    }
    metadata.stale = raw.stale;
  }
  return metadata;
}

export function validateSettingsSnapshot(value: unknown): SettingsSnapshot {
  validateActionContract("settings-snapshot", value);
  const raw = requireJsonObject(value, "settings snapshot");
  const snapshot: SettingsSnapshot = {
    target: validateSettingsTargetRef(raw.target),
    settings:
      raw.settings === undefined || raw.settings === null
        ? {}
        : requireJsonObject(raw.settings, "settings"),
  };
  setOptionalTextListValue(snapshot, "provenance", raw.provenance, "provenance");
  if (raw.schemaMetadata !== undefined && raw.schemaMetadata !== null) {
    snapshot.schemaMetadata = validateSettingsSchemaMetadata(raw.schemaMetadata);
  }
  return snapshot;
}

export function validateSettingsTargetDescription(value: unknown): SettingsTargetDescription {
  validateActionContract("settings-target-description", value);
  const raw = requireJsonObject(value, "settings target description");
  const target = validateSettingsTargetRef(raw.target);
  const description: SettingsTargetDescription = {
    target,
    providerInstanceId: requireText(raw.providerInstanceId, "providerInstanceId"),
    providerId: requireText(raw.providerId, "providerId"),
  };
  setOptionalTextValue(description, "actionId", raw.actionId, "actionId");
  setOptionalTextValue(description, "label", raw.label, "label");
  if (raw.placement !== undefined && raw.placement !== null) {
    description.placement = requireJsonObject(raw.placement, "placement");
  }
  if (raw.schemaMetadata !== undefined && raw.schemaMetadata !== null) {
    description.schemaMetadata = validateSettingsSchemaMetadata(raw.schemaMetadata);
  }
  setOptionalTextListValue(description, "provenance", raw.provenance, "provenance");
  if (description.providerInstanceId !== target.providerInstanceId) {
    throw new ValidationError("settings target description providerInstanceId must match target");
  }
  if (description.providerId !== target.providerId) {
    throw new ValidationError("settings target description providerId must match target");
  }
  if (description.actionId !== target.actionId) {
    throw new ValidationError("settings target description actionId must match target");
  }
  return description;
}

function validateCapabilityRequirement(value: unknown): CapabilityRequirement {
  const raw = requireJsonObject(value, "capability requirement");
  if (!Array.isArray(raw.preferences)) {
    throw new ValidationError("capability requirement preferences must be an array");
  }
  const requirement: CapabilityRequirement = {
    name: requireText(raw.name, "capability requirement name"),
    preferences: raw.preferences.map(validateCapabilityRequirementSelector),
  };
  if (raw.required !== undefined) {
    if (typeof raw.required !== "boolean") {
      throw new ValidationError("capability requirement required must be boolean");
    }
    requirement.required = raw.required;
  }
  setOptionalTextList(requirement, "eventTypes", raw.eventTypes, "eventTypes", false);
  setOptionalTextList(requirement, "commandTypes", raw.commandTypes, "commandTypes", false);
  setOptionalTextList(requirement, "views", raw.views, "views", false);
  return requirement;
}

function validateCapabilityRequirementSelector(value: unknown): CapabilityRequirementSelector {
  const raw = requireJsonObject(value, "capability requirement selector");
  const selector: CapabilityRequirementSelector = {};
  setOptionalText(selector, "capabilityId", raw.capabilityId, "capabilityId", true);
  setOptionalText(selector, "family", raw.family, "family", true);
  setOptionalText(selector, "type", raw.type, "type", true);
  setOptionalText(selector, "direction", raw.direction, "direction", true);
  setOptionalTextList(selector, "eventTypes", raw.eventTypes, "eventTypes", false);
  setOptionalTextList(selector, "commandTypes", raw.commandTypes, "commandTypes", false);
  return selector;
}

function validateDynamicPageTemplate(value: unknown): DynamicPageTemplateDescriptor {
  const raw = requireJsonObject(value, "dynamic page template");
  if (!Array.isArray(raw.roles)) {
    throw new ValidationError("dynamic page template roles must be an array");
  }
  return {
    templateId: requireText(raw.templateId, "templateId"),
    roles: raw.roles.map(validateDynamicPageRole),
  };
}

function validateDynamicPageRole(value: unknown): DynamicPageRoleDescriptor {
  const raw = requireJsonObject(value, "dynamic page role");
  if (!Array.isArray(raw.requirements)) {
    throw new ValidationError("dynamic page role requirements must be an array");
  }
  const role: DynamicPageRoleDescriptor = {
    roleId: requireText(raw.roleId, "roleId"),
    requirements: raw.requirements.map(validateCapabilityRequirement),
  };
  setOptionalText(role, "cardinality", raw.cardinality, "cardinality", false);
  if (raw.optional !== undefined) {
    if (typeof raw.optional !== "boolean") {
      throw new ValidationError("dynamic page role optional must be boolean");
    }
    role.optional = raw.optional;
  }
  setOptionalInteger(role, "min", raw.min, "min");
  setOptionalInteger(role, "preferred", raw.preferred, "preferred");
  setOptionalInteger(role, "max", raw.max, "max");
  setOptionalObject(role, "layout", raw.layout, "layout", false);
  return role;
}

function validateProfileCapacity(value: unknown): ProfileCapacity {
  const raw = requireJsonObject(value, "profile capacity");
  const capacity: ProfileCapacity = {};
  setOptionalInteger(capacity, "totalInstances", raw.totalInstances, "totalInstances", true);
  setOptionalInteger(capacity, "claimedInstances", raw.claimedInstances, "claimedInstances");
  setOptionalInteger(capacity, "availableInstances", raw.availableInstances, "availableInstances", true);
  return capacity;
}

function validateStringRecord(value: unknown, fieldName: string): Record<string, string> {
  const raw = requireJsonObject(value, fieldName);
  return Object.fromEntries(Object.entries(raw).map(([key, item]) => [requireText(key, `${fieldName} key`), requireText(item, `${fieldName} value`)]));
}

function setOptionalText<T extends object, K extends keyof T & string>(
  target: T,
  key: K,
  value: unknown,
  fieldName: string,
  nullable: boolean,
): void {
  if (value === undefined) {
    return;
  }
  if (value === null && nullable) {
    (target as Record<string, unknown>)[key] = null;
    return;
  }
  (target as Record<string, unknown>)[key] = requireText(value, fieldName);
}

function setOptionalTextList<T extends object, K extends keyof T & string>(
  target: T,
  key: K,
  value: unknown,
  fieldName: string,
  nullable: boolean,
): void {
  if (value === undefined) {
    return;
  }
  if (value === null && nullable) {
    (target as Record<string, unknown>)[key] = null;
    return;
  }
  if (!Array.isArray(value)) {
    throw new ValidationError(`${fieldName} must be an array`);
  }
  (target as Record<string, unknown>)[key] = value.map((item) => requireText(item, fieldName));
}

function setOptionalObject<T extends object, K extends keyof T & string>(
  target: T,
  key: K,
  value: unknown,
  fieldName: string,
  nullable: boolean,
): void {
  if (value === undefined) {
    return;
  }
  if (value === null && nullable) {
    (target as Record<string, unknown>)[key] = null;
    return;
  }
  (target as Record<string, unknown>)[key] = requireJsonObject(value, fieldName);
}

function setOptionalInteger<T extends object, K extends keyof T & string>(
  target: T,
  key: K,
  value: unknown,
  fieldName: string,
  nullable = false,
): void {
  if (value === undefined) {
    return;
  }
  if (value === null && nullable) {
    (target as Record<string, unknown>)[key] = null;
    return;
  }
  if (!isSafeJsonInteger(value)) {
    throw new ValidationError(`${fieldName} must be an integer`);
  }
  (target as Record<string, unknown>)[key] = Number(value);
}

function optionalTextValue(value: unknown, fieldName: string): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  return requireText(value, fieldName);
}

function setOptionalTextValue<T extends object, K extends keyof T & string>(
  target: T,
  key: K,
  value: unknown,
  fieldName: string,
): void {
  const text = optionalTextValue(value, fieldName);
  if (text !== undefined) {
    (target as Record<string, unknown>)[key] = text;
  }
}

function setOptionalTextListValue<T extends object, K extends keyof T & string>(
  target: T,
  key: K,
  value: unknown,
  fieldName: string,
): void {
  if (value === undefined || value === null) {
    return;
  }
  if (!Array.isArray(value)) {
    throw new ValidationError(`${fieldName} must be an array`);
  }
  (target as Record<string, unknown>)[key] = value.map((item) =>
    requireText(item, fieldName),
  );
}

function requireNonNegativeInteger(value: unknown, fieldName: string): number {
  if (!isSafeJsonInteger(value) || Number(value) < 0) {
    throw new ValidationError(`${fieldName} must be a non-negative integer`);
  }
  return Number(value);
}

function isCapabilityDirection(
  value: string,
): value is MatchedCapability["direction"] {
  return ["input", "output", "state", "command"].includes(value);
}

function isActionLifecycleRejectionTargetKind(
  value: string,
): value is ActionLifecycleRejectionTargetKind {
  return ["action_instance", "binding", "page_session"].includes(value);
}

function isActionLifecycleRejectionReason(
  value: string,
): value is ActionLifecycleRejectionReason {
  return [
    "action_not_available",
    "provider_not_ready",
    "invalid_settings",
    "unsupported_capability",
    "resource_unavailable",
    "permission_denied",
    "stale_lifecycle",
    "internal_error",
  ].includes(value);
}

export { validateActionContract, validateActionRuntimePayload, validateActionRuntimeMessage, actionRuntimeMessageDefinition, ActionRuntimePayloadSchemas, actionRuntimePayloadSchemas } from "./action-contracts.ts";
export { validateActionRuntimeReferences, type ActionRuntimeReferenceState } from "./action-references.ts";
export { ManagedActionRuntimeProvider, ManagedActionRuntimeClient, type ManagedActionRuntimeProviderOptions, type ManagedActionRuntimeClientOptions, type ActionReferenceResolver, type ActionRuntimeAuthority, type ActionRuntimeOperation, type ActionRuntimeContext, type ActionRuntimeSendReceipt, type ActionAvailabilityChange } from "./action-runtime.ts";

export interface BindingCommandTarget { kind: "binding"; bindingId: string; generation: number }
export interface DynamicPageCommand {
  pageId: string;
  bindings: {
    controlId: string;
    target: { kind: "self" } | { kind: "action"; actionId: string; providerInstanceId?: string; providerLabels?: Record<string, string>; instanceKey?: string };
    itemKey?: string; handler?: string; settings?: JsonObject; internal?: JsonObject;
  }[];
}
export interface PageSessionCommandTarget { kind: "page_session"; pageSessionId: string; generation: number }
export type PageCommandTarget = BindingCommandTarget | PageSessionCommandTarget;
export interface OpenPageBody { target: PageCommandTarget; descriptor: DynamicPageCommand }
export interface ReplacePageBody { target: PageSessionCommandTarget; descriptor: DynamicPageCommand }
export interface ClosePageBody { target: PageSessionCommandTarget }
export { matchCapabilityRequirements, validateBindingResources, type CapabilityMatchingScope } from "./action-matching.ts";
export { validatePageCommand, type PageCommandContext, type PageTargetState } from "./action-pages.ts";

export * from "./action-rendering-contracts.ts";
export * from "./action-rendering.ts";
