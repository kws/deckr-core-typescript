import type { ValidateFunction } from "ajv";
import { ValidationError } from "./errors.ts";
import { cloneJson, requireJsonObject, requireText, type JsonObject, type JsonValue } from "./json.ts";
import { validateHardwareDeviceRef } from "./hardware-messages.ts";
import { validateDeckrMessage, type DeckrMessage } from "./lanes.ts";
import { PayloadSchemas } from "./schema.ts";
import { assertJsonValue, closedObject, timestamp } from "./validation.ts";

const namespace = "dev.deckr.action_runtime.provider";
const schemaPrefix = "https://kws.github.io/deckr/contract/v1/schemas/deckr/action-runtime/";
const directions = ["input", "output", "state", "command"];
const views = ["raw", "native", "projected", "derived", "extension"];
const families = ["dev.deckr.device.power", "dev.deckr.input.button", "dev.deckr.input.encoder", "dev.deckr.input.touch", "dev.deckr.output.raster"];
const instanceFields = ["providerInstanceId", "providerId", "actionId", "actionInstanceId", "configId", "contextId"];
const bindingFields = [...instanceFields, "bindingId", "deviceRef", "controlRef", "pageSessionId", "itemKey", "handler", "matchedCapabilities", "outputGeneration"];
const pageFields = ["providerInstanceId", "providerId", "actionInstanceId", "configId", "pageId", "pageSessionId", "contextId", "ownerBindingId", "bindings"];
const payloads: Record<string, { contract: string; direction: "consumer_to_service" | "service_to_consumer"; intent: "event" | "command" }> = {
  action_instance_created: { contract: "action-instance-lifecycle-body", direction: "consumer_to_service", intent: "event" },
  action_instance_destroyed: { contract: "action-instance-lifecycle-body", direction: "consumer_to_service", intent: "event" },
  binding_attached: { contract: "binding-attached-body", direction: "consumer_to_service", intent: "event" },
  binding_detached: { contract: "binding-detached-body", direction: "consumer_to_service", intent: "event" },
  page_session_opened: { contract: "page-session-lifecycle-body", direction: "consumer_to_service", intent: "event" },
  page_session_closed: { contract: "page-session-lifecycle-body", direction: "consumer_to_service", intent: "event" },
  capability_input: { contract: "capability-input-body", direction: "consumer_to_service", intent: "event" },
  action_lifecycle_rejected: { contract: "action-lifecycle-rejected-body", direction: "service_to_consumer", intent: "event" },
  binding_output: { contract: "binding-output-body", direction: "service_to_consumer", intent: "command" },
  binding_overlay: { contract: "binding-overlay-body", direction: "service_to_consumer", intent: "command" },
  binding_overlay_clear: { contract: "binding-overlay-clear-body", direction: "service_to_consumer", intent: "command" },
  open_page: { contract: "open-page-body", direction: "service_to_consumer", intent: "command" },
  replace_page: { contract: "replace-page-body", direction: "service_to_consumer", intent: "command" },
  close_page: { contract: "empty-action-body", direction: "service_to_consumer", intent: "command" },
};
function fail(message: string): never { throw new ValidationError(message); }
function text(value: unknown, label: string): string { return requireText(value, label); }
function string(value: unknown, label: string): void { if (typeof value !== "string") fail(`${label} must be a string`); }
function choice(value: unknown, choices: string[], label: string): void { if (typeof value !== "string" || !choices.includes(value)) fail(`Invalid ${label}`); }
function integer(value: unknown, label: string): void { if (!Number.isSafeInteger(value) || Number(value) < 0) fail(`${label} must be a nonnegative safe integer`); }
function array(value: unknown, label: string): JsonValue[] { if (!Array.isArray(value)) fail(`${label} must be an array`); return value; }
function unique(values: unknown[], label: string): void { if (new Set(values).size !== values.length) fail(`Duplicate ${label}`); }
function optional(raw: JsonObject, fields: string[], validator = text): void { for (const key of fields) if (raw[key] != null) validator(raw[key], key); }
function objectFields(raw: JsonObject, fields: string[]): void { for (const key of fields) if (raw[key] !== undefined) requireJsonObject(raw[key], key); }
function names(raw: JsonObject, fields: string[]): void { for (const key of fields) if (raw[key] !== undefined) array(raw[key], key).forEach(item => actionCapabilityName(item, key)); }
export function actionProviderInstanceId(value: unknown): string {
  const id = text(value, "providerInstanceId");
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(id)) fail("Invalid providerInstanceId");
  return id;
}
export function actionCapabilityName(value: unknown, label = "capability name"): string {
  const name = text(value, label);
  if (!/^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)*$/.test(name)) fail(`Invalid ${label}`);
  return name;
}
function family(value: unknown): void {
  const name = text(value, "family");
  if (!families.includes(name) && !/^(?!dev\.deckr\.)[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$/.test(name)) fail("Invalid capability family");
}
function labels(value: unknown): void { for (const [key, item] of Object.entries(requireJsonObject(value, "labels"))) { text(key, "label key"); text(item, "label value"); } }
function instance(raw: JsonObject, fields = instanceFields): void { for (const key of fields) text(raw[key], key); actionProviderInstanceId(raw.providerInstanceId); }
function sameDevice(left: unknown, right: unknown): boolean {
  const a = validateHardwareDeviceRef(left), b = validateHardwareDeviceRef(right);
  return a.managerId === b.managerId && a.deviceId === b.deviceId && (!a.fingerprint || !b.fingerprint || a.fingerprint === b.fingerprint);
}
function capability(value: unknown, concrete = false): JsonObject {
  const raw = closedObject(value, ["deviceRef", "controlId", "capabilityId"], "capability reference");
  actionCapabilityName(raw.capabilityId, "capabilityId");
  if (concrete || raw.deviceRef != null) validateHardwareDeviceRef(raw.deviceRef);
  if (concrete || raw.controlId != null) text(raw.controlId, "controlId");
  return raw;
}

/** Native core contract validation. No schema files are copied into the package. */
export function validateActionContract(contract: string, value: unknown): JsonObject {
  assertJsonValue(value);
  let raw: JsonObject;
  switch (contract) {
    case "device-ref": validateHardwareDeviceRef(value); break;
    case "control-ref":
      raw = closedObject(value, ["deviceRef", "controlId"], contract); validateHardwareDeviceRef(raw.deviceRef); text(raw.controlId, "controlId"); break;
    case "capability-ref": capability(value); break;
    case "action-instance-metadata":
      raw = closedObject(value, instanceFields, contract); instance(raw); break;
    case "binding-metadata":
      raw = closedObject(value, bindingFields, contract); instance(raw); text(raw.bindingId, "bindingId");
      validateHardwareDeviceRef(raw.deviceRef); validateActionContract("control-ref", raw.controlRef);
      if (!sameDevice(raw.deviceRef, (raw.controlRef as JsonObject).deviceRef)) fail("Binding control belongs to another device");
      optional(raw, ["pageSessionId", "itemKey", "handler"]);
      if (raw.outputGeneration !== undefined) integer(raw.outputGeneration, "outputGeneration");
      if (raw.matchedCapabilities !== undefined) for (const item of array(raw.matchedCapabilities, "matchedCapabilities")) validateActionContract("matched-capability", item);
      break;
    case "page-session-metadata":
      raw = closedObject(value, pageFields, contract); instance(raw, pageFields.slice(0, 7)); optional(raw, ["ownerBindingId"]);
      if (raw.bindings !== undefined) {
        const bindings = array(raw.bindings, "bindings").map(item => validateActionContract("binding-metadata", item));
        unique(bindings.map(binding => binding.bindingId), "bindingId");
        for (const binding of bindings) {
          if (binding.pageSessionId != null && binding.pageSessionId !== raw.pageSessionId) fail("Binding belongs to another page session");
        }
      }
      break;
    case "matched-capability":
      raw = closedObject(value, ["capability", "commandTypes", "direction", "eventTypes", "family", "provenance", "requirementName", "source", "type"], contract);
      capability(raw.capability, true); family(raw.family); actionCapabilityName(raw.type, "type"); choice(raw.direction, directions, "direction");
      names(raw, ["commandTypes", "eventTypes"]); optional(raw, ["requirementName"]);
      if (raw.provenance !== undefined) choice(raw.provenance, ["native", "projection", "derivation", "extension"], "provenance");
      if (raw.source != null) capability(raw.source); break;
    case "capability-input-event":
      raw = closedObject(value, ["capability", "eventType", "occurredAt", "producer", "sequence", "source", "value", "view"], contract);
      capability(raw.capability, true); actionCapabilityName(raw.eventType, "eventType"); timestamp(raw.occurredAt, "occurredAt");
      optional(raw, ["producer"]); if (raw.sequence != null) integer(raw.sequence, "sequence");
      if (raw.source != null) capability(raw.source); if (raw.view != null) choice(raw.view, views, "view"); break;
    case "capability-requirement-selector":
      raw = closedObject(value, ["capabilityId", "family", "type", "direction", "eventTypes", "commandTypes"], contract);
      optional(raw, ["capabilityId", "type"], actionCapabilityName); if (raw.family != null) family(raw.family); if (raw.direction != null) choice(raw.direction, directions, "direction");
      names(raw, ["eventTypes", "commandTypes"]);
      if (!["capabilityId", "family", "type", "direction"].some(key => raw[key] != null) && !["eventTypes", "commandTypes"].some(key => Array.isArray(raw[key]) && raw[key].length)) fail("Requirement selector must constrain a capability"); break;
    case "capability-requirement":
      raw = closedObject(value, ["name", "preferences", "required", "eventTypes", "commandTypes", "views"], contract); text(raw.name, "name");
      if (raw.required !== undefined && typeof raw.required !== "boolean") fail("required must be boolean");
      if (!array(raw.preferences, "preferences").length) fail("Requirement needs at least one preference");
      for (const preference of raw.preferences as JsonValue[]) validateActionContract("capability-requirement-selector", preference);
      names(raw, ["eventTypes", "commandTypes"]); if (raw.views !== undefined) array(raw.views, "views").forEach(view => choice(view, views, "view")); break;
    case "action-descriptor":
      raw = closedObject(value, ["actionId", "controllers", "manifestDefaults", "name", "propertyInspectorPath", "providerId", "providerSettingsSchema", "requirements", "settingsSchema"], contract);
      text(raw.actionId, "actionId"); optional(raw, ["providerId"]);
      for (const field of ["name", "propertyInspectorPath"]) if (raw[field] != null) string(raw[field], field);
      if (raw.controllers != null) array(raw.controllers, "controllers").forEach(controller => string(controller, "controller"));
      for (const field of ["manifestDefaults", "providerSettingsSchema", "settingsSchema"]) if (raw[field] != null) requireJsonObject(raw[field], field);
      if (raw.requirements != null) {
        const requirements = array(raw.requirements, "requirements").map(item => validateActionContract("capability-requirement", item));
        unique(requirements.map(item => item.name), "requirement name");
      }
      break;
    case "action-availability-entry":
      raw = closedObject(value, ["actionId", "descriptor", "reason", "status"], contract); text(raw.actionId, "actionId"); choice(raw.status, ["available", "unavailable", "probing"], "status"); optional(raw, ["reason"]);
      if (raw.status === "available" && raw.descriptor == null) fail("Available action requires a descriptor");
      if (raw.descriptor != null && validateActionContract("action-descriptor", raw.descriptor).actionId !== raw.actionId) fail("Availability descriptor actionId mismatch"); break;
    case "availability-payload":
      raw = requireJsonObject(value, contract); actionProviderInstanceId(raw.providerInstanceId); text(raw.providerId, "providerId"); text(raw.serviceSessionId, "serviceSessionId");
      if (raw.serviceId !== `action-runtime.${raw.providerInstanceId}` || raw.serviceEndpoint !== `service:${raw.serviceId}`) fail("Action Runtime service identity mismatch");
      if (raw.annotations !== undefined) requireJsonObject(raw.annotations, "annotations"); if (raw.labels !== undefined) labels(raw.labels);
      if (raw.entries !== undefined) {
        const entries = array(raw.entries, "entries").map(item => validateActionContract("action-availability-entry", item));
        unique(entries.map(entry => entry.actionId), "availability actionId");
        for (const entry of entries) if ((entry.descriptor as JsonObject | null)?.providerId != null && (entry.descriptor as JsonObject).providerId !== raw.providerId) fail("Availability providerId mismatch");
      }
      break;
    case "action-availability-selector": raw = closedObject(value, ["actionId"], contract); text(raw.actionId, "actionId"); break;
    case "action-instance-lifecycle-body":
      raw = closedObject(value, ["metadata", "reason"], contract); validateActionContract("action-instance-metadata", raw.metadata); if (raw.reason != null) string(raw.reason, "reason"); break;
    case "binding-attached-body":
      raw = closedObject(value, ["binding", "internal", "settings"], contract); validateActionContract("binding-metadata", raw.binding); objectFields(raw, ["internal", "settings"]); break;
    case "binding-detached-body":
      raw = closedObject(value, ["binding", "reason"], contract); validateActionContract("binding-metadata", raw.binding); string(raw.reason, "reason"); break;
    case "page-session-lifecycle-body":
      raw = closedObject(value, ["pageSession", "reason"], contract); validateActionContract("page-session-metadata", raw.pageSession); if (raw.reason != null) string(raw.reason, "reason"); break;
    case "capability-input-body":
      raw = closedObject(value, ["binding", "event"], contract); validateActionContract("binding-metadata", raw.binding); validateActionContract("capability-input-event", raw.event); break;
    case "binding-output-body":
      raw = closedObject(value, ["binding", "capability", "commandType", "generation", "params"], contract);
      validateActionContract("binding-metadata", raw.binding); capability(raw.capability, true); actionCapabilityName(raw.commandType, "commandType"); integer(raw.generation, "generation"); objectFields(raw, ["params"]); break;
    case "binding-overlay-body":
      raw = closedObject(value, ["binding", "durationSeconds", "generation", "overlayId", "params", "template"], contract);
      validateActionContract("binding-metadata", raw.binding); integer(raw.generation, "generation"); actionCapabilityName(raw.template, "template"); optional(raw, ["overlayId"]); objectFields(raw, ["params"]);
      if (raw.durationSeconds != null && (typeof raw.durationSeconds !== "number" || raw.durationSeconds <= 0)) fail("durationSeconds must be positive"); break;
    case "binding-overlay-clear-body":
      raw = closedObject(value, ["binding", "generation", "overlayId"], contract); validateActionContract("binding-metadata", raw.binding); integer(raw.generation, "generation"); optional(raw, ["overlayId"]); break;
    case "action-lifecycle-rejected-body":
      raw = closedObject(value, ["actionInstance", "binding", "pageSession", "targetKind", "reason", "message", "retryable", "details"], contract);
      choice(raw.targetKind, ["action_instance", "binding", "page_session"], "targetKind");
      choice(raw.reason, ["action_not_available", "provider_not_ready", "invalid_settings", "unsupported_capability", "resource_unavailable", "permission_denied", "stale_lifecycle", "internal_error"], "reason");
      optional(raw, ["message"]); objectFields(raw, ["details"]); if (raw.retryable !== undefined && typeof raw.retryable !== "boolean") fail("retryable must be boolean");
      for (const [key, kind, schema] of [["actionInstance", "action_instance", "action-instance-metadata"], ["binding", "binding", "binding-metadata"], ["pageSession", "page_session", "page-session-metadata"]] as const) {
        if (raw.targetKind === kind) validateActionContract(schema, raw[key]); else if (raw[key] != null) fail("Lifecycle rejection has another target");
      }
      break;
    case "page-child-binding-target":
      raw = closedObject(value, ["actionId", "instanceKey", "kind", "providerInstanceId", "providerLabels"], contract); choice(raw.kind, ["self", "action"], "kind");
      optional(raw, ["actionId", "instanceKey"]); if (raw.providerInstanceId != null) actionProviderInstanceId(raw.providerInstanceId); if (raw.providerLabels != null) labels(raw.providerLabels);
      if (raw.kind === "self") {
        if (["actionId", "instanceKey", "providerInstanceId"].some(key => raw[key] != null) || (raw.providerLabels != null && Object.keys(raw.providerLabels).length)) fail("Self target cannot select another action");
      } else text(raw.actionId, "actionId"); break;
    case "page-child-binding-descriptor":
      raw = closedObject(value, ["controlId", "handler", "internal", "itemKey", "settings", "target"], contract); text(raw.controlId, "controlId"); optional(raw, ["handler", "itemKey"]); objectFields(raw, ["internal", "settings"]); validateActionContract("page-child-binding-target", raw.target); break;
    case "dynamic-page-command":
      raw = closedObject(value, ["bindings", "pageId"], contract); text(raw.pageId, "pageId");
      { const bindings = array(raw.bindings, "bindings").map(binding => validateActionContract("page-child-binding-descriptor", binding));
        if (!bindings.length) fail("Dynamic page requires child bindings"); unique(bindings.map(binding => binding.controlId), "dynamic child controlId"); }
      break;
    case "open-page-body": case "replace-page-body":
      raw = closedObject(value, ["descriptor"], contract); validateActionContract("dynamic-page-command", raw.descriptor); break;
    case "empty-action-body": closedObject(value, [], contract); break;
    case "settings-target-ref":
      raw = closedObject(value, ["scope", "controllerId", "configId", "providerInstanceId", "providerId", "actionId", "actionInstanceId", "stableId"], contract);
      instance(raw, ["controllerId", "configId", "providerInstanceId", "providerId"]); choice(raw.scope, ["action_provider_instance", "action_instance"], "scope"); optional(raw, ["actionId", "actionInstanceId", "stableId"]);
      if (raw.scope === "action_instance") { text(raw.actionId, "actionId"); text(raw.actionInstanceId, "actionInstanceId"); }
      else if (["actionId", "actionInstanceId", "stableId"].some(key => raw[key] != null)) fail("Provider settings cannot carry action IDs"); break;
    case "settings-schema-metadata":
      raw = closedObject(value, ["schema", "schemaId", "stale"], contract); if (raw.schemaId != null) string(raw.schemaId, "schemaId"); if (raw.schema != null) requireJsonObject(raw.schema, "schema"); if (raw.stale !== undefined && typeof raw.stale !== "boolean") fail("stale must be boolean"); break;
    case "settings-snapshot": case "settings-target-description": {
      raw = closedObject(value, contract === "settings-snapshot" ? ["target", "settings", "schemaMetadata", "provenance"] : ["target", "providerInstanceId", "providerId", "actionId", "label", "placement", "schemaMetadata", "provenance"], contract);
      const target = validateActionContract("settings-target-ref", raw.target);
      if (raw.schemaMetadata !== undefined) validateActionContract("settings-schema-metadata", raw.schemaMetadata);
      if (raw.provenance !== undefined) array(raw.provenance, "provenance").forEach(item => choice(item, ["config_default", "user_override", "template_override", "runtime", "stale_schema"], "provenance"));
      if (contract === "settings-snapshot") requireJsonObject(raw.settings, "settings");
      else {
        instance(raw, ["providerInstanceId", "providerId"]); optional(raw, ["actionId", "label"]); objectFields(raw, ["placement"]);
        for (const key of ["providerInstanceId", "providerId", "actionId"]) if ((raw[key] ?? null) !== (target[key] ?? null)) fail(`Settings description ${key} mismatch`);
      }
      break;
    }
    default: fail(`Unknown Action Runtime contract ${contract}`);
  }
  return cloneJson(requireJsonObject(value, contract));
}

export function actionRuntimeMessageDefinition(name: string) {
  if (!Object.hasOwn(payloads, name)) fail(`Unknown Action Runtime message ${name}`);
  return Object.freeze({ ...payloads[name]! });
}
export function validateActionRuntimePayload(name: string, value: unknown): JsonObject { return validateActionContract(actionRuntimeMessageDefinition(name).contract, value); }

/** Complete core message checks; live routing and agreement authority remain separate. */
export function validateActionRuntimeMessage(value: unknown): DeckrMessage {
  const message = validateDeckrMessage(value), body = closedObject(message.body, ["serviceNamespace", "name", "intent", "exchangePattern", "event", "params", "status", "result", "error"], "Action Runtime body");
  const definition = actionRuntimeMessageDefinition(text(body.name, "name"));
  if (message.lane !== "services" || message.messageType !== "serviceMessage" || !message.contract || message.recipient.targetType !== "endpoint" || !message.recipientSessionId ||
    body.serviceNamespace !== namespace || body.intent !== definition.intent || body.exchangePattern !== "one_way" || ["status", "result", "error"].some(field => body[field] != null)) fail("Invalid Action Runtime message envelope");
  const runtime = definition.direction === "consumer_to_service" ? message.recipient.endpoint : message.sender;
  const controller = definition.direction === "consumer_to_service" ? message.sender : message.recipient.endpoint;
  if (!runtime.startsWith("service:action-runtime.") || !controller.startsWith("controller:")) fail("Invalid Action Runtime message direction");
  const providerInstanceId = actionProviderInstanceId(runtime.slice("service:action-runtime.".length));
  if (definition.intent === "event") {
    if (body.params !== undefined && Object.keys(requireJsonObject(body.params, "params")).length) fail("Runtime events require empty params");
  } else if (body.event != null) fail("Runtime commands cannot carry event payloads");
  const payload = validateActionRuntimePayload(String(body.name), definition.intent === "event" ? body.event : body.params);
  for (const key of ["metadata", "binding", "pageSession", "actionInstance"]) if (payload[key] != null && (payload[key] as JsonObject).providerInstanceId !== providerInstanceId) fail("Runtime metadata providerInstanceId mismatch");
  const ids = message.subject.identifiers;
  if (message.subject.kind === "service") {
    if ((ids.serviceId !== undefined && ids.serviceId !== runtime.slice("service:".length)) || (ids.namespace !== undefined && ids.namespace !== namespace) || (ids.name !== undefined && ids.name !== body.name)) fail("Runtime subject service identity mismatch");
  }
  if (ids.providerInstanceId !== undefined && ids.providerInstanceId !== providerInstanceId) fail("Runtime subject providerInstanceId mismatch");
  for (const key of ["metadata", "binding", "pageSession", "actionInstance"]) if (payload[key] != null) {
    const metadata = payload[key] as JsonObject;
    for (const field of [...instanceFields, "bindingId", "pageSessionId"]) if (ids[field] !== undefined && metadata[field] !== undefined && ids[field] !== metadata[field]) fail(`Runtime subject ${field} mismatch`);
  }
  return message;
}

/** Core payload references use native validators; package-owned schemas still use AJV. */
export class ActionRuntimePayloadSchemas extends PayloadSchemas {
  override compile(schema: JsonObject): ValidateFunction {
    const ref = schema.$ref;
    if (Object.keys(schema).length !== 1 || typeof ref !== "string" || !ref.startsWith(schemaPrefix) || !ref.endsWith(".v1.schema.json")) return super.compile(schema);
    const contract = ref.slice(schemaPrefix.length, -".v1.schema.json".length);
    // Compile must fail immediately for unresolved references.
    const known = new Set([...Object.values(payloads).map(value => value.contract), "action-descriptor", "capability-requirement", "capability-requirement-selector", "binding-metadata", "action-instance-metadata", "page-session-metadata", "matched-capability", "capability-input-event", "availability-payload", "action-availability-entry", "action-availability-selector", "dynamic-page-command", "page-child-binding-target", "page-child-binding-descriptor", "settings-target-ref", "settings-schema-metadata", "settings-snapshot", "settings-target-description"]);
    if (!known.has(contract)) return super.compile(schema);
    const validate = ((value: unknown) => {
      try { validateActionContract(contract, value); validate.errors = null; return true; }
      catch (error) { validate.errors = [{ instancePath: "", schemaPath: ref, keyword: "actionRuntime", params: {}, message: String(error) }]; return false; }
    }) as ValidateFunction;
    return validate;
  }
}

export const actionRuntimePayloadSchemas = new ActionRuntimePayloadSchemas();
