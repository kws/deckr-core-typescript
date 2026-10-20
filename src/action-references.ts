import { capabilitySupportsView, validateBindingResources } from "./action-matching.ts";
import { validatePageCommand, type PageCommandContext } from "./action-pages.ts";
import type { ActionInstanceMetadata, BindingMetadata, PageSessionMetadata, ActionBeaconDescriptor, CapabilityRef } from "./actions.ts";
import { validateActionContract, validateActionRuntimeMessage } from "./action-contracts.ts";
import { ValidationError } from "./errors.ts";
import { canonicalJson, type JsonObject } from "./json.ts";
import { findHardwareCapability, validateHardwareCapabilityPayload, type HardwareDeviceDescriptor, type HardwareCapability } from "./hardware-descriptors.ts";
import type { DeckrMessage } from "./lanes.ts";
import { defaultPayloadSchemas, type PayloadSchemas } from "./schema.ts";

/** Host-resolved current state, already scoped to the authorized controller/session.
 * Core checks these references but never allocates or keeps a controller registry.
 */
export interface ActionRuntimeReferenceState {
  pageCommand?: PageCommandContext;
  configuredCapabilityIds?: readonly string[];
  actionInstance?: ActionInstanceMetadata;
  binding?: BindingMetadata;
  pageSession?: PageSessionMetadata;
  action?: ActionBeaconDescriptor;
  devices?: readonly { managerId: string; descriptor: HardwareDeviceDescriptor }[];
}
function fail(reason: string): never { throw new ValidationError(reason); }
function same(left: unknown, right: unknown): boolean { return canonicalJson(left as JsonObject) === canonicalJson(right as JsonObject); }
function identity(actual: JsonObject, current: JsonObject, fields: string[]): void {
  for (const field of fields) if (field === "generation" ? Number(actual[field]) !== Number(current[field]) : (actual[field] ?? null) !== (current[field] ?? null)) fail(`Action Runtime ${field} does not resolve in the current session`);
}
const common = ["providerInstanceId", "providerId", "actionInstanceId", "configId", "contextId"];
function bindingReference(actual: JsonObject, current: JsonObject): void {
  identity(actual, current, [...common, "actionId", "bindingId"]);
  if (actual.pageSessionId != null && actual.pageSessionId !== current.pageSessionId) fail("Binding crosses the current page scope");
  for (const key of ["deviceRef", "controlRef"]) {
    const left = actual[key] as JsonObject, right = current[key] as JsonObject;
    if (key === "controlRef" && left.controlId !== right.controlId) fail("Binding crosses the current control scope");
    const a = key === "deviceRef" ? left : left.deviceRef as JsonObject, b = key === "deviceRef" ? right : right.deviceRef as JsonObject;
    if (a.deviceId !== b.deviceId || a.managerId !== b.managerId || (a.fingerprint != null && b.fingerprint != null && a.fingerprint !== b.fingerprint)) fail("Binding crosses the current device scope");
  }
  {
    const normalized = (values: unknown) => (values as JsonObject[]).map(value => ({
      ...value, provenance: value.provenance ?? "native",
      eventTypes: [...(value.eventTypes ?? []) as string[]].sort(),
      commandTypes: [...(value.commandTypes ?? []) as string[]].sort(),
    }));
    if (!same(normalized(actual.matchedCapabilities ?? []), normalized(current.matchedCapabilities ?? []))) fail("Binding changes the resolved capability scope or requirement order");
  }
}

/** Verify supplied references and capability scope against current host-owned state. */
export function validateActionRuntimeReferences(value: unknown, state: ActionRuntimeReferenceState, schemas: PayloadSchemas = defaultPayloadSchemas): DeckrMessage {
  const message = validateActionRuntimeMessage(value);
  if (["open_page", "replace_page", "close_page"].includes(String(message.body.name))) {
    if (!state.pageCommand) fail("Page operation needs current target ownership and authority observations");
    validatePageCommand(message, state.pageCommand);
    return message;
  }
  const payload = (message.body.event ?? message.body.params) as JsonObject;
  const currentAction = state.actionInstance && validateActionContract("action-instance-metadata", state.actionInstance);
  const currentBinding = state.binding && validateActionContract("binding-metadata", state.binding);
  const currentPage = state.pageSession && validateActionContract("page-session-metadata", state.pageSession);
  for (const [field, current, fields] of [
    ["metadata", currentAction, [...common, "actionId"]], ["actionInstance", currentAction, [...common, "actionId"]],
    ["binding", currentBinding, [...common, "actionId", "bindingId", "pageSessionId"]],
    ["pageSession", currentPage, [...common, "pageId", "pageSessionId", "ownerBindingId", "generation"]],
  ] as const) {
    const reference = payload[field];
    if (reference == null) continue;
    if (!current) fail(`No current ${field} in the authorized session`);
    if (field === "binding") bindingReference(reference as JsonObject, current);
    else identity(reference as JsonObject, current, [...fields]);
    if (field === "pageSession") for (const child of ((reference as JsonObject).bindings ?? []) as JsonObject[]) {
      const expected = ((current.bindings ?? []) as JsonObject[]).find(binding => binding.bindingId === child.bindingId);
      if (!expected) fail("Page child binding does not resolve in the current session");
      bindingReference(child, expected);
    }
  }
  // Related records may have independent lifetimes. Resolve the operation's
  // payload target without giving an incidental binding authority over a page.
  const target = payload.binding != null ? currentBinding
    : payload.pageSession != null ? currentPage : currentAction;
  if (!target) fail("Runtime operation needs a host-resolved current target");
  const runtimeEndpoint = message.sender.startsWith("service:") ? message.sender : message.recipient.targetType === "endpoint" ? message.recipient.endpoint : "";
  if (runtimeEndpoint !== `service:action-runtime.${target.providerInstanceId}`) fail("Resolved target belongs to another provider instance");
  // Contract validation has already required safe integers. Keep the original
  // JSON tokens intact while comparing their integer identity.
  if (payload.generation !== undefined && (!currentBinding || Number(payload.generation) !== Number(currentBinding.outputGeneration ?? 0))) fail("Stale binding generation");

  const resolve = (ref: CapabilityRef, fallback?: CapabilityRef): HardwareCapability => {
    const device = ref.deviceRef ?? fallback?.deviceRef;
    if (!device) fail("Capability needs a negotiated device");
    const found = state.devices?.find(item => item.managerId === device.managerId && item.descriptor.deviceId === device.deviceId);
    if (!found || (device.fingerprint !== undefined && device.fingerprint !== found.descriptor.fingerprint)) fail("Capability device is outside negotiated descriptors");
    return findHardwareCapability(found.descriptor, { capabilityId: ref.capabilityId, controlId: ref.controlId });
  };
  const referenceIdentity = (ref: CapabilityRef, fallback?: CapabilityRef): string => {
    const device = ref.deviceRef ?? fallback?.deviceRef;
    // Fingerprints are optional assertions checked against the negotiated
    // descriptor by resolve(), not an additional capability identity field.
    return canonicalJson({ device: device ? { managerId: device.managerId, deviceId: device.deviceId } : null,
      controlId: ref.controlId ?? null, capabilityId: ref.capabilityId });
  };
  if (currentBinding && target === currentBinding) {
    const binding = state.binding!;
    const device = state.devices?.find(item => item.managerId === binding.deviceRef.managerId && item.descriptor.deviceId === binding.deviceRef.deviceId);
    if (!device || (binding.deviceRef.fingerprint !== undefined && device.descriptor.fingerprint !== binding.deviceRef.fingerprint) || !device.descriptor.controls?.some(control => control.controlId === binding.controlRef.controlId)) fail("Binding control is outside negotiated descriptors");
    if (payload.binding) {
      const raw = payload.binding as JsonObject;
      for (const ref of [raw.deviceRef, (raw.controlRef as JsonObject).deviceRef] as JsonObject[]) if (ref.fingerprint != null && ref.fingerprint !== device.descriptor.fingerprint) fail("Binding fingerprint differs from its negotiated device");
    }
    const matches = binding.matchedCapabilities ?? [];
    if (!state.action) fail("Binding requires its declared action descriptor");
    {
      validateActionContract("action-descriptor", state.action);
      if (state.action.actionId !== binding.actionId || (state.action.providerId != null && state.action.providerId !== binding.providerId)) fail("Binding action descriptor identity mismatch");
    }
    validateBindingResources(binding, state.action, { schemas, deviceDescriptor: device.descriptor, deviceRef: binding.deviceRef, controlId: binding.controlRef.controlId, ...(state.configuredCapabilityIds === undefined ? {} : { configuredCapabilityIds: state.configuredCapabilityIds }) });
    const reference = message.body.name === "binding_output" ? payload.capability : message.body.name === "capability_input" ? (payload.event as JsonObject).capability : undefined;
    if (reference) {
      const capability = resolve(reference as unknown as CapabilityRef);
      const match = matches.find(item => referenceIdentity(item.capability) === referenceIdentity(reference as unknown as CapabilityRef));
      if (!match) fail("Operation capability is outside the current binding");
      if (message.body.name === "binding_output") {
        if (!match.commandTypes?.includes(String(payload.commandType))) fail("Command is outside the matched capability");
        validateHardwareCapabilityPayload(capability, "command", String(payload.commandType), payload.params ?? {}, schemas);
      } else {
        const event = payload.event as JsonObject;
        if (event.view != null && !capabilitySupportsView(capability, String(event.view))) fail("Input view differs from the negotiated descriptor");
        if (!match.eventTypes?.includes(String(event.eventType))) fail("Event is outside the matched capability");
        if (event.source != null) {
          resolve(event.source as unknown as CapabilityRef, reference as unknown as CapabilityRef);
          if (!match.source || referenceIdentity(match.source, match.capability) !== referenceIdentity(event.source as unknown as CapabilityRef, match.capability)) fail("Event source differs from its resolved source");
        }
        validateHardwareCapabilityPayload(capability, "event", String(event.eventType), event.value ?? null, schemas);
      }
    }
  }
  return message;
}
