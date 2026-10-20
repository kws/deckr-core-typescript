import type { ActionBeaconDescriptor, BindingMetadata, CapabilityRequirement, CapabilityRequirementSelector, DeviceRef, MatchedCapability } from "./actions.ts";
import { validateActionContract } from "./action-contracts.ts";
import { findHardwareCapability, validateHardwareDeviceDescriptor, type HardwareCapability, type HardwareDeviceDescriptor } from "./hardware-descriptors.ts";
import { ValidationError } from "./errors.ts";
import type { PayloadSchemas } from "./schema.ts";

/** Inputs already scoped to a held hardware claim; these helpers grant no authority. */
export interface CapabilityMatchingScope {
  deviceDescriptor: HardwareDeviceDescriptor;
  deviceRef: DeviceRef;
  controlId: string;
  configuredCapabilityIds?: readonly string[];
  schemas?: PayloadSchemas;
}
function fail(reason: string): never { throw new ValidationError(reason); }
const includesAll = (actual: readonly string[] | undefined, wanted: readonly string[] | undefined) => (wanted ?? []).every(name => (actual ?? []).includes(name));
const sameNames = (a: readonly string[] | undefined, b: readonly string[] | undefined) => includesAll(a, b) && includesAll(b, a);
function sameDevice(a: DeviceRef | undefined, b: DeviceRef): boolean {
  return !!a && a.managerId === b.managerId && a.deviceId === b.deviceId && (a.fingerprint ?? null) === (b.fingerprint ?? null);
}
function selectorMatches(selector: CapabilityRequirementSelector, capability: HardwareCapability): boolean {
  return (["capabilityId", "family", "type", "direction"] as const).every(key => selector[key] == null || selector[key] === capability[key])
    && includesAll(capability.eventTypes, selector.eventTypes) && includesAll(capability.commandTypes, selector.commandTypes);
}
function views(capability: HardwareCapability): string[] {
  const result: string[] = [];
  const core = capability.family.startsWith("dev.deckr.");
  if (!core) result.push("extension");
  if (capability.projection) result.push(capability.projection.type === "derivation" ? "derived" : "projected");
  else { result.push("raw"); if (core) result.push("native"); }
  return result;
}
export function capabilitySupportsView(capability: HardwareCapability, view: string): boolean { return views(capability).includes(view); }
function candidates(requirement: CapabilityRequirement, capabilities: HardwareCapability[]): HardwareCapability[] {
  const eligible = capabilities.filter(cap => includesAll(cap.eventTypes, requirement.eventTypes)
    && includesAll(cap.commandTypes, requirement.commandTypes)
    && (!requirement.views?.length || requirement.views.some(view => views(cap).includes(view))));
  for (const selector of requirement.preferences) {
    const result = eligible.filter(cap => selectorMatches(selector, cap));
    if (result.length) return result;
  }
  return [];
}
function scopeCapabilities(scope: CapabilityMatchingScope): HardwareCapability[] {
  const device = validateHardwareDeviceDescriptor(scope.deviceDescriptor, scope.schemas);
  validateActionContract("device-ref", scope.deviceRef);
  if (scope.deviceRef.deviceId !== device.deviceId || (scope.deviceRef.fingerprint != null && scope.deviceRef.fingerprint !== device.fingerprint)) fail("Device differs from the negotiated descriptor");
  const control = device.controls?.find(item => item.controlId === scope.controlId);
  if (!control) fail("Control is outside the negotiated descriptor");
  const capabilities = [...control.inputCapabilities ?? [], ...control.outputCapabilities ?? [], ...control.stateCapabilities ?? [], ...control.configCapabilities ?? [], ...control.diagnosticCapabilities ?? []];
  if (scope.configuredCapabilityIds === undefined) return capabilities;
  const ids = new Set(scope.configuredCapabilityIds);
  if ([...ids].some(id => !capabilities.some(cap => cap.capabilityId === id))) fail("Configured capability is outside the bound control");
  return capabilities.filter(cap => ids.has(cap.capabilityId));
}
function match(requirement: CapabilityRequirement, cap: HardwareCapability, scope: CapabilityMatchingScope): MatchedCapability {
  const result: MatchedCapability = {
    requirementName: requirement.name,
    capability: { deviceRef: { ...scope.deviceRef }, controlId: scope.controlId, capabilityId: cap.capabilityId },
    family: cap.family, type: cap.type, direction: cap.direction,
    eventTypes: [...cap.eventTypes ?? []], commandTypes: [...cap.commandTypes ?? []],
    provenance: cap.projection ? (cap.projection.type === "derivation" ? "derivation" : "projection") : cap.family.startsWith("dev.deckr.") ? "native" : "extension",
  };
  if (cap.projection) {
    findHardwareCapability(scope.deviceDescriptor, cap.projection.source);
    result.source = { deviceRef: { ...scope.deviceRef }, capabilityId: cap.projection.source.capabilityId };
    if (cap.projection.source.controlId != null) result.source.controlId = cap.projection.source.controlId;
  }
  return result;
}

/** Pick the first descriptor-order candidate within the first eligible preference.
 * Applications may choose another equivalent candidate and validate it below.
 */
export function matchCapabilityRequirements(action: ActionBeaconDescriptor, scope: CapabilityMatchingScope): MatchedCapability[] {
  validateActionContract("action-descriptor", action);
  const capabilities = scopeCapabilities(scope);
  return (action.requirements ?? []).flatMap(requirement => {
    const eligible = candidates(requirement, capabilities);
    if (eligible[0]) return [match(requirement, eligible[0], scope)];
    if (requirement.required !== false) fail(`Required capability ${requirement.name} is unavailable`);
    return [];
  });
}

/** Validate a complete ordered result without prescribing equivalent-candidate policy. */
export function validateBindingResources(binding: BindingMetadata, action: ActionBeaconDescriptor, scope: CapabilityMatchingScope): void {
  validateActionContract("binding-metadata", binding);
  validateActionContract("action-descriptor", action);
  const capabilities = scopeCapabilities(scope);
  if (binding.actionId !== action.actionId || !sameDevice(binding.deviceRef, scope.deviceRef) || !sameDevice(binding.controlRef.deviceRef, scope.deviceRef) || binding.controlRef.controlId !== scope.controlId) fail("Binding differs from the selected action or control scope");
  const matches = binding.matchedCapabilities ?? [];
  let index = 0;
  for (const requirement of action.requirements ?? []) {
    const eligible = candidates(requirement, capabilities);
    if (!eligible.length) {
      if (requirement.required !== false) fail(`Required capability ${requirement.name} is unavailable`);
      continue;
    }
    const supplied = matches[index++];
    if (!supplied || supplied.requirementName !== requirement.name) fail("Matches must occur once per satisfied requirement in declaration order");
    const cap = eligible.find(cap => cap.capabilityId === supplied.capability.capabilityId);
    if (!cap) fail("Match is not eligible for the first available preference");
    const expected = match(requirement, cap, scope);
    if (!sameDevice(supplied.capability.deviceRef, scope.deviceRef) || supplied.capability.controlId !== scope.controlId ||
      supplied.family !== expected.family || supplied.type !== expected.type || supplied.direction !== expected.direction ||
      !sameNames(supplied.eventTypes, expected.eventTypes) || !sameNames(supplied.commandTypes, expected.commandTypes) ||
      (supplied.provenance ?? "native") !== expected.provenance) fail("Match changes descriptor identity, names, or provenance");
    if (expected.source) {
      if (!supplied.source || !sameDevice(supplied.source.deviceRef, scope.deviceRef) || supplied.source.capabilityId !== expected.source.capabilityId || (supplied.source.controlId ?? null) !== (expected.source.controlId ?? null)) fail("Projection source differs from the descriptor");
    } else if (supplied.source != null) fail("Unprojected capability cannot supply a source");
  }
  if (index !== matches.length) fail("Match names an undeclared or unsatisfied requirement");
}
