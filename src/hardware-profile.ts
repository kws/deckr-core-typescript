import type { Candidate } from "./beacon.ts";
import type { ContractRecord } from "./concord.ts";
import { deckrEndpointAddress, parseEndpointAddress } from "./endpoint.ts";
import { ValidationError } from "./errors.ts";
import { isSafeJsonInteger, cloneJson, requireJsonObject, requirePositiveInteger, requireText, type JsonObject } from "./json.ts";
import { validateHardwareDeviceDescriptor, type HardwareDeviceDescriptor } from "./hardware-descriptors.ts";
import { validateHardwareDeviceRef, type HardwareDeviceRef } from "./hardware-messages.ts";
import { immutable } from "./runtime.ts";
import { defaultPayloadSchemas } from "./schema.ts";
import { closedObject } from "./validation.ts";

export const HARDWARE_FEATURE_ID = "dev.deckr.hardware";
export const HARDWARE_ADVERTISEMENT_PROFILE = "dev.deckr.profile.hardware.v1";
export const HARDWARE_CLAIM_PROFILE = "dev.deckr.profile.hardware_claim.v1";
export interface HardwareCapacity { totalInstances?: number | null; claimedInstances?: number; availableInstances?: number | null; }
export interface HardwareAdvertisementDevice { deviceRef: HardwareDeviceRef; descriptor: HardwareDeviceDescriptor; capacity?: HardwareCapacity; }
export interface HardwareAdvertisementPayload {
  profile: typeof HARDWARE_ADVERTISEMENT_PROFILE; managerId: string; managerEndpoint: string; sessionId: string;
  devices: Record<string, HardwareAdvertisementDevice>; labels?: Record<string, string>;
}
export interface HardwareClaimTerms {
  profile: typeof HARDWARE_CLAIM_PROFILE; claimId: string; controllerEndpoint: string; managerEndpoint: string;
  devices: { deviceRef: HardwareDeviceRef; instanceCount: number }[];
}
export interface HardwareDescriptor extends HardwareAdvertisementPayload { candidate: Candidate; }

export function validateHardwareCapacity(value: unknown): HardwareCapacity {
  const raw = closedObject(value, ["totalInstances", "claimedInstances", "availableInstances"], "capacity");
  for (const key of ["totalInstances", "claimedInstances", "availableInstances"]) if (raw[key] != null && (!isSafeJsonInteger(raw[key]) || Number(raw[key]) < 0)) throw new ValidationError("Capacity counts must be nonnegative integers");
  if (raw.claimedInstances === null) throw new ValidationError("claimedInstances cannot be null");
  if (raw.totalInstances != null && raw.availableInstances != null && raw.availableInstances !== Math.max(Number(raw.totalInstances) - Number(raw.claimedInstances ?? 0), 0)) throw new ValidationError("Inconsistent capacity counts");
  return immutable(cloneJson(raw)) as HardwareCapacity;
}

export function validateHardwareAdvertisementPayload(value: unknown, schemas = defaultPayloadSchemas): HardwareAdvertisementPayload {
  const raw = closedObject(value, ["profile", "managerId", "managerEndpoint", "sessionId", "devices", "labels"], "hardware advertisement");
  if (raw.profile !== undefined && raw.profile !== HARDWARE_ADVERTISEMENT_PROFILE) throw new ValidationError("Invalid hardware advertisement profile");
  const managerId = requireText(raw.managerId, "managerId"), managerEndpoint = deckrEndpointAddress(requireText(raw.managerEndpoint, "managerEndpoint"));
  if (managerEndpoint !== `hardware_manager:${managerId}`) throw new ValidationError("Hardware manager identity mismatch");
  requireText(raw.sessionId, "sessionId");
  const devices = requireJsonObject(raw.devices ?? {}, "devices");
  for (const [id, item] of Object.entries(devices)) {
    const device = closedObject(item, ["deviceRef", "descriptor", "capacity"], "advertised device");
    const ref = validateHardwareDeviceRef(device.deviceRef), descriptor = validateHardwareDeviceDescriptor(device.descriptor, schemas);
    if (id !== ref.deviceId || id !== descriptor.deviceId || ref.managerId !== managerId || (ref.fingerprint !== undefined && ref.fingerprint !== descriptor.fingerprint)) throw new ValidationError("Advertised device identity mismatch");
    if (device.capacity !== undefined) validateHardwareCapacity(device.capacity);
  }
  if (raw.labels !== undefined) for (const [key, item] of Object.entries(requireJsonObject(raw.labels, "labels"))) { requireText(key, "label key"); requireText(item, "label value"); }
  return immutable(cloneJson({ ...raw, profile: HARDWARE_ADVERTISEMENT_PROFILE, devices })) as unknown as HardwareAdvertisementPayload;
}

export function parseHardwareDescriptor(candidate: Candidate, schemas = defaultPayloadSchemas): HardwareDescriptor | null {
  try {
    const payload = validateHardwareAdvertisementPayload(candidate.advertisement.payload, schemas);
    if (candidate.advertisement.featureId !== HARDWARE_FEATURE_ID || payload.managerEndpoint !== candidate.advertisement.endpoint || payload.sessionId !== candidate.advertisement.sessionId) return null;
    return immutable({ ...payload, candidate: structuredClone(candidate) });
  } catch { return null; }
}

export function validateHardwareClaimTerms(value: unknown): HardwareClaimTerms {
  const raw = closedObject(value, ["profile", "claimId", "controllerEndpoint", "managerEndpoint", "devices"], "hardware claim");
  if (raw.profile !== undefined && raw.profile !== HARDWARE_CLAIM_PROFILE) throw new ValidationError("Invalid hardware claim profile");
  requireText(raw.claimId, "claimId");
  const controller = parseEndpointAddress(deckrEndpointAddress(requireText(raw.controllerEndpoint, "controllerEndpoint")));
  const manager = parseEndpointAddress(deckrEndpointAddress(requireText(raw.managerEndpoint, "managerEndpoint")));
  if (controller.family !== "controller" || manager.family !== "hardware_manager") throw new ValidationError("Invalid hardware claim participants");
  if (!Array.isArray(raw.devices) || !raw.devices.length) throw new ValidationError("A hardware claim requires devices");
  const ids = new Set<string>();
  for (const item of raw.devices) {
    const device = closedObject(item, ["deviceRef", "instanceCount"], "claimed device");
    const ref = validateHardwareDeviceRef(device.deviceRef); requirePositiveInteger(device.instanceCount, "instanceCount");
    if (ref.managerId !== manager.endpointId || ids.has(ref.deviceId)) throw new ValidationError("Invalid or duplicate claimed device");
    ids.add(ref.deviceId);
  }
  return immutable(cloneJson({ ...raw, profile: HARDWARE_CLAIM_PROFILE })) as unknown as HardwareClaimTerms;
}

export function hardwareClaimMatches(record: ContractRecord, terms: HardwareClaimTerms): boolean {
  const participants = [terms.controllerEndpoint, terms.managerEndpoint].sort();
  return record.profile === HARDWARE_CLAIM_PROFILE && record.participants.length === 2 && record.participants.every((participant, index) => participant === participants[index]);
}

export function hardwareDeviceMatches(ref: HardwareDeviceRef, device: HardwareAdvertisementDevice): boolean {
  return ref.managerId === device.deviceRef.managerId && ref.deviceId === device.deviceRef.deviceId && (ref.fingerprint === undefined || ref.fingerprint === device.descriptor.fingerprint);
}
