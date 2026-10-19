import { ValidationError } from "./errors.ts";
import { requireJsonObject, requireText, type JsonObject } from "./json.ts";
import { closedObject, timestamp } from "./validation.ts";

export const HARDWARE_LANE = "hardware_messages";
export const HARDWARE_MESSAGE_TYPES = Object.freeze([
  "controlInput", "controlCommand", "capabilityStateChanged", "capabilityStateRequest",
  "capabilityStateReply", "commandAccepted", "commandRejected", "commandReply",
] as const);
export type HardwareMessageType = typeof HARDWARE_MESSAGE_TYPES[number];
export interface HardwareDeviceRef { managerId: string; deviceId: string; fingerprint?: string; }

export function validateHardwareDeviceRef(value: unknown): HardwareDeviceRef {
  const raw = closedObject(value, ["managerId", "deviceId", "fingerprint"], "deviceRef");
  const managerId = localId(raw.managerId, "managerId"), deviceId = localId(raw.deviceId, "deviceId");
  return { managerId, deviceId, ...(raw.fingerprint == null ? {} : { fingerprint: requireText(raw.fingerprint, "fingerprint") }) };
}

function localId(value: unknown, name: string): string {
  const text = requireText(value, name);
  if (/^(action_provider|controller|hardware_manager):/.test(text)) throw new ValidationError(`${name} must be a local identifier`);
  return text;
}

/** Body validation is independent of claim authority and descriptor semantics. */
export function validateHardwareMessageBody(messageType: string, value: unknown): JsonObject {
  const common = ["deviceRef", "controlId", "capabilityId"];
  const fields: Record<HardwareMessageType, string[]> = {
    controlInput: ["eventType", "value", "occurredAt", "sequence", "sources"],
    controlCommand: ["commandType", "params"],
    capabilityStateChanged: ["stateType", "value", "occurredAt", "sequence"],
    capabilityStateRequest: ["stateType", "params"],
    capabilityStateReply: ["stateType", "value", "status", "error"],
    commandAccepted: ["commandType", "acceptedAt"],
    commandRejected: ["commandType", "reason", "message"],
    commandReply: ["commandType", "result"],
  };
  if (!Object.hasOwn(fields, messageType)) throw new ValidationError(`Unknown hardware message ${messageType}`);
  const raw = closedObject(value, [...common, ...fields[messageType as HardwareMessageType]], messageType);
  validateHardwareDeviceRef(raw.deviceRef);
  requireText(raw.capabilityId, "capabilityId");
  if (raw.controlId != null || messageType === "controlInput") requireText(raw.controlId, "controlId");
  if (messageType === "controlInput") requireText(raw.eventType, "eventType");
  if (messageType === "controlCommand" || messageType.startsWith("command")) requireText(raw.commandType, "commandType");
  for (const key of ["stateType", "error", "message"]) if (raw[key] != null) requireText(raw[key], key);
  for (const key of ["occurredAt", "acceptedAt"]) if (raw[key] !== undefined) timestamp(raw[key], key);
  if (raw.sequence != null && (!Number.isSafeInteger(raw.sequence) || Number(raw.sequence) < 0)) throw new ValidationError("sequence must be a non-negative integer");
  if (raw.params !== undefined) requireJsonObject(raw.params, "params");
  if (raw.status !== undefined && !["ok", "unavailable", "unsupported", "rejected"].includes(String(raw.status))) throw new ValidationError("Invalid capability state status");
  if (messageType === "commandRejected" && !["malformed", "unsupported", "expired", "unauthorized", "stale", "rejected"].includes(String(raw.reason))) throw new ValidationError("Invalid command rejection reason");
  if (raw.sources !== undefined) {
    if (!Array.isArray(raw.sources)) throw new ValidationError("sources must be an array");
    for (const value of raw.sources) {
      const source = closedObject(value, ["sourceId", "type", "label", "connectionId", "facts"], "source");
      localId(source.sourceId, "sourceId");
      if (!/^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)*$/.test(requireText(source.type, "source type"))) throw new ValidationError("Invalid source type");
      for (const key of ["label", "connectionId"]) if (source[key] != null) requireText(source[key], key);
      if (source.facts !== undefined) requireJsonObject(source.facts, "source facts");
    }
  }
  return raw;
}
