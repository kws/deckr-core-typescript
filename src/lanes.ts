import { randomUUID } from "node:crypto";

import {
  endpointAddress,
  deckrEndpointAddress,
  endpointTarget,
  parseEndpointAddress,
  type BroadcastTarget,
  type EndpointTarget,
  type MessageTarget,
} from "./endpoint.ts";
import { validateContractPointer, type ContractPointer } from "./authority.ts";
import { ValidationError } from "./errors.ts";
import { isSafeJsonInteger, cloneJson, parseJson, requireJsonObject, requireText, type JsonObject } from "./json.ts";
import { encodeKeyToken } from "./keys.ts";
import { assertJsonValue, closedObject, timestamp } from "./validation.ts";

const LANE_PREFIX = "deckr.msg";
const receivedSizes = new WeakMap<object, number>();

/** Received UTF-8 envelope size, or serialized size for a locally built message. */
export function messageWireSizeBytes(message: DeckrMessage): number {
  return receivedSizes.get(message) ?? new TextEncoder().encode(JSON.stringify(message)).byteLength;
}

export function parseDeckrMessage(bytes: Uint8Array): DeckrMessage {
  const message = validateDeckrMessage(parseJson(new TextDecoder("utf-8", { fatal: true }).decode(bytes)));
  receivedSizes.set(message, bytes.byteLength);
  return message;
}
const REQUIRED_CONTRACT_ACTION_MESSAGES = new Set([
  "actionExtension",
  "actionInstanceCreated",
  "actionInstanceDestroyed",
  "actionLifecycleRejected",
  "bindingAttached",
  "bindingDetached",
  "bindingOutput",
  "bindingOverlay",
  "bindingOverlayClear",
  "capabilityInput",
  "closePage",
  "openPage",
  "pageSessionClosed",
  "pageSessionOpened",
  "replacePage",
  "settingsRequest",
  "settingsSnapshot",
]);
const FORBIDDEN_CONTRACT_ACTION_MESSAGES = new Set([
  "actionAvailabilityChanged",
  "actionAvailabilityRequest",
  "actionAvailabilitySnapshot",
  "actionInterestUpdate",
]);

export interface EntitySubject {
  kind: string;
  identifiers: Record<string, string>;
}

export interface DeckrMessage {
  messageId: string;
  protocolVersion: "1";
  schemaVersion: string;
  lane: string;
  messageType: string;
  sender: string;
  senderSessionId: string;
  recipient: MessageTarget;
  recipientSessionId?: string;
  contract?: ContractPointer;
  subject: EntitySubject;
  createdAt: string;
  expiresAt?: string;
  ttlMs?: number;
  inReplyTo?: string;
  causationId?: string;
  trace?: JsonObject;
  body: JsonObject;
}

export interface BuildMessageInput {
  lane?: string;
  messageType: string;
  sender: string;
  senderSessionId: string;
  recipient: string | MessageTarget;
  recipientSessionId?: string;
  contract?: ContractPointer;
  subject: EntitySubject;
  body: JsonObject;
  messageId?: string;
  schemaVersion?: string;
  createdAt?: string;
  ttlMs?: number;
  expiresAt?: string;
  inReplyTo?: string;
  causationId?: string;
  trace?: JsonObject;
}

export function entitySubject(kind: string, identifiers: Record<string, string> = {}): EntitySubject {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(identifiers)) {
    if (value !== undefined && value !== "") {
      out[key] = String(value);
    }
  }
  return { kind: requireText(kind, "subject kind"), identifiers: out };
}

export function buildMessage(input: BuildMessageInput): DeckrMessage {
  assertJsonValue(input.body);
  const createdAt = input.createdAt ?? new Date().toISOString();
  const expiresAt =
    input.expiresAt ??
    (input.ttlMs === undefined
      ? undefined
      : new Date(Date.parse(createdAt) + input.ttlMs).toISOString());
  return validateDeckrMessage({
    messageId: input.messageId ?? randomUUID(),
    protocolVersion: "1",
    schemaVersion: input.schemaVersion ?? "1",
    lane: input.lane ?? "services",
    messageType: requireText(input.messageType, "message type"),
    sender: endpointAddress(input.sender),
    senderSessionId: requireText(input.senderSessionId, "sender session id"),
    recipient:
      typeof input.recipient === "string" ? endpointTarget(input.recipient) : input.recipient,
    ...(input.recipientSessionId === undefined
      ? {}
      : { recipientSessionId: requireText(input.recipientSessionId, "recipient session id") }),
    ...(input.contract === undefined ? {} : { contract: input.contract }),
    subject: input.subject,
    createdAt,
    ...(expiresAt === undefined ? {} : { expiresAt }),
    ...(input.ttlMs === undefined ? {} : { ttlMs: input.ttlMs }),
    ...(input.inReplyTo === undefined ? {} : { inReplyTo: input.inReplyTo }),
    ...(input.causationId === undefined ? {} : { causationId: input.causationId }),
    ...(input.trace === undefined ? {} : { trace: cloneJson(input.trace) }),
    body: cloneJson(input.body),
  });
}

export function validateDeckrMessage(value: unknown): DeckrMessage {
  const raw = closedObject(value, ["messageId", "protocolVersion", "schemaVersion", "lane", "messageType", "sender", "senderSessionId", "recipient", "recipientSessionId", "contract", "subject", "createdAt", "expiresAt", "ttlMs", "inReplyTo", "causationId", "trace", "body"], "Deckr message");
  const recipient = validateRecipient(raw.recipient);
  const subject = validateSubject(raw.subject);
  const body = requireJsonObject(raw.body, "Deckr message body");
  const message: DeckrMessage = {
    messageId: requireText(raw.messageId, "messageId"),
    protocolVersion: raw.protocolVersion === undefined || raw.protocolVersion === "1" ? "1" : fail("protocolVersion must be 1"),
    schemaVersion: requireText(raw.schemaVersion ?? "1", "schemaVersion"),
    lane: requireText(raw.lane, "lane"),
    messageType: requireText(raw.messageType, "messageType"),
    sender: deckrEndpointAddress(requireText(raw.sender, "sender")),
    senderSessionId: requireText(raw.senderSessionId, "senderSessionId"),
    recipient,
    subject,
    createdAt: timestamp(raw.createdAt, "createdAt"),
    body,
  };
  if (raw.recipientSessionId != null) {
    if (recipient.targetType !== "endpoint") throw new ValidationError("Only direct recipients can specify a session");
    message.recipientSessionId = requireText(raw.recipientSessionId, "recipientSessionId");
  }
  if (raw.contract != null) {
    closedObject(raw.contract, ["contractId", "generation"], "contract pointer");
    message.contract = validateContractPointer(raw.contract);
  }
  if (raw.expiresAt != null) {
    message.expiresAt = timestamp(raw.expiresAt, "expiresAt");
  }
  if (raw.ttlMs != null) {
    if (!isSafeJsonInteger(raw.ttlMs) || Number(raw.ttlMs) < 0) {
      throw new ValidationError("ttlMs must be a non-negative integer");
    }
    message.ttlMs = Number(raw.ttlMs);
  }
  if (raw.inReplyTo != null) {
    message.inReplyTo = requireText(raw.inReplyTo, "inReplyTo");
  }
  if (raw.causationId != null) {
    message.causationId = requireText(raw.causationId, "causationId");
  }
  if (raw.trace != null) {
    message.trace = closedObject(raw.trace, ["traceParent", "traceState"], "trace");
    for (const item of Object.values(message.trace)) if (item !== null && typeof item !== "string") throw new ValidationError("Trace fields must be strings or null");
  }
  validateContractRequirement(message);
  const received = receivedSizes.get(value as object);
  if (received !== undefined) receivedSizes.set(message, received);
  return message;
}

export function subjectFor(message: DeckrMessage): string {
  if (message.recipient.targetType === "broadcast") {
    return [
      LANE_PREFIX,
      encodeKeyToken(message.lane),
      "broadcast",
      encodeKeyToken(message.recipient.scope),
      encodeKeyToken(message.recipient.endpointFamily),
    ].join(".");
  }
  const recipient = parseEndpointAddress(message.recipient.endpoint);
  return [
    LANE_PREFIX,
    encodeKeyToken(message.lane),
    "to",
    encodeKeyToken(recipient.family),
    encodeKeyToken(recipient.endpointId),
  ].join(".");
}

export function recipientHeader(message: DeckrMessage): string {
  if (message.recipient.targetType === "endpoint") {
    return message.recipient.endpoint;
  }
  return ["broadcast", message.recipient.scope, message.recipient.endpointFamily].join(":");
}

export function headersFor(message: DeckrMessage): Record<string, string> {
  return {
    "Deckr-Message-Id": message.messageId,
    "Deckr-Message-Type": message.messageType,
    "Deckr-Sender": message.sender,
    "Deckr-Sender-Session": message.senderSessionId,
    "Deckr-Recipient": recipientHeader(message),
    ...(message.recipientSessionId === undefined
      ? {}
      : { "Deckr-Recipient-Session": message.recipientSessionId }),
    ...(message.contract === undefined
      ? {}
      : {
          "Deckr-Contract-Id": message.contract.contractId,
          "Deckr-Contract-Generation": String(message.contract.generation),
        }),
    ...(message.inReplyTo === undefined ? {} : { "Deckr-In-Reply-To": message.inReplyTo }),
  };
}

export interface HeaderReader {
  get(name: string): string | string[] | undefined | null;
}

export function validateHeaderHints(
  headers: HeaderReader | Record<string, string> | undefined | null,
  message: DeckrMessage,
): void {
  if (headers === undefined || headers === null) {
    return;
  }
  const contractId = headerValue(headers, "Deckr-Contract-Id");
  const contractGeneration = headerValue(headers, "Deckr-Contract-Generation");
  if ((contractId === undefined) !== (contractGeneration === undefined)) {
    throw new ValidationError("NATS contract headers must be provided together");
  }
  if (message.contract === undefined && contractId !== undefined) {
    throw new ValidationError("NATS contract headers disagree with Deckr envelope");
  }
  const expectedHeaders = headersFor(message);
  for (const name of ["Deckr-Message-Id", "Deckr-Message-Type", "Deckr-Sender", "Deckr-Sender-Session", "Deckr-Recipient", "Deckr-Recipient-Session", "Deckr-Contract-Id", "Deckr-Contract-Generation", "Deckr-In-Reply-To"]) {
    const expected = expectedHeaders[name];
    const actualText = headerValue(headers, name);
    if (actualText !== undefined && actualText !== null && actualText !== expected) {
      throw new ValidationError(`NATS header ${name} disagrees with Deckr envelope`);
    }
  }
}

export function validateSubjectHint(subject: string, message: DeckrMessage): void {
  if (!subject.startsWith(`${LANE_PREFIX}.`)) {
    return;
  }
  if (subject !== subjectFor(message)) {
    throw new ValidationError("NATS subject disagrees with Deckr envelope recipient");
  }
}

export function messageIsExpired(message: DeckrMessage, now: Date = new Date()): boolean {
  const absolute = message.expiresAt === undefined ? Infinity : Date.parse(message.expiresAt);
  const relative = message.ttlMs === undefined ? Infinity : Date.parse(message.createdAt) + message.ttlMs;
  return Math.min(absolute, relative) <= now.getTime();
}

export function messageTargetsEndpoint(message: DeckrMessage, endpoint: string): boolean {
  const parsed = parseEndpointAddress(endpoint);
  if (message.recipient.targetType === "endpoint") {
    return message.recipient.endpoint === endpointAddress(parsed);
  }
  return message.recipient.endpointFamily === parsed.family;
}

export function messageIsDeliverableTo(
  message: DeckrMessage,
  endpoint: string,
  options: { endpointSessionId?: string; now?: Date } = {},
): boolean {
  if (messageIsExpired(message, options.now)) {
    return false;
  }
  if (!messageTargetsEndpoint(message, endpoint)) {
    return false;
  }
  if (
    message.recipientSessionId !== undefined &&
    message.recipientSessionId !== options.endpointSessionId
  ) {
    return false;
  }
  return true;
}

function validateRecipient(value: unknown): MessageTarget {
  const raw = requireJsonObject(value, "recipient");
  if (raw.targetType === "endpoint") {
    closedObject(raw, ["targetType", "endpoint"], "recipient");
    return { targetType: "endpoint", endpoint: deckrEndpointAddress(requireText(raw.endpoint, "recipient endpoint")) };
  }
  if (raw.targetType === "broadcast") {
    closedObject(raw, ["targetType", "scope", "endpointFamily", "domain", "hopLimit"], "recipient");
    if (!["controller", "hardware_manager", "action_provider", "service"].includes(String(raw.endpointFamily))) throw new ValidationError("Invalid broadcast endpoint family");
    const target: BroadcastTarget = {
      targetType: "broadcast",
      scope: requireText(raw.scope, "broadcast scope"),
      endpointFamily: requireText(raw.endpointFamily, "broadcast endpointFamily"),
    };
    if (raw.domain != null) {
      target.domain = requireText(raw.domain, "broadcast domain");
    }
    if (raw.hopLimit != null) {
      if (!isSafeJsonInteger(raw.hopLimit) || Number(raw.hopLimit) < 0) {
        throw new ValidationError("broadcast hopLimit must be a non-negative integer");
      }
      target.hopLimit = Number(raw.hopLimit);
    }
    return target;
  }
  throw new ValidationError("recipient targetType must be endpoint or broadcast");
}

function validateSubject(value: unknown): EntitySubject {
  const raw = closedObject(value, ["kind", "identifiers"], "subject");
  const identifiers = requireJsonObject(raw.identifiers ?? {}, "subject identifiers");
  const out: Record<string, string> = {};
  for (const [key, item] of Object.entries(identifiers)) {
    out[requireText(key, "subject identifier key")] = requireText(item, "subject identifier value");
  }
  return { kind: requireText(raw.kind, "subject kind"), identifiers: out };
}

function validateContractRequirement(message: DeckrMessage): void {
  if (message.lane === "hardware_messages") {
    if (message.contract === undefined) {
      throw new ValidationError("hardware_messages messages require a Concord contract pointer");
    }
    return;
  }
  if (message.lane === "services") {
    if (message.messageType === "serviceMessage") {
      if (message.contract === undefined) {
        throw new ValidationError("services messages require a Concord contract pointer");
      }
    }
    return;
  }
  if (message.lane !== "actions") {
    return;
  }
  if (FORBIDDEN_CONTRACT_ACTION_MESSAGES.has(message.messageType)) {
    if (message.contract !== undefined) {
      throw new ValidationError("public action messages must not carry a Concord contract pointer");
    }
    return;
  }
  if (
    REQUIRED_CONTRACT_ACTION_MESSAGES.has(message.messageType) &&
    message.contract === undefined
  ) {
    throw new ValidationError("protected action messages require a Concord contract pointer");
  }
}

function headerValue(
  headers: HeaderReader | Record<string, string>,
  name: string,
): string | undefined {
  let actual: string | string[] | undefined | null;
  if (typeof (headers as HeaderReader).get === "function") {
    actual = (headers as HeaderReader).get(name);
  } else {
    actual = (headers as Record<string, string>)[name];
  }
  if (Array.isArray(actual) && new Set(actual).size > 1) throw new ValidationError(`Conflicting duplicate header ${name}`);
  const actualText = Array.isArray(actual) ? actual[0] : actual;
  return actualText || undefined;
}

function fail(message: string): never {
  throw new ValidationError(message);
}

export type { BroadcastTarget, EndpointTarget, MessageTarget };
