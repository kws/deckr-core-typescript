import { ValidationError } from "./errors.ts";
import { requireJsonObject, requireText, type JsonObject } from "./json.ts";

/** Check the wire value before JSON.stringify can discard or change it. */
export function assertJsonValue(value: unknown, parents = new Set<object>()): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (typeof value !== "object" || parents.has(value)) throw new ValidationError("Expected a finite, acyclic JSON value");
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new ValidationError("Expected a JSON object");
  }
  parents.add(value);
  for (const child of Array.isArray(value) ? value : Object.values(value)) assertJsonValue(child, parents);
  parents.delete(value);
}

export function closedObject(value: unknown, fields: readonly string[], label: string): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new ValidationError(`${label} must be an object`);
  assertJsonValue(value);
  const object = requireJsonObject(value, label);
  for (const key of Object.keys(object)) if (!fields.includes(key)) throw new ValidationError(`${label} has unknown field ${key}`);
  return object;
}

export function timestamp(value: unknown, label: string): string {
  const text = requireText(value, label);
  const match = /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/.exec(text);
  if (!match) throw new ValidationError(`${label} must be an RFC 3339 timestamp`);
  const [, year, month, day, hour, minute, second, , offsetHour, offsetMinute] = match;
  const days = new Date(Date.UTC(Number(year), Number(month), 0)).getUTCDate();
  if (Number(month) < 1 || Number(month) > 12 || Number(day) < 1 || Number(day) > days || Number(hour) > 23 || Number(minute) > 59 || Number(second) > 59 || Number(offsetHour ?? 0) > 23 || Number(offsetMinute ?? 0) > 59 || !Number.isFinite(Date.parse(text))) {
    throw new ValidationError(`${label} is not a valid timestamp`);
  }
  return text;
}
