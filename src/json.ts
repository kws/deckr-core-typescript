import { createHash } from "node:crypto";

import { ValidationError } from "./errors.ts";

export type JsonPrimitive = boolean | null | number | string;
export type JsonValue = JsonObject | JsonPrimitive | JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue;
}

export function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function requireJsonObject(value: unknown, fieldName: string): JsonObject {
  if (!isJsonObject(value)) {
    throw new ValidationError(`${fieldName} must be an object`);
  }
  return freezeJson(value) as JsonObject;
}

export function cloneJson<T extends JsonValue | undefined>(value: T): T {
  if (value === undefined) {
    return value;
  }
  return JSON.parse(JSON.stringify(value)) as T;
}

export function freezeJson<T extends JsonValue>(value: T): T {
  if (Array.isArray(value)) {
    return Object.freeze(value.map((item) => freezeJson(item))) as T;
  }
  if (isJsonObject(value)) {
    const out: JsonObject = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] = freezeJson(item);
    }
    return Object.freeze(out) as T;
  }
  return value;
}

export function thawJson<T extends JsonValue | undefined>(value: T): T {
  return cloneJson(value);
}

export function canonicalJson(value: JsonValue): string {
  return canonicalJsonValue(value);
}

export function canonicalJsonBytes(value: JsonValue): Uint8Array {
  return new TextEncoder().encode(canonicalJson(value));
}

export function canonicalJsonHash(value: JsonValue): string {
  return `sha256:${createHash("sha256").update(canonicalJson(value), "utf8").digest("hex")}`;
}

/** Canonicalize wire JSON without losing integer precision or numeric token kinds. */
export function canonicalJsonFromText(source: string): string {
  // Validate the JSON grammar first, but do not use the decoded numbers: that
  // would erase the v1 distinction between 1 and 1.0 and round large integers.
  try {
    JSON.parse(source);
  } catch {
    throw new ValidationError("terms must contain valid JSON");
  }
  const tokens = source.match(/"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}\[\],:]/g)!;
  let position = 0;

  function readValue(): string {
    const token = tokens[position++]!;
    if (token === "{") {
      const entries = new Map<string, string>();
      while (tokens[position] !== "}") {
        const key = JSON.parse(tokens[position++]!) as string;
        scalarString(key);
        if (entries.has(key)) {
          throw new ValidationError("JSON object keys must be unique");
        }
        position++; // colon, already checked by JSON.parse
        entries.set(key, readValue());
        if (tokens[position] !== ",") break;
        position++;
      }
      position++; // closing brace
      return `{${[...entries].sort(([a], [b]) => compareScalarStrings(a, b))
        .map(([key, value]) => `${JSON.stringify(key)}:${value}`).join(",")}}`;
    }
    if (token === "[") {
      const items: string[] = [];
      while (tokens[position] !== "]") {
        items.push(readValue());
        if (tokens[position] !== ",") break;
        position++;
      }
      position++; // closing bracket
      return `[${items.join(",")}]`;
    }
    if (token.startsWith('"')) {
      return scalarString(JSON.parse(token) as string);
    }
    if (token === "true" || token === "false" || token === "null") {
      return token;
    }
    return /[.eE]/.test(token) ? canonicalFloat(Number(token)) : BigInt(token).toString();
  }

  return readValue();
}

export function canonicalJsonHashFromText(source: string): string {
  return `sha256:${createHash("sha256").update(canonicalJsonFromText(source), "utf8").digest("hex")}`;
}

function scalarString(value: string): string {
  for (const character of value) {
    const codePoint = character.codePointAt(0)!;
    if (codePoint >= 0xd800 && codePoint <= 0xdfff) {
      throw new ValidationError("JSON strings must contain Unicode scalar values");
    }
  }
  return JSON.stringify(value);
}

function compareScalarStrings(left: string, right: string): number {
  const a = Array.from(left, (character) => character.codePointAt(0)!);
  const b = Array.from(right, (character) => character.codePointAt(0)!);
  for (let index = 0; index < Math.min(a.length, b.length); index++) {
    if (a[index] !== b[index]) return a[index]! - b[index]!;
  }
  return a.length - b.length;
}

function canonicalFloat(value: number): string {
  if (!Number.isFinite(value)) {
    throw new ValidationError("JSON numbers must be finite");
  }
  if (Object.is(value, -0)) return "-0.0";
  const [significand, exponentText] = value.toExponential().split("e");
  const exponent = Number(exponentText);
  if (exponent < -4 || exponent >= 16) {
    return `${significand}e${exponent < 0 ? "-" : "+"}${String(Math.abs(exponent)).padStart(2, "0")}`;
  }
  return Number.isInteger(value) ? `${value}.0` : String(value);
}

function canonicalJsonValue(value: JsonValue): string {
  if (typeof value === "string") {
    return scalarString(value);
  }
  if (value === null || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new ValidationError("JSON numbers must be finite");
    }
    if (!Number.isInteger(value) || Object.is(value, -0)) {
      return canonicalFloat(value);
    }
    if (!Number.isSafeInteger(value)) {
      throw new ValidationError("Use canonicalJsonFromText for numbers outside the safe integer range");
    }
    return String(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJsonValue(item)).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([left], [right]) =>
    compareScalarStrings(left, right),
  );
  return `{${entries
    .map(([key, item]) => `${scalarString(key)}:${canonicalJsonValue(item)}`)
    .join(",")}}`;
}

export function requireText(value: unknown, fieldName: string): string {
  if (typeof value !== "string") {
    throw new ValidationError(`${fieldName} must be a string`);
  }
  if (value.trim() !== value) {
    throw new ValidationError(`${fieldName} must not contain leading or trailing whitespace`);
  }
  if (value.length === 0) {
    throw new ValidationError(`${fieldName} must not be empty`);
  }
  return value;
}

export function requirePositiveInteger(value: unknown, fieldName: string): number {
  if (!Number.isInteger(value) || Number(value) <= 0) {
    throw new ValidationError(`${fieldName} must be a positive integer`);
  }
  return Number(value);
}

export function optionalText(value: unknown, fieldName: string): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  return requireText(value, fieldName);
}

export function utcIsoNow(): string {
  return new Date().toISOString();
}

export function parseUtcMillis(value: string | undefined): number {
  if (value === undefined) {
    return Number.NEGATIVE_INFINITY;
  }
  const millis = Date.parse(value);
  return Number.isFinite(millis) ? millis : Number.NEGATIVE_INFINITY;
}
