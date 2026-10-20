import { createHash } from "node:crypto";

import { ValidationError } from "./errors.ts";

/** A wire number whose kind or precision cannot be represented by a JS number. */
export class JsonNumber {
  readonly text: string;
  constructor(text: string) {
    if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(text)) throw new ValidationError("Invalid JSON number");
    this.text = /[.eE]/.test(text) ? canonicalFloat(Number(text)) : BigInt(text).toString();
    Object.freeze(this);
  }
  valueOf(): number { return Number(this.text); }
  toJSON(): unknown {
    // Node >=22.18 is the package runtime baseline. rawJSON prevents the
    // serializer from quoting numbers or rounding integers through binary64.
    return (JSON as typeof JSON & { rawJSON(text: string): unknown }).rawJSON(this.text);
  }
}

export type JsonPrimitive = boolean | null | number | string | JsonNumber;
export type JsonValue = JsonObject | JsonPrimitive | JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue;
}

/** Numeric wire fields may use an integral floating-point token, e.g. 1.0. */
export function isJsonNumber(value: unknown): value is number | JsonNumber {
  return typeof value === "number" && Number.isFinite(value) || value instanceof JsonNumber;
}

export function isSafeJsonInteger(value: unknown): value is number | JsonNumber {
  return isJsonNumber(value) && Number.isSafeInteger(Number(value));
}

/** A validation-only projection for libraries whose numeric domain is binary64.
 * Preserve the original wire value; never silently round an arbitrary integer.
 */
export function nativeJsonNumbers(value: unknown): unknown {
  if (value instanceof JsonNumber) {
    const number = Number(value);
    if (!/[.eE]/.test(value.text) && (!Number.isFinite(number) || BigInt(number) !== BigInt(value.text))) {
      throw new ValidationError("JSON Schema numeric validation cannot represent this integer exactly");
    }
    return number;
  }
  if (Array.isArray(value)) return value.map(nativeJsonNumbers);
  if (isJsonObject(value)) return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, nativeJsonNumbers(item)]));
  return value;
}

export function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof JsonNumber);
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
  if (Array.isArray(value)) return value.map(item => cloneJson(item)) as T;
  if (isJsonObject(value)) return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined).map(([key, item]) => [key, cloneJson(item)])) as T;
  return value;
}

export function freezeJson<T extends JsonValue>(value: T): T {
  if (Array.isArray(value)) {
    return Object.freeze(value.map((item) => freezeJson(item))) as T;
  }
  if (isJsonObject(value)) {
    // JSON keys such as __proto__ are data, including during immutable copies.
    const out: JsonObject = Object.fromEntries(Object.entries(value).map(([key, item]) => [key, freezeJson(item)]));
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
  return canonicalJson(parseJson(source));
}

/** Strict, lossless decoding used by the actual wire and state adapters. */
export function parseJson(source: string): JsonValue {
  // Validate the JSON grammar first, but do not use the decoded numbers: that
  // would erase the v1 distinction between 1 and 1.0 and round large integers.
  try {
    JSON.parse(source);
  } catch {
    throw new ValidationError("terms must contain valid JSON");
  }
  const tokens = source.match(/"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}\[\],:]/g)!;
  let position = 0;

  function readValue(): JsonValue {
    const token = tokens[position++]!;
    if (token === "{") {
      const entries = new Map<string, JsonValue>();
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
      return Object.fromEntries(entries);
    }
    if (token === "[") {
      const items: JsonValue[] = [];
      while (tokens[position] !== "]") {
        items.push(readValue());
        if (tokens[position] !== ",") break;
        position++;
      }
      position++; // closing bracket
      return items;
    }
    if (token.startsWith('"')) {
      const value = JSON.parse(token) as string;
      scalarString(value);
      return value;
    }
    if (token === "true" || token === "false" || token === "null") {
      return JSON.parse(token) as boolean | null;
    }
    const number = Number(token);
    if (/[.eE]/.test(token)) {
      if (!Number.isFinite(number) || Number.isInteger(number)) return new JsonNumber(token);
      return number;
    }
    return Number.isSafeInteger(number) ? (number === 0 ? 0 : number) : new JsonNumber(token);
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
  if (value instanceof JsonNumber) return value.text;
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
  if (value instanceof JsonNumber) value = value.valueOf();
  if (!Number.isSafeInteger(value) || Number(value) <= 0) {
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
