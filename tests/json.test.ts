import assert from "node:assert/strict";
import test from "node:test";

import { ValidationError } from "../src/errors.ts";
import {
  canonicalJson,
  canonicalJsonBytes,
  canonicalJsonFromText,
  canonicalJsonHash,
  canonicalJsonHashFromText,
  requireJsonObject,
  type JsonValue,
} from "../src/json.ts";

test("immutable JSON copies preserve prototype-like keys as ordinary data", () => {
  const source = '{"__proto__":{"value":1},"constructor":{"nested":{"__proto__":2}}}';
  const value = requireJsonObject(JSON.parse(source), "value");
  assert.equal(JSON.stringify(value), source);
  assert.equal(Object.getPrototypeOf(value), Object.prototype);
  assert.ok(Object.hasOwn(value, "__proto__"));
  assert.ok(Object.isFrozen(value.__proto__));
  assert.equal(canonicalJsonHash(value), canonicalJsonHashFromText(source));
});

test("value and wire canonicalization agree for representable JSON values", () => {
  const values: JsonValue[] = [
    null, true, false, 0, 1, -12, 0.00001, -0.25,
    "quote\" slash/ backslash\\\n\u0000 café 😀",
    [], {}, [null, true, [], {}],
    { "😀": 1, "\ue000": 2, "é": "café", a: [1, 2] },
    { "2": "two", "10": "ten" },
  ];
  for (const value of values) {
    const source = JSON.stringify(value);
    assert.equal(canonicalJson(value), canonicalJsonFromText(source));
    assert.equal(canonicalJsonHash(value), canonicalJsonHashFromText(source));
    assert.equal(new TextDecoder().decode(canonicalJsonBytes(value)), canonicalJson(value));
  }
});

test("wire canonicalization preserves numeric kinds and exact integer precision", () => {
  assert.notEqual(canonicalJsonHashFromText("1"), canonicalJsonHashFromText("1.0"));
  assert.equal(canonicalJsonFromText("-0"), "0");
  assert.equal(canonicalJsonFromText("-0.0"), "-0.0");
  assert.equal(canonicalJson(-0), "-0.0");
  assert.equal(canonicalJsonFromText("-9007199254740993"), "-9007199254740993");
  assert.equal(canonicalJsonFromText("5e-324"), "5e-324");
  assert.equal(canonicalJsonFromText("-1e-400"), "-0.0");
});

test("wire canonicalization keeps unusual object keys and orders by Unicode scalar", () => {
  assert.equal(
    canonicalJsonFromText(' { "😀": [], "\\ue000": {}, "__proto__": 1, "10": 10, "2": 2 } '),
    '{"10":10,"2":2,"__proto__":1,"":{},"😀":[]}',
  );
  assert.equal(canonicalJson({ "😀": 1, "\ue000": 2, a: 3 }), '{"a":3,"":2,"😀":1}');
});

test("wire canonicalization rejects malformed JSON, duplicate keys, and invalid scalars", () => {
  for (const source of [
    "", "undefined", "NaN", "Infinity", "1e400", "01", "1.",
    "[1,]", '{"a":1,}', "{} false", '"\\ud800"', '"\\udfff"',
    '{"\\ud800":1}', '{"a":1,"a":2}',
    '{"a":1,"\\u0061":2}', '[{"a":1,"a":2}]',
  ]) {
    assert.throws(() => canonicalJsonFromText(source), ValidationError, source);
  }
});

test("value canonicalization rejects unsafe numbers and invalid Unicode scalars", () => {
  for (const value of [NaN, Infinity, -Infinity, 9007199254740992]) {
    assert.throws(() => canonicalJson(value), ValidationError);
  }
  assert.throws(() => canonicalJson("\ud800"), ValidationError);
  assert.throws(() => canonicalJson({ "\udfff": 1 }), ValidationError);
});

test("lossless JSON values survive immutable copies and actual serialization", async () => {
  const { parseJson, cloneJson, freezeJson } = await import("../src/json.ts");
  for (const source of ['{"x":1.0}', '{"x":9007199254740993}', '{"x":-0.0}', '{"a":[1,1.0,1e20,-9007199254740993]}']) {
    const value = freezeJson(cloneJson(parseJson(source)));
    assert.equal(canonicalJsonHash(value), canonicalJsonHashFromText(source));
    assert.equal(canonicalJsonHashFromText(JSON.stringify(value)), canonicalJsonHashFromText(source));
  }
});
