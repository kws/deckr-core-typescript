import assert from "node:assert/strict";
import test from "node:test";

import { ValidationError } from "../src/errors.ts";
import {
  canonicalJson,
  canonicalJsonBytes,
  canonicalJsonFromText,
  canonicalJsonHash,
  canonicalJsonHashFromText,
  type JsonValue,
} from "../src/json.ts";

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
