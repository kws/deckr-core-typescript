import assert from "node:assert/strict";
import test from "node:test";
import { validateAdvertisementRecord } from "../src/beacon.ts";
import { validateContractRecord, validateParticipantTokenRecord, validateStaleObservationRecord } from "../src/concord.ts";
import { parseJson } from "../src/json.ts";
import { PayloadSchemas } from "../src/schema.ts";
import type { JsonObject } from "../src/json.ts";

const contract = { contractId: "wire", generation: 1, participants: ["controller:one"], attachedParticipants: [], state: "open" };
const token = { contractId: "wire", generation: 1, participant: "controller:one", sessionId: "one", tokenId: "token", refreshSeq: 1, ttlSeconds: 120 };
const advertisement = { advertisementId: "ad", featureId: "org.example.feature", advertiser: "controller:one", endpoint: "controller:one", sessionId: "one", refreshSeq: 1, ttlSeconds: 120 };

test("wire validators reject unknown fields, invalid dates, missing membership and disallowed nulls", () => {
  assert.throws(() => validateContractRecord({ ...contract, extra: true }), /unknown field/);
  assert.throws(() => validateContractRecord({ ...contract, attachedParticipants: undefined }));
  assert.throws(() => validateContractRecord({ ...contract, participantSessions: null }));
  assert.throws(() => validateContractRecord({ ...contract, createdAt: "2026-02-30T00:00:00Z" }), /timestamp/);
  assert.throws(() => validateContractRecord({ ...contract, cancelledAt: "yesterday" }), /timestamp/);
  assert.throws(() => validateParticipantTokenRecord({ ...token, observed: null }));
  assert.throws(() => validateParticipantTokenRecord({ ...token, observed: { "controller:one": { generation: 1, extra: true } } }), /unknown field/);
  assert.throws(() => validateParticipantTokenRecord({ ...token, observed: { invalid: { generation: 1 } } }));
  assert.throws(() => validateAdvertisementRecord({ ...advertisement, unknown: 1 }), /unknown field/);
  for (const field of ["operations", "labels", "hints"]) assert.throws(() => validateAdvertisementRecord({ ...advertisement, [field]: null }));
  assert.throws(() => validateAdvertisementRecord({ ...advertisement, updatedAt: "2026-01-01" }), /timestamp/);
  assert.throws(() => validateAdvertisementRecord({ ...advertisement, protocol: { name: "example", version: "1", extra: true } }), /unknown field/);
  assert.throws(() => validateStaleObservationRecord({ contractId: "wire", generation: 1, firstObservedStaleAt: "invalid", status: "missing_token" }), /timestamp/);
});

test("wire validators accept schema nullable fields and numeric integer values without altering terms", () => {
  const record = validateContractRecord({ ...contract, createdAt: null, cancelledAt: null, cancelledBy: null, cancelRevision: null });
  assert.equal(record.cancelledAt, undefined);
  const ad = validateAdvertisementRecord({ ...advertisement, createdAt: null, updatedAt: null, protocol: null, payload: null });
  assert.equal(ad.protocol, undefined);
  const parsed = validateParticipantTokenRecord(parseJson('{"contractId":"wire","generation":1.0,"participant":"controller:one","sessionId":"one","tokenId":"token","refreshSeq":1.0,"ttlSeconds":120.0,"observed":{"controller:two":{"generation":1.0,"refreshSeq":0.0,"revision":null,"tokenHash":""}}}'));
  assert.equal(parsed.generation, 1);
  assert.deepEqual(parsed.observed["controller:two"], { generation: 1, refreshSeq: 0, tokenHash: "" });
  assert.equal(validateContractRecord({ ...contract, cancelRevision: parseJson("0.0") }).cancelRevision, 0);
});

test("payload schema validation uses numeric values without changing lossless wire payloads", () => {
  const schemas = new PayloadSchemas();
  const schema = parseJson('{"type":"object","properties":{"number":{"type":"number","minimum":1.0,"maximum":1.0}},"required":["number"]}') as JsonObject;
  const value = parseJson('{"number":1.0}');
  schemas.validate(schema, value);
  assert.equal(JSON.stringify(value), '{"number":1.0}');
  assert.throws(() => schemas.validate(schema, parseJson('{"number":2.0}')));
  // AJV has a binary64 numeric domain. Never authorize a rounded comparison.
  assert.throws(() => schemas.validate(parseJson('{"type":"integer","maximum":9007199254740992}') as JsonObject, parseJson("9007199254740993")), /cannot represent this integer exactly/);
});
