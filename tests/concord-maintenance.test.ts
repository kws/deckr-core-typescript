import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import path from "node:path";
import { ConcordCoordinator, ConcordService, ConcordReaperService, CONCORD_TOKEN_STORE_POLICY, CONCORD_CONTRACT_STORE_POLICY,
  concordTokenCleanupKey, validateTokenCleanupRecord, type ConcordMaintenanceAudit } from "../src/concord.ts";
import { StateConflict, StateUnavailable } from "../src/errors.ts";
import { MemoryStateStore, type StateEntry } from "../src/state.ts";
import { PayloadSchemas } from "../src/schema.ts";
import type { JsonObject } from "../src/json.ts";
import { RecoverableStore } from "./runtime-support.ts";

const clock = () => "2999-01-01T00:00:00.000Z";
function environment(tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })) {
  const contracts = new MemoryStateStore(), maintenance = new MemoryStateStore();
  const audit: ConcordMaintenanceAudit[] = [];
  const options = { contractState: contracts, tokenState: tokens, maintenanceState: maintenance, clock, cancelledRetentionSeconds: 0,
    audit: (event: ConcordMaintenanceAudit) => { audit.push(event); } };
  return { contracts, tokens, maintenance, options, audit, coordinator: new ConcordCoordinator(contracts, tokens) };
}
async function retained(env: ReturnType<typeof environment>) {
  const contract = await env.coordinator.createContract(["worker:one"], { contractId: "retained", terms: { private: "excluded-from-audit" } });
  const token = await env.coordinator.attach(contract, "worker:one", "session");
  await env.coordinator.cancel(contract, "worker:one");
  return { contract, token };
}

test("maintenance uses dedicated exact stores without starting the participation runtime or watchers", async () => {
  const forbidden = new RecoverableStore(CONCORD_CONTRACT_STORE_POLICY), forbiddenTokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  forbidden.get = async () => { throw new Error("Participation store must not be accessed"); };
  const normal = new ConcordService(new ConcordCoordinator(forbidden, forbiddenTokens));
  const env = environment(); let now = "2026-01-01T00:00:00.000Z";
  const contract = await env.coordinator.createContract(["worker:one"], { contractId: "pending" });
  const reaper = new ConcordReaperService(normal, { ...env.options, staleGraceSeconds: 10, clock: () => now });
  assert.equal(normal.state.status, "idle"); assert.equal(forbidden.observers.size, 0); assert.equal(forbiddenTokens.observers.size, 0);
  assert.equal((await reaper.scanOnce()).staleObservationsCreated, 1);
  now = "2026-01-01T00:00:11.000Z";
  assert.equal((await reaper.scanOnce()).contractsCancelled, 1);
  assert.equal((await env.contracts.get(contract.key))!.value.cancelledBy, "concord:maintenance");
  assert.equal(normal.state.status, "idle"); assert.equal(forbidden.exactReads, 0); assert.equal(forbidden.observers.size, 0);
  await assert.rejects(normal.maintenanceCancelContract(contract), /dedicated/);
  await reaper.close(); await normal.close();
});

test("pending contracts with a valid participant token are not reaped", async () => {
  const env = environment(), reaper = new ConcordReaperService({ ...env.options, staleGraceSeconds: 0 });
  const contract = await env.coordinator.createContract(["worker:one", "worker:two"]);
  await env.coordinator.attach(contract, "worker:one", "session");
  const result = await reaper.scanOnce();
  assert.equal(result.staleObservationsCreated, 0); assert.equal(result.contractsCancelled, 0);
  assert.equal((await env.contracts.get(contract.key))!.value.state, "open");
  await reaper.close();
});

test("unavailable token validation is not evidence of stale participation", async () => {
  const env = environment(), contract = await env.coordinator.createContract(["worker:one"]);
  env.tokens.get = async () => { throw new StateUnavailable("Token reads unavailable"); };
  const reaper = new ConcordReaperService({ ...env.options, staleGraceSeconds: 0 });
  const result = await reaper.scanOnce();
  assert.equal(result.staleObservationsCreated, 0); assert.equal(result.contractsCancelled, 0);
  assert.equal((await env.contracts.get(contract.key))!.value.state, "open"); await reaper.close();
});

test("malformed contract mutations remain terminal conflicts while maintenance continues with independent records", async () => {
  const env = environment(), reaper = new ConcordReaperService({ ...env.options, staleGraceSeconds: 0 });
  const corruptions: [string, JsonObject, string][] = [
    ["malformed", { participants: [] }, "contract_invalid"],
    ["mismatched", { contractId: "another-identity" }, "contract_identity_mismatch"],
  ];
  for (const [contractId, corruption, code] of corruptions) {
    const contract = await env.coordinator.createContract(["worker:one"], { contractId });
    const original = (await env.contracts.get(contract.key))!;
    await env.contracts.put(contract.key, { ...original.value, ...corruption });
    const corrupted = (await env.contracts.get(contract.key))!;
    await assert.rejects(env.coordinator.maintenanceCancel({ ...contract, revision: corrupted.revision }), error => error instanceof StateConflict && error.code === code);
    const result = await reaper.scanOnce();
    assert.equal(result.contractsCancelled, 0); assert.equal(result.contractsDeleted, 0);
    assert.deepEqual(await env.contracts.get(contract.key), corrupted);
    assert.ok(env.audit.some(event => event.event === "scan_failure" && event.contractId === contractId && event.validationStatus === "invalid_contract"));
  }
  const independent = await env.coordinator.createContract(["worker:one"], { contractId: "independent" });
  const result = await reaper.scanOnce();
  assert.equal(result.contractsCancelled, 1);
  assert.equal((await env.contracts.get(independent.key))!.value.cancelledBy, "concord:maintenance");
  assert.equal(result.staleObservationCount, 2);
  await reaper.close();
});

test("malformed generation keys cannot create invalid maintenance records or block unrelated cleanup", async () => {
  const env = environment(), reaper = new ConcordReaperService({ ...env.options, staleGraceSeconds: 0 });
  for (const generation of ["0", "-1", "01", "9007199254740992"]) await env.contracts.put(`contracts.invalid.${generation}.meta`, {});
  const independent = await env.coordinator.createContract(["worker:one"], { contractId: "canonical" });
  const result = await reaper.scanOnce();
  assert.equal(result.contractsCancelled, 1); assert.equal(result.staleObservationCount, 0);
  assert.equal((await env.contracts.get(independent.key))!.value.state, "cancelled");
  assert.equal(env.audit.filter(event => event.event === "scan_failure").length, 4);
  await reaper.close();
});

test("retained deletion audits metadata, keeps a conforming marker through eight conflicts, and resumes after restart", async () => {
  class ConflictingTokens extends MemoryStateStore {
    conflict = true; attempts = 0;
    override async delete(...args: Parameters<MemoryStateStore["delete"]>) {
      this.attempts++; if (this.conflict) throw new StateConflict("Concurrent token refresh", "revision_changed");
      return super.delete(...args);
    }
  }
  const tokens = new ConflictingTokens({ policy: CONCORD_TOKEN_STORE_POLICY }), env = environment(tokens);
  const { contract, token } = await retained(env);
  const originalDelete = env.contracts.delete.bind(env.contracts);
  env.contracts.delete = async (key, options) => {
    assert.equal(env.audit[0]!.event, "delete_contract");
    assert.ok(await env.maintenance.get(concordTokenCleanupKey(contract)), "The persistent marker precedes contract deletion");
    return originalDelete(key, options);
  };
  const reaper = new ConcordReaperService(env.options), result = await reaper.scanOnce();
  assert.equal(result.contractsDeleted, 1); assert.equal(result.tokenKeysDeleted, 0); assert.equal(result.cleanupPendingCount, 1);
  assert.equal(tokens.attempts, 8); assert.equal(await env.contracts.get(contract.key), null); assert.ok(await tokens.get(token.key));
  const marker = (await env.maintenance.get(concordTokenCleanupKey(contract)))!;
  assert.equal(validateTokenCleanupRecord(marker.value).termsHash, contract.termsHash);
  const root = process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr");
  const schema = JSON.parse(readFileSync(path.join(root, "contract/v1/schemas/protocols/concord/token-cleanup.v1.schema.json"), "utf8"));
  const text = JSON.parse(readFileSync(path.join(root, "contract/v1/schemas/protocols/common/non-empty-text.v1.schema.json"), "utf8"));
  new PayloadSchemas({ [text.$id]: text }).validate(schema, marker.value);
  const audit = env.audit[0]!;
  assert.equal(audit.listedTokenKeyCount, 1); assert.equal(audit.tokens!.length, 1);
  assert.equal(audit.contract!.terms, undefined); assert.equal(audit.contract!.termsHash, contract.termsHash);
  assert.equal(Object.isFrozen(audit.contract), true);
  await reaper.close(); tokens.conflict = false;
  const successor = new ConcordReaperService(env.options), resumed = await successor.scanOnce();
  assert.equal(resumed.scannedContractCount, 0); assert.equal(resumed.tokenKeysDeleted, 1); assert.equal(resumed.cleanupPendingCount, 0);
  assert.equal(await tokens.get(token.key), null); assert.equal(await env.maintenance.get(marker.key), null);
  await successor.close();
});

test("malformed listed tokens and mismatched cleanup markers prevent retained contract deletion", async () => {
  const env = environment(), { contract, token } = await retained(env);
  const original = (await env.tokens.get(token.key))!;
  await env.tokens.put(token.key, { ...original.value, contractId: "foreign" });
  const reaper = new ConcordReaperService(env.options);
  assert.equal((await reaper.scanOnce()).contractsDeleted, 0); assert.ok(await env.contracts.get(contract.key));
  assert.equal((await env.maintenance.items("cleanup.")).length, 0);
  await env.tokens.put(token.key, original.value);
  const current = (await env.contracts.get(contract.key))!;
  await env.maintenance.create(concordTokenCleanupKey(contract), { schema: "dev.deckr.concord.token-cleanup.v1", contractId: contract.contractId,
    generation: contract.generation, contractRevision: current.revision, cleanupStartedAt: clock(), termsHash: "another-hash" });
  assert.equal((await reaper.scanOnce()).contractsDeleted, 0); assert.ok(await env.contracts.get(contract.key)); assert.ok(await env.tokens.get(token.key));
  await reaper.close();
});

test("cleanup retains its marker when the token store is unavailable and completes on a later scan", async () => {
  class UnavailableTokens extends MemoryStateStore {
    fail = true;
    override async delete(...args: Parameters<MemoryStateStore["delete"]>) { if (this.fail) throw new StateUnavailable("Offline"); return super.delete(...args); }
  }
  const tokens = new UnavailableTokens({ policy: CONCORD_TOKEN_STORE_POLICY }), env = environment(tokens), { contract } = await retained(env);
  const reaper = new ConcordReaperService(env.options), result = await reaper.scanOnce();
  assert.equal(result.contractsDeleted, 1); assert.equal(result.cleanupPendingCount, 1); assert.ok(await env.maintenance.get(concordTokenCleanupKey(contract)));
  tokens.fail = false; assert.equal((await reaper.scanOnce()).cleanupPendingCount, 0);
  assert.equal((await env.maintenance.items()).length, 0); await reaper.close();
});

test("malformed tokens found after contract deletion leave resumable cleanup pending", async () => {
  const env = environment(), { contract, token } = await retained(env), original = (await env.tokens.get(token.key))!;
  const remove = env.tokens.delete.bind(env.tokens);
  env.tokens.delete = async () => { throw new StateUnavailable("Offline"); };
  const reaper = new ConcordReaperService(env.options);
  assert.equal((await reaper.scanOnce()).contractsDeleted, 1);
  env.tokens.delete = remove;
  await env.tokens.put(token.key, { broken: true });
  assert.equal((await reaper.scanOnce()).cleanupPendingCount, 1);
  assert.ok(await env.maintenance.get(concordTokenCleanupKey(contract))); assert.ok(await env.tokens.get(token.key));
  await env.tokens.put(token.key, original.value);
  assert.equal((await reaper.scanOnce()).cleanupPendingCount, 0); await reaper.close();
});

test("a contract deletion conflict preserves tokens and the next scan can update its retained revision", async () => {
  const env = environment(), { contract, token } = await retained(env), originalDelete = env.contracts.delete.bind(env.contracts);
  let conflict = true;
  env.contracts.delete = async (key, options) => {
    if (conflict) { conflict = false; const entry = (await env.contracts.get(key))!; await env.contracts.update(key, entry.value, { revision: entry.revision }); }
    return originalDelete(key, options);
  };
  const reaper = new ConcordReaperService(env.options);
  assert.equal((await reaper.scanOnce()).contractsDeleted, 0); assert.ok(await env.contracts.get(contract.key)); assert.ok(await env.tokens.get(token.key));
  assert.ok(env.audit.some(event => event.event === "delete_conflict"));
  const next = await reaper.scanOnce(); assert.equal(next.contractsDeleted, 1); assert.equal(next.tokenKeysDeleted, 1); assert.equal(next.cleanupPendingCount, 0);
  await reaper.close();
});

test("out-of-prefix cleanup entries cannot cause deletion of another generation", async () => {
  const env = environment(), { contract, token } = await retained(env), list = env.tokens.items.bind(env.tokens);
  const foreign = await env.tokens.put("contracts.foreign.1.participants.other", { untrusted: true });
  env.tokens.items = async (_prefix?: string): Promise<StateEntry[]> => [foreign, ...(await list(_prefix))];
  const reaper = new ConcordReaperService(env.options);
  assert.equal((await reaper.scanOnce()).contractsDeleted, 0); assert.ok(await env.contracts.get(contract.key));
  assert.ok(await env.tokens.get(foreign.key)); assert.ok(await env.tokens.get(token.key)); await reaper.close();
});

test("a failed required pre-deletion audit preserves the contract", async () => {
  const env = environment(), { contract } = await retained(env);
  const reaper = new ConcordReaperService({ ...env.options, audit: event => { if (event.event === "delete_contract") throw new Error("Audit sink unavailable"); } });
  assert.equal((await reaper.scanOnce()).contractsDeleted, 0); assert.ok(await env.contracts.get(contract.key));
  assert.equal((await env.maintenance.items("cleanup.")).length, 0); await reaper.close();
});
