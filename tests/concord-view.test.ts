import assert from "node:assert/strict";
import test from "node:test";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY, type ConcordMember } from "../src/concord.ts";
import { MemoryStateStore } from "../src/state.ts";
import { flush } from "./runtime-support.ts";

function environment() {
  const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const coordinator = new ConcordCoordinator(contracts, tokens), service = new ConcordService(coordinator);
  return { contracts, tokens, coordinator, service };
}

test("shared indexes select the smallest set and unrelated traffic cannot reevaluate a held agreement", async t => {
  const { contracts, tokens, coordinator, service } = environment();
  const peer = new ConcordService(coordinator);
  t.after(async () => { await peer.close(); await service.close(); });
  await service.start(); await peer.start();
  assert.equal(service.contractSource.subscriberCount, 1);
  assert.equal(service.tokenSource.subscriberCount, 1);
  const held = await service.ensureAgreement({ profile: "selected", participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "one" });
  const unrelated = [];
  for (let i = 0; i < 200; i++) unrelated.push(await coordinator.createContract(["service:other"], { profile: "unrelated", contractId: `unrelated-${i}` }));
  await service.contractSource.observed(unrelated.at(-1)!.revision);
  const watch = service.watchContracts({ profile: "selected" })[Symbol.asyncIterator]();
  assert.equal((await watch.next()).value!.contracts.length, 1);
  const before = service.viewStats;
  await coordinator.attach(unrelated[0]!, "service:other", "other");
  await service.contractSource.observed((await contracts.get(unrelated[0]!.key))!.revision);
  assert.equal(service.viewStats.validityEvaluations, before.validityEvaluations);
  assert.equal(service.viewStats.candidateVisits, before.candidateVisits);
  assert.equal(service.viewStats.contractParses - before.contractParses, 1);
  const queried = service.viewStats;
  assert.equal((await peer.contracts("selected")).length, 1);
  assert.equal(service.viewStats.contractParses, queried.contractParses);
  assert.equal(service.viewStats.candidateVisits - queried.candidateVisits, 1);
  assert.equal(held.valid, true);
  assert.equal((await service.validate({ ...held.contract, generation: 2 })).valid, false, "a different pointer cannot reuse a cached validity");
  await tokens.put("not-a-token", {}); await contracts.put("not-a-contract", {}); await flush();
  assert.equal(held.valid, true);
  await watch.return?.();
});

test("Concord watch overflow retains complete membership and invalid observations", async t => {
  const { contracts, coordinator, service } = environment(); t.after(() => service.close());
  const watch = service.watchContracts({ profile: "selected" })[Symbol.asyncIterator]();
  const first = (await watch.next()).value!;
  assert.equal(first.current, true); assert.equal(first.contracts.length, 0);
  let revision = 0;
  for (let i = 0; i < 260; i++) revision = (await coordinator.createContract(["controller:one"], { profile: "selected", contractId: `selected-${i}` })).revision;
  await service.contractSource.observed(revision);
  const next = (await watch.next()).value!;
  assert.equal(next.contracts.length, 260); assert.ok(next.version > first.version);
  assert.equal(next.resnapshotRequired, true); assert.deepEqual(next.changed, []);
  const bad = next.contracts[0]!.contract;
  await contracts.put(bad.key, { profile: "selected", participants: ["controller:one"], state: "open" });
  const invalid = (await watch.next()).value!;
  assert.equal(invalid.contracts.length, 260);
  assert.equal(invalid.contracts.find((item: ConcordMember) => item.contract.key === bad.key)!.validity.status, "invalid_contract");
  await watch.return?.();
});

test("Concord watchers enforce their registration limit and release cancelled readers", async t => {
  const { service } = environment(); t.after(() => service.close());
  const readers = [];
  for (let i = 0; i < 256; i++) {
    const reader = service.watchContracts()[Symbol.asyncIterator](); await reader.next(); readers.push(reader);
  }
  await assert.rejects(service.watchContracts()[Symbol.asyncIterator]().next(), /subscriber limit/);
  for (const reader of readers) await reader.return?.();
  assert.equal(service.viewStats.subscribers, 0);
});

test("attachment contention waits for the selected contract rather than unrelated source revisions", async t => {
  const { StateConflict } = await import("../src/errors.ts");
  const { TestClock } = await import("./runtime-support.ts");
  const clock = new TestClock();
  const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const service = new ConcordService(new ConcordCoordinator(contracts, tokens), { scheduler: clock });
  t.after(() => service.close());
  const contract = await service.createContract(["controller:one"]);
  let attempts = 0;
  contracts.update = async () => {
    attempts++;
    await contracts.put("contracts.unrelated.1.meta", { noise: attempts });
    throw new StateConflict("contended", "revision_changed");
  };
  const lease = service.participantLease({ contract, participant: "controller:one", sessionId: "one" });
  const attached = assert.rejects(lease.attachOrRefresh());
  await flush(); await flush();
  assert.equal(attempts, 1);
  for (let i = 0; i < 20; i++) await contracts.put("contracts.unrelated.1.meta", { noise: i });
  await flush(); assert.equal(attempts, 1);
  await clock.advance(5000); await attached; await lease.close();
  assert.equal((await tokens.items()).length, 0);
});
