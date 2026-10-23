import assert from "node:assert/strict";
import test from "node:test";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { MemoryStateStore } from "../src/state.ts";
import { VerifiedRestart } from "../src/restart.ts";

const provider = "service:restart", client = "controller:restart";
function handoff(session = "dead") {
  return { schema: "dev.deckr.runtime.restart_handoff.v1", instanceId: "provider", predecessors: [{ predecessorId: "container:process-start", exitCode: 137, participants: [{ endpoint: provider, sessionId: session }] }] };
}

test("verified restart cleans all predecessor profiles before token expiry and preserves live competitors and successors", async () => {
  const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const coordinator = new ConcordCoordinator(contracts, tokens);
  const service = new ConcordService(coordinator, { restart: new VerifiedRestart(handoff(), "provider") });
  async function make(session: string, profile: string, owned = false, attached = true) {
    const contract = await service.createContract([client, provider], { profile, createdBy: owned ? provider : client, participantSessions: { [client]: "client", [provider]: session } });
    await coordinator.attach(contract, client, "client");
    const token = attached ? await coordinator.attach(contract, provider, session) : undefined;
    return { contract, token };
  }
  const dead = await make("dead", "example.service.v1"), owned = await make("dead", "example.other.v1", true);
  const live = await make("live", "example.service.v1"), successor = await make("successor", "example.service.v1");
  const pending = await make("dead", "example.service.v1", false, false);
  assert.equal((await service.validateExact(dead.contract)).valid, true);
  const start = performance.now();
  await service.bootstrap(provider, "successor", { profile: "example.service.v1" });
  assert.ok(performance.now() - start < 2000);
  assert.ok(await tokens.get(dead.token!.key), "old token has not expired");
  for (const item of [dead, owned]) assert.equal((await service.validateExact(item.contract)).status, "cancelled");
  for (const item of [live, successor]) assert.equal((await service.validateExact(item.contract)).valid, true);
  assert.equal((await service.validateExact(pending.contract)).status, "not_yet_fulfilled");
  await service.bootstrap(provider, "next-successor");
  assert.equal((await service.validateExact(successor.contract)).valid, true);
  await assert.rejects(service.bootstrap(provider, "dead"), /fresh session/);
  await assert.rejects(service.bootstrap(provider, "new", { exclusive: true }), /owns participation/);
  await service.close();
});

for (const proof of [undefined, "wrong-session"]) test(`unverified predecessor remains valid (${proof})`, async () => {
  const coordinator = new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY }));
  const service = new ConcordService(coordinator, { restart: proof ? new VerifiedRestart(handoff(proof), "provider") : undefined });
  const contract = await service.createContract([provider], { participantSessions: { [provider]: "dead" } });
  await coordinator.attach(contract, provider, "dead");
  await service.bootstrap(provider, "new");
  assert.equal((await service.validateExact(contract)).valid, true);
  await service.close();
});

test("handoff rejects wrong instance, duplicate identity, unknown fields and invalid exit code", () => {
  const predecessor = handoff().predecessors[0]!;
  for (const value of [{ ...handoff(), instanceId: "other" }, { ...handoff(), extra: true },
    ...[{ ...predecessor, participants: [...predecessor.participants, ...predecessor.participants] }, { ...predecessor, exitCode: true }, { ...predecessor, exitCode: 2147483648 }].map(item => ({ ...handoff(), predecessors: [item] }))]) assert.throws(() => new VerifiedRestart(value, "provider"));
});

test("cleanup failure prevents readiness and a later confirmed retry succeeds", async () => {
  const coordinator = new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY }));
  const service = new ConcordService(coordinator, { restart: new VerifiedRestart(handoff(), "provider") });
  const contract = await service.createContract([provider]);
  await coordinator.attach(contract, provider, "dead");
  const cancel = service.cancelContract.bind(service);
  service.cancelContract = async () => false;
  await assert.rejects(service.bootstrap(provider, "new"), /cleanup is incomplete/);
  assert.equal((await service.validateExact(contract)).valid, true);
  service.cancelContract = cancel;
  await service.bootstrap(provider, "new");
  assert.equal((await service.validateExact(contract)).status, "cancelled");
  await service.close();
});

test("discovery cleanup is restricted to exited sessions and proof history survives repeated crashes", async () => {
  const { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } = await import("../src/beacon.ts");
  const store = new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY });
  const beacon = new BeaconService(new BeaconDiscovery(store));
  const records = await Promise.all(["dead", "older", "live", "successor"].map(sessionId => beacon.advertise({ featureId: "example.restart.v1", endpoint: provider, sessionId })));
  const value = handoff();
  value.predecessors.push({ ...handoff("older").predecessors[0]!, predecessorId: "container:older" });
  const proof = new VerifiedRestart(value, "provider");
  await beacon.cleanupRestart(proof, provider);
  await beacon.cleanupRestart(proof, provider);
  assert.equal(await store.get(records[0]!.key), null);
  assert.equal(await store.get(records[1]!.key), null);
  assert.ok(await store.get(records[2]!.key));
  assert.ok(await store.get(records[3]!.key));
  await beacon.close();
});

test("cancelled discovery cleanup cannot start a late delete after a blocked exact read", async () => {
  const { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } = await import("../src/beacon.ts");
  const { flush } = await import("./runtime-support.ts");
  const store = new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY });
  const beacon = new BeaconService(new BeaconDiscovery(store));
  const record = await beacon.advertise({ featureId: "example.restart.v1", endpoint: provider, sessionId: "dead" });
  const get = store.get.bind(store);
  let resume!: () => void, entered = false;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  store.get = async key => { entered = true; await gate; return get(key); };
  const signal = new AbortController();
  const result = assert.rejects(beacon.cleanupRestart(new VerifiedRestart(handoff(), "provider"), provider, { signal: signal.signal }), /cancel/i);
  await flush(); assert.ok(entered);
  signal.abort(); await result;
  resume(); await flush();
  assert.ok(await get(record.key), "closed cleanup must not delete after the delayed read resolves");
  store.get = get;
  await beacon.close();
});
