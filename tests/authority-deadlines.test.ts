import assert from "node:assert/strict";
import test from "node:test";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { MemoryStateStore } from "../src/state.ts";
import { RenewalWorker, StateCell } from "../src/runtime.ts";
import { flush, RecoverableStore, TestClock } from "./runtime-support.ts";
import { PERSISTENT_STATE_STORE_POLICY } from "../src/state.ts";

test("a delayed renewal timer revokes without attempting a renewal after expiry", async () => {
  let now = 0;
  const jobs: { callback: () => void; delay: number }[] = [];
  let renewed = false, failed = false;
  const worker = new RenewalWorker(() => 1, async () => { renewed = true; }, () => { failed = true; }, {
    now: () => now, random: () => 0, schedule: (delay, callback) => { jobs.push({ delay, callback }); return () => {}; },
  });
  worker.start(); now = 1100; jobs.shift()!.callback(); await flush();
  assert.equal(failed, true); assert.equal(renewed, false);
  assert.equal(jobs.length, 0);
});

test("expired authority is rejected before delayed timers run and late writes cannot restore it", async () => {
  const clock = new TestClock();
  const contracts = new MemoryStateStore();
  const tokens = new MemoryStateStore({ policy: { ...CONCORD_TOKEN_STORE_POLICY, brokerTtlSeconds: 1 } });
  const service = new ConcordService(new ConcordCoordinator(contracts, tokens), { scheduler: clock });
  const agreement = await service.ensureAgreement({ participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "one" });
  let release!: () => void;
  const update = tokens.update.bind(tokens);
  tokens.update = async (...args) => { await new Promise<void>(resolve => { release = resolve; }); return update(...args); };
  await clock.advance(500);
  clock.time = 1100; // Event loop delay: the expiry timer has not run.
  assert.equal(agreement.valid, false);
  release(); await flush();
  assert.equal(agreement.valid, false);
  assert.equal(agreement.localToken, null);
  await service.close();
});

test("proposal bootstrap cancels proven lost ownership and preserves pending and live sessions", async () => {
  const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const coordinator = new ConcordCoordinator(contracts, tokens), service = new ConcordService(coordinator);
  const old = await service.createContract(["controller:one"], { createdBy: "controller:one", participantSessions: { "controller:one": "old" } });
  const token = await coordinator.attach(old, "controller:one", "old");
  await coordinator.withdraw(token);
  const pending = await service.createContract(["controller:one"], { createdBy: "controller:one", participantSessions: { "controller:one": "pending" } });
  const live = await service.createContract(["controller:one"], { createdBy: "controller:one", participantSessions: { "controller:one": "live" } });
  await coordinator.attach(live, "controller:one", "live");
  const fresh = await service.ensureAgreement({ participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "new" });
  assert.equal((await service.validateExact(old)).status, "cancelled");
  assert.equal((await service.validateExact(pending)).status, "not_yet_fulfilled");
  assert.equal((await service.validateExact(live)).valid, true);
  assert.equal(fresh.valid, true);
  await assert.rejects(service.bootstrap("controller:one", "other", { exclusive: true }), /owns participation/);
  await service.close();
});

test("bootstrap attempt deadline includes shared runtime startup", async () => {
  const clock = new TestClock();
  let resume!: () => void;
  const blocked = new Promise<void>(resolve => { resume = resolve; });
  class SlowService extends ConcordService {
    override async start() { await blocked; await super.start(); }
  }
  const service = new SlowService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })), { scheduler: clock });
  const rejected = assert.rejects(service.bootstrap("controller:one", "new"), /deadline/);
  await clock.advance(5000);
  await rejected;
  resume(); await flush();
  await service.close();
});

for (const cancelled of [false, true]) {
  test(`bootstrap ${cancelled ? "cancellation" : "timeout"} prevents cleanup after a delayed ownership read`, async () => {
    const clock = new TestClock(), signal = new AbortController();
    const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
    const coordinator = new ConcordCoordinator(contracts, tokens), service = new ConcordService(coordinator, { scheduler: clock });
    const old = await service.createContract(["controller:one"], { participantSessions: { "controller:one": "old" } });
    await coordinator.withdraw(await coordinator.attach(old, "controller:one", "old"));
    await service.start();
    const validate = service.validateExact.bind(service);
    let resume!: () => void, entered = false;
    const blocked = new Promise<void>(resolve => { resume = resolve; });
    service.validateExact = async (...args) => { entered = true; await blocked; return validate(...args); };
    const rejected = assert.rejects(service.bootstrap("controller:one", "new", { signal: signal.signal }), cancelled ? /cancelled/ : /deadline/);
    await flush(); assert.equal(entered, true);
    if (cancelled) signal.abort();
    else await clock.advance(5000);
    await rejected;
    resume(); await flush();
    assert.equal((await contracts.get(old.key))!.value.state, "open", "the ended attempt must not start late cleanup");
    service.validateExact = validate;
    await service.bootstrap("controller:one", "new");
    assert.equal((await validate(old)).status, "cancelled");
    await service.close();
  });
}

test("state watch registration 257 is rejected without evicting existing readers", async () => {
  const state = new StateCell({ status: "ready" });
  const readers = Array.from({ length: 256 }, () => state.watch()[Symbol.asyncIterator]());
  assert.throws(() => state.watch(), /subscriber limit/);
  state.set({ status: "closed" });
  for (const reader of readers) { assert.equal((await reader.next()).value.status, "closed"); await reader.return?.(); }
  state.close();
});

test("malformed matching ownership evidence blocks bootstrap before proposal creation", async () => {
  const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const service = new ConcordService(new ConcordCoordinator(contracts, tokens));
  await contracts.create("contracts.invalid.1.meta", { contractId: "invalid", generation: 1, participants: ["controller:one"], attachedParticipants: [], state: "open", unknown: true });
  await assert.rejects(service.ensureAgreement({ participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "one" }), /Bootstrap authority is unresolved/);
  assert.equal((await contracts.items()).length, 1);
  assert.equal((await tokens.items()).length, 0);
  await service.close();
});

test("bootstrap cleans locally revoked ownership even while the stored token remains valid", async () => {
  const contracts = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY), tokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  const service = new ConcordService(new ConcordCoordinator(contracts, tokens));
  const old = await service.ensureAgreement({ participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "one" });
  contracts.failWrites = true; tokens.failWrites = true;
  await old.close();
  contracts.failWrites = false; tokens.failWrites = false;
  assert.equal((await service.validateExact(old.contract)).valid, true, "failed cleanup leaves stored evidence but no local authority");
  const fresh = await service.ensureAgreement({ participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "one" });
  assert.equal((await service.validateExact(old.contract)).status, "cancelled");
  assert.equal(fresh.valid, true);
  await service.close();
});

test("an unacknowledged token commit recovers only its prepared identity and cleans up", async () => {
  const { StateUnavailable } = await import("../src/errors.ts");
  for (const recoverable of [true, false]) {
    const contracts = new MemoryStateStore(), tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
    const service = new ConcordService(new ConcordCoordinator(contracts, tokens));
    const contract = await service.createContract(["controller:one"]);
    const create = tokens.create.bind(tokens), get = tokens.get.bind(tokens);
    let rejectReads = false;
    tokens.create = async (...args) => { await create(...args); rejectReads = !recoverable; throw new StateUnavailable("ack lost"); };
    tokens.get = async key => { if (rejectReads) { rejectReads = false; throw new StateUnavailable("read unavailable"); } return get(key); };
    const lease = service.participantLease({ contract, participant: "controller:one", sessionId: "one" });
    if (recoverable) await lease.attachOrRefresh();
    else await assert.rejects(lease.attachOrRefresh(), /ack lost/);
    await lease.close();
    assert.equal((await tokens.items()).length, 0, "the prepared token must be withdrawn even without an acknowledged handle");
    await service.close();
  }
});
