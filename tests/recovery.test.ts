import assert from "node:assert/strict";
import test from "node:test";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_CONTRACT_STORE_POLICY, CONCORD_TOKEN_STORE_POLICY, evaluateContract } from "../src/concord.ts";
import { StateUnavailable } from "../src/errors.ts";
import { MemoryStateStore, PERSISTENT_STATE_STORE_POLICY, type StateObservation } from "../src/state.ts";
import { AsyncQueue } from "../src/runtime.ts";
import { MaterializedStateSource } from "../src/source.ts";
import { ManagedServiceViewAccess, ServiceAdvertiser, ServiceMessageChannel, ServiceUseLeaseManager, parseServiceDescriptor, serviceUseTerms, serviceViewReadContextFromLease, serviceViewWriteContextFromLease, type ServiceProtocol, type ServiceUseLease } from "../src/services.ts";
import { flush, RecoverableStore, TestClock } from "./runtime-support.ts";
import type { JsonObject } from "../src/json.ts";

const protocol: ServiceProtocol = {
  namespace: "dev.deckr.recovery", featureId: "dev.deckr.recovery", advertisementProfile: "dev.deckr.recovery.ad.v1", useProfile: "dev.deckr.recovery.use.v1",
  operations: { play: {} }, messages: { play: { operation: "play", direction: "consumer_to_service", intent: "command", exchangePattern: "request_reply" } },
  viewFamilies: { status: { storeName: "review", keyPrefix: "status.", writer: "service" }, input: { storeName: "review", keyPrefix: "input.", writer: "consumer" } },
};

async function serviceLease(): Promise<{ lease: ServiceUseLease; concord: ConcordService; beacon: BeaconService }> {
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const advertiser = new ServiceAdvertiser({ protocol, serviceId: "music", endpoint: { endpoint: "service:music", sessionId: "provider" }, beacon });
  await advertiser.publish("available");
  const descriptor = parseServiceDescriptor((await beacon.find(protocol.featureId))[0]!, protocol)!;
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const terms = serviceUseTerms(descriptor, "controller:main", { operations: ["play"], views: { status: ["status.allowed."], input: ["input.allowed."] } });
  const agreement = await concord.ensureAgreement({ participants: ["controller:main", "service:music"], localParticipant: "controller:main", localSessionId: "consumer", profile: protocol.useProfile });
  await concord.attach(agreement.contract, "service:music", "provider");
  assert.equal((await agreement.refresh()).valid, true);
  return { lease: { agreement, descriptor, terms }, concord, beacon };
}

test("source recovery is atomic, immutable, and removes records whose deletion was missed", async () => {
  const store = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY);
  await store.put("old", { nested: { value: 1 } });
  const source = new MaterializedStateSource(store);
  await source.start();
  const original = source.snapshot;
  assert.throws(() => { (original.entries[0]!.value.nested as { value: number }).value = 2; }, TypeError);
  store.disconnect(); await flush();
  assert.equal(source.snapshot.current, false);
  assert.throws(() => source.get("old"), StateUnavailable);
  await store.delete("old"); await store.put("new", { value: 3 });
  assert.equal(source.snapshot.entries[0]!.key, "old");
  await store.recover(); await flush();
  assert.equal(source.snapshot.current, true);
  assert.equal(source.snapshot.generation, 2);
  assert.equal(source.get("old"), null);
  assert.equal(source.get("new")!.value.value, 3);
  assert.equal(original.entries[0]!.key, "old");
  await source.close(); assert.equal(store.observers.size, 0);
});

test("keyed observations preserve their version through unrelated traffic and recovery", async t => {
  const store = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY);
  const source = new MaterializedStateSource(store); t.after(() => source.close());
  await source.start();
  const iterator = source.watchKey("selected")[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value!.entry, undefined);
  const entry = await store.put("selected", { nested: { value: 1 } }); await source.observed(entry.revision);
  const snapshot = source.snapshot;
  for (let i = 0; i < 300; i++) await store.put(`other.${i}`, { value: i });
  await flush();
  const selected = (await iterator.next()).value!;
  assert.equal(selected.entry!.revision, entry.revision);
  assert.equal(selected.revision, entry.revision, "The entry and notification describe the same version");
  assert.equal(snapshot.entries.length, 1, "A retained full snapshot is immutable across later updates");
  assert.throws(() => { (selected.entry!.value.nested as JsonObject).value = 2; }, TypeError);
  await store.delete("selected"); await store.put("unrelated", {}); await flush();
  assert.equal((await iterator.next()).value!.entry, undefined);
  store.disconnect(); await flush();
  assert.equal((await iterator.next()).value!.current, false);
  await store.put("selected", { value: 3 }); await store.recover(); await flush();
  const recovered = (await iterator.next()).value!;
  assert.equal(recovered.current, true); assert.equal(recovered.generation, 2);
  assert.equal(recovered.entry!.value.value, 3);
  await iterator.return?.();
  assert.equal(source.subscriberCount, 0);
});

test("initial view access cancels without closing another owner's observation", async t => {
  const { lease, concord, beacon } = await serviceLease();
  t.after(async () => { await concord.close(); await beacon.close(); });
  let resume!: () => void;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  const store = new MemoryStateStore();
  const observe = store.observe.bind(store);
  store.observe = async function* (prefix, options) { await gate; yield* observe(prefix, options); };
  const context = serviceViewReadContextFromLease(lease);
  const cancelled = new AbortController();
  const first = new ManagedServiceViewAccess({ state: store, readContext: { ...context, authority: { ...context.authority, signal: cancelled.signal } } });
  const second = new ManagedServiceViewAccess({ state: store, readContext: context });
  t.after(async () => { resume(); await first.close(); await second.close(); });
  const view = { storeName: "review", key: "status.allowed.current" };
  const pending = assert.rejects(first.read(view), /cancelled|ended/);
  await flush(); cancelled.abort(); await pending;
  await first.close();
  resume();
  assert.equal(await second.read(view), null);
});

test("constructed protocol runtimes retain shared sources before starting and release their own lifetime", async () => {
  const ads = new RecoverableStore(BEACON_ADVERTISEMENT_STORE_POLICY);
  const first = new BeaconService(new BeaconDiscovery(ads)), second = new BeaconService(new BeaconDiscovery(ads));
  await first.start(); await first.close();
  assert.equal(ads.observers.size, 1, "The constructed second runtime owns the source even before start");
  await second.start(); assert.equal(second.source.snapshot.current, true);
  await second.close(); assert.equal(ads.observers.size, 0);

  const contracts = new RecoverableStore(CONCORD_CONTRACT_STORE_POLICY), tokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  const a = new ConcordService(new ConcordCoordinator(contracts, tokens)), b = new ConcordService(new ConcordCoordinator(contracts, tokens));
  await a.start(); await a.close(); await b.start();
  assert.equal(b.contractSource.snapshot.current, true); assert.equal(b.tokenSource.snapshot.current, true);
  await b.close(); assert.equal(contracts.observers.size, 0); assert.equal(tokens.observers.size, 0);
  const unstarted = new ConcordService(new ConcordCoordinator(contracts, tokens));
  await unstarted.close();
  assert.equal(unstarted.contractSource.isClosed, true, "An unstarted owner's close releases its retained sources");
});

test("a protected view accessor can start after an earlier sibling closes", async t => {
  const { lease, concord, beacon } = await serviceLease();
  const store = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY);
  const context = serviceViewReadContextFromLease(lease);
  const first = new ManagedServiceViewAccess({ state: store, readContext: context });
  const second = new ManagedServiceViewAccess({ state: store, readContext: context });
  t.after(async () => { await first.close(); await second.close(); await beacon.close(); await concord.close(); });
  const view = { storeName: "review", key: "status.allowed.current" };
  assert.equal(await first.read(view), null);
  await first.close(); assert.equal(store.observers.size, 1);
  assert.equal(await second.read(view), null);
  await second.close(); assert.equal(store.observers.size, 0);
});

test("independent service advertisers keep separate publication and renewal lifetimes", async () => {
  const clock = new TestClock(), store = new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY, scheduler: clock });
  const beacon = new BeaconService(new BeaconDiscovery(store, { scheduler: clock }));
  const options = { protocol, serviceId: "music", endpoint: { endpoint: "service:music", sessionId: "session" }, beacon };
  const first = new ServiceAdvertiser(options), second = new ServiceAdvertiser(options);
  await first.publish("available"); await second.publish("degraded");
  assert.notEqual(first.advertisement!.key, second.advertisement!.key);
  assert.equal((await store.items()).length, 2);
  await first.withdraw();
  const secondKey = second.advertisement!.key;
  assert.equal(((await store.get(secondKey))!.value.payload as { backendStatus: string }).backendStatus, "degraded");
  await clock.advance(150_000);
  assert.ok(Number((await store.get(secondKey))!.value.refreshSeq) > 1, "The surviving owner still renews");
  await second.withdraw(); assert.equal((await store.items()).length, 0); await beacon.close();
});

test("Beacon retains committed ownership when initial publication loses ordered observation", async () => {
  class LostObservation extends RecoverableStore {
    override async create(...args: Parameters<RecoverableStore["create"]>) {
      const result = await super.create(...args); this.disconnect(); await flush(); return result;
    }
  }
  const store = new LostObservation(BEACON_ADVERTISEMENT_STORE_POLICY);
  const beacon = new BeaconService(new BeaconDiscovery(store));
  const ad = beacon.createAdvertisement({ featureId: "feature", endpoint: "service:one", sessionId: "session" });
  await assert.rejects(ad.publish(), StateUnavailable);
  assert.ok(ad.handle, "An observation failure must not discard the committed handle");
  assert.equal(ad.state.status, "unavailable"); assert.equal(ad.state.failure!.code, "beacon_publication_failed");
  await ad.close(); assert.equal((await store.items()).length, 0); await beacon.close();
});

test("protocol runtime states expose currentness, immutable resource counts and terminal closure", async () => {
  const ads = new RecoverableStore(BEACON_ADVERTISEMENT_STORE_POLICY), beacon = new BeaconService(new BeaconDiscovery(ads));
  const watching = beacon.watch()[Symbol.asyncIterator]();
  const initial = (await watching.next()).value!;
  assert.equal(initial.status, "idle"); assert.equal(initial.version, 0);
  await beacon.start(); assert.equal(beacon.state.status, "ready");
  const first = beacon.watchFeature("feature")[Symbol.asyncIterator](), second = beacon.watchFeature("feature")[Symbol.asyncIterator]();
  await first.next(); await second.next();
  assert.equal(beacon.state.resources.featureViews, 1); assert.equal(beacon.source.subscriberCount, 2);
  await first.return?.(); assert.equal(beacon.state.resources.featureViews, 1);
  await second.return?.(); assert.equal(beacon.state.resources.featureViews, 0); assert.equal(beacon.source.subscriberCount, 1);
  assert.throws(() => { (beacon.state.resources as Record<string, number>).featureViews = 4; }, TypeError);
  ads.disconnect(); await flush();
  assert.equal(beacon.state.status, "unavailable"); assert.equal(beacon.state.failure!.code, "beacon_source_unavailable");
  await ads.recover(); await beacon.waitFor(state => state.status === "ready");
  assert.equal(initial.status, "idle", "Past state snapshots remain immutable");
  const closing = beacon.close(); assert.equal(beacon.close(), closing); await closing;
  assert.equal((await watching.next()).value!.status, "closed"); assert.equal((await watching.next()).done, true);

  const contracts = new RecoverableStore(CONCORD_CONTRACT_STORE_POLICY), tokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens));
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  assert.equal(concord.state.status, "ready"); assert.equal(concord.state.resources.agreements, 1); assert.equal(concord.state.resources.participants, 1);
  tokens.disconnect(); await flush();
  assert.equal(concord.state.status, "unavailable"); assert.equal(concord.state.failure!.code, "concord_source_unavailable");
  assert.equal(agreement.valid, false); assert.equal(concord.state.resources.agreements, 0);
  await tokens.recover(); await concord.waitFor(state => state.status === "ready");
  assert.equal(agreement.valid, false, "Runtime recovery cannot revive old authority");
  const closed = concord.close(); assert.equal(concord.close(), closed); await closed;
  const ended = concord.watch()[Symbol.asyncIterator]();
  assert.equal((await ended.next()).value!.status, "closed"); assert.equal((await ended.next()).done, true);
});

test("Beacon withdraws an initial write that commits after bounded closure", async () => {
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; }), started = new Promise<void>(resolve => { entered = resolve; });
  class SlowCreate extends MemoryStateStore {
    override async create(...args: Parameters<MemoryStateStore["create"]>) { entered(); await gate; return super.create(...args); }
  }
  const clock = new TestClock(), store = new SlowCreate({ policy: BEACON_ADVERTISEMENT_STORE_POLICY, scheduler: clock });
  const beacon = new BeaconService(new BeaconDiscovery(store, { scheduler: clock }));
  const ad = beacon.createAdvertisement({ featureId: "feature", endpoint: "service:one", sessionId: "session" });
  const publishing = assert.rejects(ad.publish()); await started;
  const closing = assert.rejects(ad.close(), /deadline/);
  await clock.advance(5000); await closing; await beacon.close();
  release(); await publishing; await flush();
  assert.equal((await store.items()).length, 0, "Late completion still belongs to the closed owner");
});

test("source currentness loss permanently revokes authority even when exact reads and stored tokens remain valid", async () => {
  const contracts = new RecoverableStore(CONCORD_CONTRACT_STORE_POLICY);
  const tokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens));
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  assert.equal(agreement.valid, true);
  contracts.failWrites = true; tokens.failWrites = true;
  tokens.disconnect(); await flush();
  assert.equal(agreement.closed, true);
  assert.equal(agreement.valid, false);
  assert.equal((await concord.validateExact(agreement.contract)).valid, true);
  await tokens.recover(); await flush();
  assert.equal((await agreement.refresh()).valid, false);
  assert.equal((await concord.validate(agreement.contract)).valid, true);
  contracts.failWrites = false; tokens.failWrites = false;
  const successor = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session", supersedes: agreement.contract });
  assert.notEqual(successor.contract.contractId, agreement.contract.contractId);
  assert.equal(successor.contract.generation, 1);
  await concord.close();
});

test("source closure wakes idle watchers and enforces the public registration limit", async () => {
  const source = new MaterializedStateSource(new MemoryStateStore());
  await source.start();
  const watches = Array.from({ length: 256 }, () => source.watch()[Symbol.asyncIterator]());
  assert.throws(() => source.watch(), /watcher limit/);
  await Promise.all(watches.map(watch => watch.next()));
  const pending = watches.map(watch => watch.next());
  await source.close();
  await Promise.all(pending);
  assert.ok((await Promise.all(watches.map(watch => watch.next()))).every(item => item.done));
  assert.equal(source.subscriberCount, 0);
});

test("the tombstone limit immediately reopens the watch to compact recovery state", async () => {
  class TombstoneStore extends MemoryStateStore {
    opened = 0;
    override async *observe(_prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateObservation> {
      this.opened++;
      if (this.opened === 1) {
        yield { operation: "snapshot", entries: [], revision: 0 };
        for (let revision = 1; revision <= 2000; revision++) yield { operation: "delete", key: `missing-${revision}`, revision };
        assert.fail("The source must reopen at the tombstone limit");
      }
      const queue = new AsyncQueue<StateObservation>();
      const close = () => queue.close();
      options.signal?.addEventListener("abort", close, { once: true });
      try {
        yield { operation: "snapshot", entries: [], revision: 2000 };
        yield* queue;
      } finally { options.signal?.removeEventListener("abort", close); queue.close(); }
    }
  }
  const clock = new TestClock();
  const store = new TombstoneStore();
  const source = new MaterializedStateSource(store, clock);
  await source.start();
  for (let turn = 0; turn < 300 && source.snapshot.generation < 2; turn++) await flush();
  assert.equal(store.opened, 2);
  assert.equal(source.snapshot.generation, 2);
  assert.equal(source.snapshot.current, true);
  assert.equal(clock.now(), 0, "Compaction does not wait for a retry timer");
  await source.close();
});

test("failed service-use index writes retire their provisional agreement", async () => {
  const { lease, concord, beacon } = await serviceLease();
  const index = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY);
  index.failWrites = true;
  const manager = new ServiceUseLeaseManager({ endpoint: { endpoint: "controller:main", sessionId: "consumer" }, concord, serviceUseIndex: index });
  await assert.rejects(manager.ensure(lease.descriptor, { operations: ["play"] }), /unavailable/);
  const open = await concord.contracts(protocol.useProfile, { state: "open" });
  assert.deepEqual(open.map(contract => contract.contractId), [lease.agreement.contract.contractId]);
  await manager.close(); await concord.close(); await beacon.close();
});

test("token restoration cannot revive a terminated agreement", async () => {
  const contracts = new MemoryStateStore();
  const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens));
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  const held = (await tokens.get(agreement.localToken!.key))!;
  await tokens.delete(held.key); await flush();
  assert.equal(agreement.valid, false);
  await tokens.put(held.key, held.value); await flush();
  assert.equal((await agreement.refresh()).valid, false);
  await concord.close();
});

test("runtime shutdown revokes every owner before waiting for unavailable cleanup", async () => {
  const clock = new TestClock();
  class HangingContracts extends MemoryStateStore {
    hang = false;
    override async update(...args: Parameters<MemoryStateStore["update"]>) {
      if (this.hang) return new Promise<Awaited<ReturnType<MemoryStateStore["update"]>>>(() => {});
      return super.update(...args);
    }
  }
  const contracts = new HangingContracts();
  const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY, scheduler: clock });
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens), { scheduler: clock });
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "one" });
  const contract = await concord.createContract(["service:two"]);
  const participant = concord.participantLease({ contract, participant: "service:two", sessionId: "two" });
  await participant.attachOrRefresh();
  assert.throws(() => { participant.token!.tokenId = "replacement"; }, TypeError);
  assert.throws(() => { contract.participants.push("service:other"); }, TypeError);
  const watch = agreement.watch()[Symbol.asyncIterator]();
  assert.equal((await watch.next()).value!.closed, false);
  const changed = watch.next();
  contracts.hang = true;
  const closing = concord.close();
  assert.equal(agreement.valid, false);
  assert.equal(participant.closed, true);
  assert.equal((await changed).value!.closed, true);
  assert.equal((await watch.next()).done, true);
  await clock.advance(5000);
  await closing;
});

test("exact and cached validation share pure evaluation and reject pointer/key mismatches", async () => {
  const contracts = new RecoverableStore(CONCORD_CONTRACT_STORE_POLICY);
  const tokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens));
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  assert.deepEqual(await concord.validate(agreement.contract), await concord.validateExact(agreement.contract));
  const reads = contracts.exactReads + tokens.exactReads;
  for (let i = 0; i < 10; i++) await concord.validate(agreement.contract);
  assert.equal(contracts.exactReads + tokens.exactReads, reads);
  const entry = (await contracts.get(agreement.contract.key))!;
  const evaluated = evaluateContract({ pointer: { ...agreement.contract, contractId: "other" }, contract: entry, tokens: {} });
  assert.equal(evaluated.reasonCode, "contract_key_mismatch");
  assert.equal(evaluated.valid, false);
  await concord.close();
});

test("proposal tokens and advertisements renew from their store TTL without caller refresh loops", async () => {
  const clock = new TestClock();
  const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY, scheduler: clock });
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), tokens), { scheduler: clock });
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  const ads = new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY, scheduler: clock });
  const beacon = new BeaconService(new BeaconDiscovery(ads, { scheduler: clock }));
  const advertisement = await beacon.ensureAdvertisement({ featureId: "test", endpoint: "service:one", sessionId: "session" });
  await advertisement.publish();
  await clock.advance(320_000);
  assert.equal(agreement.valid, true);
  assert.ok((await tokens.get(agreement.localToken!.key))!.value.refreshSeq as number >= 6);
  assert.ok((await ads.get(advertisement.handle!.key))!.value.refreshSeq as number >= 3);
  const tokenKey = agreement.localToken!.key;
  await agreement.close();
  assert.equal(agreement.valid, false);
  assert.equal(agreement.localToken, null);
  assert.equal(await tokens.get(tokenKey), null);
  await beacon.close(); await concord.close();
});

test("a missed renewal deadline revokes authority before remote cleanup completes", async () => {
  const clock = new TestClock();
  class HungTokens extends MemoryStateStore {
    override async update(): Promise<never> { return new Promise(() => {}); }
  }
  const tokens = new HungTokens({ policy: CONCORD_TOKEN_STORE_POLICY, scheduler: clock });
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), tokens), { scheduler: clock });
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  await clock.advance(120_000);
  assert.equal(agreement.valid, false);
  assert.equal(agreement.closed, true);
  await concord.close();
});

test("Beacon currentness loss does not revoke independent Concord authority", async () => {
  const ads = new RecoverableStore(BEACON_ADVERTISEMENT_STORE_POLICY);
  const beacon = new BeaconService(new BeaconDiscovery(ads));
  await beacon.start();
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  ads.disconnect(); await flush();
  assert.equal(agreement.valid, true);
  assert.equal((await agreement.refresh()).valid, true);
  await beacon.close(); await concord.close();
});

test("service requests reject cancelled authority without invoking the endpoint", async () => {
  const { lease, concord, beacon } = await serviceLease();
  let calls = 0;
  const channel = new ServiceMessageChannel({ endpoint: { endpoint: "controller:main", sessionId: "consumer", request: async () => { calls++; throw new Error("Must not publish"); } } });
  await concord.cancelContract(lease.agreement.contract, "service:music");
  const reply = await channel.request(lease, "play");
  assert.equal(reply.status, "unavailable"); assert.equal(calls, 0);
  await concord.close(); await beacon.close();
});

test("protected views retain negotiated prefixes, revoke access, and wake pending watchers", async () => {
  const { lease, concord, beacon } = await serviceLease();
  const store = new MemoryStateStore();
  const readContext = serviceViewReadContextFromLease(lease);
  const writeContext = serviceViewWriteContextFromLease(lease);
  const access = new ManagedServiceViewAccess({ state: store, readContext, writeContext });
  await assert.rejects(access.put({ storeName: "review", key: "input.other.value" }, {}));
  await assert.rejects(access.read({ storeName: "review", key: "status.other.value" }));
  await access.put({ storeName: "review", key: "input.allowed.value" }, { value: 1 });
  const watch = access.watch({ storeName: "review", key: "status.allowed.value" })[Symbol.asyncIterator]();
  assert.equal((await watch.next()).value!.operation, "snapshot");
  const pending = watch.next(); await flush();
  await concord.cancelContract(lease.agreement.contract, "service:music");
  assert.equal((await pending).done, true);
  await assert.rejects(access.put({ storeName: "review", key: "input.allowed.value" }, {}));
  await assert.rejects(access.read({ storeName: "review", key: "status.allowed.value" }));
  await access.close(); await concord.close(); await beacon.close();
});

test("protected view currentness can recover independently of Concord authority", async () => {
  const { lease, concord, beacon } = await serviceLease();
  const store = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY);
  const access = new ManagedServiceViewAccess({ state: store, readContext: serviceViewReadContextFromLease(lease) });
  const watch = access.watch({ storeName: "review", key: "status.allowed.value" })[Symbol.asyncIterator]();
  assert.equal((await watch.next()).value!.current, true);
  const stale = watch.next(); await flush();
  store.disconnect();
  assert.equal((await stale).value!.current, false);
  assert.equal(lease.agreement.valid, true);
  const recovered = watch.next(); await flush();
  await store.recover();
  const snapshot = (await recovered).value!;
  assert.equal(snapshot.current, true);
  assert.equal(snapshot.generation, 2);
  assert.equal(lease.agreement.valid, true);
  const pending = watch.next(); await flush();
  await access.close();
  assert.equal((await pending).done, true);
  await concord.close(); await beacon.close();
});

test("participant manager shutdown cancels idle subscriptions and cannot reattach", async () => {
  const contracts = new RecoverableStore(CONCORD_CONTRACT_STORE_POLICY);
  const tokens = new RecoverableStore(CONCORD_TOKEN_STORE_POLICY);
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens));
  const manager = concord.participantManager({ participant: "service:one", sessionId: "session", acceptContract: () => true });
  const states = manager.watch()[Symbol.asyncIterator]();
  assert.equal((await states.next()).value!.status, "idle");
  manager.start(); await flush();
  await manager.waitFor(state => state.status === "ready");
  const subscribed = concord.viewStats.subscribers;
  const closing = manager.aclose(); assert.equal(manager.aclose(), closing); await closing;
  assert.equal((await states.next()).value!.status, "closed"); assert.equal((await states.next()).done, true);
  assert.equal(concord.viewStats.subscribers, subscribed - 1);
  assert.equal(concord.contractSource.subscriberCount, 1, "all managers share one source subscription");
  assert.deepEqual(await manager.reconcile(), []);
  await concord.close();
  assert.equal(contracts.observers.size, 0); assert.equal(tokens.observers.size, 0);
});

test("ordered memory observations cannot coalesce away temporary token loss", async () => {
  const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), tokens));
  const agreement = await concord.ensureAgreement({ participants: ["service:one"], localParticipant: "service:one", localSessionId: "session" });
  const entry = (await tokens.get(agreement.localToken!.key))!;
  const deletion = tokens.delete(entry.key);
  const restoration = tokens.put(entry.key, entry.value);
  await Promise.all([deletion, restoration]); await flush();
  assert.equal(agreement.closed, true);
  assert.equal(agreement.valid, false);
  await concord.close();
});

test("protected writes wait for observations instead of patching the local cache", async () => {
  const { lease, concord, beacon } = await serviceLease();
  const store = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY);
  const access = new ManagedServiceViewAccess({ state: store, writeContext: serviceViewWriteContextFromLease(lease) });
  const view = { storeName: "review", key: "input.allowed.value" };
  await access.put(view, { value: 1 });
  store.current = false; // Delay delivery without marking the source stale.
  let completed = false;
  const pending = access.put(view, { value: 2 }).then(() => { completed = true; });
  await flush();
  assert.equal(completed, false);
  assert.equal((await store.items())[0]!.value.value, 2, "Exact mutation is already committed");
  await store.recover(); await pending;
  assert.equal(completed, true);
  await access.close(); await concord.close(); await beacon.close();
});

test("closing a participant manager during policy evaluation cannot attach a late token", async () => {
  const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), tokens));
  await concord.createContract(["service:one"]);
  let accept!: (value: boolean) => void;
  const policy = new Promise<boolean>(resolve => { accept = resolve; });
  const manager = concord.participantManager({ participant: "service:one", sessionId: "session", acceptContract: () => policy });
  const pending = manager.reconcile(); await flush();
  const closing = manager.aclose();
  accept(true); await closing;
  assert.deepEqual(await pending, []);
  assert.deepEqual(await tokens.items(), []);
  await concord.close();
});

test("provider view capabilities require local token ownership, not matching stored sessions", async () => {
  const { lease, concord, beacon } = await serviceLease();
  // The provider token in this fixture was attached directly. A fresh manager
  // must not adopt it as local authority merely because its session matches.
  const { ServiceUseAuthorizer } = await import("../src/services.ts");
  const provider = new ServiceUseAuthorizer({ protocol, serviceId: "music", endpoint: { endpoint: "service:music", sessionId: "provider" }, concord });
  await assert.rejects(provider.viewContext(lease.agreement.contract));
  await provider.close(); await concord.close(); await beacon.close();
});

test("protected writes recheck authority after waiting for their first view snapshot", async (t) => {
  let resume!: () => void;
  let observing!: () => void;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  const entered = new Promise<void>(resolve => { observing = resolve; });
  class DelayedViewStore extends MemoryStateStore {
    override async *observe(prefix = "", options: { signal?: AbortSignal } = {}): AsyncIterable<StateObservation> {
      observing(); await gate;
      yield* super.observe(prefix, options);
    }
  }
  const { lease, concord, beacon } = await serviceLease();
  const store = new DelayedViewStore();
  const access = new ManagedServiceViewAccess({ state: store, writeContext: serviceViewWriteContextFromLease(lease) });
  t.after(async () => { resume(); await access.close(); await concord.close(); await beacon.close(); });
  const write = access.put({ storeName: "review", key: "input.allowed.value" }, { value: 1 });
  const rejected = assert.rejects(write, /authority|Authority/);
  await entered;
  lease.agreement.terminate();
  resume(); await rejected;
  assert.deepEqual(await store.items(), [], "No mutation may start after authority revocation");
});

test("protected deletion rechecks authority after the exact revision read", async (t) => {
  let resume!: () => void;
  let reading!: () => void;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  const entered = new Promise<void>(resolve => { reading = resolve; });
  class DelayedReadStore extends MemoryStateStore {
    blockReads = false;
    override async get(key: string) {
      const entry = await super.get(key);
      if (this.blockReads) { reading(); await gate; }
      return entry;
    }
  }
  const { lease, concord, beacon } = await serviceLease();
  const store = new DelayedReadStore();
  const access = new ManagedServiceViewAccess({ state: store, writeContext: serviceViewWriteContextFromLease(lease) });
  t.after(async () => { resume(); await access.close(); await concord.close(); await beacon.close(); });
  const view = { storeName: "review", key: "input.allowed.value" };
  await access.put(view, { value: 1 });
  store.blockReads = true;
  const rejected = assert.rejects(access.delete(view), /authority|Authority/);
  await entered;
  lease.agreement.terminate(); resume(); await rejected;
  assert.equal((await store.items()).length, 1, "A revoked context cannot delete the retained record");
});

test("participant changes during policy evaluation trigger another reconciliation without a repair timer", async (t) => {
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const first = await concord.ensureAgreement({ participants: ["controller:a", "service:music"], localParticipant: "controller:a", localSessionId: "a" });
  let resume!: () => void;
  let evaluating!: () => void;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  const entered = new Promise<void>(resolve => { evaluating = resolve; });
  const manager = concord.participantManager({ participant: "service:music", sessionId: "provider", acceptContract: async contract => {
    if (contract.contractId === first.contract.contractId) { evaluating(); await gate; }
    return true;
  } });
  t.after(async () => { resume(); await manager.aclose(); await concord.close(); });
  manager.start(); await entered;
  const second = await concord.ensureAgreement({ participants: ["controller:b", "service:music"], localParticipant: "controller:b", localSessionId: "b" });
  // A blocked policy on the first pointer must not delay the second pointer.
  for (let index = 0; index < 20 && !second.valid; index++) await flush();
  assert.equal(second.valid, true);
  assert.equal(first.valid, false);
  resume();
  // Only queued microtasks run: no periodic or retry timer can repair a lost wakeup.
  for (let index = 0; index < 20 && !first.valid; index++) await flush();
  assert.equal(first.valid, true);
});

test("a competing participant session cannot cancel a live owner through session assertions", async (t) => {
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const agreement = await concord.ensureAgreement({ participants: ["controller:main", "service:music"], localParticipant: "controller:main", localSessionId: "consumer" });
  const incumbent = concord.participantManager({ participant: "service:music", sessionId: "incumbent", acceptContract: () => true });
  const contender = concord.participantManager({ participant: "service:music", sessionId: "contender", acceptContract: () => true, cancelTerminalStatuses: ["session_mismatch"] });
  t.after(async () => { await contender.aclose(); await incumbent.aclose(); await concord.close(); });
  await incumbent.reconcile();
  assert.equal(agreement.valid, true);
  assert.deepEqual(await contender.reconcile(), []);
  await contender.aclose();
  assert.equal(agreement.valid, true);
  assert.equal((await concord.validateExact(agreement.contract)).tokens["service:music"]!.sessionId, "incumbent");
});

test("closing an unattached participant lease does not cancel an unowned proposal", async () => {
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const proposal = await concord.ensureAgreement({ participants: ["controller:main", "service:music"], localParticipant: "controller:main", localSessionId: "consumer" });
  const unattached = concord.participantLease({ contract: proposal.contract, participant: "service:music", sessionId: "provider" });
  await unattached.close();
  assert.equal((await concord.contractRecord(proposal.contract))!.state, "open");
  assert.equal(proposal.closed, false);
  await concord.close();
});
