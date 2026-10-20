import assert from "node:assert/strict";
import test from "node:test";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { MemoryStateStore } from "../src/state.ts";
import { StateUnavailable } from "../src/errors.ts";
import { ManagedServiceClient, ManagedServiceProvider, ManagedServiceViewAccess, ServiceAdvertiser, ServiceUseAuthorizer, ServiceUseLeaseManager, parseServiceDescriptor, type ServiceProtocol } from "../src/services.ts";
import { flush } from "./runtime-support.ts";

const protocol: ServiceProtocol = {
  namespace: "dev.deckr.client", featureId: "dev.deckr.client", advertisementProfile: "dev.deckr.client.ad.v1", useProfile: "dev.deckr.client.use.v1",
  operations: { play: {} }, messages: { play: { operation: "play", direction: "consumer_to_service", intent: "command", exchangePattern: "request_reply" } }, viewFamilies: { status: { storeName: "client_views", keyPrefix: "status.", writer: "service" } },
};

for (const failShutdown of [false, true]) {
  test(`recovery cleanup stays diagnostic; current shutdown ${failShutdown ? "fails" : "succeeds"}`, async () => {
    const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
    const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
    const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), tokens));
    const provider = new ManagedServiceProvider({ protocol, serviceId: "cleanup", endpoint: { endpoint: "service:cleanup", sessionId: "provider" }, beacon, concord });
    const client = new ManagedServiceClient({ protocol, beacon, concord, endpoint: { endpoint: "controller:main", sessionId: "consumer" } });
    const remove = tokens.delete.bind(tokens);
    let failedKey: string | undefined;
    tokens.delete = async (key, options) => {
      if (key === failedKey) throw new StateUnavailable("Injected token cleanup failure");
      return remove(key, options);
    };
    try {
      await provider.start();
      const old = await client.ready();
      failedKey = old.agreement.localToken!.key;
      await old.agreement.close();
      await flush();
      const fresh = await client.ready();
      assert.notEqual(fresh.agreement.contract.contractId, old.agreement.contract.contractId);
      assert.equal(old.agreement.valid, false);
      assert.equal(fresh.agreement.valid, true);
      assert.equal(client.state.cleanupFailures, 1);
      assert.match(client.state.lastCleanupFailure!.message, /Injected token cleanup/);
      failedKey = failShutdown ? fresh.agreement.localToken!.key : undefined;
      if (failShutdown) await assert.rejects(client.close(), /Injected token cleanup/);
      else await client.close();
      assert.equal(client.state.cleanupFailures, failShutdown ? 2 : 1);
      assert.equal(fresh.agreement.valid, false);
    } finally {
      failedKey = undefined;
      await client.close().catch(() => {});
      await provider.close(); await beacon.close(); await concord.close();
    }
  });
}

test("shutdown joins cleanup already pending from recovery and reports its failure", async () => {
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const tokens = new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY });
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), tokens));
  const provider = new ManagedServiceProvider({ protocol, serviceId: "cleanup", endpoint: { endpoint: "service:cleanup", sessionId: "provider" }, beacon, concord });
  const client = new ManagedServiceClient({ protocol, beacon, concord, endpoint: { endpoint: "controller:main", sessionId: "consumer" } });
  let release!: () => void, entered!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const started = new Promise<void>(resolve => { entered = resolve; });
  const remove = tokens.delete.bind(tokens);
  try {
    await provider.start();
    const old = await client.ready(), key = old.agreement.localToken!.key;
    tokens.delete = async (candidate, options) => {
      if (candidate === key) { entered(); await blocked; throw new StateUnavailable("Pending cleanup failure"); }
      return remove(candidate, options);
    };
    old.agreement.terminate();
    await started;
    const closed = assert.rejects(client.close(), /Pending cleanup failure/);
    release(); await closed;
    assert.equal(client.state.cleanupFailures, 1);
    assert.equal(old.agreement.valid, false);
  } finally {
    release(); tokens.delete = remove;
    await client.close().catch(() => {});
    await provider.close(); await beacon.close(); await concord.close();
  }
});

test("managed provider starts participation before discovery, revokes domain consent, and withdraws on close", async (t) => {
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  let accepted = true;
  const provider = new ManagedServiceProvider({ protocol, serviceId: "managed", endpoint: { endpoint: "service:managed", sessionId: "provider" }, beacon, concord, acceptContract: () => accepted });
  const client = new ManagedServiceClient({ protocol, beacon, concord, operations: ["play"], views: ["status"], endpoint: { endpoint: "controller:main", sessionId: "consumer" }, serviceUseIndex: new MemoryStateStore(), stateFor: () => new MemoryStateStore() });
  t.after(async () => { await client.close(); await provider.close(); await beacon.close(); await concord.close(); });
  assert.equal(provider.ready, false);
  assert.equal((await beacon.find(protocol.featureId)).length, 0);
  await provider.start();
  assert.equal(provider.ready, true);
  assert.equal((await beacon.find(protocol.featureId)).length, 1);
  const lease = await client.ready();
  const context = await provider.viewContext(lease.agreement.contract);
  await context.authority.assertActive();
  accepted = false;
  await provider.reconcile();
  assert.equal(context.authority.signal.aborted, true);
  assert.equal(lease.agreement.valid, false);
  await assert.rejects(context.authority.assertActive());
  await provider.close();
  assert.equal(provider.ready, false);
  assert.equal((await beacon.find(protocol.featureId)).length, 0);
  await assert.rejects(provider.start(), /closed/);
});

test("managed client selects one winner, closes losers, survives Beacon withdrawal, and negotiates a successor", async () => {
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const providers: ServiceUseAuthorizer[] = [];
  const advertisers: ServiceAdvertiser[] = [];
  for (const id of ["one", "two"]) {
    const endpoint = { endpoint: `service:${id}`, sessionId: id };
    const provider = new ServiceUseAuthorizer({ protocol, serviceId: id, endpoint, concord });
    await provider.start(); providers.push(provider);
    const advertiser = new ServiceAdvertiser({ protocol, serviceId: id, endpoint, beacon });
    await advertiser.publish("available"); advertisers.push(advertiser);
  }
  let requests = 0;
  const client = new ManagedServiceClient({ protocol, beacon, concord, operations: ["play"], views: ["status"],
    endpoint: { endpoint: "controller:main", sessionId: "controller", request: async () => { requests++; throw new Error("Transport failed after publish"); } },
    serviceUseIndex: new MemoryStateStore(), stateFor: () => new MemoryStateStore(),
  });
  const winner = await client.ready();
  assert.equal(client.state.status, "ready");
  assert.equal(beacon.source.subscriberCount, 2, "Runtime and client share one feature watch");
  const provider = providers[winner.descriptor.serviceId === "one" ? 0 : 1]!;
  const access = new ManagedServiceViewAccess({ state: new MemoryStateStore(), writeContext: await provider.viewContext(winner.agreement.contract) });
  await access.put({ storeName: "client_views", key: "status.current" }, { value: 1 });
  const open = await concord.contracts(protocol.useProfile, { state: "open" });
  assert.equal(open.length, 1);
  await Promise.all(advertisers.map(advertiser => advertiser.withdraw()));
  assert.equal(winner.agreement.valid, true);
  assert.equal(client.state.status, "ready");
  await assert.rejects(client.request("play"), /Transport failed/);
  assert.equal(requests, 1);
  await Promise.all(advertisers.map(advertiser => advertiser.publish("available")));
  await concord.cancelContract(winner.agreement.contract, winner.descriptor.endpoint);
  await flush();
  await assert.rejects(access.put({ storeName: "client_views", key: "status.current" }, {}));
  await access.close();
  const successor = await client.ready();
  assert.notEqual(successor.agreement.contract.contractId, winner.agreement.contract.contractId);
  assert.equal(successor.agreement.contract.generation, 1);
  assert.equal(requests, 1, "Recovery must not replay a published command");
  await client.close(); await Promise.all(providers.map(provider => provider.close()));
  await beacon.close(); await concord.close();
});

test("managed client admits later candidates while negotiation is pending", async () => {
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const client = new ManagedServiceClient({ protocol, beacon, concord, operations: ["play"], endpoint: { endpoint: "controller:main", sessionId: "controller" }, serviceUseIndex: new MemoryStateStore(), stateFor: () => new MemoryStateStore() });
  await client.start(); await flush();
  const endpoint = { endpoint: "service:late", sessionId: "late" };
  const provider = new ServiceUseAuthorizer({ protocol, serviceId: "late", endpoint, concord });
  await provider.start();
  const advertiser = new ServiceAdvertiser({ protocol, serviceId: "late", endpoint, beacon });
  await advertiser.publish("available");
  assert.equal((await client.ready()).descriptor.serviceId, "late");
  await client.close(); await provider.close(); await beacon.close(); await concord.close();
});

test("a negotiation race admits at most 64 unique endpoint/session candidates", async (t) => {
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  for (let index = 0; index < 65; index++) {
    const serviceId = `candidate-${index}`;
    await new ServiceAdvertiser({ protocol, serviceId, endpoint: { endpoint: `service:${serviceId}`, sessionId: "session" }, beacon }).publish("available");
  }
  const client = new ManagedServiceClient({ protocol, beacon, concord, operations: ["play"], endpoint: { endpoint: "controller:main", sessionId: "controller" }, serviceUseIndex: new MemoryStateStore(), stateFor: () => new MemoryStateStore() });
  t.after(async () => { await client.close(); await beacon.close(); await concord.close(); });
  await client.start();
  for (let index = 0; index < 50; index++) {
    await flush();
    if ((await concord.contracts(protocol.useProfile, { state: "open" })).length === 64) break;
  }
  assert.equal((await concord.contracts(protocol.useProfile, { state: "open" })).length, 64);
  await client.close();
  assert.equal((await concord.contracts(protocol.useProfile, { state: "open" })).length, 0);
  await beacon.close(); await concord.close();
});


test("closing a lease manager revokes an agreement whose scope-index commit is blocked", async (t) => {
  let resume!: () => void;
  let writing!: () => void;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  const entered = new Promise<void>(resolve => { writing = resolve; });
  class SlowIndex extends MemoryStateStore {
    override async create(...args: Parameters<MemoryStateStore["create"]>) {
      writing(); await gate;
      return super.create(...args);
    }
  }
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const endpoint = { endpoint: "service:managed", sessionId: "provider" };
  const provider = new ManagedServiceProvider({ protocol, serviceId: "managed", endpoint, beacon, concord });
  await provider.start();
  const descriptor = parseServiceDescriptor((await beacon.find(protocol.featureId))[0]!, protocol)!;
  const manager = new ServiceUseLeaseManager({ endpoint: { endpoint: "controller:main", sessionId: "consumer" }, concord, serviceUseIndex: new SlowIndex() });
  t.after(async () => { resume(); await manager.close(); await provider.close(); await beacon.close(); await concord.close(); });
  const rejected = assert.rejects(manager.ensure(descriptor, { operations: ["play"] }), /closed/);
  await entered; await provider.reconcile();
  const before = await concord.contracts(protocol.useProfile, { state: "open" });
  assert.equal(before.length, 1);
  assert.equal((await concord.validate(before[0]!)).valid, true);
  await manager.close();
  assert.equal((await concord.validate(before[0]!)).valid, false);
  assert.equal((await concord.contracts(protocol.useProfile, { state: "open" })).length, 0);
  resume(); await rejected;
});
