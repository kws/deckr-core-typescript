import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { buildMessage, entitySubject, type EntitySubject } from "../src/lanes.ts";
import { EndpointSession, MemoryMessageTransport } from "../src/messaging.ts";
import type { JsonObject } from "../src/json.ts";
import { PayloadSchemas } from "../src/schema.ts";
import { MemoryStateStore } from "../src/state.ts";
import { ManagedServiceClient, ManagedServiceProvider, ServiceMessageChannel, ServiceViewStoreWriter, validateServiceUseContract, parseServiceDescriptor, validateServiceProtocol,
  type ServiceProtocol, type ServiceProviderContext, type ServiceUseLease } from "../src/services.ts";
import { flush, TestClock } from "./runtime-support.ts";

const protocol: ServiceProtocol = {
  namespace: "test.player", featureId: "test.player", advertisementProfile: "test.player.ad.v1", useProfile: "test.player.use.v1",
  operations: { play: {} },
  messages: {
    play: { operation: "play", direction: "consumer_to_service", intent: "command", exchangePattern: "request_reply",
      paramsSchema: { schemaId: "test.play.params", schema: { type: "object", properties: { track: { type: "string" } }, required: ["track"], additionalProperties: false } },
      resultSchema: { schemaId: "test.play.result", schema: { type: "object", properties: { accepted: { const: true } }, required: ["accepted"], additionalProperties: false } },
    },
    changed: { direction: "service_to_consumer", intent: "event", exchangePattern: "one_way", eventSchema: { schemaId: "test.changed", schema: { type: "object", required: ["track"] } } },
  },
  viewFamilies: { status: { storeName: "player_views", keyPrefix: "status.", writer: "service" } },
};
function deferred() { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; }
function environment(t: TestContext, clock?: TestClock, views = new MemoryStateStore()) {
  const transport = new MemoryMessageTransport();
  const consumer = new EndpointSession({ endpoint: "controller:main", sessionId: "consumer", transport, scheduler: clock });
  const service = new EndpointSession({ endpoint: "service:player", sessionId: "provider", transport, scheduler: clock });
  const records = new MemoryStateStore();
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(records, new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })), { scheduler: clock });
  const shared = { protocol, beacon, concord, stateFor: () => views };
  const client = new ManagedServiceClient({ ...shared, endpoint: consumer.lane("services"), operations: ["play"], views: ["status"] });
  const providers: ManagedServiceProvider[] = [];
  const provider = (options: Partial<ConstructorParameters<typeof ManagedServiceProvider>[0]> = {}) => {
    const result = new ManagedServiceProvider({ ...shared, serviceId: "player", endpoint: service.lane("services"), ...options });
    providers.push(result); return result;
  };
  t.after(async () => { await client.close(); await Promise.all(providers.map(item => item.close())); await beacon.close(); await concord.close(); await consumer.close(); await service.close(); transport.close(); });
  return { ...shared, client, provider, consumer, service, transport, records, views };
}
function operation(lease: ServiceUseLease, params: JsonObject = { track: "a" }, ttlMs?: number) {
  return buildMessage({ lane: "services", sender: "controller:main", senderSessionId: "consumer", recipient: "service:player", recipientSessionId: "provider",
    messageType: "serviceMessage", contract: { contractId: lease.agreement.contract.contractId, generation: 1 },
    subject: entitySubject("service", { serviceId: "player", namespace: protocol.namespace, name: "play" }),
    body: { serviceNamespace: protocol.namespace, name: "play", intent: "command", exchangePattern: "request_reply", params }, ttlMs });
}
function upstreamUseSchema() {
  const specRoot = process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr");
  const prefix = "https://kws.github.io/deckr/contract/v1/schemas/";
  const references: Record<string, JsonObject> = {};
  function load(id: string): JsonObject {
    id = id.split("#")[0]!;
    if (references[id]) return references[id]!;
    const schema = JSON.parse(readFileSync(path.join(specRoot, "contract/v1/schemas", id.slice(prefix.length)), "utf8"));
    references[id] = schema;
    function visit(value: unknown): void {
      if (!value || typeof value !== "object") return;
      for (const [key, child] of Object.entries(value)) {
        if (key === "$ref" && typeof child === "string" && child.startsWith(prefix)) load(child);
        else visit(child);
      }
    }
    visit(schema); return schema;
  }
  const id = `${prefix}deckr/services/use-contract.v1.schema.json`;
  load(id); return { schemas: new PayloadSchemas(references), schema: { $ref: id } };
}

test("managed service roundtrip validates payloads, publishes protected views, and emits terms-free wire contracts", async t => {
  const env = environment(t); let calls = 0;
  const provider = env.provider({ handle: async (body, context) => {
    calls++;
    assert.deepEqual(context.message.subject, entitySubject("service", { serviceId: "player", namespace: protocol.namespace, name: "play" }));
    const writer = await context.viewWriter("player_views");
    await writer.put({ storeName: "player_views", key: "status.current" }, { track: body.params!.track! });
    await context.send("changed", { track: body.params!.track! }, { resourceId: "speaker" });
    return { result: { accepted: true } };
  } });
  await provider.start();
  const lease = await env.client.ready();
  const record = (await env.records.get(lease.agreement.contract.key))!.value;
  assert.equal(Object.hasOwn(record, "terms"), false);
  assert.equal(Object.hasOwn(record, "termsHash"), false);
  validateServiceUseContract(record);
  const upstream = upstreamUseSchema(); upstream.schemas.validate(upstream.schema, record);
  assert.throws(() => validateServiceUseContract({ ...record, terms: {} }));
  assert.throws(() => upstream.schemas.validate(upstream.schema, { ...record, terms: {} }));
  await assert.rejects(env.client.request("play", { track: 42 }), /schema/);
  assert.equal(calls, 0);
  const events = env.consumer.lane("services").watch()[Symbol.asyncIterator]();
  const result = await env.client.request("play", { track: "track-a" });
  assert.deepEqual(result.result, { accepted: true });
  const event = (await events.next()).value!.message;
  assert.equal(event.body.name, "changed");
  assert.deepEqual(event.subject, entitySubject("service", { serviceId: "player", namespace: protocol.namespace, name: "changed", resourceId: "speaker" }));
  assert.equal((await env.client.read({ storeName: "player_views", key: "status.current" }))!.track, "track-a");
  await events.return?.();
  await provider.close();
  assert.equal((await env.views.items()).length, 0);
});

function invalidServiceSubjects(name: string): EntitySubject[] {
  const subject = entitySubject("service", { serviceId: "player", namespace: protocol.namespace, name });
  const invalid = [{ ...subject, kind: "application" }];
  for (const field of ["serviceId", "namespace", "name"]) {
    const missing = { ...subject.identifiers };
    delete missing[field];
    invalid.push(entitySubject("service", missing));
    invalid.push(entitySubject("service", { ...subject.identifiers, [field]: "other" }));
  }
  return invalid;
}

test("service boundaries reject missing and mismatched subject identity and accept application identifiers", async t => {
  const env = environment(t);
  let calls = 0;
  const provider = env.provider({ handle: () => { calls++; return { result: { accepted: true } }; } });
  await provider.start(); const lease = await env.client.ready();
  const request = operation(lease);
  for (const subject of invalidServiceSubjects("play")) {
    await assert.rejects(provider.dispatch({ message: { ...request, subject }, reply: async () => {} }), /not addressed/);
  }
  assert.equal(calls, 0);
  await provider.dispatch({ message: { ...request, subject: entitySubject("service", { ...request.subject.identifiers, resourceId: "speaker" }) }, reply: async () => {} });
  assert.equal(calls, 1);

  const channel = new ServiceMessageChannel({ endpoint: env.consumer.lane("services") });
  const event = buildMessage({ lane: "services", sender: "service:player", senderSessionId: "provider",
    recipient: "controller:main", recipientSessionId: "consumer", messageType: "serviceMessage", contract: request.contract,
    subject: entitySubject("service", { serviceId: "player", namespace: protocol.namespace, name: "changed", resourceId: "speaker" }),
    body: { serviceNamespace: protocol.namespace, name: "changed", intent: "event", exchangePattern: "one_way", event: { track: "a" } } });
  for (const subject of invalidServiceSubjects("changed")) {
    await assert.rejects(channel.receive(lease, { ...event, subject }), /does not belong/);
  }
  assert.equal((await channel.receive(lease, event)).name, "changed");

  for (const subject of invalidServiceSubjects("play")) {
    const replies = new ServiceMessageChannel({ endpoint: {
      endpoint: "controller:main", sessionId: "consumer", request: async input => {
        assert.deepEqual(input.subject, request.subject);
        const reply = buildMessage({ lane: "services", sender: "service:player", senderSessionId: "provider",
          recipient: "controller:main", recipientSessionId: "consumer", messageType: "serviceMessage", contract: request.contract, subject,
          body: { ...request.body, status: "ok", result: { accepted: true } } });
        assert.equal(input.accept!({ ...reply, subject: entitySubject("service", { ...request.subject.identifiers, resourceId: "speaker" }) }), true);
        assert.equal(input.accept!(reply), false);
        return reply;
      },
    } });
    await assert.rejects(replies.request(lease, "play", { track: "a" }), /Invalid service reply/);
  }
});

test("backend loss revokes a blocked handler and its view writer before any late reply", async t => {
  const env = environment(t), entered = deferred(), resume = deferred();
  let context!: ServiceProviderContext, writer!: ServiceViewStoreWriter, replies = 0;
  const provider = env.provider({ handle: async (_body, scope) => {
    context = scope; writer = await scope.viewWriter("player_views");
    await writer.put({ storeName: "player_views", key: "status.current" }, {});
    entered.resolve(); await resume.promise;
    return { result: { accepted: true } };
  } });
  t.after(() => resume.resolve());
  await provider.start(); const lease = await env.client.ready();
  const dispatched = provider.dispatch({ message: operation(lease), reply: async () => { replies++; } });
  const rejected = assert.rejects(dispatched, /not ready|ended/);
  await entered.promise;
  const oldVersion = provider.state.version;
  await provider.setBackendStatus("unavailable", { reason: "backend disconnected" });
  assert.equal(provider.state.status, "unavailable");
  assert.ok(provider.state.version > oldVersion);
  assert.equal(context.signal.aborted, true);
  await assert.rejects(context.assertActive());
  await assert.rejects(writer.put({ storeName: "player_views", key: "status.late" }, {}), /closed/);
  resume.resolve(); await rejected; assert.equal(replies, 0);
  await provider.setBackendStatus("available");
  await assert.rejects(context.assertActive(), /ended|terminated/);
  const successor = await env.client.ready();
  assert.notEqual(successor.agreement.contract.contractId, lease.agreement.contract.contractId);
});

test("provider validates schema, resource policy, and expiry before calling backend behavior", async t => {
  const clock = new TestClock(); clock.time = Date.now();
  const env = environment(t, clock), entered = deferred(), resume = deferred();
  let calls = 0, allow = false, block = false;
  const provider = env.provider({ authorizeOperation: async () => { if (block) { entered.resolve(); await resume.promise; } return allow; },
    handle: () => { calls++; return { result: { accepted: true } }; } });
  await provider.start(); const lease = await env.client.ready();
  const delivery = (params: JsonObject) => ({ message: operation(lease, params), reply: async () => {} });
  await assert.rejects(provider.dispatch(delivery({ track: 7 })), /schema/);
  await assert.rejects(provider.dispatch(delivery({ track: "a" })), /policy/);
  assert.equal(await provider.authorizeMessage(operation(lease)), "denied");
  allow = true; block = true;
  const rejected = assert.rejects(provider.dispatch({ message: operation(lease, { track: "a" }, 50), reply: async () => {} }), /expired/);
  await entered.promise; await clock.advance(100); resume.resolve(); await rejected;
  assert.equal(calls, 0);
});

test("service requests discard schema-invalid replies and abort their inbox on authority loss", async t => {
  const env = environment(t), provider = env.provider();
  await provider.start(); const lease = await env.client.ready();
  const incoming = env.service.lane("services").watch()[Symbol.asyncIterator]();
  const waiting = env.client.request("play", { track: "a" });
  const delivery = (await incoming.next()).value!;
  let settled = false; void waiting.then(() => { settled = true; });
  await delivery.reply({ ...delivery.message.body, status: "ok", result: { accepted: false } });
  await flush(); assert.equal(settled, false);
  await delivery.reply({ ...delivery.message.body, status: "ok", result: { accepted: true } });
  assert.deepEqual((await waiting).result, { accepted: true });
  const next = env.client.request("play", { track: "b" });
  const rejected = assert.rejects(next, /cancelled|ended/);
  await incoming.next();
  await env.concord.cancelContract(lease.agreement.contract, "service:player");
  await rejected; assert.equal(env.consumer.subscriptionCount, 0);
  await incoming.return?.();
});

test("view writer closes locally before blocked cleanup and removes a late committed write", async t => {
  const entered = deferred(), resume = deferred();
  class SlowStore extends MemoryStateStore {
    override async put(...args: Parameters<MemoryStateStore["put"]>) { entered.resolve(); await resume.promise; return super.put(...args); }
  }
  const views = new SlowStore(), env = environment(t, undefined, views), provider = env.provider();
  t.after(() => resume.resolve());
  await provider.start(); const lease = await env.client.ready();
  const writer = new ServiceViewStoreWriter({ state: views, context: await provider.viewContext(lease.agreement.contract) });
  const rejected = assert.rejects(writer.put({ storeName: "player_views", key: "status.current" }, {}), /closed/);
  await entered.promise;
  const closing = writer.close();
  await assert.rejects(writer.put({ storeName: "player_views", key: "status.late" }, {}), /closed/);
  resume.resolve(); await rejected; await closing;
  assert.equal((await views.items()).length, 0);
});

test("exclusive provider bootstrap preserves an incumbent's live authority and does not advertise", async t => {
  const env = environment(t), incumbent = env.provider();
  await incumbent.start(); const lease = await env.client.ready();
  const replacement = env.provider({ endpoint: { endpoint: "service:player", sessionId: "replacement" }, exclusive: true });
  await assert.rejects(replacement.start(), /owns participation/);
  assert.equal(replacement.state.status, "failed");
  assert.equal(lease.agreement.valid, true);
  assert.deepEqual((await env.beacon.find(protocol.featureId)).map(candidate => candidate.advertisement.sessionId), ["provider"]);
});

test("a changed local scope ends existing view contexts instead of widening a live agreement", async t => {
  const env = environment(t); let prefix = "status.allowed.";
  const provider = env.provider({ scope: scope => ({ ...scope, allowedViews: { status: [prefix] } }) });
  await provider.start(); const lease = await env.client.ready();
  const context = await provider.viewContext(lease.agreement.contract);
  await context.authority.assertActive();
  prefix = "status.";
  await provider.reconcile();
  assert.equal(context.authority.signal.aborted, true);
  await assert.rejects(context.authority.assertActive());
  assert.equal(lease.agreement.valid, false);
});

test("backend startup is owned before availability and late completion cannot reopen a closed provider", async t => {
  const env = environment(t), entered = deferred(), resume = deferred(); let closes = 0;
  const provider = env.provider({ backend: { start: async () => { entered.resolve(); await resume.promise; }, close: () => { closes++; } } });
  const rejected = assert.rejects(provider.start(), /cancelled/);
  await entered.promise;
  assert.equal(provider.ready, false);
  assert.equal((await env.beacon.find(protocol.featureId)).length, 0);
  await provider.close(); await rejected;
  resume.resolve(); await flush();
  assert.equal(provider.state.status, "closed");
  assert.equal((await env.beacon.find(protocol.featureId)).length, 0);
  assert.ok(closes >= 2, "Late backend resources are closed after startup finishes");
});

test("exact-candidate service clients retain the supplied endpoint and session through recovery", async t => {
  const env = environment(t), provider = env.provider();
  const other = env.provider({ serviceId: "other", endpoint: { endpoint: "service:other", sessionId: "other" } });
  await provider.start(); await other.start();
  const descriptor = parseServiceDescriptor((await env.beacon.find(protocol.featureId)).find(candidate => candidate.advertisement.endpoint === "service:player")!, protocol)!;
  const exact = new ManagedServiceClient({ protocol, beacon: env.beacon, concord: env.concord, endpoint: env.consumer.lane("services"), descriptor });
  t.after(() => exact.close());
  const lease = await exact.ready(); assert.equal(lease.descriptor.endpoint, "service:player");
  await provider.close(); await flush();
  assert.equal(exact.state.status, "reconnecting");
  const remaining = await env.concord.contracts(protocol.useProfile, { state: "open" });
  assert.ok(remaining.every(contract => !contract.participants.includes("service:other")));
});

test("ending Beacon discovery cannot revoke the client's accepted Concord agreement", async t => {
  const env = environment(t), provider = env.provider();
  await provider.start(); const lease = await env.client.ready();
  await env.beacon.close(); await flush();
  assert.equal(env.client.state.status, "ready");
  assert.equal(lease.agreement.valid, true);
  assert.equal(await env.client.ready(), lease);
});

test("readiness deadline includes shared runtime startup and current-state watches finish on close", async () => {
  const clock = new TestClock(), entered = deferred(), resume = deferred();
  class SlowBeacon extends BeaconService { override async start() { entered.resolve(); await resume.promise; await super.start(); } }
  const beacon = new SlowBeacon(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })), { scheduler: clock });
  const client = new ManagedServiceClient({ protocol, beacon, concord, endpoint: { endpoint: "controller:main", sessionId: "consumer" } });
  const watch = client.watch()[Symbol.asyncIterator]();
  const initial = (await watch.next()).value!;
  assert.equal(initial.status, "connecting"); assert.equal(initial.version, 0); assert.ok(Object.isFrozen(initial));
  const rejected = assert.rejects(client.ready(100), /deadline/);
  await entered.promise; await clock.advance(100); await rejected;
  resume.resolve(); await client.start();
  const next = watch.next(); await client.close();
  assert.equal((await next).value!.status, "closed");
  assert.equal((await watch.next()).done, true);
  assert.equal((await client.waitFor(state => state.status === "closed")).status, "closed");
  await beacon.close(); await concord.close();
});

test("service declarations preserve prototype-like names and reject inherited operations", () => {
  const definition = { operation: "__proto__", direction: "consumer_to_service", intent: "command", exchangePattern: "one_way" };
  const special = validateServiceProtocol({ ...protocol, operations: JSON.parse('{"__proto__":{}}'), messages: Object.fromEntries([["__proto__", definition]]) } as ServiceProtocol);
  assert.deepEqual(Object.keys(special.operations), ["__proto__"]);
  assert.deepEqual(Object.keys(special.messages), ["__proto__"]);
  assert.throws(() => validateServiceProtocol({ ...protocol, messages: { inherited: { ...definition, operation: "toString" } } } as ServiceProtocol), /unknown operation/);
});

test("managed inbound service messages reject bad payloads and retain the accepted lease fence", async t => {
  const env = environment(t), provider = env.provider();
  await provider.start(); const lease = await env.client.ready();
  const messages = env.client.messages()[Symbol.asyncIterator]();
  const pending = messages.next(); await flush();
  const input = { recipient: "controller:main", recipientSessionId: "consumer", messageType: "serviceMessage",
    contract: { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation },
    subject: entitySubject("service", { serviceId: "player", namespace: protocol.namespace, name: "changed" }),
    body: { serviceNamespace: protocol.namespace, name: "changed", intent: "event", exchangePattern: "one_way", event: {} },
  };
  let received = false; void pending.then(() => { received = true; });
  await env.service.lane("services").publish(input);
  await env.service.lane("services").publish({ ...input, contract: { contractId: "another", generation: 1 }, body: { ...input.body, event: { track: "wrong" } } });
  await flush(); assert.equal(received, false);
  await provider.send(lease.agreement.contract, "changed", { track: "accepted" });
  const delivery = (await pending).value!;
  assert.deepEqual(delivery.body.event, { track: "accepted" });
  await assert.rejects(delivery.reply({ result: {} }), /request\/reply/);
  await env.concord.cancelContract(lease.agreement.contract, "service:player");
  assert.equal(delivery.signal.aborted, true);
  await assert.rejects(delivery.assertActive(), /ended/);
  assert.equal((await messages.next()).done, true);
  assert.equal(env.consumer.subscriptionCount, 0);
});

test("a failed shared startup publishes a structured failure and remains explicitly closable", async () => {
  class FailedBeacon extends BeaconService { override async start(): Promise<void> { throw new Error("discovery unavailable"); } }
  const beacon = new FailedBeacon(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const client = new ManagedServiceClient({ protocol, beacon, concord, endpoint: { endpoint: "controller:main", sessionId: "consumer" } });
  await assert.rejects(client.ready(), /discovery unavailable/);
  assert.equal(client.state.status, "failed"); assert.equal(client.state.failure?.code, "service_client_failed");
  await client.close(); assert.equal(client.state.status, "closed");
  await beacon.close(); await concord.close();
});
