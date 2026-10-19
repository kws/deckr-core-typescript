import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { endpointAddress } from "../src/endpoint.ts";
import { validateLaneMessage } from "../src/lane-validation.ts";
import { buildMessage, entitySubject, headersFor, messageIsExpired, subjectFor, validateDeckrMessage, validateHeaderHints, validateSubjectHint, type DeckrMessage } from "../src/lanes.ts";
import { EndpointSession, MemoryMessageTransport, type LaneMessageInput } from "../src/messaging.ts";
import { MemoryStateStore } from "../src/state.ts";
import { flush, TestClock } from "./runtime-support.ts";

const specRoot = process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr");
function fixture(relative: string): unknown { return JSON.parse(readFileSync(path.join(specRoot, "contract/v1/fixtures", relative), "utf8")); }
const pointer = { contractId: "request-agreement", generation: 1 };
function requestInput(): LaneMessageInput {
  return { recipient: "service:music", recipientSessionId: "provider", messageType: "serviceMessage", contract: pointer,
    subject: entitySubject("service", { serviceId: "music", namespace: "example.music", name: "play" }),
    body: { serviceNamespace: "example.music", name: "play", intent: "command", exchangePattern: "request_reply", params: {} },
  };
}
function wireMessage(): DeckrMessage {
  return buildMessage({ ...requestInput(), lane: "services", sender: "controller:main", senderSessionId: "consumer", createdAt: "2026-10-19T00:00:00Z" });
}
async function sendRaw(transport: MemoryMessageTransport, message: DeckrMessage, subject = subjectFor(message), headers = headersFor(message)): Promise<void> {
  await transport.publish(subject, new TextEncoder().encode(JSON.stringify(message)), { headers });
}

test("core lane validators read all upstream hardware message fixtures and service envelopes", () => {
  for (const expectation of ["valid", "invalid"] as const) {
    const directory = path.join(specRoot, `contract/v1/fixtures/${expectation}/coverage/deckr/hardware/messages`);
    for (const file of readdirSync(directory)) {
      const value = JSON.parse(readFileSync(path.join(directory, file), "utf8"));
      if (expectation === "valid") assert.doesNotThrow(() => validateLaneMessage(value), file);
      else assert.throws(() => validateLaneMessage(value), file);
    }
    const value = fixture(`${expectation}/coverage/deckr/services/message.v1.json`);
    if (expectation === "valid") assert.doesNotThrow(() => validateLaneMessage(value));
    else assert.throws(() => validateLaneMessage(value));
  }
  validateLaneMessage(fixture("valid/hardware/control-input.v1.json"));
  validateLaneMessage(fixture("valid/services/service-message.v1.json"));
  assert.throws(() => validateLaneMessage(fixture("invalid/hardware/control-input-missing-device-ref.v1.json")));
  assert.throws(() => validateLaneMessage(fixture("invalid/services/service-message-missing-namespace.v1.json")));
});

test("envelopes enforce timestamp, endpoint, broadcast session and header boundaries", () => {
  const message = wireMessage();
  assert.equal(endpointAddress("worker:other application"), "worker:other application");
  for (const bad of ["worker: padded", "Worker:one", "worker:one:two"]) assert.throws(() => endpointAddress(bad));
  for (const patch of [
    { createdAt: "yesterday" }, { createdAt: "2026-02-30T00:00:00Z" }, { ttlMs: 0.5 }, { sender: "worker:one" },
    { recipient: { targetType: "broadcast", scope: "controllers", endpointFamily: "controller" } },
    { unexpected: true }, { body: { invalid: Number.NaN } },
  ]) assert.throws(() => validateDeckrMessage({ ...message, ...patch }));
  assert.throws(() => validateSubjectHint(`${subjectFor(message)}.suffix`, message));
  assert.throws(() => validateHeaderHints({ "Deckr-In-Reply-To": "unexpected" }, message));
  assert.throws(() => validateHeaderHints({ "Deckr-Contract-Id": pointer.contractId }, message));
  assert.throws(() => validateHeaderHints({ get: () => ["a", "b"] }, message));
  assert.doesNotThrow(() => validateHeaderHints({ get: () => "" }, message));
  assert.throws(() => validateLaneMessage({ ...message, recipientSessionId: null, recipient: { targetType: "broadcast", scope: "controllers", endpointFamily: "controller" } }));
  assert.throws(() => validateLaneMessage({ ...message, sender: "hardware_manager:one" }));
  assert.throws(() => validateLaneMessage({ ...message, messageType: "madeUp" }));
});

test("effective expiry uses the earlier of explicit expiry and producer TTL", () => {
  const message = { ...wireMessage(), ttlMs: 1000, expiresAt: "2026-10-20T00:00:00Z" };
  assert.equal(messageIsExpired(message, new Date("2026-10-19T00:00:01Z")), true);
  assert.equal(messageIsExpired({ ...message, ttlMs: 100_000, expiresAt: "2026-10-19T00:00:00.500Z" }, new Date("2026-10-19T00:00:00.500Z")), true);
});

test("endpoint requests discard bad replies, then accept a correlated session-fenced reply", async (t) => {
  const transport = new MemoryMessageTransport();
  const client = new EndpointSession({ endpoint: "controller:main", sessionId: "consumer", transport });
  const provider = new EndpointSession({ endpoint: "service:music", sessionId: "provider", transport });
  t.after(async () => { await client.close(); await provider.close(); transport.close(); });
  const receiver = provider.lane("services").watch()[Symbol.asyncIterator]();
  const raw = transport.subscribe("deckr.msg.services.to.service.music");
  const rawIterator = raw.messages[Symbol.asyncIterator]();
  const pending = client.lane("services").request({ ...requestInput(), accept: reply => reply.body.status === "ok" });
  const delivery = (await receiver.next()).value!;
  const sent = (await rawIterator.next()).value!;
  assert.ok(sent.reply);
  assert.equal(delivery.message.sender, "controller:main");
  const response = buildMessage({ lane: "services", sender: "service:music", senderSessionId: "provider", recipient: "controller:main", recipientSessionId: "consumer",
    contract: pointer, messageType: "serviceMessage", subject: delivery.message.subject, inReplyTo: delivery.message.messageId,
    body: { ...delivery.message.body, status: "ok", result: { accepted: true } },
  });
  let settled = false; void pending.then(() => { settled = true; });
  for (const patch of [
    { messageId: delivery.message.messageId }, { inReplyTo: "another-request" }, { contract: { ...pointer, generation: 2 } },
    { recipientSessionId: "old-consumer" }, { senderSessionId: "old-provider" }, { sender: "service:impostor" },
    { expiresAt: "2000-01-01T00:00:00Z" }, { body: { ...response.body, status: "rejected" } }, { lane: "hardware_messages" },
  ]) await sendRaw(transport, { ...response, ...patch } as DeckrMessage, sent.reply);
  await transport.publish(sent.reply, new TextEncoder().encode("{invalid"));
  await flush(); assert.equal(settled, false);
  await delivery.reply(response.body);
  const reply = await pending;
  assert.equal(reply.inReplyTo, delivery.message.messageId);
  assert.equal(reply.recipientSessionId, "consumer");
  assert.deepEqual(reply.contract, pointer);
  assert.equal(client.subscriptionCount, 0, "The reply inbox is released");
  raw.close(); await receiver.return?.();
  assert.equal(transport.subscriptionCount, 0);
});

test("request timeout, cancellation and endpoint closure release idle inboxes", async (t) => {
  for (const reason of ["timeout", "cancel", "close"] as const) await t.test(reason, async () => {
    const transport = new MemoryMessageTransport(); const clock = new TestClock();
    const session = new EndpointSession({ endpoint: "controller:main", sessionId: "consumer", transport, scheduler: clock });
    const abort = new AbortController();
    const pending = session.lane("services").request({ ...requestInput(), signal: abort.signal, timeout: 100 });
    const rejected = assert.rejects(pending, /deadline|cancelled/);
    assert.equal(session.subscriptionCount, 1);
    if (reason === "timeout") await clock.advance(100);
    else if (reason === "cancel") abort.abort();
    else await session.close();
    await rejected;
    assert.equal(transport.subscriptionCount, 0);
    await session.close(); transport.close();
  });
});

test("watchers share lane subscriptions and filter sessions before delivery", async () => {
  const transport = new MemoryMessageTransport();
  const session = new EndpointSession({ endpoint: "service:music", sessionId: "provider", transport });
  const lane = session.lane("services");
  const first = lane.watch()[Symbol.asyncIterator](), second = lane.watch()[Symbol.asyncIterator]();
  assert.equal(session.subscriptionCount, 1);
  assert.equal(session.state.resources.subscriptions, 1);
  assert.equal(session.state.resources.lanes, 1);
  const health = session.watch()[Symbol.asyncIterator]();
  assert.equal((await health.next()).value!.status, "ready");
  const pending = first.next(); let delivered = false; void pending.then(() => { delivered = true; });
  const message = { ...wireMessage(), createdAt: new Date().toISOString() };
  await sendRaw(transport, { ...message, recipientSessionId: "old" });
  await flush(); assert.equal(delivered, false);
  await sendRaw(transport, message);
  assert.equal((await pending).value!.message.messageId, message.messageId);
  const secondMessage = (await second.next()).value!.message;
  assert.equal(secondMessage.messageId, message.messageId);
  assert.throws(() => { secondMessage.body.changed = true; }, TypeError);
  await first.return?.(); assert.equal(session.subscriptionCount, 1);
  await second.return?.(); assert.equal(session.subscriptionCount, 0);
  assert.equal(session.state.resources.subscriptions, 0);
  const closing = session.close(); assert.equal(session.close(), closing); await closing;
  assert.equal((await health.next()).value!.status, "closed"); assert.equal((await health.next()).done, true);
  transport.close();
});

test("closing an endpoint session leaves separately owned Beacon and Concord resources alive", async () => {
  const transport = new MemoryMessageTransport();
  const session = new EndpointSession({ endpoint: "service:music", sessionId: "provider", transport });
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const advertisement = await beacon.ensureAdvertisement({ featureId: "music", endpoint: session.endpoint, sessionId: session.sessionId });
  await advertisement.publish();
  const agreement = await concord.ensureAgreement({ participants: [session.endpoint], localParticipant: session.endpoint, localSessionId: session.sessionId });
  await session.close();
  assert.equal(agreement.valid, true);
  assert.equal((await beacon.find("music")).length, 1);
  await concord.close(); await beacon.close(); transport.close();
});

test("publication failures release request inboxes without retrying the command", async () => {
  class FailedTransport extends MemoryMessageTransport {
    publications = 0;
    override async publish(): Promise<void> { this.publications++; throw new Error("publish failed"); }
  }
  const transport = new FailedTransport();
  const session = new EndpointSession({ endpoint: "controller:main", transport });
  await assert.rejects(session.lane("services").request(requestInput()), /publish failed/);
  assert.equal(transport.subscriptionCount, 0);
  assert.equal(transport.publications, 1);
  await session.close(); transport.close();
});

test("hardware broadcasts keep their canonical envelope and invalid sends never reach transport", async () => {
  const transport = new MemoryMessageTransport();
  const manager = new EndpointSession({ endpoint: "hardware_manager:deck", transport });
  const controller = new EndpointSession({ endpoint: "controller:main", transport });
  const listener = controller.lane("hardware_messages").watch()[Symbol.asyncIterator]();
  const hardware = fixture("valid/hardware/control-input.v1.json") as DeckrMessage;
  await manager.lane("hardware_messages").publish({ recipient: { targetType: "broadcast", scope: "controllers", endpointFamily: "controller", hopLimit: 1 },
    contract: pointer, messageType: "controlInput", subject: hardware.subject, body: hardware.body,
  });
  const received = (await listener.next()).value!.message;
  assert.equal(received.sender, manager.endpoint);
  assert.deepEqual(received.recipient, { targetType: "broadcast", scope: "controllers", endpointFamily: "controller", hopLimit: 1 });
  await assert.rejects(manager.lane("hardware_messages").publish({ recipient: "controller:main", contract: pointer, messageType: "controlInput", subject: hardware.subject, body: {} }), /deviceRef/);
  await listener.return?.();
  await manager.close(); await controller.close(); transport.close();
});
