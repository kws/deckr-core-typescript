import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { PNG } from "pngjs";
import jpeg from "jpeg-js";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { ManagedHardwareClient, ManagedHardwareManager, HARDWARE_FEATURE_ID, HARDWARE_CLAIM_PROFILE, hardwareCapabilitySubject,
  buttonMomentaryValueSchema, encoderRelativeValueSchema, touchGestureValueSchema, rasterBitmapCommandSchema,
  parseHardwareDescriptor, validateHardwareAdvertisementPayload, validateHardwareCapacity, validateHardwareClaimTerms,
  validateHardwareDeviceDescriptor, validateHardwareCapability, validateHardwareCapabilityPayload,
  type HardwareAdvertisementDevice, type HardwareOperationContext, type ManagedHardwareManagerOptions } from "../src/hardware.ts";
import { buildMessage, entitySubject } from "../src/lanes.ts";
import { EndpointSession, MemoryMessageTransport } from "../src/messaging.ts";
import { PayloadSchemas } from "../src/schema.ts";
import { StateCell } from "../src/runtime.ts";
import { MemoryStateStore } from "../src/state.ts";
import type { JsonObject } from "../src/json.ts";
import { flush, TestClock } from "./runtime-support.ts";

const specRoot = process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr");
function fixture(relative: string): any { return JSON.parse(readFileSync(path.join(specRoot, "contract/v1/fixtures", relative), "utf8")); }
function devices(): Record<string, HardwareAdvertisementDevice> { return fixture("valid/beacon/hardware-advertisement.v1.json").payload.devices; }
function command(): JsonObject { return { deviceRef: { deviceId: "deck-1", managerId: "mirabox-main", fingerprint: "fingerprint:deck-1" }, controlId: "key.0.0", capabilityId: "screen", commandType: "clear", params: {} }; }

test("hardware subjects and explicit capability schemas are consumable across languages", () => {
  assert.deepEqual(hardwareCapabilitySubject(command()), entitySubject("hardware_capability", {
    managerId: "mirabox-main", deviceId: "deck-1", controlId: "key.0.0", capabilityId: "screen",
  }));
  const schemas = new PayloadSchemas();
  schemas.validate(rasterBitmapCommandSchema({ width: 96, height: 64 }).schema, { image: "aA==", encoding: "png", width: 96, height: 64 });
  assert.throws(() => schemas.validate(rasterBitmapCommandSchema({ width: 96 }).schema, { image: "aA==", encoding: "png", width: 64 }));
  for (const [schema, valid, invalid] of [
    [buttonMomentaryValueSchema(), { eventType: "down" }, { eventType: "press" }],
    [encoderRelativeValueSchema(), { delta: -2 }, { delta: 0 }],
    [touchGestureValueSchema(), { eventType: "swipe", direction: "left" }, { eventType: "swipe" }],
    [rasterBitmapCommandSchema(), {}, { encoding: "png" }],
  ] as const) {
    schemas.validate(schema.schema, valid);
    assert.throws(() => schemas.validate(schema.schema, invalid));
  }
});

test("hardware validators consume the upstream profile and descriptor coverage fixtures", () => {
  const validators = { "claim-terms": validateHardwareClaimTerms, "advertisement-payload": validateHardwareAdvertisementPayload, "device-descriptor": validateHardwareDeviceDescriptor, "capability-descriptor": validateHardwareCapability, "profile-capacity": validateHardwareCapacity };
  for (const [name, validate] of Object.entries(validators)) {
    assert.doesNotThrow(() => validate(fixture(`valid/coverage/deckr/hardware/${name}.v1.json`)), name);
    assert.throws(() => validate(fixture(`invalid/coverage/deckr/hardware/${name}.v1.json`)), name);
  }
  const advertisement = fixture("valid/beacon/hardware-advertisement.v1.json");
  assert.ok(parseHardwareDescriptor({ key: "fixture", revision: 1, observedAt: advertisement.createdAt, advertisement }));
  advertisement.payload.sessionId = "different";
  assert.equal(parseHardwareDescriptor({ key: "fixture", revision: 1, observedAt: advertisement.createdAt, advertisement }), null);
});

test("hardware semantic validation checks relationships, inventory scope, and capacity", () => {
  const descriptor = () => fixture("valid/coverage/deckr/hardware/device-descriptor.v1.json");
  const mutations: ((value: any) => void)[] = [
    value => value.controls.push(structuredClone(value.controls[0])),
    value => value.controls[0].outputCapabilities.push(structuredClone(value.controls[0].inputCapabilities[0])),
    value => value.controls[0].parentControlId = "missing",
    value => value.controls[0].relatedControlIds = [value.controls[0].controlId],
    value => value.defaultStatusIndicator = { controlId: "key.0.0", capabilityId: "button.press" },
    value => value.controls[0].outputCapabilities[0].projection = { owner: "adapter", source: { controlId: "key.0.0", capabilityId: "screen" } },
    value => value.sources = [{ sourceId: "usb", type: "usb", connectionId: "missing" }],
    value => value.controls[0].outputCapabilities[0].constraints = [{ type: "range", subject: "width", minimum: 3, maximum: 2 }],
    value => value.controls[0].inputCapabilities[0].family = "dev.deckr.unknown",
  ];
  for (const mutate of mutations) { const value = descriptor(); mutate(value); assert.throws(() => validateHardwareDeviceDescriptor(value)); }
  const ad = fixture("valid/beacon/hardware-advertisement.v1.json").payload;
  ad.devices["deck-1"].deviceRef.managerId = "wrong";
  assert.throws(() => validateHardwareAdvertisementPayload(ad));
  assert.throws(() => validateHardwareCapacity({ totalInstances: 2, claimedInstances: 1, availableInstances: 2 }));
  const terms = fixture("valid/coverage/deckr/hardware/claim-terms.v1.json"); terms.devices.push(terms.devices[0]);
  assert.throws(() => validateHardwareClaimTerms(terms));
  assert.throws(() => validateHardwareDeviceDescriptor(JSON.parse('{"deviceId":"d","fingerprint":"f","displayName":"D","__proto__":{}}')), /unknown field/);
});

test("extension capability payloads use declared dialects and references without coercion", () => {
  const schemas = new PayloadSchemas({ "https://example.test/value": { type: "integer", minimum: 2 } });
  const capability = validateHardwareCapability({ capabilityId: "custom", family: "org.example.knob", type: "input", direction: "input", access: ["emits"], eventTypes: ["change"], valueSchema: { schema: { $ref: "https://example.test/value" } } }, schemas);
  validateHardwareCapabilityPayload(capability, "event", "change", 2, schemas);
  assert.throws(() => validateHardwareCapabilityPayload(capability, "event", "change", "2", schemas));
  assert.throws(() => validateHardwareCapabilityPayload(capability, "event", "change", 1, schemas));
  assert.throws(() => validateHardwareCapability(capability), /unresolved/);
  schemas.validate({ $schema: "http://json-schema.org/draft-07/schema#", type: "array", items: [{ type: "integer" }], additionalItems: false }, [2]);
  assert.throws(() => schemas.validate({ $schema: "urn:unknown", type: "object" }, {}), /dialect/);
  assert.throws(() => schemas.compile({ $async: true, type: "object" }), /Asynchronous/);
});

test("core values select the semantic family and command, including actual raster bytes", () => {
  const raster = validateHardwareCapability({ capabilityId: "anything", family: "dev.deckr.output.raster", type: "bitmap", direction: "output", access: ["settable"], commandTypes: ["set_frame", "clear"], constraints: [{ type: "fixed", subject: "width", value: 2, unit: "pixel" }, { type: "fixed", subject: "height", value: 1, unit: "pixel" }] });
  const pixels = { width: 2, height: 1, data: Buffer.alloc(8, 255) };
  const image = new PNG({ width: 2, height: 1 }); image.data = pixels.data;
  const png = PNG.sync.write(image).toString("base64"), jpg = jpeg.encode(pixels).data.toString("base64");
  validateHardwareCapabilityPayload(raster, "command", "set_frame", { image: png, encoding: "png", width: 2 });
  validateHardwareCapabilityPayload(raster, "command", "set_frame", { image: jpg, encoding: "jpeg" });
  validateHardwareCapabilityPayload(raster, "command", "clear", {});
  for (const params of [{ image: png, encoding: "jpeg" }, { image: jpg, encoding: "png" }, { image: png, encoding: "png", height: 2 }, { image: "aW52YWxpZA==", encoding: "png" }, {}] as JsonObject[]) assert.throws(() => validateHardwareCapabilityPayload(raster, "command", "set_frame", params));
  assert.throws(() => validateHardwareCapabilityPayload(raster, "command", "clear", { image: png, encoding: "png" }));
  const touch = validateHardwareCapability({ capabilityId: "touch", family: "dev.deckr.input.touch", type: "gesture", direction: "input", access: ["emits"], eventTypes: ["tap", "swipe"] });
  assert.throws(() => validateHardwareCapabilityPayload(touch, "event", "swipe", { eventType: "swipe" }));
  validateHardwareCapabilityPayload(touch, "event", "swipe", { eventType: "swipe", direction: "left" });
});

function runtime(t: TestContext, options: Partial<Pick<ManagedHardwareManagerOptions, "acceptClaim" | "handle">> = {}) {
  const beaconStore = new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY });
  const beacon = new BeaconService(new BeaconDiscovery(beaconStore));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const transport = new MemoryMessageTransport(), sessions: EndpointSession[] = [], clients: ManagedHardwareClient[] = [];
  const session = new EndpointSession({ endpoint: "hardware_manager:mirabox-main", sessionId: "manager", transport }); sessions.push(session);
  const manager = new ManagedHardwareManager({ endpoint: session.lane("hardware_messages"), beacon, concord, devices: devices(), ...options });
  const client = (id: string) => {
    const session = new EndpointSession({ endpoint: `controller:${id}`, sessionId: id, transport }); sessions.push(session);
    const client = new ManagedHardwareClient({ endpoint: session.lane("hardware_messages"), beacon, concord, managerId: "mirabox-main", devices: [{ deviceId: "deck-1", instanceCount: 1 }] }); clients.push(client);
    return client;
  };
  t.after(async () => { await Promise.all(clients.map(client => client.close())); await manager.close(); await Promise.all(sessions.map(session => session.close())); await beacon.close(); await concord.close(); transport.close(); });
  return { beacon, beaconStore, concord, transport, manager, session, client };
}

test("managed hardware dispatch validates exact claim, sessions, and descriptor before driver work", async t => {
  let calls = 0;
  const { manager, client, concord } = runtime(t, { handle: async (message, context) => { await context.assertActive(); calls++; return { messageType: "commandReply", body: { deviceRef: message.body.deviceRef!, controlId: message.body.controlId!, capabilityId: message.body.capabilityId!, commandType: message.body.commandType!, result: { done: true } } }; } });
  await manager.start(); const controller = client("main"), agreement = await controller.ready();
  const response = await controller.request("controlCommand", command());
  assert.equal(response.messageType, "commandReply"); assert.equal(calls, 1);
  assert.deepEqual(response.subject, hardwareCapabilitySubject(command()));
  await assert.rejects(controller.send("controlCommand", { ...command(), capabilityId: "missing" }));
  const message = buildMessage({ lane: "hardware_messages", sender: "controller:main", senderSessionId: "different", recipient: "hardware_manager:mirabox-main", recipientSessionId: "manager", messageType: "controlCommand", subject: entitySubject("device"), contract: { contractId: agreement.contract.contractId, generation: 1 }, body: command() });
  await assert.rejects(manager.authorize(message), /session/);
  await concord.cancelContract(agreement.contract, "controller:main");
  await assert.rejects(manager.authorize({ ...message, senderSessionId: "main" }));
  assert.equal(calls, 1);
});

test("concurrent hardware claims cannot exceed valid capacity", async t => {
  const { manager, client, concord, beacon } = runtime(t);
  await manager.start(); const first = client("first"), second = client("second");
  await Promise.all([first.start(), second.start()]);
  for (let i = 0; i < 40 && !first.claim && !second.claim; i++) await flush();
  const contracts = await concord.contracts(HARDWARE_CLAIM_PROFILE);
  assert.equal(Number(Boolean(first.claim)) + Number(Boolean(second.claim)), 1, JSON.stringify({ first: first.state, second: second.state, contracts: await Promise.all(contracts.map(contract => concord.validate(contract))) }));
  assert.equal((await Promise.all(contracts.map(contract => concord.validate(contract)))).filter(value => value.valid).length, 1);
  assert.equal(beacon.source.subscriberCount, 2, "Both clients share one feature selection and source subscription");
  const winner = first.claim ? first : second, loser = first.claim ? second : first;
  await winner.close();
  assert.ok(await loser.ready());
  await loser.close();
  assert.equal(beacon.source.subscriberCount, 1, "The last feature subscriber releases the shared selection");
});

test("Beacon withdrawal leaves hardware authority intact and descriptor change revokes before republishing", async t => {
  const { manager, client, beacon, beaconStore, concord } = runtime(t);
  await manager.start(); const controller = client("main"), agreement = await controller.ready();
  const request = buildMessage({ lane: "hardware_messages", sender: "controller:main", senderSessionId: "main", recipient: "hardware_manager:mirabox-main", recipientSessionId: "manager", messageType: "controlCommand", subject: entitySubject("device"), contract: { contractId: agreement.contract.contractId, generation: 1 }, body: command() });
  const context = await manager.authorize(request);
  // Removing discovery is independent of the held claim.
  const advertisements = await beacon.find(HARDWARE_FEATURE_ID);
  for (const ad of advertisements) await beaconStore.delete(ad.key, { revision: ad.revision });
  await flush(); assert.equal(agreement.valid, true); await context.assertActive();
  const changed = devices(); changed["deck-1"]!.descriptor.displayName = "Changed descriptor";
  const changing = manager.setDevices(changed);
  assert.equal(context.signal.aborted, true);
  await assert.rejects(context.assertActive()); await changing;
  assert.equal((await concord.validateExact(agreement.contract)).status, "cancelled");
  const candidate = parseHardwareDescriptor((await beacon.find(HARDWARE_FEATURE_ID))[0]!)!;
  assert.equal(candidate.devices["deck-1"]!.descriptor.displayName, "Changed descriptor");
});

test("device disconnection revokes a driver context while its handler is blocked", async t => {
  let entered!: () => void, resume!: () => void, context!: HardwareOperationContext;
  const gate = new Promise<void>(resolve => { resume = resolve; }), started = new Promise<void>(resolve => { entered = resolve; });
  const { manager, client } = runtime(t, { handle: async (_message, active) => { context = active; entered(); await gate; await active.assertActive(); } });
  t.after(() => resume());
  await manager.start(); const controller = client("main"); await controller.ready();
  await controller.send("controlCommand", command()); await started;
  await manager.setDevices({});
  assert.equal(context.signal.aborted, true); await assert.rejects(context.assertActive()); resume();
});

test("hardware bootstrap preserves another live manager lifetime and blocks advertisement", async t => {
  const { manager, client, beacon, concord, transport } = runtime(t);
  await manager.start(); const controller = client("main"), agreement = await controller.ready();
  const session = new EndpointSession({ endpoint: "hardware_manager:mirabox-main", sessionId: "contender", transport });
  const contender = new ManagedHardwareManager({ endpoint: session.lane("hardware_messages"), beacon, concord, devices: devices() });
  t.after(async () => { await contender.close(); await session.close(); });
  await assert.rejects(contender.start(), /Another hardware-manager/);
  assert.equal(contender.state.status, "failed"); assert.equal(agreement.valid, true);
  assert.equal((await beacon.find(HARDWARE_FEATURE_ID)).length, 1);
});

test("pending proposals for a previous session do not block a replacement manager", async t => {
  const { manager, concord, beacon } = runtime(t);
  const pending = await concord.createContract(["controller:main", "hardware_manager:mirabox-main"], {
    profile: HARDWARE_CLAIM_PROFILE, createdBy: "controller:main",
    participantSessions: { "hardware_manager:mirabox-main": "previous-session" },
  });
  await manager.start();
  assert.equal(manager.ready, true);
  assert.equal((await beacon.find(HARDWARE_FEATURE_ID)).length, 1);
  assert.equal((await concord.validateExact(pending)).status, "not_yet_fulfilled");
  assert.equal((await concord.validateExact(pending)).tokens["hardware_manager:mirabox-main"], undefined);
});

test("device replacement waits for exact cleanup confirmation when remote cancellation fails", async t => {
  const { manager, client, concord, beacon } = runtime(t);
  await manager.start(); const controller = client("main"), agreement = await controller.ready();
  const previous = (await beacon.find(HARDWARE_FEATURE_ID))[0]!;
  const cancel = concord.cancelContract.bind(concord), withdraw = concord.withdrawToken.bind(concord);
  concord.cancelContract = async () => { throw new Error("contract store down"); };
  concord.withdrawToken = async () => { throw new Error("token store down"); };
  t.after(() => { concord.cancelContract = cancel; concord.withdrawToken = withdraw; });
  const changed = devices(); changed["deck-1"]!.descriptor.displayName = "Replacement";
  await assert.rejects(manager.setDevices(changed), /store down/);
  assert.equal((await concord.validateExact(agreement.contract)).valid, true, "Remote authority can remain valid while local cleanup is unavailable");
  assert.equal((await beacon.find(HARDWARE_FEATURE_ID))[0]!.key, previous.key, "A new candidate must wait for confirmed cleanup");
  concord.cancelContract = cancel; concord.withdrawToken = withdraw;
  await manager.setDevices(changed);
  assert.notEqual((await beacon.find(HARDWARE_FEATURE_ID))[0]!.key, previous.key);
  assert.equal((await concord.validateExact(agreement.contract)).status, "cancelled");
});

test("device replacement retires pending claims without waiting for domain policy", async t => {
  let entered!: () => void, resume!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; }), gate = new Promise<void>(resolve => { resume = resolve; });
  const { manager, client, concord } = runtime(t, { acceptClaim: async () => { entered(); await gate; return true; } });
  t.after(() => resume());
  await manager.start(); const controller = client("main"); await controller.start(); await started;
  const before = (await concord.contracts(HARDWARE_CLAIM_PROFILE))[0]!;
  const changed = devices(); changed["deck-1"]!.descriptor.displayName = "Replacement";
  await manager.setDevices(changed);
  assert.equal((await concord.validateExact(before)).status, "cancelled");
  resume(); await flush();
  assert.equal((await concord.validateExact(before)).status, "cancelled");
});

test("versioned state watches retain terminal state and readiness deadlines include startup", async t => {
  const cell = new StateCell({ status: "starting" }), iterator = cell.watch()[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.version, 0);
  cell.set({ status: "closed" }); cell.close();
  assert.deepEqual((await iterator.next()).value, { status: "closed", version: 1 });
  assert.equal((await iterator.next()).done, true);
  const clock = new TestClock(), transport = new MemoryMessageTransport();
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  beacon.start = async () => { await new Promise(() => {}); };
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })), { scheduler: clock });
  const session = new EndpointSession({ endpoint: "controller:main", transport });
  const client = new ManagedHardwareClient({ endpoint: session.lane("hardware_messages"), beacon, concord, managerId: "m", devices: [{ deviceId: "d", instanceCount: 1 }] });
  t.after(async () => { await client.close(); await concord.close(); await session.close(); transport.close(); });
  const rejected = assert.rejects(client.ready(20), /deadline/);
  await clock.advance(20); await rejected;
});

test("hardware startup failures remain observable and explicitly closable", async () => {
  const transport = new MemoryMessageTransport();
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  beacon.start = async () => { throw new Error("Discovery offline"); };
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const controller = new EndpointSession({ endpoint: "controller:failed", transport });
  const managerEndpoint = new EndpointSession({ endpoint: "hardware_manager:failed", transport });
  const client = new ManagedHardwareClient({ beacon, concord, endpoint: controller.lane("hardware_messages"), managerId: "failed", devices: [] });
  const manager = new ManagedHardwareManager({ beacon, concord, endpoint: managerEndpoint.lane("hardware_messages"), devices: {} });
  await assert.rejects(client.start(), /Discovery offline/); await assert.rejects(manager.start(), /Discovery offline/);
  assert.equal(client.state.status, "failed"); assert.equal(client.state.failure!.code, "hardware_start_failed");
  assert.equal(manager.state.status, "failed"); assert.equal(manager.state.failure!.code, "hardware_bootstrap_failed");
  const closing = manager.close(); assert.equal(manager.close(), closing); await closing;
  await client.close(); assert.equal(client.state.status, "closed"); assert.equal(manager.state.status, "closed");
  await controller.close(); await managerEndpoint.close(); await beacon.close(); await concord.close(); transport.close();
});
