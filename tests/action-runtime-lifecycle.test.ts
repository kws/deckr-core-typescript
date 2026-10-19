import assert from "node:assert/strict";
import test from "node:test";
import { ManagedActionRuntimeClient, ManagedActionRuntimeProvider, validateActionRuntimeReferences, actionRuntimeProtocol,
  type ActionInstanceMetadata, type BindingMetadata, type ActionRuntimeContext, type ActionRuntimeReferenceState } from "../src/actions.ts";
import { BeaconService, BeaconDiscovery, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordService, ConcordCoordinator, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { EndpointSession, MemoryMessageTransport } from "../src/messaging.ts";
import { MemoryStateStore, PERSISTENT_STATE_STORE_POLICY } from "../src/state.ts";
import { buildMessage, entitySubject } from "../src/lanes.ts";
import { validateHardwareDeviceDescriptor } from "../src/hardware-descriptors.ts";
import type { JsonObject } from "../src/json.ts";
import { flush, RecoverableStore, TestClock } from "./runtime-support.ts";

const metadata: ActionInstanceMetadata = { providerInstanceId: "clock-main", providerId: "dev.deckr.clock", actionId: "dev.deckr.clock.time", actionInstanceId: "clock-one", configId: "config", contextId: "context" };
const availability = { entries: [{ actionId: metadata.actionId, status: "available" as const, descriptor: { actionId: metadata.actionId, providerId: metadata.providerId } }] };

test("availability retries the latest value after failures and source recovery without replacing its agreement", async t => {
  const clock = new TestClock(), views = new RecoverableStore(PERSISTENT_STATE_STORE_POLICY), transport = new MemoryMessageTransport();
  views.failWrites = true;
  const runtime = new EndpointSession({ endpoint: "service:action-runtime.clock-main", sessionId: "runtime", transport });
  const controller = new EndpointSession({ endpoint: "controller:main", sessionId: "controller", transport });
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY }), { scheduler: clock }));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })), { scheduler: clock });
  let failed!: () => void;
  const failure = new Promise<void>(resolve => { failed = resolve; });
  const shared = { beacon, concord, stateFor: () => views, resolveReferences: () => ({ actionInstance: metadata }) };
  const provider = new ManagedActionRuntimeProvider({ ...shared, endpoint: runtime.lane("services"), providerInstanceId: metadata.providerInstanceId, providerId: metadata.providerId,
    availability, handle: () => {}, onError: () => failed() });
  const client = new ManagedActionRuntimeClient({ ...shared, endpoint: controller.lane("services") });
  t.after(async () => { await client.close(); await provider.close(); await beacon.close(); await concord.close(); await runtime.close(); await controller.close(); transport.close(); });
  await provider.start(); const lease = await client.ready(); await failure;
  await assert.rejects(provider.setAvailability({ entries: [{ ...availability.entries[0]!, status: "probing" }] }));
  views.failWrites = false;
  await clock.advance(2000); await flush();
  let rows = await views.items();
  assert.equal(rows.length, 1, "A write failure recovers even without a source-currentness transition");
  assert.equal((rows[0]!.value.entries as { status: string }[])[0]!.status, "probing");
  assert.equal(rows[0]!.value.contractId, lease.agreement.contract.contractId);
  const revision = rows[0]!.revision;
  views.disconnect(); await flush();
  await views.delete(rows[0]!.key); await views.recover(); await flush();
  await clock.advance(1000); await flush();
  rows = await views.items(); assert.equal(rows.length, 1); assert.ok(rows[0]!.revision > revision);
  assert.equal((await lease.agreement.refresh()).valid, true);
  assert.equal(rows[0]!.value.contractId, lease.agreement.contract.contractId);
  views.failWrites = true;
  await assert.rejects(provider.setAvailability(availability));
  await provider.setBackendStatus("unavailable");
  views.failWrites = false; await clock.advance(30_000);
  assert.equal(lease.agreement.valid, false);
  assert.ok((await views.items()).every(entry => entry.revision === rows[0]!.revision), "A revoked agreement cannot publish a retry");
});

test("managed Action Runtime publishes fenced availability before lifecycle dispatch and retains message authority", async t => {
  const transport = new MemoryMessageTransport(), views = new MemoryStateStore();
  const controller = new EndpointSession({ endpoint: "controller:main", sessionId: "controller-session", transport });
  const runtime = new EndpointSession({ endpoint: "service:action-runtime.clock-main", sessionId: "runtime-session", transport });
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const shared = { beacon, concord, stateFor: () => views, resolveReferences: () => ({ actionInstance: metadata }) };
  let releaseResolution!: () => void, enteredResolution!: () => void, blockResolution = false;
  const entered = new Promise<void>(resolve => { enteredResolution = resolve; });
  const released = new Promise<void>(resolve => { releaseResolution = resolve; });
  let lastContext: ActionRuntimeContext | undefined, calls = 0;
  const provider = new ManagedActionRuntimeProvider({ ...shared, endpoint: runtime.lane("services"), providerInstanceId: metadata.providerInstanceId, providerId: metadata.providerId, availability,
    handle: async context => { calls++; lastContext = context; await context.send("action_lifecycle_rejected", { targetKind: "action_instance", actionInstance: metadata as unknown as JsonObject, reason: "resource_unavailable" }); },
  });
  const client = new ManagedActionRuntimeClient({ ...shared, endpoint: controller.lane("services"), providerInstanceId: metadata.providerInstanceId,
    resolveReferences: async () => { if (blockResolution) { enteredResolution(); await released; } return { actionInstance: metadata }; },
  });
  t.after(async () => { await client.close(); await provider.close(); await beacon.close(); await concord.close(); await controller.close(); await runtime.close(); transport.close(); });
  await provider.start(); const lease = await client.ready();
  const abort = new AbortController(), watching = client.watchAvailability({ signal: abort.signal })[Symbol.asyncIterator]();
  let current = (await watching.next()).value!;
  while (!current.availability) current = (await watching.next()).value!;
  assert.equal(current.availability!.entries[0]!.actionId, metadata.actionId);
  assert.equal(current.entry!.contract.contractId, lease.agreement.contract.contractId);
  assert.equal(current.entry!.consumerSessionId, "controller-session");
  assert.equal(calls, 0, "Availability is published without requiring a lifecycle message");
  await provider.setAvailability({ entries: [{ ...availability.entries[0]!, status: "probing" }] });
  assert.equal((await watching.next()).value!.availability!.entries[0]!.status, "probing");
  const inbound = client.messages()[Symbol.asyncIterator](), received = inbound.next(); await flush();
  await client.send("action_instance_created", { metadata: metadata as unknown as JsonObject });
  const rejection = (await received).value!;
  assert.equal(rejection.name, "action_lifecycle_rejected");
  assert.equal(rejection.payload.reason, "resource_unavailable");
  assert.equal(calls, 1);
  blockResolution = true;
  const cancelled = assert.rejects(client.send("action_instance_destroyed", { metadata: metadata as unknown as JsonObject }), /cancelled|ended|terminated/);
  await entered;
  await provider.setBackendStatus("unavailable");
  await cancelled;
  releaseResolution(); await flush(); assert.equal(calls, 1, "A blocked send is cancelled without publishing under a successor");
  assert.equal(lastContext!.signal.aborted, true);
  await assert.rejects(lastContext!.send("close_page", {}));
  await assert.rejects(rejection.assertActive());
  abort.abort(); await watching.return?.(); await inbound.return?.();
  await provider.close(); assert.equal((await views.items()).length, 0);
});

function routedMessage(name: string, payload: JsonObject, identifiers: Record<string, string> = {}) {
  const definition = actionRuntimeProtocol().messages[name]!;
  const outbound = definition.direction === "service_to_consumer";
  return buildMessage({ lane: "services", messageType: "serviceMessage", sender: outbound ? "service:action-runtime.clock-main" : "controller:main", senderSessionId: outbound ? "runtime-session" : "controller-session",
    recipient: outbound ? "controller:main" : "service:action-runtime.clock-main", recipientSessionId: outbound ? "controller-session" : "runtime-session", contract: { contractId: "agreement", generation: 1 },
    subject: entitySubject("service", { ...identifiers, serviceId: "action-runtime.clock-main", namespace: "dev.deckr.action_runtime.provider", name }),
    body: { serviceNamespace: "dev.deckr.action_runtime.provider", name, intent: definition.intent, exchangePattern: "one_way", [definition.intent === "event" ? "event" : "params"]: payload },
  });
}

test("Action Runtime routing checks current generation, negotiated capability and requirement scope", () => {
  const deviceRef = { managerId: "manager", deviceId: "deck", fingerprint: "fingerprint" };
  const capability = { deviceRef, controlId: "button", capabilityId: "activation" };
  const descriptor = validateHardwareDeviceDescriptor({ deviceId: "deck", fingerprint: "fingerprint", displayName: "Deck", controls: [{ controlId: "button", kind: "button", inputCapabilities: [{ capabilityId: "activation", family: "dev.deckr.input.button", type: "activation", direction: "input", access: ["emits"], eventTypes: ["press"] }] }] });
  const binding: BindingMetadata = { ...metadata, bindingId: "binding", deviceRef, controlRef: { deviceRef, controlId: "button" }, outputGeneration: 4,
    matchedCapabilities: [{ requirementName: "button", capability, family: "dev.deckr.input.button", type: "activation", direction: "input", eventTypes: ["press"] }],
  };
  const state: ActionRuntimeReferenceState = { binding, actionInstance: metadata, devices: [{ managerId: "manager", descriptor }],
    action: { actionId: metadata.actionId, requirements: [{ name: "button", preferences: [{ family: "dev.deckr.input.button", eventTypes: ["press"] }], eventTypes: ["press"] }] },
  };
  const input = routedMessage("capability_input", { binding: binding as unknown as JsonObject, event: { capability, eventType: "press", occurredAt: new Date().toISOString(), value: { eventType: "press" } } });
  assert.doesNotThrow(() => validateActionRuntimeReferences(input, state));
  assert.throws(() => validateActionRuntimeReferences(input, { ...state, devices: [] }), /negotiated/);
  assert.throws(() => validateActionRuntimeReferences(input, { ...state, binding: { ...binding, contextId: "another" } }), /current session/);
  assert.throws(() => validateActionRuntimeReferences(input, { ...state, action: { ...state.action!, requirements: [{ name: "button", preferences: [{ family: "dev.deckr.output.raster" }] }] } }), /requirement/);
  const overlay = routedMessage("binding_overlay", { binding: binding as unknown as JsonObject, template: "label", generation: 3 });
  assert.throws(() => validateActionRuntimeReferences(overlay, state), /Stale binding generation/);
  assert.doesNotThrow(() => validateActionRuntimeReferences(routedMessage("binding_overlay", { binding: binding as unknown as JsonObject, template: "label", generation: 4 }), state));
  assert.throws(() => validateActionRuntimeReferences(routedMessage("close_page", {}), state), /page session/);
  assert.throws(() => validateActionRuntimeReferences(input, { ...state, action: undefined }), /action descriptor/);
  const noFingerprint = structuredClone(input);
  delete ((((noFingerprint.body.event as JsonObject).event as JsonObject).capability as JsonObject).deviceRef as JsonObject).fingerprint;
  assert.doesNotThrow(() => validateActionRuntimeReferences(noFingerprint, state), "An omitted fingerprint does not change capability identity");
  const wrongFingerprint = structuredClone(input);
  ((((wrongFingerprint.body.event as JsonObject).event as JsonObject).capability as JsonObject).deviceRef as JsonObject).fingerprint = "another";
  assert.throws(() => validateActionRuntimeReferences(wrongFingerprint, state), /negotiated/);
  const sourceDescriptor = structuredClone(descriptor);
  sourceDescriptor.controls![0]!.inputCapabilities!.push({ ...sourceDescriptor.controls![0]!.inputCapabilities![0]!, capabilityId: "raw" });
  const sourceBinding = structuredClone(binding);
  sourceBinding.matchedCapabilities![0]!.source = { controlId: "button", capabilityId: "raw" };
  const sourced = routedMessage("capability_input", { binding: sourceBinding as unknown as JsonObject, event: {
    capability, eventType: "press", occurredAt: new Date().toISOString(), value: { eventType: "press" },
    source: { deviceRef, controlId: "button", capabilityId: "raw" },
  } });
  const sourceState = { ...state, binding: sourceBinding, devices: [{ managerId: "manager", descriptor: sourceDescriptor }] };
  assert.doesNotThrow(() => validateActionRuntimeReferences(sourced, sourceState), "Explicit and enclosing-device source references resolve to the same capability");
  const wrongSource = structuredClone(sourced);
  (((wrongSource.body.event as JsonObject).event as JsonObject).source as JsonObject).capabilityId = "activation";
  assert.throws(() => validateActionRuntimeReferences(wrongSource, sourceState), /resolved source/);
});

test("Action Runtime targets the operation's action or page when related binding state is supplied", () => {
  const { actionId: _actionId, ...owner } = metadata;
  const pageSession = { ...owner, pageId: "page", pageSessionId: "page-session", ownerBindingId: "owner" };
  const deviceRef = { managerId: "manager", deviceId: "deck" };
  // A related binding has its own context, and its hardware has disappeared.
  // This cannot prevent action destruction or page closure.
  const binding = { ...metadata, actionInstanceId: "child", contextId: "child-context", bindingId: "child-binding", deviceRef, controlRef: { deviceRef, controlId: "button" } };
  const state = { actionInstance: metadata, binding, pageSession };
  const closing = routedMessage("close_page", {}, { pageSessionId: pageSession.pageSessionId, contextId: pageSession.contextId });
  assert.doesNotThrow(() => validateActionRuntimeReferences(closing, state));
  assert.throws(() => validateActionRuntimeReferences(closing, { ...state, pageSession: { ...pageSession, pageSessionId: "other" } }), /pageSessionId/);
  assert.throws(() => validateActionRuntimeReferences(closing, { ...state, pageSession: undefined }), /page session/);
  for (const name of ["action_instance_created", "action_instance_destroyed"]) {
    assert.doesNotThrow(() => validateActionRuntimeReferences(routedMessage(name, { metadata: { ...metadata } }, { actionInstanceId: metadata.actionInstanceId }), state));
  }
  for (const name of ["page_session_opened", "page_session_closed"]) {
    assert.doesNotThrow(() => validateActionRuntimeReferences(routedMessage(name, { pageSession }, { pageSessionId: pageSession.pageSessionId }), state));
  }
  for (const [targetKind, field, value] of [["action_instance", "actionInstance", metadata], ["page_session", "pageSession", pageSession]] as const) {
    assert.doesNotThrow(() => validateActionRuntimeReferences(routedMessage("action_lifecycle_rejected", { targetKind, [field]: { ...value }, reason: "stale_lifecycle" }), state));
  }
  const descriptor = { pageId: "page", bindings: [{ controlId: "button", target: { kind: "self" } }] };
  assert.doesNotThrow(() => validateActionRuntimeReferences(routedMessage("open_page", { descriptor }, { actionInstanceId: metadata.actionInstanceId }), state));
  assert.doesNotThrow(() => validateActionRuntimeReferences(routedMessage("replace_page", { descriptor }, { pageSessionId: pageSession.pageSessionId }), state));
});

test("blocked availability publication for one peer does not block another, and close cleans late commits", async t => {
  let release!: () => void, entered!: () => void;
  const blocked = new Promise<void>(resolve => { entered = resolve; }), gate = new Promise<void>(resolve => { release = resolve; });
  class SlowViews extends MemoryStateStore {
    override async put(key: string, value: JsonObject, options?: Parameters<MemoryStateStore["put"]>[2]) {
      if (value.consumerEndpoint === "controller:slow") { entered(); await gate; }
      return super.put(key, value, options);
    }
  }
  const views = new SlowViews(), transport = new MemoryMessageTransport();
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const runtime = new EndpointSession({ endpoint: "service:action-runtime.clock-main", sessionId: "runtime", transport });
  const slowEndpoint = new EndpointSession({ endpoint: "controller:slow", sessionId: "slow", transport });
  const fastEndpoint = new EndpointSession({ endpoint: "controller:fast", sessionId: "fast", transport });
  const shared = { beacon, concord, stateFor: () => views, resolveReferences: () => ({ actionInstance: metadata }) };
  const provider = new ManagedActionRuntimeProvider({ ...shared, endpoint: runtime.lane("services"), providerInstanceId: metadata.providerInstanceId, providerId: metadata.providerId, availability, handle: () => {} });
  const slow = new ManagedActionRuntimeClient({ ...shared, endpoint: slowEndpoint.lane("services") });
  const fast = new ManagedActionRuntimeClient({ ...shared, endpoint: fastEndpoint.lane("services") });
  t.after(async () => { release(); await slow.close(); await fast.close(); await provider.close(); await beacon.close(); await concord.close(); await runtime.close(); await slowEndpoint.close(); await fastEndpoint.close(); transport.close(); });
  await provider.start(); const slowLease = await slow.ready(); await blocked;
  const fastLease = await fast.ready(), watch = fast.watchAvailability()[Symbol.asyncIterator]();
  let snapshot = (await watch.next()).value!;
  while (!snapshot.availability) snapshot = (await watch.next()).value!;
  assert.equal(snapshot.entry!.contract.contractId, fastLease.agreement.contract.contractId);
  const closing = provider.close();
  assert.equal(provider.ready, false);
  await flush();
  assert.equal(slowLease.agreement.valid, false);
  assert.equal(fastLease.agreement.valid, false);
  release(); await closing; await watch.return?.();
  assert.equal((await views.items()).length, 0);
});
