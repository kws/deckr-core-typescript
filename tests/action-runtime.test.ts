import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { actionRuntimeProtocol, actionsPayloadFromAdvertisement, actionsBeaconPayload, actionProviderSessionTerms, actionProviderSessionContractId, actionAvailabilityPayload, validateActionAvailabilityPayload, ACTION_INSTANCE_CREATED, actionBody, validateActionContract, validateActionRuntimeMessage, actionRuntimePayloadSchemas, actionBeaconDescriptor } from "../src/actions.ts";
import { validateAdvertisementRecord } from "../src/beacon.ts";
import { actionProviderAddress, parseActionProviderAddress } from "../src/endpoint.ts";
import { buildMessage, entitySubject } from "../src/lanes.ts";
import { parseServiceDescriptor } from "../src/services.ts";

const specRoot = process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr");
function contract(file: string) { return JSON.parse(readFileSync(path.join(specRoot, "contract/v1", file), "utf8")); }

test("Action Runtime implementation matches the upstream protocol declaration", () => {
  const reference = contract("catalogs/action-runtime-protocol.v1.json");
  const actual = actionRuntimeProtocol("clock-main");
  assert.equal(actual.namespace, reference.namespace);
  assert.equal(actual.featureId, reference.featureId);
  assert.equal(actual.advertisementProfile, reference.advertisementProfile);
  assert.equal(actual.useProfile, reference.useProfile);
  assert.deepEqual(actual.operations, reference.operations);
  assert.deepEqual(actual.messages, reference.messages);
  for (const [name, view] of Object.entries(actual.viewFamilies)) {
    const { keyPrefix, ...declaration } = view;
    assert.deepEqual(declaration, reference.viewFamilies[name]);
    assert.ok(keyPrefix!.endsWith(".action_availability."));
  }
  assert.deepEqual(actionRuntimeProtocol(), reference);
});

test("Action Runtime helpers accept current upstream advertisements and availability", () => {
  const advertisement = validateAdvertisementRecord(contract("fixtures/valid/beacon/action-runtime-advertisement.v1.json"));
  const payload = actionsPayloadFromAdvertisement(advertisement);
  assert.equal(payload.serviceEndpoint, actionProviderAddress("clock-main"));
  assert.equal(parseActionProviderAddress(payload.serviceEndpoint), "clock-main");
  assert.equal(validateActionAvailabilityPayload(contract("fixtures/valid/action-runtime/action-availability-view.v1.json")).entries[0]!.status, "available");
  assert.equal(actionsBeaconPayload({ providerInstanceId: "clock-main", providerId: "dev.deckr.clock", sessionId: "session", actions: [] }).serviceEndpoint, "service:action-runtime.clock-main");
  const availability = validateActionAvailabilityPayload(contract("fixtures/valid/action-runtime/action-availability-view.v1.json"));
  assert.deepEqual(actionAvailabilityPayload({ ...availability, sessionId: availability.serviceSessionId }), availability);
  for (const id of ["clock-main", "clock-other"]) {
    const descriptor = parseServiceDescriptor({ key: "", revision: 1, observedAt: "", advertisement: {
      ...advertisement, endpoint: actionProviderAddress(id), advertiser: actionProviderAddress(id),
      payload: actionsBeaconPayload({ providerInstanceId: id, providerId: "dev.deckr.clock", sessionId: advertisement.sessionId }) as never,
    } }, actionRuntimeProtocol());
    assert.equal(descriptor?.serviceId, `action-runtime.${id}`);
    assert.ok(descriptor?.views.action_availability?.keyPrefix);
  }
});

test("Action Runtime uses local service-use scope and opaque proposal identities", () => {
  const terms = actionProviderSessionTerms({ providerInstanceId: "clock-main", providerId: "dev.deckr.clock", sessionId: "session", controllerEndpoint: "controller:main", providerEndpoint: actionProviderAddress("clock-main") });
  assert.equal(terms.serviceNamespace, "dev.deckr.action_runtime.provider");
  assert.ok(terms.allowedOperations.includes(ACTION_INSTANCE_CREATED));
  assert.ok(terms.allowedViews.action_availability);
  assert.notEqual(actionProviderSessionContractId("controller:main", "service:action-runtime.clock-main"), actionProviderSessionContractId("controller:main", "service:action-runtime.clock-main"));
  const event = { actionInstanceId: "instance", contextId: "context" };
  const message = buildMessage({ sender: "controller:main", senderSessionId: "session", recipient: "service:action-runtime.clock-main", contract: { contractId: "opaque", generation: 1 }, messageType: "serviceMessage", subject: entitySubject("service", { serviceId: "action-runtime.clock-main" }), body: { serviceNamespace: terms.serviceNamespace, name: ACTION_INSTANCE_CREATED, intent: "event", exchangePattern: "one_way", event } });
  assert.equal(message.lane, "services");
  assert.deepEqual(actionBody(message), event);
});

test("native Action Runtime validators consume upstream payload and message fixtures directly", () => {
  const excluded = new Set(["advertisement.v1.json", "availability-view.v1.json", "messages.v1.json", "use-contract.v1.json"]);
  for (const expectation of ["valid", "invalid"] as const) {
    const directory = `fixtures/${expectation}/coverage/deckr/action-runtime`;
    for (const file of readdirSync(path.join(specRoot, "contract/v1", directory), { withFileTypes: true })) {
      if (!file.isFile() || excluded.has(file.name)) continue;
      const name = file.name.replace(".v1.json", ""), value = contract(`${directory}/${file.name}`);
      if (expectation === "valid") assert.doesNotThrow(() => validateActionContract(name, value), name);
      else assert.throws(() => validateActionContract(name, value), name);
    }
    for (const file of readdirSync(path.join(specRoot, "contract/v1", directory, "messages"))) {
      const value = contract(`${directory}/messages/${file}`);
      if (expectation === "valid") {
        const payload = value.body.event ?? value.body.params;
        const metadata = payload.metadata ?? payload.binding ?? payload.pageSession ?? payload.actionInstance;
        // Coverage fixtures establish schema validity. Several deliberately use
        // generic metadata IDs, so they do not establish runtime identity.
        const runtimeEndpoint = value.sender.startsWith("service:") ? value.sender : value.recipient.endpoint;
        if (metadata && runtimeEndpoint !== `service:action-runtime.${metadata.providerInstanceId}`) {
          assert.throws(() => validateActionRuntimeMessage(value), /providerInstanceId mismatch/);
          const matching = structuredClone(value), endpoint = `service:action-runtime.${metadata.providerInstanceId}`;
          if (matching.sender.startsWith("service:")) matching.sender = endpoint; else matching.recipient.endpoint = endpoint;
          matching.subject.identifiers.serviceId = `action-runtime.${metadata.providerInstanceId}`;
          assert.doesNotThrow(() => validateActionRuntimeMessage(matching), file);
        } else assert.doesNotThrow(() => validateActionRuntimeMessage(value), file);
      }
      else assert.throws(() => validateActionRuntimeMessage(value), file);
    }
  }
});

test("the Action Runtime payload registry resolves every catalog reference without external files", () => {
  for (const [name, definition] of Object.entries(actionRuntimeProtocol().messages)) {
    const declaration = definition.eventSchema ?? definition.paramsSchema!;
    const schemaPath = `schemas/deckr/action-runtime/messages/${name.replaceAll("_", "-")}.v1.schema.json`;
    const fixture = contract("manifest.json").artifacts.find((item: { kind: string; valid: boolean; schemaPath: string }) => item.kind === "fixture" && item.valid && item.schemaPath === schemaPath);
    assert.ok(fixture, `Missing upstream fixture for ${name}`);
    const body = contract(fixture.path).body;
    assert.doesNotThrow(() => actionRuntimePayloadSchemas.validate(declaration.schema, definition.intent === "event" ? body.event : body.params), name);
    if (name !== "close_page") assert.throws(() => actionRuntimePayloadSchemas.validate(declaration.schema, {}), name);
  }
  assert.throws(() => actionRuntimePayloadSchemas.compile({ $ref: "https://unregistered.invalid/schema" }), /unresolved/);
});

test("runtime messages enforce payload position, direction, session, and provider metadata", () => {
  const message = contract("fixtures/valid/action-runtime/action-instance-created.v1.json");
  for (const patch of [
    { recipientSessionId: null }, { sender: "service:another" },
    { body: { ...message.body, params: { hidden: true } } },
    { body: { ...message.body, status: "ok" } },
    { body: { ...message.body, event: { metadata: { ...message.body.event.metadata, providerInstanceId: "other" } } } },
  ]) assert.throws(() => validateActionRuntimeMessage({ ...message, ...patch }));
  const descriptor = actionBeaconDescriptor({ actionId: "example", name: "", hints: { local: true } });
  validateActionContract("action-descriptor", descriptor);
  assert.equal(Object.hasOwn(descriptor, "hints"), false);
  const requirement = { name: "button", preferences: [{ family: "dev.deckr.input.button" }] };
  assert.throws(() => validateActionContract("action-descriptor", { actionId: "example", requirements: [requirement, requirement] }), /Duplicate/);
  assert.throws(() => validateActionContract("capability-requirement-selector", {}), /constrain/);
  assert.throws(() => validateActionContract("capability-requirement-selector", { family: "dev.deckr.unknown" }), /family/);
});
