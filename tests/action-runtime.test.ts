import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { actionRuntimeProtocol, actionsPayloadFromAdvertisement, actionsBeaconPayload, actionProviderSessionTerms, actionProviderSessionContractId, actionAvailabilityPayload, validateActionAvailabilityPayload, ACTION_INSTANCE_CREATED, actionBody } from "../src/actions.ts";
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

test("Action Runtime uses standard service-use terms and opaque proposal identities", () => {
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
