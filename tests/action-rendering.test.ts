import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { validateActionContract, validateActionRuntimeMessage, validateRenderCommand, RenderRejection, validateActionRuntimeReferences, type RenderCommandContext } from "../src/actions.ts";
import { parseJson, type JsonObject } from "../src/json.ts";
import { messageWireSizeBytes, parseDeckrMessage, subjectFor, validateDeckrMessage } from "../src/lanes.ts";
import { ManagedActionRuntimeClient, ManagedActionRuntimeProvider, type ActionReferenceResolver } from "../src/actions.ts";
import { BeaconService, BeaconDiscovery, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordService, ConcordCoordinator, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { EndpointSession, MemoryMessageTransport } from "../src/messaging.ts";
import { MemoryStateStore } from "../src/state.ts";
import { deckrMessageFromNatsMessage } from "../src/nats.ts";

const root = path.join(process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr"), "contract/v1");
function source(name: string): string { return readFileSync(path.join(root, name), "utf8"); }
const manifest = JSON.parse(source("manifest.json"));
for (const [encoding, parse] of [["native", JSON.parse], ["lossless", parseJson]] as const) {
  for (const item of manifest.artifacts.filter((item: { kind: string; path: string }) => item.kind === "fixture" && item.path.includes("/action-runtime/rendering/"))) {
    test(`rendering fixture ${encoding}: ${item.path}`, () => {
      const value = parse(source(item.path));
      const check = () => item.schemaPath.includes("/messages/") ? validateActionRuntimeMessage(value)
        : validateActionContract(path.basename(item.schemaPath).replace(".v1.schema.json", ""), value);
      if (item.valid) assert.doesNotThrow(check); else assert.throws(check);
    });
  }
  const vectors = parse(source("vectors/action-runtime-rendering.v1.json")) as unknown as { cases: { id: string; instance: JsonObject; context: RenderCommandContext; expectedCode: string | null }[] };
  for (const item of vectors.cases) test(`render admission ${encoding}: ${item.id}`, () => {
    const check = () => validateRenderCommand(item.instance, item.context);
    if (item.expectedCode === null) assert.doesNotThrow(check);
    else assert.throws(check, error => error instanceof RenderRejection && error.code === item.expectedCode);
  });
}

test("received byte limits preserve envelope whitespace across validation", () => {
  const raw = source("fixtures/valid/action-runtime/rendering/messages-binding-render.v1.json");
  const bytes = new TextEncoder().encode(raw), message = parseDeckrMessage(bytes);
  assert.equal(messageWireSizeBytes(message), bytes.length);
  assert.equal(messageWireSizeBytes(validateActionRuntimeMessage(message)), bytes.length);
  assert.equal(messageWireSizeBytes(validateDeckrMessage(message)), bytes.length);
  assert.equal(messageWireSizeBytes(deckrMessageFromNatsMessage({ subject: subjectFor(message), data: bytes })), bytes.length);
  assert.ok(messageWireSizeBytes(message) > new TextEncoder().encode(JSON.stringify(message)).length);
  assert.ok(!JSON.stringify(message).includes("wireSize"));
});

test("result correlation is mandatory and a result can describe a detached target", () => {
  const message = JSON.parse(source("fixtures/valid/action-runtime/rendering/messages-binding-render-result.v1.json"));
  assert.doesNotThrow(() => validateActionRuntimeReferences(message, {}));
  assert.throws(() => validateActionRuntimeMessage({ ...message, causationId: "another" }), /causation/);
  assert.throws(() => validateActionRuntimeMessage({ ...message, causationId: undefined }), /causation/);
  assert.throws(() => validateActionRuntimeMessage({ ...message, inReplyTo: "request" }), /envelope/);
  const event = message.body.event;
  assert.doesNotThrow(() => validateActionContract("binding-render-result-body", { ...event, reason: "😀".repeat(512) }));
  assert.throws(() => validateActionContract("binding-render-result-body", { ...event, reason: "😀".repeat(513) }));
});

test("managed render references require current admission observations", () => {
  const item = JSON.parse(source("vectors/action-runtime-rendering.v1.json")).cases[0];
  assert.throws(() => validateActionRuntimeReferences(item.instance, {}), /current target/);
  assert.doesNotThrow(() => validateActionRuntimeReferences(item.instance, { renderCommand: item.context }));
  item.context.bindings["binding-17"].active = false;
  assert.throws(() => validateActionRuntimeReferences(item.instance, { renderCommand: item.context }), /target_not_current/);
});

test("managed rendering preserves request IDs and delivers correlated results after detachment", { timeout: 5000 }, async t => {
  const sample = JSON.parse(source("vectors/action-runtime-rendering.v1.json")).cases[0];
  const transport = new MemoryMessageTransport(), views = new MemoryStateStore();
  const controller = new EndpointSession({ endpoint: sample.context.controllerEndpoint, sessionId: sample.context.controllerSessionId, transport });
  const runtime = new EndpointSession({ endpoint: sample.context.runtimeEndpoint, sessionId: sample.context.runtimeSessionId, transport });
  const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
  const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
  const binding = sample.context.bindings["binding-17"].binding;
  const metadata = Object.fromEntries(["providerInstanceId", "providerId", "actionId", "actionInstanceId", "configId", "contextId"].map(key => [key, binding[key]]));
  const resolveReferences: ActionReferenceResolver = (message, authority) => ({
    actionInstance: metadata as never,
    renderCommand: { ...sample.context, contract: authority.contract, messageBytes: messageWireSizeBytes(message),
      bindings: { "binding-17": { ...sample.context.bindings["binding-17"], contract: authority.contract } } },
  });
  const shared = { beacon, concord, stateFor: () => views, resolveReferences };
  let sentId: string | undefined;
  let receivedResult!: (message: unknown) => void;
  const result = new Promise<unknown>(resolve => { receivedResult = resolve; });
  const provider = new ManagedActionRuntimeProvider({ ...shared, endpoint: runtime.lane("services"), providerInstanceId: binding.providerInstanceId, providerId: binding.providerId,
    handle: async context => {
      if (context.name === "action_instance_created") sentId = (await context.send("binding_render", sample.instance.body.params)).messageId;
      else if (context.name === "binding_render_result") receivedResult(context.message);
    },
  });
  const client = new ManagedActionRuntimeClient({ ...shared, endpoint: controller.lane("services") });
  const abort = new AbortController();
  t.after(async () => { abort.abort(); await client.close(); await provider.close(); await beacon.close(); await concord.close(); await runtime.close(); await controller.close(); transport.close(); });
  await provider.start(); await client.ready();
  const incoming = client.messages({ signal: abort.signal })[Symbol.asyncIterator]();
  const next = incoming.next();
  await client.send("action_instance_created", { metadata });
  const render = (await next).value!;
  assert.equal(render.name, "binding_render");
  // Let the provider finish its send before comparing the returned identity.
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(render.message.messageId, sentId);
  assert.equal(messageWireSizeBytes(render.message), new TextEncoder().encode(JSON.stringify(render.message)).length);
  sample.context.bindings["binding-17"].active = false;
  const sent = await client.send("binding_render_result", { requestMessageId: render.message.messageId,
    target: sample.instance.body.params.target, requirementName: "display", stage: "controller", status: "superseded", code: "target_not_current" });
  const received = validateActionRuntimeMessage(await result);
  assert.equal(received.messageId, sent.messageId);
  assert.equal(received.causationId, render.message.messageId);
  assert.equal(received.inReplyTo, undefined);
});
