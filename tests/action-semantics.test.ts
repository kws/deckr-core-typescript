import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { matchCapabilityRequirements, validateBindingResources, validatePageCommand, validateActionRuntimeMessage } from "../src/actions.ts";
import { parseJson } from "../src/json.ts";

const root = process.env.DECKR_SPEC_ROOT ?? path.resolve(import.meta.dirname, "../../deckr");
const source = readFileSync(path.join(root, "contract/v1/vectors/action-runtime-semantics.v1.json"), "utf8");
// Test both ordinary JSON and transport-preserved numeric tokens.
for (const [encoding, vectors] of [["native", JSON.parse(source)], ["lossless", parseJson(source)]] as const) {
  for (const item of vectors.cases) test(`Action Runtime ${encoding}: ${item.id}`, () => {
    const context = vectors.contexts[item.contextRef];
    const check = () => {
      if (item.rule === "SEM-ACT-002") validateBindingResources(item.instance, context.actionDescriptor, context);
      else if (item.rule === "SEM-ACT-003") validatePageCommand(item.instance, context);
      else if (item.rule === "SEM-ACT-005") validateActionRuntimeMessage(item.instance);
      else assert.fail(`Unknown rule ${item.rule}`);
    };
    if (item.semanticValid) assert.doesNotThrow(check);
    else assert.throws(check);
    if (item.rule === "SEM-ACT-002" && item.semanticValid) {
      const selected = matchCapabilityRequirements(context.actionDescriptor, context);
      validateBindingResources({ ...item.instance, matchedCapabilities: selected }, context.actionDescriptor, context);
    }
  });
}
