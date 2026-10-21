import { validateActionRuntimeMessage, validateActionContract } from "./action-contracts.ts";
import { RENDER_IMAGE_FORMAT, RENDER_TEXT_FORMAT, validateRenderContract, type BindingRenderBody, type RenderingSupport, type RenderTargetSupport } from "./action-rendering-contracts.ts";
import type { BindingMetadata, MatchedCapability } from "./actions.ts";
import type { ContractPointer } from "./authority.ts";
import { ValidationError } from "./errors.ts";
import { canonicalJson, isSafeJsonInteger, type JsonObject } from "./json.ts";

export interface RenderBindingState {
  binding: BindingMetadata; rendering?: RenderingSupport | null; active: boolean;
  contract: ContractPointer; runtimeEndpoint: string; runtimeSessionId: string;
}
export interface RenderCommandContext {
  contract: ContractPointer; controllerEndpoint: string; controllerSessionId: string;
  runtimeEndpoint: string; runtimeSessionId: string; localAuthority: boolean; hardwareAuthority: boolean;
  bindings: Readonly<Record<string, RenderBindingState>>; messageBytes: number;
}
export class RenderRejection extends ValidationError {
  readonly code: "unauthorized" | "target_not_current" | "unsupported_target" | "unsupported_format" | "limit_exceeded";
  constructor(code: RenderRejection["code"]) { super(code); this.name = "RenderRejection"; this.code = code; }
}
function reject(code: RenderRejection["code"]): never { throw new RenderRejection(code); }
function sameContract(a: ContractPointer | undefined, b: ContractPointer): boolean { return !!a && a.contractId === b.contractId && Number(a.generation) === Number(b.generation); }
function sameDevice(a: MatchedCapability["capability"]["deviceRef"], b: BindingMetadata["deviceRef"]): boolean {
  return !!a && a.managerId === b.managerId && a.deviceId === b.deviceId && a.fingerprint === b.fingerprint;
}
export function renderMatch(binding: BindingMetadata, name: string): MatchedCapability {
  const matches = binding.matchedCapabilities?.filter(item => item.requirementName === name) ?? [];
  const match = matches[0];
  if (matches.length !== 1 || !match || match.family !== "dev.deckr.output.raster" || match.type !== "bitmap" || match.direction !== "output" ||
    !match.commandTypes?.includes("set_frame") || !match.commandTypes.includes("clear") || !sameDevice(match.capability.deviceRef, binding.deviceRef) ||
    match.capability.controlId !== binding.controlRef.controlId) reject("unsupported_target");
  return match;
}
/** Verify named targets and equal declarations for aliases of one physical output. */
export function validateRenderingSupport(binding: BindingMetadata, support: RenderingSupport): void {
  validateActionContract("binding-metadata", binding); validateRenderContract("rendering-support", support);
  const aliases = new Map<string, string>();
  for (const target of support.targets) {
    const match = renderMatch(binding, target.requirementName);
    const { requirementName: _, ...shape } = target;
    const key = match.capability.capabilityId, normalized = canonicalJson(shape as unknown as JsonObject);
    if (aliases.has(key) && aliases.get(key) !== normalized) reject("unsupported_target");
    aliases.set(key, normalized);
  }
}
/** Check current admission facts again at final dispatch. This helper does not acquire authority or execute content. */
export function validateRenderCommand(value: unknown, context: RenderCommandContext): { binding: RenderBindingState; target: RenderTargetSupport } {
  const message = validateActionRuntimeMessage(value);
  if (message.body.name !== "binding_render") throw new ValidationError("Message is not binding_render");
  if (!isSafeJsonInteger(context.messageBytes) || Number(context.messageBytes) < 0) throw new ValidationError("Invalid received message size");
  const body = message.body.params as unknown as BindingRenderBody;
  if (!context.localAuthority || !sameContract(message.contract, context.contract) || message.sender !== context.runtimeEndpoint ||
    message.senderSessionId !== context.runtimeSessionId || message.recipient.targetType !== "endpoint" || message.recipient.endpoint !== context.controllerEndpoint ||
    message.recipientSessionId !== context.controllerSessionId) reject("unauthorized");
  const current = Object.hasOwn(context.bindings, body.target.bindingId) ? context.bindings[body.target.bindingId] : undefined;
  if (!current || !current.active || !context.hardwareAuthority || current.binding.bindingId !== body.target.bindingId ||
    Number(current.binding.outputGeneration ?? 0) !== Number(body.target.generation) || !sameContract(current.contract, context.contract) ||
    current.runtimeEndpoint !== context.runtimeEndpoint || current.runtimeSessionId !== context.runtimeSessionId) reject("target_not_current");
  if (!current.rendering) reject("unsupported_target");
  validateRenderingSupport(current.binding, current.rendering);
  const target = current.rendering.targets.find(item => item.requirementName === body.requirementName);
  if (!target) reject("unsupported_target");
  if (Number(context.messageBytes) > Number(target.limits.maxMessageBytes)) reject("limit_exceeded");
  if (body.operation === "set") {
    const selected = target.formats.find(item => item.id === body.content.format);
    if (!selected) reject("unsupported_format");
    if (body.content.format === RENDER_IMAGE_FORMAT && !(selected.options?.encodings as string[]).includes(String(body.content.data.encoding))) reject("unsupported_format");
    if (body.content.format === RENDER_TEXT_FORMAT && Array.from(String(body.content.data.text)).length > Number(target.limits.maxTextCodePoints)) reject("limit_exceeded");
  }
  return { binding: current, target };
}
