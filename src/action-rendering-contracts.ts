import { ValidationError } from "./errors.ts";
import { cloneJson, isSafeJsonInteger, requireJsonObject, type JsonObject } from "./json.ts";
import { closedObject } from "./validation.ts";
import type { BindingCommandTarget, BindingMetadata } from "./actions.ts";

export const RENDER_TEXT_FORMAT = "dev.deckr.render.text.v1";
export const RENDER_IMAGE_FORMAT = "dev.deckr.render.image.v1";
export const RENDER_CONTRACTS = ["render-limits", "render-text", "render-image", "render-format", "render-target-support", "rendering-support", "render-content", "binding-render-body", "binding-render-result-body"] as const;

export interface RenderLimits {
  maxMessageBytes: number; maxImageBytes: number; maxImagePixels: number;
  maxTextCodePoints: number; maxRenderTimeMs: number;
}
export interface RenderText { text: string; foreground?: string; background?: string; }
export interface RenderImage { image: string; encoding: "png" | "jpeg"; fit?: "contain" | "cover" | "stretch"; background?: string; }
export interface RenderFormat { id: string; options?: JsonObject; }
export interface RenderTargetSupport { requirementName: string; width: number; height: number; formats: RenderFormat[]; limits: RenderLimits; }
export interface RenderingSupport { version: 1; targets: RenderTargetSupport[]; }
export interface BindingAttachedBody { binding: BindingMetadata; settings?: JsonObject; internal?: JsonObject; rendering?: RenderingSupport; }
export interface RenderContent { format: string; data: JsonObject; }
export type BindingRenderBody = { target: BindingCommandTarget; requirementName: string } & (
  { operation: "set"; content: RenderContent } | { operation: "clear"; content?: never }
);
export type RenderResultCode = "invalid_content" | "unsupported_format" | "unsupported_target" | "target_not_current" | "limit_exceeded" | "busy" | "render_failed" | "deadline_exceeded" | "dispatch_failed" | "newer_output" | "hardware_rejected";
export interface BindingRenderResultBody {
  requestMessageId: string; target: BindingCommandTarget; requirementName: string;
  stage: "controller" | "hardware"; status: "dispatched" | "rejected" | "failed" | "superseded";
  commandMessageId?: string; code?: RenderResultCode; reason?: string;
}

const extension = /^(?!dev\.deckr\.)[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+\.v[1-9][0-9]*$/;
function fail(message: string): never { throw new ValidationError(message); }
function nonempty(value: unknown): asserts value is string { if (typeof value !== "string" || !value.length) fail("Rendering requires nonempty text"); }
function formatId(value: unknown): asserts value is string {
  nonempty(value);
  if (value !== RENDER_TEXT_FORMAT && value !== RENDER_IMAGE_FORMAT && !extension.test(value)) fail("Invalid rendering format ID");
}
function positive(value: unknown): void { if (!isSafeJsonInteger(value) || Number(value) <= 0) fail("Rendering requires a positive safe integer"); }
function color(value: unknown): void { if (typeof value !== "string" || !/^#[0-9a-fA-F]{6}$/.test(value)) fail("Invalid rendering color"); }
function choices(value: unknown, values: string[]): void { if (typeof value !== "string" || !values.includes(value)) fail("Invalid rendering value"); }
function object(value: unknown, fields: string[], label: string): JsonObject {
  const raw = closedObject(value, fields, label);
  if (Object.values(raw).some(item => item === null)) fail("Rendering fields cannot be null");
  return raw;
}
function array(value: unknown): unknown[] { if (!Array.isArray(value)) fail("Rendering requires an array"); return value; }
function unique(values: unknown[]): void { if (new Set(values).size !== values.length) fail("Duplicate rendering declaration"); }
function target(value: unknown): void {
  const raw = object(value, ["kind", "bindingId", "generation"], "render target");
  if (raw.kind !== "binding" || !isSafeJsonInteger(raw.generation) || Number(raw.generation) < 0) fail("Invalid binding render target");
  nonempty(raw.bindingId);
}

/** Structural and intrinsic rendering checks. Content execution belongs to the controller. */
export function validateRenderContract(contract: string, value: unknown): JsonObject {
  let raw: JsonObject;
  switch (contract) {
    case "render-limits":
      raw = object(value, ["maxMessageBytes", "maxImageBytes", "maxImagePixels", "maxTextCodePoints", "maxRenderTimeMs"], contract);
      for (const key of ["maxMessageBytes", "maxImageBytes", "maxImagePixels", "maxTextCodePoints", "maxRenderTimeMs"]) positive(raw[key]);
      break;
    case "render-text":
      raw = object(value, ["text", "foreground", "background"], contract);
      if (typeof raw.text !== "string") fail("Render text must be a string");
      for (const key of ["foreground", "background"]) if (raw[key] !== undefined) color(raw[key]);
      break;
    case "render-image":
      raw = object(value, ["image", "encoding", "fit", "background"], contract); nonempty(raw.image);
      choices(raw.encoding, ["png", "jpeg"]);
      if (raw.fit !== undefined) choices(raw.fit, ["contain", "cover", "stretch"]);
      if (raw.background !== undefined) color(raw.background);
      break;
    case "render-format": {
      raw = object(value, ["id", "options"], contract); formatId(raw.id);
      if (raw.options !== undefined) requireJsonObject(raw.options, "format options");
      if (raw.id === RENDER_TEXT_FORMAT && raw.options !== undefined) fail("Text format has no options");
      if (raw.id === RENDER_IMAGE_FORMAT) {
        const options = object(raw.options, ["encodings"], "image options"), encodings = array(options.encodings);
        unique(encodings); encodings.forEach(item => choices(item, ["png", "jpeg"]));
        if (!encodings.includes("png")) fail("Image formats require PNG support");
      }
      break;
    }
    case "render-target-support": {
      raw = object(value, ["requirementName", "width", "height", "formats", "limits"], contract);
      nonempty(raw.requirementName); positive(raw.width); positive(raw.height);
      validateRenderContract("render-limits", raw.limits);
      const ids = array(raw.formats).map(item => validateRenderContract("render-format", item).id);
      unique(ids);
      if (!ids.includes(RENDER_TEXT_FORMAT) || !ids.includes(RENDER_IMAGE_FORMAT)) fail("Render targets require both baseline formats");
      break;
    }
    case "rendering-support":
      raw = object(value, ["version", "targets"], contract);
      if (!isSafeJsonInteger(raw.version) || Number(raw.version) !== 1) fail("Unsupported rendering version");
      unique(array(raw.targets).map(item => validateRenderContract("render-target-support", item).requirementName));
      break;
    case "render-content":
      raw = object(value, ["format", "data"], contract); formatId(raw.format); requireJsonObject(raw.data, "render data");
      if (raw.format === RENDER_TEXT_FORMAT) validateRenderContract("render-text", raw.data);
      if (raw.format === RENDER_IMAGE_FORMAT) validateRenderContract("render-image", raw.data);
      break;
    case "binding-render-body":
      raw = object(value, ["target", "requirementName", "operation", "content"], contract);
      target(raw.target); nonempty(raw.requirementName); choices(raw.operation, ["set", "clear"]);
      if ((raw.operation === "set") !== (raw.content !== undefined)) fail("Set requires content; clear prohibits it");
      if (raw.content !== undefined) validateRenderContract("render-content", raw.content);
      break;
    case "binding-render-result-body": {
      raw = object(value, ["requestMessageId", "target", "requirementName", "stage", "status", "commandMessageId", "code", "reason"], contract);
      nonempty(raw.requestMessageId); target(raw.target); nonempty(raw.requirementName);
      const codes: Record<string, string[]> = {
        "controller:dispatched": [],
        "controller:rejected": ["invalid_content", "unsupported_format", "unsupported_target", "target_not_current", "limit_exceeded", "busy"],
        "controller:failed": ["invalid_content", "limit_exceeded", "render_failed", "deadline_exceeded", "dispatch_failed"],
        "controller:superseded": ["newer_output", "target_not_current"],
        "hardware:rejected": ["hardware_rejected"],
      };
      const allowed = codes[`${raw.stage}:${raw.status}`];
      if (!allowed || (allowed.length ? !allowed.includes(String(raw.code)) : raw.code !== undefined)) fail("Invalid render result stage, status, or code");
      const command = raw.status === "dispatched" || raw.stage === "hardware";
      if (command !== (raw.commandMessageId !== undefined)) fail("Invalid hardware command correlation");
      if (command) nonempty(raw.commandMessageId);
      if (raw.reason !== undefined && (typeof raw.reason !== "string" || Array.from(raw.reason).length > 512)) fail("Render reason exceeds 512 code points");
      break;
    }
    default: fail(`Unknown rendering contract ${contract}`);
  }
  return cloneJson(raw);
}
