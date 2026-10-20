import { PNG } from "pngjs";
import jpeg from "jpeg-js";
import { ValidationError } from "./errors.ts";
import { isJsonNumber, isSafeJsonInteger, isJsonObject, canonicalJson, cloneJson, requireJsonObject, requireText, type JsonObject, type JsonValue } from "./json.ts";
import { validateHardwareDeviceRef } from "./hardware-messages.ts";
import { immutable } from "./runtime.ts";
import { defaultPayloadSchemas, type PayloadSchemas } from "./schema.ts";
import { closedObject } from "./validation.ts";

export interface HardwareCapabilitySchema { schemaId?: string | null; schema: JsonObject; }
export interface HardwareCapability {
  capabilityId: string; family: string; type: string;
  direction: "input" | "output" | "state" | "command";
  access: string[]; eventTypes?: string[]; commandTypes?: string[];
  valueSchema?: HardwareCapabilitySchema | null; commandSchema?: HardwareCapabilitySchema | null;
  constraints?: JsonObject[]; units?: JsonObject[]; sources?: JsonObject[];
  projection?: { owner: string; source: { controlId?: string | null; capabilityId: string }; type?: string; description?: string | null } | null;
}
export interface HardwareControl {
  controlId: string; kind: string; parentControlId?: string | null; relatedControlIds?: string[];
  inputCapabilities?: HardwareCapability[]; outputCapabilities?: HardwareCapability[];
  stateCapabilities?: HardwareCapability[]; configCapabilities?: HardwareCapability[]; diagnosticCapabilities?: HardwareCapability[];
  sources?: JsonObject[];
}
export interface HardwareDeviceDescriptor {
  deviceId: string; fingerprint: string; displayName: string;
  controls?: HardwareControl[]; capabilities?: HardwareCapability[];
  connections?: JsonObject[]; sources?: JsonObject[];
  defaultStatusIndicator?: { controlId?: string | null; capabilityId: string } | null;
}

const collections = {
  inputCapabilities: ["input"], outputCapabilities: ["output"], stateCapabilities: ["state"],
  configCapabilities: ["state", "command"], diagnosticCapabilities: ["state"],
};
const vocabulary: Record<string, Record<string, { direction: string; events?: string[]; commands?: string[] }>> = {
  "dev.deckr.input.button": { activation: { direction: "input", events: ["press"] }, momentary: { direction: "input", events: ["down", "up"] } },
  "dev.deckr.input.encoder": { relative: { direction: "input", events: ["rotate"] } },
  "dev.deckr.input.touch": { gesture: { direction: "input", events: ["tap", "swipe"] } },
  "dev.deckr.output.raster": { bitmap: { direction: "output", commands: ["set_frame", "clear"] } },
  "dev.deckr.device.power": { screen: { direction: "command", commands: ["sleep", "wake"] } },
};
function fail(reason: string): never { throw new ValidationError(reason); }
function name(value: unknown, label: string, qualified = false): string {
  const text = requireText(value, label);
  if (!(qualified ? /^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$/ : /^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)*$/).test(text)) fail(`Invalid ${label}`);
  return text;
}
function local(value: unknown, label: string): string {
  const text = requireText(value, label);
  if (/^(controller|hardware_manager|action_provider):/.test(text)) fail(`${label} must be a local ID`);
  return text;
}
function optionalText(raw: JsonObject, fields: string[]): void { for (const field of fields) if (raw[field] != null) requireText(raw[field], field); }
function array(value: JsonValue | undefined, label: string): JsonValue[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) fail(`${label} must be an array`);
  return value;
}
function unique(values: unknown[], label: string): void { if (new Set(values).size !== values.length) fail(`Duplicate ${label}`); }
function names(value: JsonValue | undefined, label: string): string[] {
  const values = array(value, label).map(item => name(item, label)); unique(values, label); return values;
}
function choice(value: unknown, values: string[], label: string): void { if (!values.includes(String(value)) || typeof value !== "string") fail(`Invalid ${label}`); }
function sources(value: JsonValue | undefined): void {
  for (const item of array(value, "sources")) {
    const source = closedObject(item, ["sourceId", "type", "label", "connectionId", "facts"], "source");
    local(source.sourceId, "sourceId"); name(source.type, "source type"); optionalText(source, ["label", "connectionId"]);
    if (source.facts !== undefined) requireJsonObject(source.facts, "source facts");
  }
}
function capabilityRef(value: unknown): void {
  const ref = closedObject(value, ["controlId", "capabilityId"], "capability reference");
  name(ref.capabilityId, "capabilityId"); optionalText(ref, ["controlId"]);
}
function schema(value: JsonValue | undefined, schemas: PayloadSchemas): void {
  if (value == null) return;
  const raw = closedObject(value, ["schemaId", "schema"], "capability schema");
  if (raw.schemaId != null) name(raw.schemaId, "schemaId", true);
  const definition = requireJsonObject(raw.schema, "schema");
  if (!["$ref", "allOf", "anyOf", "const", "enum", "items", "oneOf", "properties", "type"].some(key => Object.hasOwn(definition, key))) fail("Capability schema requires an assertion");
  schemas.compile(definition);
}

export function validateHardwareCapability(value: unknown, schemas = defaultPayloadSchemas): HardwareCapability {
  const raw = closedObject(value, ["capabilityId", "family", "type", "direction", "access", "eventTypes", "commandTypes", "valueSchema", "commandSchema", "constraints", "units", "projection", "sources"], "capability");
  name(raw.capabilityId, "capabilityId"); const family = name(raw.family, "family", true), type = name(raw.type, "type");
  choice(raw.direction, ["input", "output", "state", "command"], "direction");
  const access = names(raw.access, "access"); if (!access.length) fail("Capability access must not be empty");
  for (const item of access) choice(item, ["emits", "readable", "settable", "requestable", "invokable"], "access");
  const events = names(raw.eventTypes, "eventTypes"), commands = names(raw.commandTypes, "commandTypes");
  const permitted: Record<string, string[]> = { input: ["emits"], output: ["invokable", "settable"], state: ["emits", "readable", "requestable", "settable"], command: ["invokable"] };
  if (!access.some(item => permitted[String(raw.direction)]!.includes(item))) fail("Access does not match direction");
  if (["input", "state"].includes(String(raw.direction)) && (commands.length || raw.commandSchema != null)) fail("Input/state capabilities cannot declare commands");
  if (["output", "command"].includes(String(raw.direction)) && events.length) fail("Output/command capabilities cannot declare events");
  if (family.startsWith("dev.deckr.")) {
    const core = vocabulary[family]?.[type];
    if (!core || core.direction !== raw.direction || (core.events && canonicalJson(events) !== canonicalJson(core.events)) || (core.commands && canonicalJson(commands) !== canonicalJson(core.commands))) fail("Unknown or inconsistent core capability vocabulary");
  }
  schema(raw.valueSchema, schemas); schema(raw.commandSchema, schemas); sources(raw.sources);
  for (const item of array(raw.constraints, "constraints")) {
    const constraint = closedObject(item, ["type", "subject", "unit", "value", "values", "minimum", "maximum", "step"], "constraint");
    name(constraint.type, "constraint type"); name(constraint.subject, "constraint subject");
    if (constraint.unit != null) name(constraint.unit, "constraint unit");
    const values = array(constraint.values, "constraint values");
    for (const field of ["minimum", "maximum", "step"]) if (constraint[field] != null && !isJsonNumber(constraint[field])) fail(`Constraint ${field} must be a number`);
    if (constraint.step != null && Number(constraint.step) <= 0) fail("Constraint step must be positive");
    if (constraint.minimum != null && constraint.maximum != null && Number(constraint.minimum) > Number(constraint.maximum)) fail("Constraint minimum exceeds maximum");
    if (!["value", "minimum", "maximum", "step"].some(key => constraint[key] != null) && !values.length) fail("Constraint has no limits");
  }
  for (const item of array(raw.units, "units")) {
    const unit = closedObject(item, ["subject", "unit", "symbol", "scale"], "unit");
    name(unit.subject, "unit subject"); name(unit.unit, "unit"); if (unit.symbol != null) name(unit.symbol, "symbol");
    if (unit.scale !== undefined && (!isJsonNumber(unit.scale) || Number(unit.scale) <= 0)) fail("Unit scale must be positive");
  }
  if (raw.projection != null) {
    const projection = closedObject(raw.projection, ["owner", "source", "type", "description"], "projection");
    choice(projection.owner, ["hardware_manager", "adapter", "component"], "projection owner");
    if (projection.type !== undefined) choice(projection.type, ["projection", "derivation"], "projection type");
    optionalText(projection, ["description"]); capabilityRef(projection.source);
  }
  return immutable(cloneJson(raw)) as unknown as HardwareCapability;
}

export function controlCapabilities(control: HardwareControl): HardwareCapability[] {
  return Object.keys(collections).flatMap(key => control[key as keyof typeof collections] ?? []);
}
export function findHardwareCapability(descriptor: HardwareDeviceDescriptor, ref: { controlId?: string | null; capabilityId: string }): HardwareCapability {
  const capabilities = ref.controlId != null ? controlCapabilities(descriptor.controls?.find(control => control.controlId === ref.controlId) ?? { controlId: "", kind: "" }) : descriptor.capabilities ?? [];
  return capabilities.find(capability => capability.capabilityId === ref.capabilityId) ?? fail("Capability reference does not resolve");
}

export function validateHardwareDeviceDescriptor(value: unknown, schemas = defaultPayloadSchemas): HardwareDeviceDescriptor {
  const raw = closedObject(value, ["deviceId", "fingerprint", "displayName", "manufacturer", "model", "modelId", "serialNumber", "hardwareVersion", "firmwareVersion", "parent", "identifiers", "connections", "controls", "capabilities", "defaultStatusIndicator", "sources"], "device descriptor");
  local(raw.deviceId, "deviceId"); requireText(raw.fingerprint, "fingerprint"); requireText(raw.displayName, "displayName");
  optionalText(raw, ["manufacturer", "model", "modelId", "serialNumber", "hardwareVersion", "firmwareVersion"]);
  if (raw.parent != null) validateHardwareDeviceRef(raw.parent);
  sources(raw.sources);
  for (const item of array(raw.identifiers, "identifiers")) {
    const identifier = closedObject(item, ["type", "value", "issuer", "namespace"], "identifier");
    name(identifier.type, "identifier type"); requireText(identifier.value, "identifier value"); optionalText(identifier, ["issuer", "namespace"]);
  }
  const connections = array(raw.connections, "connections").map(item => {
    const connection = closedObject(item, ["connectionId", "type", "status", "label", "transport", "facts"], "connection");
    local(connection.connectionId, "connectionId"); name(connection.type, "connection type"); optionalText(connection, ["label", "transport"]);
    if (connection.status !== undefined) choice(connection.status, ["connected", "available", "unavailable", "unknown"], "connection status");
    if (connection.facts !== undefined) requireJsonObject(connection.facts, "connection facts");
    return connection;
  });
  unique(connections.map(item => item.connectionId), "connectionId");
  const capabilities = array(raw.capabilities, "capabilities").map(item => validateHardwareCapability(item, schemas));
  if (capabilities.some(item => item.direction === "input")) fail("Device capabilities cannot have input direction");
  unique(capabilities.map(item => item.capabilityId), "device capabilityId");
  const controls = array(raw.controls, "controls").map(item => {
    const control = closedObject(item, ["controlId", "kind", "label", "parentControlId", "relatedControlIds", "groupId", "surfaceId", "geometry", "sources", ...Object.keys(collections)], "control");
    requireText(control.controlId, "controlId"); name(control.kind, "control kind"); optionalText(control, ["label", "parentControlId", "groupId", "surfaceId"]); sources(control.sources);
    const related = array(control.relatedControlIds, "relatedControlIds");
    for (const ref of related) requireText(ref, "related control"); unique(related, "related control");
    for (const [key, directions] of Object.entries(collections)) for (const item of array(control[key], key)) {
      const capability = validateHardwareCapability(item, schemas);
      if (!directions.includes(capability.direction)) fail(`Wrong direction in ${key}`);
    }
    if (control.geometry != null) {
      const geometry = closedObject(control.geometry, ["x", "y", "width", "height", "rotation", "layer", "unit"], "geometry");
      if (geometry.unit !== undefined) choice(geometry.unit, ["grid", "pixel", "normalized", "millimeter"], "geometry unit");
      for (const field of ["x", "y", "width", "height", "rotation", "layer"]) if (geometry[field] != null && !isJsonNumber(geometry[field])) fail(`Invalid geometry ${field}`);
      for (const field of ["x", "y"]) if (geometry[field] === null) fail(`Invalid geometry ${field}`);
      for (const field of ["width", "height"]) if (geometry[field] != null && Number(geometry[field]) <= 0) fail("Geometry dimensions must be positive");
      for (const field of ["layer", "rotation"]) if (geometry[field] != null && !isSafeJsonInteger(geometry[field])) fail(`Geometry ${field} must be integral`);
      if (geometry.unit === "normalized") for (const field of ["x", "y", "width", "height"]) if (geometry[field] != null && (Number(geometry[field]) < 0 || Number(geometry[field]) > 1)) fail("Normalized geometry must lie within [0,1]");
    }
    unique(controlCapabilities(control as unknown as HardwareControl).map(capability => capability.capabilityId), "control capabilityId");
    return control as unknown as HardwareControl;
  });
  unique(controls.map(control => control.controlId), "controlId");
  const controlIds = new Set(controls.map(control => control.controlId)), connectionIds = new Set(connections.map(item => item.connectionId));
  const descriptor = { ...raw, controls, capabilities } as unknown as HardwareDeviceDescriptor;
  const validateSources = (items: JsonObject[] | undefined) => { for (const item of items ?? []) if (item.connectionId != null && !connectionIds.has(item.connectionId)) fail("Unresolved source connection"); };
  validateSources(descriptor.sources);
  for (const control of controls) {
    for (const id of [...(control.relatedControlIds ?? []), ...(control.parentControlId == null ? [] : [control.parentControlId])]) if (!controlIds.has(id) || id === control.controlId) fail("Unresolved or self-referencing control");
    validateSources(control.sources);
  }
  for (const [controlId, capability] of [...capabilities.map(capability => [undefined, capability] as const), ...controls.flatMap(control => controlCapabilities(control).map(capability => [control.controlId, capability] as const))]) {
    validateSources(capability.sources);
    if (capability.projection) {
      const ref = capability.projection.source; findHardwareCapability(descriptor, ref);
      if ((ref.controlId ?? undefined) === controlId && ref.capabilityId === capability.capabilityId) fail("Capability projection cannot reference itself");
    }
  }
  if (raw.defaultStatusIndicator != null) {
    capabilityRef(raw.defaultStatusIndicator);
    if (findHardwareCapability(descriptor, raw.defaultStatusIndicator as unknown as { controlId?: string; capabilityId: string }).direction !== "output") fail("Default status indicator must resolve to an output");
  }
  return immutable(cloneJson(raw)) as unknown as HardwareDeviceDescriptor;
}

/** Validate the selected capability, then its core semantics and descriptor limits. */
export function validateHardwareCapabilityPayload(capability: HardwareCapability, kind: "event" | "command" | "state", name: string | undefined, value: JsonValue, schemas = defaultPayloadSchemas): void {
  if (kind === "event" && (capability.direction !== "input" || !capability.access.includes("emits") || !capability.eventTypes?.includes(name ?? ""))) fail("Capability does not emit this event");
  if (kind === "command" && (!["output", "command"].includes(capability.direction) || !capability.commandTypes?.includes(name ?? ""))) fail("Capability does not accept this command");
  if (kind === "state" && capability.direction !== "state") fail("Capability is not state");
  const declaration = kind === "command" ? capability.commandSchema : capability.valueSchema;
  if (declaration) schemas.validate(declaration.schema, value);
  let constrainedValue = value;
  if (capability.family === "dev.deckr.input.button" || capability.family === "dev.deckr.input.touch") {
    const raw = closedObject(value, capability.family.endsWith("touch") ? ["eventType", "direction"] : ["eventType"], "input value");
    if (raw.eventType !== name) fail("Input event type disagrees with its value");
    if (capability.family.endsWith("touch") && name === "swipe" && raw.direction == null) fail("Swipe requires direction");
    if (raw.direction != null && (name !== "swipe" || !["left", "right"].includes(String(raw.direction)))) fail("Invalid touch direction");
  } else if (capability.family === "dev.deckr.input.encoder") {
    const raw = closedObject(value, ["delta", "direction"], "encoder value");
    if (!isSafeJsonInteger(raw.delta) || Number(raw.delta) === 0) fail("Encoder delta must be a nonzero integer");
    if (raw.direction != null && raw.direction !== (Number(raw.delta) > 0 ? "clockwise" : "counterclockwise")) fail("Encoder direction disagrees with delta");
  } else if (kind === "command" && (capability.family === "dev.deckr.device.power" || (capability.family === "dev.deckr.output.raster" && name === "clear"))) {
    closedObject(value, [], "empty command params");
  } else if (kind === "command" && capability.family === "dev.deckr.output.raster") {
    constrainedValue = validateRasterFrame(value);
  }
  for (const constraint of capability.constraints ?? []) {
    const subject = String(constraint.subject);
    let actual: JsonValue | undefined = subject === "value" ? constrainedValue : subject.split(".").reduce<JsonValue | undefined>((node, part) => isJsonObject(node) ? node[part] : undefined, constrainedValue);
    if (actual === undefined) continue;
    const unit = capability.units?.find(item => item.subject === subject);
    if (constraint.unit != null && unit && unit.unit !== constraint.unit) fail("Constraint and descriptor units disagree");
    if (isJsonNumber(actual) && unit) actual = Number(actual) * Number(unit.scale ?? 1);
    if (constraint.value != null && canonicalJson(actual) !== canonicalJson(constraint.value)) fail(`Value violates ${subject} constraint`);
    if (Array.isArray(constraint.values) && constraint.values.length && !constraint.values.some(item => canonicalJson(item) === canonicalJson(actual!))) fail(`Value violates ${subject} enumeration`);
    if (constraint.minimum != null || constraint.maximum != null || constraint.step != null) {
      if (!isJsonNumber(actual) || (constraint.minimum != null && Number(actual) < Number(constraint.minimum)) || (constraint.maximum != null && Number(actual) > Number(constraint.maximum))) fail(`Value violates ${subject} range`);
      if (constraint.step != null) { const quotient = (Number(actual) - Number(constraint.minimum ?? 0)) / Number(constraint.step); if (Math.abs(quotient - Math.round(quotient)) > 1e-9) fail(`Value violates ${subject} step`); }
    }
  }
}

function validateRasterFrame(value: JsonValue): JsonObject {
  const raw = closedObject(value, ["image", "encoding", "width", "height"], "raster frame");
  choice(raw.encoding, ["png", "jpeg"], "raster encoding");
  const text = requireText(raw.image, "image");
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(text)) fail("Invalid base64 image");
  const bytes = Buffer.from(text, "base64");
  if (bytes.toString("base64") !== text) fail("Invalid base64 image");
  let dimensions: { width: number; height: number };
  try {
    if (raw.encoding === "png") {
      if (bytes.length < 24 || !bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) fail("Invalid PNG image");
      if (bytes.readUInt32BE(16) * bytes.readUInt32BE(20) > 16_000_000) fail("Raster image exceeds decode limit");
      dimensions = PNG.sync.read(bytes, { checkCRC: true });
    } else {
      dimensions = jpeg.decode(bytes, { maxResolutionInMP: 16, maxMemoryUsageInMB: 128, tolerantDecoding: false });
    }
  } catch (error) { throw new ValidationError(`Invalid raster image: ${String(error)}`); }
  for (const field of ["width", "height"] as const) if (raw[field] != null && raw[field] !== dimensions[field]) fail(`Raster ${field} disagrees with decoded image`);
  return { ...raw, width: dimensions.width, height: dimensions.height };
}
