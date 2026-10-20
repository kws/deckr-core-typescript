/** Explicit core capability schemas for descriptor-driven consumers in every language. */
import type { HardwareCapabilitySchema } from "./hardware-descriptors.ts";

export function buttonMomentaryValueSchema(): HardwareCapabilitySchema {
  return { schemaId: "dev.deckr.value.input.button.momentary.v1", schema: {
    type: "object", required: ["eventType"], properties: { eventType: { enum: ["down", "up"] } }, additionalProperties: false,
  } };
}
export function encoderRelativeValueSchema(): HardwareCapabilitySchema {
  return { schemaId: "dev.deckr.value.input.encoder.relative.v1", schema: {
    type: "object", required: ["delta"], properties: { delta: { type: "integer", not: { const: 0 } }, direction: { enum: ["clockwise", "counterclockwise"] } }, additionalProperties: false,
  } };
}
export function touchGestureValueSchema(): HardwareCapabilitySchema {
  return { schemaId: "dev.deckr.value.input.touch.gesture.v1", schema: { oneOf: [
    { type: "object", required: ["eventType"], properties: { eventType: { const: "tap" } }, additionalProperties: false },
    { type: "object", required: ["eventType", "direction"], properties: { eventType: { const: "swipe" }, direction: { enum: ["left", "right"] } }, additionalProperties: false },
  ] } };
}
export function rasterBitmapCommandSchema(options: { width?: number; height?: number } = {}): HardwareCapabilitySchema {
  const dimensions = Object.fromEntries(Object.entries(options).map(([axis, value]) => {
    if (!Number.isInteger(value) || value <= 0) throw new Error(`Invalid raster ${axis}`);
    return [axis, { const: value }];
  }));
  return { schemaId: "dev.deckr.command.output.raster.bitmap.v1", schema: { oneOf: [
    { type: "object", required: ["image", "encoding"], properties: { image: { type: "string", contentEncoding: "base64" }, encoding: { enum: ["jpeg", "png"] }, ...dimensions }, additionalProperties: false },
    { type: "object", maxProperties: 0 },
  ] } };
}
