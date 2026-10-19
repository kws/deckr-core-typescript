import { Ajv as Ajv7, type ValidateFunction } from "ajv";
import { Ajv2019 } from "ajv/dist/2019.js";
import { Ajv2020 } from "ajv/dist/2020.js";
import formats from "ajv-formats";
import { ValidationError } from "./errors.ts";
import { canonicalJson, cloneJson, requireJsonObject, type JsonObject } from "./json.ts";
import { assertJsonValue } from "./validation.ts";

/** Validates declared schemas without loading remote references or changing data.
 * Hosts supply any referenced schemas through this registry, not filesystem paths.
 */
export class PayloadSchemas {
  private readonly references: Record<string, JsonObject>;
  private readonly compiled = new Map<string, ValidateFunction>();
  constructor(references: Record<string, JsonObject> = {}) { this.references = cloneJson(references); }

  compile(schema: JsonObject): ValidateFunction {
    assertJsonValue(schema);
    requireJsonObject(schema, "payload schema");
    const key = canonicalJson(schema);
    const cached = this.compiled.get(key);
    if (cached) return cached;
    const dialect = schema.$schema ?? "https://json-schema.org/draft/2020-12/schema";
    const Constructor = dialect === "http://json-schema.org/draft-07/schema#" || dialect === "https://json-schema.org/draft-07/schema" ? Ajv7
      : dialect === "https://json-schema.org/draft/2019-09/schema" ? Ajv2019
      : dialect === "https://json-schema.org/draft/2020-12/schema" ? Ajv2020 : null;
    if (!Constructor) throw new ValidationError(`Unsupported JSON Schema dialect ${String(dialect)}`);
    try {
      // JSON Schema permits annotations and vocabulary extensions. No defaults,
      // coercion, removal of fields, asynchronous validation, or remote fetching.
      const ajv = new Constructor({ strict: false, validateFormats: true, ownProperties: true });
      const addFormats = formats as unknown as (instance: Ajv7) => void;
      addFormats(ajv);
      for (const [id, reference] of Object.entries(this.references)) ajv.addSchema(reference, id);
      const validate = ajv.compile(cloneJson(schema));
      if ("$async" in validate && validate.$async) throw new ValidationError("Asynchronous payload schemas are unsupported");
      if (this.compiled.size >= 256) this.compiled.delete(this.compiled.keys().next().value!);
      this.compiled.set(key, validate);
      return validate;
    } catch (error) {
      throw new ValidationError(`Invalid or unresolved payload schema: ${String(error)}`);
    }
  }

  validate(schema: JsonObject, value: unknown): void {
    assertJsonValue(value);
    const validate = this.compile(schema);
    if (!validate(value)) throw new ValidationError(`Payload does not match its schema: ${JSON.stringify(validate.errors)}`);
  }
}

export const defaultPayloadSchemas = new PayloadSchemas();
