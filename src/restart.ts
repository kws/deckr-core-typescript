import { readFileSync } from "node:fs";
import { isAbsolute } from "node:path";
import { endpointAddress } from "./endpoint.ts";
import { ValidationError } from "./errors.ts";
import { isSafeJsonInteger, parseJson, requireText } from "./json.ts";
import { closedObject } from "./validation.ts";

export const RESTART_HANDOFF_SCHEMA = "dev.deckr.runtime.restart_handoff.v1";

/** Trusted host assertion after confirmed exit. Parsing alone does not prove death. */
export class VerifiedRestart {
  readonly instanceId: string;
  private readonly participants = new Set<string>();

  constructor(value: unknown, expectedInstanceId: string) {
    const raw = closedObject(value, ["schema", "instanceId", "predecessors"], "restart handoff");
    if (raw.schema !== RESTART_HANDOFF_SCHEMA) throw new ValidationError("Invalid restart handoff schema");
    this.instanceId = requireText(raw.instanceId, "instanceId");
    if (this.instanceId !== requireText(expectedInstanceId, "expectedInstanceId")) throw new ValidationError("Restart handoff belongs to a different instance");
    if (!Array.isArray(raw.predecessors) || raw.predecessors.length < 1 || raw.predecessors.length > 256) throw new ValidationError("Restart predecessors must contain 1 to 256 lifetimes");
    const lifetimes = new Set<string>();
    for (const entry of raw.predecessors) {
      const predecessor = closedObject(entry, ["predecessorId", "exitCode", "participants"], "restart predecessor");
      const lifetime = requireText(predecessor.predecessorId, "predecessorId");
      if (lifetimes.has(lifetime)) throw new ValidationError("Duplicate restart predecessor");
      lifetimes.add(lifetime);
      if (!isSafeJsonInteger(predecessor.exitCode) || Number(predecessor.exitCode) < -2147483648 || Number(predecessor.exitCode) > 2147483647) throw new ValidationError("Restart exitCode must be a signed 32-bit integer");
      if (!Array.isArray(predecessor.participants) || predecessor.participants.length < 1 || predecessor.participants.length > 256) throw new ValidationError("Restart participants must contain 1 to 256 identities");
      for (const value of predecessor.participants) {
        const row = closedObject(value, ["endpoint", "sessionId"], "restart participant");
        const endpoint = endpointAddress(requireText(row.endpoint, "endpoint")), session = requireText(row.sessionId, "sessionId");
        const identity = JSON.stringify([endpoint, session]);
        if (this.participants.has(identity)) throw new ValidationError("Duplicate restart participant");
        this.participants.add(identity);
      }
    }
    Object.freeze(this);
  }
  contains(endpoint: string, sessionId: string): boolean {
    return this.participants.has(JSON.stringify([endpoint, sessionId]));
  }
}

/** Only use a supervisor-controlled file; the caller authenticates that channel. */
export function loadRestartHandoff(path: string, instanceId: string): VerifiedRestart | undefined {
  if (!isAbsolute(path)) throw new ValidationError("Restart handoff path must be absolute");
  let bytes: Buffer;
  try { bytes = readFileSync(path); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  if (bytes.length > 262144) throw new ValidationError("Restart handoff is too large");
  return new VerifiedRestart(parseJson(bytes.toString("utf8")), instanceId);
}

export function restartHandoffFromEnv(): VerifiedRestart | undefined {
  const path = process.env.DECKR_RESTART_HANDOFF;
  if (path === undefined) return undefined;
  const instance = process.env.DECKR_INSTANCE_ID;
  if (!instance) throw new ValidationError("DECKR_INSTANCE_ID is required with DECKR_RESTART_HANDOFF");
  return loadRestartHandoff(path, instance);
}
