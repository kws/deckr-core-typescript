import { validateActionRuntimeMessage } from "./action-contracts.ts";
import type { DeviceRef, PageCommandTarget } from "./actions.ts";
import type { ContractPointer } from "./concord.ts";
import { ValidationError } from "./errors.ts";
import type { JsonObject } from "./json.ts";

export interface PageTargetState {
  actionInstanceId: string; active: boolean; generation: number;
  contract: ContractPointer; runtimeEndpoint: string; runtimeSessionId: string;
  deviceRef: DeviceRef; controlIds: readonly string[]; pageId?: string;
}
export interface PageCommandContext {
  contract: ContractPointer; controllerEndpoint: string; controllerSessionId: string;
  runtimeEndpoint: string; runtimeSessionId: string; localAuthority: boolean; hardwareAuthority: boolean;
  bindings: Readonly<Record<string, PageTargetState>>;
  pageSessions: Readonly<Record<string, PageTargetState>>;
}
function fail(reason: string): never { throw new ValidationError(reason); }
function sameContract(a: ContractPointer | undefined, b: ContractPointer): boolean {
  return !!a && a.contractId === b.contractId && Number(a.generation) === Number(b.generation);
}
/** Check current observations again at commit, while the application serializes
 * transitions. This helper acquires no authority and changes no page state.
 */
export function validatePageCommand(value: unknown, context: PageCommandContext): PageTargetState {
  const message = validateActionRuntimeMessage(value);
  const name = message.body.name;
  if (!["open_page", "replace_page", "close_page"].includes(String(name))) fail("Message is not a page command");
  if (!context.localAuthority || !context.hardwareAuthority || !sameContract(message.contract, context.contract) ||
    message.sender !== context.runtimeEndpoint || message.senderSessionId !== context.runtimeSessionId ||
    message.recipient.targetType !== "endpoint" || message.recipient.endpoint !== context.controllerEndpoint ||
    message.recipientSessionId !== context.controllerSessionId) fail("Page command has no current service or hardware authority");
  const params = message.body.params as JsonObject;
  const target = params.target as unknown as PageCommandTarget;
  const records = target.kind === "binding" ? context.bindings : context.pageSessions;
  const id = target.kind === "binding" ? target.bindingId : target.pageSessionId;
  const current = Object.hasOwn(records, id) ? records[id] : undefined;
  if (!current || !current.active || Number(current.generation) !== Number(target.generation) ||
    current.runtimeEndpoint !== context.runtimeEndpoint || current.runtimeSessionId !== context.runtimeSessionId ||
    !sameContract(current.contract, context.contract)) fail("Page target is stale, inactive, or belongs to another agreement");
  if (name !== "close_page") {
    const descriptor = params.descriptor as JsonObject;
    if (!(descriptor.bindings as JsonObject[]).every(child => current.controlIds.includes(String(child.controlId)))) fail("Page child is outside the retained device and claim scope");
    if (name === "replace_page" && descriptor.pageId !== current.pageId) fail("Replacement must preserve the retained page ID");
  }
  return current;
}
