import { parseEndpointAddress } from "./endpoint.ts";
import { ValidationError } from "./errors.ts";
import { validateHardwareMessageBody } from "./hardware-messages.ts";
import { validateDeckrMessage, type DeckrMessage } from "./lanes.ts";
import { validateServiceMessageBody } from "./services.ts";
import { validateActionRuntimeMessage } from "./action-contracts.ts";

export type LaneValidator = (message: DeckrMessage) => void;

/** Core lane structure. Managed profiles additionally check authority and domain scope. */
export function validateLaneMessage(value: unknown, extension?: LaneValidator): DeckrMessage {
  const message = validateDeckrMessage(value);
  const sender = parseEndpointAddress(message.sender).family;
  const recipient = message.recipient;
  const family = recipient.targetType === "endpoint" ? parseEndpointAddress(recipient.endpoint).family : recipient.endpointFamily;
  if (message.lane === "services") {
    if (message.messageType !== "serviceMessage" || !message.contract || recipient.targetType !== "endpoint" ||
      !["controller", "action_provider", "service"].includes(sender) || !["controller", "action_provider", "service"].includes(family)) throw new ValidationError("Invalid services lane message");
    validateServiceMessageBody(message.body);
    if (message.body.serviceNamespace === "dev.deckr.action_runtime.provider") validateActionRuntimeMessage(message);
  } else if (message.lane === "hardware_messages") {
    if (!message.contract || !["controller", "hardware_manager"].includes(sender) || !["controller", "hardware_manager"].includes(family) ||
      (recipient.targetType === "broadcast" && (recipient.scope !== "controllers" || family !== "controller"))) throw new ValidationError("Invalid hardware lane message");
    validateHardwareMessageBody(message.messageType, message.body);
  } else if (!extension) throw new ValidationError(`No contract registered for lane ${message.lane}`);
  extension?.(message);
  return message;
}
