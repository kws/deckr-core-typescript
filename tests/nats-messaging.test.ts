import assert from "node:assert/strict";
import test from "node:test";
import type { Msg, MsgHdrs, SubscriptionOptions } from "nats";
import { EndpointSession, MemoryMessageTransport } from "../src/messaging.ts";
import { NatsMessageTransport } from "../src/nats.ts";
import { entitySubject } from "../src/lanes.ts";

class Connection {
  readonly bus = new MemoryMessageTransport();
  readonly events: string[] = [];
  subscribe(subject: string, options: SubscriptionOptions) {
    this.events.push(`subscribe ${subject}`);
    const subscription = this.bus.subscribe(subject);
    const closed = (async () => {
      for await (const message of subscription.messages) options.callback!(null, {
        subject: message.subject, data: message.data, reply: message.reply ?? "",
        headers: { get: (name: string) => (message.headers as Record<string, string> | undefined)?.[name] ?? "" },
      } as Msg);
    })();
    return { unsubscribe: () => subscription.close(), closed };
  }
  publish(subject: string, data: Uint8Array, options: { headers: MsgHdrs; reply?: string }) {
    this.events.push(`publish ${subject}`);
    void this.bus.publish(subject, data, { reply: options.reply, headers: Object.fromEntries([...options.headers].map(([name, values]) => [name, values[0]!])) });
  }
}

test("NATS endpoint delivery arms the inbox before publishing and retains the shared connection", async () => {
  const connection = new Connection();
  const transport = new NatsMessageTransport(connection);
  const controller = new EndpointSession({ endpoint: "controller:main", transport });
  const service = new EndpointSession({ endpoint: "service:music", transport });
  const listening = service.lane("services").watch()[Symbol.asyncIterator]();
  const pending = controller.lane("services").request({ recipient: service.endpoint, recipientSessionId: service.sessionId,
    messageType: "serviceMessage", subject: entitySubject("service"), contract: { contractId: "agreement", generation: 1 },
    body: { serviceNamespace: "example.music", name: "play", intent: "command", exchangePattern: "request_reply", params: {} },
  });
  const received = (await listening.next()).value!;
  const inboxSubscription = connection.events.findIndex(event => event.startsWith("subscribe _INBOX"));
  assert.ok(inboxSubscription >= 0 && inboxSubscription < connection.events.findIndex(event => event.startsWith("publish ")));
  await received.reply({ ...received.message.body, status: "ok", result: {} });
  assert.equal((await pending).body.status, "ok");
  assert.equal(controller.subscriptionCount, 0);
  await controller.close();
  assert.equal(connection.bus.subscriptionCount, 1, "Closing one endpoint retains the other endpoint subscription");
  await listening.return?.(); await service.close(); transport.close();
  assert.equal(connection.bus.subscriptionCount, 0);
  const stillUsable = connection.bus.subscribe("host.owned"); stillUsable.close();
  connection.bus.close();
});
