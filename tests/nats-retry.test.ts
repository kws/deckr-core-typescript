import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import type { NatsConnection, ConnectionOptions } from "nats";
import { connectWithRetry, ReconnectPolicy } from "../src/nats-retry.ts";
import { AsyncQueue } from "../src/runtime.ts";

function client() {
  const server = { lastConnect: 1, reconnects: 0 };
  const statuses = new AsyncQueue<{ type: string }>();
  const state = { closed: false, protocol: { servers: { getServers: () => [server] } }, status: () => statuses,
    close: async () => { state.closed = true; statuses.finish(); } };
  return { state, server, connection: state as unknown as NatsConnection };
}

test("reconnect delay counts failed attempts, not delay callbacks, and resets", () => {
  let now = 0;
  const policy = new ReconnectPolicy(5000, () => now, () => 1);
  assert.equal(policy.delay(0), 1000);
  now = 400; assert.equal(policy.delay(0), 600);
  now = 1000; assert.equal(policy.delay(0), 0);
  assert.equal(policy.delay(1), 2000);
  now += 2000; assert.equal(policy.delay(1), 0);
  assert.equal(policy.delay(2), 4000);
  now += 4000; assert.equal(policy.delay(2), 0);
  assert.equal(policy.delay(3), 5000);
  policy.reset(); assert.equal(policy.delay(0), 1000);
});

test("initial connection outlasts the old attempt limit and shutdown closes the established client", async () => {
  let attempts = 0;
  const fake = client(), abort = new AbortController();
  const connected = await connectWithRetry(async options => {
    assert.equal(options.maxReconnectAttempts, -1);
    assert.equal(options.waitOnFirstConnect, false);
    assert.equal(options.timeout, 5000);
    if (++attempts <= 12) throw new Error("unavailable");
    return fake.connection;
  }, { reconnectMaxIntervalSeconds: 0.001, signal: abort.signal });
  assert.equal(attempts, 13); assert.equal(connected, fake.connection);
  abort.abort(); await Promise.resolve(); assert.equal(fake.state.closed, true);
});

test("initial cancellation rejects immediately and closes a late dial success", async () => {
  let complete!: (value: NatsConnection) => void;
  const fake = client(), abort = new AbortController();
  const connecting = connectWithRetry(() => new Promise(resolve => { complete = resolve; }), { signal: abort.signal });
  abort.abort(); await assert.rejects(connecting, /cancelled/);
  complete(fake.connection); await Promise.resolve(); assert.equal(fake.state.closed, true);
});

test("the installed nats.js reconnect loop waits before every attempt and survives more than ten failures", async () => {
  const require = createRequire(import.meta.url);
  const { ProtocolHandler } = require("nats/lib/nats-base-client/protocol.js");
  const fake = client();
  let options!: ConnectionOptions;
  await connectWithRetry(async supplied => { options = supplied; return fake.connection; }, { reconnectMaxIntervalSeconds: 0.004 });
  let attempts = 0;
  let last = performance.now();
  const loop = {
    options, servers: fake.state.protocol.servers, connectedOnce: true, _closed: false, abortReconnect: false,
    selectServer: () => fake.server,
    _doDial: async () => {
      const now = performance.now();
      assert.ok(now - last >= 1, "the first and subsequent dials must wait");
      last = now;
      if (++attempts <= 12) throw new Error("unavailable");
    },
  };
  await ProtocolHandler.prototype.dodialLoop.call(loop);
  assert.equal(attempts, 13);
  await fake.connection.close();
});
