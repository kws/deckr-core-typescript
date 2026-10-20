import assert from "node:assert/strict";
import test from "node:test";
import { NatsStateStore } from "../src/nats.ts";
import { MaterializedStateSource } from "../src/source.ts";
import { AsyncQueue } from "../src/runtime.ts";
import { flush, TestClock } from "./runtime-support.ts";

type WireMessage = { subject: string; reply: string; data: Uint8Array; headers: { code: number; get(name: string): string }; respond(): void };

class Broker {
  config: Record<string, unknown> = {};
  highWater = 0;
  initial: { key: string; revision: number; value: Record<string, unknown> }[] = [];
  subscribed = 0;
  deleted = 0;
  failDeletion = false;
  consumerConfig: Record<string, unknown> = {};
  updates: number[] = [];
  deletes: { subject: string; revision: string; operation: string }[] = [];
  statuses = new Set<AsyncQueue<{ type: string }>>();
  callback?: (error: unknown, message: WireMessage) => void;
  onReplay?: () => void;
  sequence = 0;
  deletionGate?: Promise<void>;
  jetstream() {
    return { publish: async (subject: string, _data: Uint8Array, options: { headers: { get(name: string): string } }) => {
      this.deletes.push({ subject, revision: options.headers.get("Nats-Expected-Last-Subject-Sequence"), operation: options.headers.get("KV-Operation") });
      return { seq: ++this.highWater };
    }, views: { kv: async () => ({
      status: async () => ({ streamInfo: { config: this.config, state: { last_seq: this.highWater } } }),
      update: async (_key: string, _value: string, revision: number) => {
        assert.equal(typeof revision, "number"); this.updates.push(revision); return revision + 1;
      },
    }) } };
  }
  jetstreamManager() {
    return {
      streams: { add: async (config: Record<string, unknown>) => { this.config = config; } },
      consumers: {
        add: async (_stream: string, config: Record<string, unknown>) => {
          this.consumerConfig = config; this.sequence = 0;
          if (this.onReplay) this.onReplay();
          else this.initial.forEach((item, index) => this.emit(item.key, item.revision, item.value, this.initial.length - index - 1));
          return { name: "consumer", num_pending: 0, delivered: { consumer_seq: this.sequence } };
        },
        delete: async () => { this.deleted++; if (this.failDeletion) throw new Error("broker unavailable"); await this.deletionGate; },
      },
    };
  }
  subscribe(_subject: string, options: { callback: (error: unknown, message: WireMessage) => void }) {
    this.callback = options.callback; this.subscribed++;
    return { unsubscribe: () => { this.subscribed--; } };
  }
  status() {
    const queue = new AsyncQueue<{ type: string }>(256, false, () => { this.statuses.delete(queue); });
    this.statuses.add(queue); return queue;
  }
  emit(key: string, revision: number, value: Record<string, unknown>, pending = 0, sequence = ++this.sequence): void {
    this.callback!(null, { subject: `$KV.review.${key}`, reply: `$JS.ACK.KV_review.consumer.1.${revision}.${sequence}.0.${pending}`,
      data: new TextEncoder().encode(JSON.stringify(value)), headers: { code: 0, get: () => "" }, respond() {} });
  }
  heartbeat(lastConsumer = this.sequence): void {
    this.callback!(null, { subject: "heartbeat", reply: "", data: new Uint8Array(), headers: {
      code: 100, get: name => name === "Nats-Last-Consumer" ? String(lastConsumer) : "",
    }, respond() {} });
  }
}

test("NATS CAS passes the numeric revision required by the installed client API", async () => {
  const broker = new Broker();
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  assert.equal((await store.update("key", {}, { revision: 7 })).revision, 8);
  assert.deepEqual(broker.updates, [7]);
});

test("failed consumer deletion is retried before a replacement snapshot and lists fail closed", async () => {
  const broker = new Broker();
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  const first = store.observe()[Symbol.asyncIterator]();
  assert.equal((await first.next()).value.operation, "snapshot");
  broker.failDeletion = true;
  await first.return?.();
  assert.equal(broker.deleted, 1);
  await assert.rejects(store.items(), /lost currentness|cleanup is pending/);
  broker.failDeletion = false;
  const attempted = broker.deleted;
  const replacement = store.observe()[Symbol.asyncIterator]();
  assert.equal((await replacement.next()).value.operation, "snapshot");
  assert.equal(broker.deleted, attempted + 1, "delete the old owned name before opening a replacement");
  await replacement.return?.();
  assert.equal(broker.subscribed, 0);
});

test("NATS deletion preserves the guard and returns its committed revision", async () => {
  const broker = new Broker(); broker.highWater = 7;
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  assert.equal(await store.delete("key", { revision: 7 }), 8);
  assert.deepEqual(broker.deletes, [{ subject: "$KV.review.key", revision: "7", operation: "DEL" }]);
});

test("NATS replay separates the captured high-water snapshot from later observations", async () => {
  const broker = new Broker(); broker.highWater = 5;
  broker.onReplay = () => {
    broker.emit("old", 3, { value: "old" }, 1);
    broker.emit("new", 6, { value: "new" }, 0);
  };
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  const abort = new AbortController();
  const watch = store.observe("", { signal: abort.signal })[Symbol.asyncIterator]();
  const initial = (await watch.next()).value;
  assert.equal(initial.operation, "snapshot");
  assert.equal(initial.revision, 5);
  assert.deepEqual(initial.entries.map((entry: { key: string }) => entry.key), ["old"]);
  const later = initial.afterBarrier[0];
  assert.equal(later.operation, "put"); assert.equal(later.entry.key, "new");
  assert.equal(broker.consumerConfig.deliver_policy, "last_per_subject");
  abort.abort(); await watch.return?.();
  assert.equal(broker.subscribed, 0); assert.equal(broker.deleted, 1); assert.equal(broker.statuses.size, 0);
});

test("replay-time writes are applied before the source publishes its first current snapshot", async () => {
  const broker = new Broker(); broker.highWater = 5;
  broker.onReplay = () => {
    broker.emit("old", 3, { value: "old" }, 1);
    broker.emit("changed-during-replay", 6, { value: "new" }, 0);
  };
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  const source = new MaterializedStateSource(store);
  const snapshots: string[][] = [];
  source.subscribe(state => { if (state.current) snapshots.push(state.entries.map(entry => entry.key)); });
  await source.start();
  assert.deepEqual(snapshots, [["old", "changed-during-replay"]]);
  assert.equal(source.snapshot.revision, 6);
  await source.close();
});

test("NATS empty stores establish a ready snapshot and cancellation wakes an idle watch", async () => {
  const broker = new Broker();
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  const abort = new AbortController();
  const watch = store.observe("", { signal: abort.signal })[Symbol.asyncIterator]();
  assert.deepEqual((await watch.next()).value, { operation: "snapshot", entries: [], revision: 0 });
  const pending = watch.next(); await flush(); abort.abort();
  assert.equal((await pending).done, true);
  assert.equal(broker.subscribed, 0);
});

test("missed NATS heartbeat makes the source stale before atomic recovery removes absent keys", async () => {
  const clock = new TestClock(); const broker = new Broker();
  broker.highWater = 1; broker.initial = [{ key: "expired", revision: 1, value: { value: 1 } }];
  const store = await NatsStateStore.open({ connection: broker, bucket: "review", scheduler: clock });
  const source = new MaterializedStateSource(store, clock); await source.start();
  assert.ok(source.get("expired"));
  await clock.advance(10_000);
  assert.equal(source.snapshot.current, false);
  broker.initial = []; broker.highWater = 2;
  await clock.advance(100);
  assert.equal(source.snapshot.current, true);
  assert.equal(source.get("expired"), null);
  assert.equal(source.snapshot.generation, 2);
  await source.close(); assert.equal(broker.subscribed, 0);
});

test("NATS consumer sequence loss cannot be hidden by a connected transport", async () => {
  const clock = new TestClock(); const broker = new Broker();
  const store = await NatsStateStore.open({ connection: broker, bucket: "review", scheduler: clock });
  const source = new MaterializedStateSource(store, clock); await source.start();
  broker.heartbeat(4); await flush();
  assert.equal(source.snapshot.current, false);
  await source.close();
});

test("NATS reconnect explicitly invalidates the source", async () => {
  const clock = new TestClock(); const broker = new Broker();
  const store = await NatsStateStore.open({ connection: broker, bucket: "review", scheduler: clock });
  const source = new MaterializedStateSource(store, clock); await source.start();
  for (const statuses of broker.statuses) statuses.push({ type: "reconnect" });
  await flush(); assert.equal(source.snapshot.current, false);
  await source.close();
});

test("watch loss is visible before unavailable consumer cleanup finishes", async (t) => {
  for (const failure of ["disconnect", "heartbeat", "continuity"] as const) {
    await t.test(failure, async () => {
      const clock = new TestClock(); const broker = new Broker();
      let release!: () => void;
      broker.deletionGate = new Promise<void>(resolve => { release = resolve; });
      const store = await NatsStateStore.open({ connection: broker, bucket: "review", scheduler: clock });
      const source = new MaterializedStateSource(store, clock);
      await source.start();
      if (failure === "disconnect") for (const statuses of broker.statuses) statuses.push({ type: "disconnect" });
      else if (failure === "heartbeat") await clock.advance(10_000);
      else broker.heartbeat(99);
      await flush();
      assert.equal(source.snapshot.current, false, "Cleanup cannot delay stale authority notification");
      assert.equal(broker.deleted, 1);
      assert.throws(() => source.get("anything"), /Connection|heartbeat|continuity/);
      release();
      await source.close();
      assert.equal(broker.subscribed, 0);
    });
  }
});

test("malformed records remain observations without poisoning unrelated source currentness", async () => {
  const broker = new Broker();
  const store = await NatsStateStore.open({ connection: broker, bucket: "review" });
  const source = new MaterializedStateSource(store); await source.start();
  broker.callback!(null, { subject: "$KV.review.bad", reply: "$JS.ACK.KV_review.consumer.1.1.1.0.0", data: new TextEncoder().encode("{bad"), headers: { code: 0, get: () => "" }, respond() {} });
  await flush();
  assert.equal(source.snapshot.current, true);
  assert.ok(source.get("bad")!.parseError);
  await source.close();
});
