// Opt-in diagnostic against a disposable broker, never an application bucket.
// DECKR_NATS_TEST_URL=nats://127.0.0.1:4222 node --expose-gc tests/nats-performance.ts
import { randomUUID } from "node:crypto";
import { setImmediate } from "node:timers/promises";
import type { NatsConnection } from "nats";
import { connectNats, NatsStateStore } from "../src/nats.ts";
import { systemScheduler } from "../src/runtime.ts";
import type { StateObservation } from "../src/state.ts";

if (!process.env.DECKR_NATS_TEST_URL) throw new Error("Set DECKR_NATS_TEST_URL to a disposable JetStream broker");
const connection = await connectNats({ servers: process.env.DECKR_NATS_TEST_URL }) as NatsConnection;
const bucket = `perf_${randomUUID().replaceAll("-", "")}`;
const manager = await connection.jetstreamManager();
const requests: Record<string, number> = {};
const originalRequest = connection.request.bind(connection);
connection.request = (...args: Parameters<NatsConnection["request"]>) => {
  // Count API request categories, not per-key identifiers.
  const subject = args[0].split(".").slice(0, 5).join(".");
  requests[subject] = (requests[subject] ?? 0) + 1;
  return originalRequest(...args);
};
let timers = 0;
const scheduler = { ...systemScheduler, schedule(ms: number, callback: () => void) {
  timers++; return systemScheduler.schedule(ms, callback);
} };
let iterator: AsyncIterator<StateObservation> | undefined;
async function initialValue(): Promise<WeakRef<object>> {
  const result = await iterator!.next();
  if (result.value?.operation !== "snapshot") throw new Error("Expected initial snapshot");
  return new WeakRef(result.value.entries[0]!.value);
}
const latencies: number[] = [];
try {
  const store = await NatsStateStore.open({ connection, bucket, scheduler });
  for (let i = 0; i < 1000; i++) await store.put(`key.${i}`, { payload: "x".repeat(1024) });
  const beforeRequests = { ...requests }, beforeTimers = timers;
  const started = performance.now();
  iterator = store.observe()[Symbol.asyncIterator]();
  const replay = await initialValue();
  const snapshotMs = performance.now() - started;
  for (let i = 0; i < 1000; i++) {
    const start = performance.now();
    const pending = iterator.next();
    const entry = await store.put("key.0", { value: i });
    const observed = await pending;
    if (observed.value?.revision !== entry.revision) throw new Error("Observation revision differs from committed write");
    latencies.push(performance.now() - start);
  }
  // Advancing the generator also releases the previously yielded snapshot.
  // WeakRef collection is diagnostic evidence, not a deterministic unit test.
  for (let i = 0; i < 3; i++) { await setImmediate(); global.gc?.(); }
  await setImmediate();
  latencies.sort((a, b) => a - b);
  const activeConsumers = (await manager.streams.info(`KV_${bucket}`)).state.consumer_count;
  const observationRequests = Object.fromEntries(Object.entries(requests).map(([key, count]) => [key, count - (beforeRequests[key] ?? 0)]).filter(([, count]) => count));
  const result = { node: process.version, forcedGC: !!global.gc, snapshotMs, updates: latencies.length,
    p50: latencies[499], p95: latencies[949], p99: latencies[989],
    replayValueCollected: !replay.deref(), adapterTimers: timers - beforeTimers, observationRequests,
    activeConsumers, memory: process.memoryUsage() };
  await iterator.return?.();
  const closedConsumers = (await manager.streams.info(`KV_${bucket}`)).state.consumer_count;
  console.log(JSON.stringify({ ...result, closedConsumers }, null, 2));
} finally {
  await iterator?.return?.();
  await manager.streams.delete(`KV_${bucket}`).catch(() => {});
  connection.request = originalRequest;
  await connection.close();
}
