// Opt-in diagnostic: node --expose-gc tests/performance.ts
// Run unchanged before/after a change. Forced GC is diagnostic only.
import { monitorEventLoopDelay, PerformanceObserver, constants, type PerformanceEntry } from "node:perf_hooks";
import { setImmediate } from "node:timers/promises";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY } from "../src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import { ManagedServiceClient, ManagedServiceProvider, type ServiceProtocol } from "../src/services.ts";
import { MemoryStateStore, type StateObservation } from "../src/state.ts";
import { MaterializedStateSource } from "../src/source.ts";
import { AsyncQueue } from "../src/runtime.ts";
import { buildMessage, entitySubject } from "../src/lanes.ts";
import { EndpointSession, MemoryMessageTransport } from "../src/messaging.ts";

const iterations = Number(process.env.DECKR_PERF_ITERATIONS ?? 1000);
if (!Number.isSafeInteger(iterations) || iterations < 1000) throw new Error("DECKR_PERF_ITERATIONS must be an integer >= 1000");
const sampleEvery = Math.floor(iterations / 4);
const gc = { count: 0, major: 0, milliseconds: 0 };
const observer = new PerformanceObserver(list => {
  for (const item of list.getEntries() as (PerformanceEntry & { detail: { kind: number } })[]) {
    gc.count++; gc.milliseconds += item.duration;
    if (item.detail.kind === constants.NODE_PERFORMANCE_GC_MAJOR) gc.major++;
  }
});
observer.observe({ entryTypes: ["gc"] });
const delay = monitorEventLoopDelay({ resolution: 10 }); delay.enable();
const samples: object[] = [];
async function sample(phase: string, resources: object = {}) {
  await setImmediate();
  samples.push({ phase, forcedGC: false, ...process.memoryUsage(), gc: { ...gc }, ...resources });
  if (global.gc) {
    global.gc(); await setImmediate(); await setImmediate();
    samples.push({ phase, forcedGC: true, ...process.memoryUsage(), gc: { ...gc }, ...resources });
  }
}
const protocol: ServiceProtocol = {
  namespace: "perf.player", featureId: "perf.player", advertisementProfile: "perf.player.ad.v1", useProfile: "perf.player.use.v1",
  operations: { play: {} }, messages: { play: { operation: "play", direction: "consumer_to_service", intent: "command", exchangePattern: "request_reply" } },
  viewFamilies: { status: { storeName: "perf_views", keyPrefix: "status.", writer: "service" } },
};
class CountStore extends MemoryStateStore {
  opens = 0; closes = 0;
  override async *observe(prefix = "", options: { signal?: AbortSignal } = {}) {
    this.opens++;
    try { yield* super.observe(prefix, options); } finally { this.closes++; }
  }
}
const views = new CountStore();
for (let i = 0; i < 1000; i++) await views.put(`unrelated.${i}`, { value: "x".repeat(256) });
const beacon = new BeaconService(new BeaconDiscovery(new MemoryStateStore({ policy: BEACON_ADVERTISEMENT_STORE_POLICY })));
const concord = new ConcordService(new ConcordCoordinator(new MemoryStateStore(), new MemoryStateStore({ policy: CONCORD_TOKEN_STORE_POLICY })));
const shared = { protocol, beacon, concord, stateFor: () => views };
const transport = new MemoryMessageTransport();
const consumer = new EndpointSession({ endpoint: "controller:perf", sessionId: "consumer", transport });
const service = new EndpointSession({ endpoint: "service:perf", sessionId: "provider", transport });
const provider = new ManagedServiceProvider({ ...shared, serviceId: "perf", endpoint: service.lane("services"), handle: () => ({ result: {} }) });
const client = new ManagedServiceClient({ ...shared, endpoint: consumer.lane("services"), operations: ["play"], views: ["status"] });
const keepAlive = setInterval(() => {}, 1000);
const latencies: number[] = [];
const results: Record<string, unknown> = {};
try {
  await provider.start(); const lease = await client.ready();
  await sample("ready");
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    const before = performance.now(); await client.read({ storeName: "perf_views", key: "status.current" });
    latencies.push(performance.now() - before);
    if ((i + 1) % sampleEvery === 0) await sample(`reads-${i + 1}`, { observationOpens: views.opens, observationCloses: views.closes });
  }
  latencies.sort((a, b) => a - b);
  results.reads = { count: latencies.length, milliseconds: performance.now() - start,
    p50: latencies[Math.floor(iterations * .5) - 1], p95: latencies[Math.floor(iterations * .95) - 1], p99: latencies[Math.floor(iterations * .99) - 1],
    observationOpens: views.opens, observationCloses: views.closes };
  let completed = 0;
  try {
    for (let i = 0; i < iterations; i++) {
      const context = await provider.operationContext(buildMessage({
        lane: "services", sender: "controller:perf", senderSessionId: "consumer", recipient: "service:perf", recipientSessionId: "provider",
        messageType: "serviceMessage", contract: { contractId: lease.agreement.contract.contractId, generation: 1 },
        subject: entitySubject("service", { serviceId: "perf", namespace: protocol.namespace, name: "play" }),
        body: { serviceNamespace: protocol.namespace, name: "play", intent: "command", exchangePattern: "request_reply", params: {} },
      }));
      const writer = await context.viewWriter("perf_views");
      await writer.put({ storeName: "perf_views", key: "status.current" }, { sequence: i });
      completed++;
      if (completed % sampleEvery === 0) await sample(`writes-${completed}`, { observationOpens: views.opens, observationCloses: views.closes, storedViews: (await views.items("status.")).length });
    }
    results.writers = { completed };
  } catch (error) { results.writers = { completed, failure: String(error) }; }
  const requestStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    const reply = await client.request("play");
    if (reply.status !== "ok") throw new Error(`Request failed: ${JSON.stringify(reply)}`);
    if ((i + 1) % sampleEvery === 0) await sample(`requests-${i + 1}`, { subscriptions: consumer.subscriptionCount });
  }
  results.requests = { count: iterations, milliseconds: performance.now() - requestStart, subscriptions: consumer.subscriptionCount };
  await sample("before-close", { observationOpens: views.opens, observationCloses: views.closes });
} finally {
  await client.close(); await provider.close(); await beacon.close(); await concord.close();
  await consumer.close(); await service.close(); transport.close();
  clearInterval(keepAlive);
}
await sample("closed", { observationOpens: views.opens, observationCloses: views.closes, storedViews: (await views.items("status.")).length });
for (const size of [100, 1000, 10000]) {
  const queue = new AsyncQueue<StateObservation>(2048);
  const store = new MemoryStateStore();
  const entries = Array.from({ length: size }, (_, i) => ({ key: `key-${i}`, revision: i + 1, value: { n: i } }));
  store.observe = async function* (_prefix = "", { signal } = {}) {
    const close = () => queue.close(); signal?.addEventListener("abort", close, { once: true });
    try { yield { operation: "snapshot", revision: size, entries }; yield* queue; }
    finally { signal?.removeEventListener("abort", close); queue.close(); }
  };
  const source = new MaterializedStateSource(store); await source.start();
  const start = performance.now();
  for (let i = 0; i < 1000; i++) {
    const entry = { key: "key-0", revision: size + i + 1, value: { n: i } };
    queue.push({ operation: "put", key: entry.key, revision: entry.revision, entry }); await source.observed(entry.revision);
  }
  results[`updates-${size}`] = { count: 1000, milliseconds: performance.now() - start };
  await source.close();
}
await setImmediate(); observer.disconnect(); delay.disable();
console.log(JSON.stringify({ node: process.version, forcedGC: !!global.gc, results, samples,
  gc, eventLoopDelayMs: { mean: delay.mean / 1e6, p99: delay.percentile(99) / 1e6, max: delay.max / 1e6 } }, null, 2));
