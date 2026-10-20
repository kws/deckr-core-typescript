import type { NatsConnection, ConnectionOptions } from "nats";
import { StateUnavailable, ValidationError } from "./errors.ts";
import { abortable } from "./runtime.ts";

export interface NatsRetryOptions {
  signal?: AbortSignal;
  reconnectMaxIntervalSeconds?: number;
}

/** nats.js calls its delay callback for both waits and dials, and measures
 * delay from the previous dial's start. Adapt that seam to Deckr's delay after
 * failure, without counting extra callback invocations as failed attempts.
 * Keep this adapter covered against the supported nats.js 2.x dial loop.
 */
export class ReconnectPolicy {
  private window: number;
  private deadline?: number;
  private attempts = 0;
  private maximum: number;
  private now: () => number;
  private random: () => number;
  constructor(maximum: number, now = () => performance.now(), random = Math.random) {
    this.maximum = maximum; this.now = now; this.random = random;
    this.window = Math.min(1000, maximum);
  }
  reset(): void { this.window = Math.min(1000, this.maximum); this.deadline = undefined; this.attempts = 0; }
  delay(failures: number): number {
    const now = this.now();
    if (this.deadline === undefined || failures !== this.attempts) {
      if (this.deadline !== undefined) this.window = Math.min(this.window * 2, this.maximum);
      this.attempts = failures;
      this.deadline = now + this.window * (0.5 + this.random() * 0.5);
    }
    return Math.max(0, this.deadline - now);
  }
}

type RetryServer = { lastConnect: number; reconnects: number };
// nats.js exposes no notification between its dial failure and delay callback.
// This narrow compatibility seam supplies attempt counts and prevents a long
// healthy connection from bypassing the first reconnect wait.
function retryServers(connection: NatsConnection): RetryServer[] {
  const protocol = (connection as unknown as { protocol: { servers: { getServers(): RetryServer[] } } }).protocol;
  const servers = protocol?.servers?.getServers?.();
  if (!Array.isArray(servers)) throw new StateUnavailable("Unsupported nats.js reconnect adapter");
  return servers;
}

export async function connectWithRetry(
  connect: (options: ConnectionOptions) => Promise<NatsConnection>,
  options: Record<string, unknown> & NatsRetryOptions,
): Promise<NatsConnection> {
  const { signal, reconnectMaxIntervalSeconds = 60, ...clientOptions } = options;
  const maximum = reconnectMaxIntervalSeconds * 1000;
  const timeout = Number(options.timeout ?? 5000);
  if (!Number.isFinite(maximum) || maximum <= 0 || !Number.isFinite(timeout) || timeout <= 0) {
    throw new ValidationError("NATS retry interval and dial timeout must be finite and positive");
  }
  const cancelled = () => { if (signal?.aborted) throw new StateUnavailable("NATS connection cancelled"); };
  for (;;) {
    cancelled();
    let connection: NatsConnection | undefined;
    const policy = new ReconnectPolicy(maximum);
    const delay = () => {
      if (!connection) return 0;
      const servers = retryServers(connection);
      const wait = policy.delay(servers.reduce((sum, server) => sum + server.reconnects, 0));
      // The client compares lastConnect + wait to Date.now() on every loop.
      for (const server of servers) server.lastConnect = Date.now();
      return wait;
    };
    try {
      const pending = connect({ ...clientOptions, timeout, reconnect: true, maxReconnectAttempts: -1,
        waitOnFirstConnect: false, ignoreAuthErrorAbort: true, reconnectDelayHandler: delay });
      // Cancellation ends the caller's wait immediately. A bounded pending dial
      // retains responsibility for closing a client that arrives afterward.
      void pending.then(client => { if (signal?.aborted) void client.close(); }, () => {});
      connection = await (signal ? abortable(pending, signal) : pending);
    } catch (error) {
      cancelled();
      // Malformed configuration is not a broker outage.
      if (error && typeof error === "object" && "code" in error && ["BAD_AUTHENTICATION", "BAD_OPTIONS", "INVALID_OPTION"].includes(String(error.code))) throw error;
      const pause = new Promise<void>(resolve => {
        const timer = setTimeout(done, maximum);
        function done() { clearTimeout(timer); signal?.removeEventListener("abort", done); resolve(); }
        signal?.addEventListener("abort", done, { once: true });
        if (signal?.aborted) done();
      });
      await pause;
      continue;
    }
    try { retryServers(connection); }
    catch (error) { await connection.close(); throw error; }
    const owned = connection;
    const close = () => { void owned.close(); };
    signal?.addEventListener("abort", close, { once: true });
    if (signal?.aborted) { await owned.close(); cancelled(); }
    void (async () => {
      try { for await (const status of owned.status()) if (status.type === "reconnect") policy.reset(); }
      finally { signal?.removeEventListener("abort", close); }
    })().catch(() => {});
    return owned;
  }
}
