import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import test from "node:test";
import { abortable, linkedAbortSignal, subscribeAbort } from "../src/runtime.ts";

test("concurrent cancellable waits share native listeners and detach before late I/O finishes", async () => {
  const first = new AbortController(), second = new AbortController();
  let finish!: () => void;
  const work = new Promise<void>(resolve => { finish = resolve; });
  const waiting = Array.from({ length: 64 }, () => assert.rejects(abortable(work, [first.signal, second.signal]), /cancelled/));
  assert.equal(getEventListeners(first.signal, "abort").length, 1);
  assert.equal(getEventListeners(second.signal, "abort").length, 1);
  second.abort();
  await Promise.all(waiting);
  assert.equal(getEventListeners(first.signal, "abort").length, 0);
  assert.equal(getEventListeners(second.signal, "abort").length, 0);
  finish();
  assert.equal(await abortable(Promise.resolve(42), first.signal), 42);
  assert.equal(getEventListeners(first.signal, "abort").length, 0);
});

test("signal linking preserves synchronous revocation, early cancellation, and explicit disposal", () => {
  const first = new AbortController(), second = new AbortController();
  const linked = linkedAbortSignal([first.signal, second.signal]);
  second.abort("ended");
  assert.equal(linked.signal.aborted, true);
  assert.equal(linked.signal.reason, "ended");
  assert.equal(getEventListeners(first.signal, "abort").length, 0);
  linked.dispose();
  const early = linkedAbortSignal([first.signal, second.signal]);
  assert.equal(early.signal.aborted, true);
  assert.equal(getEventListeners(first.signal, "abort").length, 0);
  const disposed = linkedAbortSignal([first.signal]);
  disposed.dispose();
  first.abort();
  assert.equal(disposed.signal.aborted, false);
});

test("separate abort subscriptions can use the same callback independently", () => {
  const controller = new AbortController();
  let calls = 0;
  const callback = () => { calls++; };
  const remove = subscribeAbort([controller.signal], callback);
  const other = subscribeAbort([controller.signal], callback);
  remove();
  controller.abort();
  assert.equal(calls, 1);
  other();
});
