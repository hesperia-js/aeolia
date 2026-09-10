import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import { adopt, snapshot } from "../../src/realm.ts";
import { createGraph } from "../../src/contract/index.ts";
import { Fault } from "../../src/fault.ts";
import { affects, createMutation, createQuery, defineContract } from "../../src/contract/index.ts";
import { subscribe, watch } from "../../src/reactive.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function query(backend: ReturnType<typeof testBackend>) {
  return createQuery({
    name: "ready.query",
    key: (input: string) => `ready/${input}`,
    fetch: backend.respond<string, number | undefined>("ready.query"),
  });
}

function graphFor(
  backend: ReturnType<typeof testBackend>,
  options: { readonly idleMs?: number } = {},
) {
  return createGraph({
    contract: defineContract({ namespace: "query-ready", operations: { query: query(backend) } }),
    ...options,
  });
}

describe("QueryStore.ready", () => {
  it("is lazy, waits through stale state, and resolves initial or undefined values", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const initial = graph.api.query("initial", { initial: 7, revalidateAfterMs: 1 });
    const initialReady = initial.ready;
    expect(backend.calls).toHaveLength(0);
    await expect(initialReady).resolves.toBe(7);

    jest.advanceTimersByTime(2);
    expect(initial.status.get()).toBe("stale");
    const staleReady = initial.ready;
    expect(backend.calls).toHaveLength(0);
    const refresh = initial.revalidate();
    backend.resolve(backend.calls[0]!, 8);
    await refresh;
    await expect(staleReady).resolves.toBe(8);

    const undefinedStore = graph.api.query("undefined");
    const call = backend.calls[1]!;
    backend.resolve(call, undefined);
    await flush();
    await expect(undefinedStore.ready).resolves.toBeUndefined();
    expect(undefinedStore.value.peek()).toBeUndefined();
    graph.dispose();
  });

  it("rejects a pending wait on failure when no committed value exists", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("failure");
    const ready = store.ready;
    const failure = new Error("offline");
    backend.reject(backend.calls[0]!, failure);
    await expect(ready).rejects.toBe(failure);
    await expect(store.ready).rejects.toBe(failure);
    graph.dispose();
  });

  it("resolves a zero-window landing before status becomes stale without refetching", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("zero-window", { revalidateAfterMs: 0 });
    const ready = store.ready;
    backend.resolve(backend.calls[0]!, 8);
    await expect(ready).resolves.toBe(8);
    expect(store.status.get()).toBe("ready");

    jest.advanceTimersByTime(1);
    expect(store.status.get()).toBe("stale");
    expect(backend.calls).toHaveLength(1);

    graph.dispose();
  });

  it("revalidates a zero-window landing when its value is live", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("zero-live", { revalidateAfterMs: 0 });
    const stop = watch(store.value, () => undefined);
    const initialReady = store.ready;
    backend.resolve(backend.calls[0]!, 8);
    await expect(initialReady).resolves.toBe(8);
    expect(store.status.get()).toBe("ready");

    jest.advanceTimersByTime(1);
    expect(store.status.get()).toBe("revalidating");
    expect(backend.calls).toHaveLength(2);
    const refreshedReady = store.ready;
    backend.resolve(backend.calls[1]!, 9);
    await expect(refreshedReady).resolves.toBe(9);

    stop();
    graph.dispose();
  });

  it("does not refetch a stale zero-window caller beside a fresh status-only caller", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const zero = graph.api.query("mixed-window", { revalidateAfterMs: 0 });
    const long = graph.api.query("mixed-window", { revalidateAfterMs: 100 });
    backend.resolve(backend.calls[0]!, 8);
    await expect(zero.ready).resolves.toBe(8);
    await expect(long.ready).resolves.toBe(8);

    jest.advanceTimersByTime(1);
    expect(zero.status.get()).toBe("stale");
    expect(long.status.get()).toBe("ready");
    expect(backend.calls).toHaveLength(1);
    graph.dispose();
  });

  it("keeps an error subscriber's follow-up ready wait aligned with request-start status", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("error-retry");
    const failure = new Error("offline");
    backend.reject(backend.calls[0]!, failure);
    await flush();

    let armed = false;
    let retryReady: Promise<number | undefined> | undefined;
    const stop = subscribe(store.error, (error) => {
      if (armed && error === undefined) retryReady = store.ready;
    });
    armed = true;
    const retry = store.revalidate();
    backend.resolve(backend.calls[1]!, 42);
    await retry;
    await expect(retryReady).resolves.toBe(42);
    stop();
    graph.dispose();
  });

  it("follows a superseding request and ignores the aborted request's failure", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("supersede");
    const ready = store.ready;
    const first = backend.calls[0]!;
    store.revalidate();
    const second = backend.calls[1]!;
    expect(first.aborted).toBe(true);
    backend.reject(first, new Error("superseded"));
    await flush();
    backend.resolve(second, 42);
    await expect(ready).resolves.toBe(42);
    graph.dispose();
  });

  it("resolves a waiting value from a local committed write", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("local");
    const ready = store.ready;
    store.set(9);
    await expect(ready).resolves.toBe(9);
    graph.dispose();
  });

  it("does not let a late abort affect readiness after a local landing", async () => {
    const backend = testBackend();
    const controller = new AbortController();
    const graph = graphFor(backend);
    const store = graph.api.query("late-abort", { abortSignal: controller.signal });
    const ready = store.ready;
    store.set(9);
    expect(store.status.get()).toBe("ready");
    controller.abort();
    expect(store.status.get()).toBe("ready");
    expect(store.error.get()).toBeUndefined();
    await expect(ready).resolves.toBe(9);
    graph.dispose();
  });

  it("returns the committed base while an optimistic prediction is visible", async () => {
    const backend = testBackend();
    const queryDefinition = query(backend);
    const mutation = createMutation({
      name: "ready.mutate",
      affects: [
        affects<string, string, number | undefined>(queryDefinition, {
          select: (input: string) => input,
          on: "invalidate",
          optimistic: (current: number | undefined) => (current ?? 0) + 1,
        }),
      ],
      run: backend.perform<string, string>("ready.mutate"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "query-ready",
        operations: { query: queryDefinition, mutation },
      }),
    });
    const store = graph.api.query("optimistic", { initial: 0 });
    const pendingMutation = graph.api.mutation("optimistic");
    expect(store.value.get()).toBe(1);
    const ready = store.ready;
    await expect(ready).resolves.toBe(0);
    backend.resolve(backend.calls[0]!, "ok");
    await pendingMutation;
    graph.dispose();
  });

  it("resolves with the actual committed value after a landing watcher writes again", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("reentrant");
    const ready = store.ready;
    let rewrites = 0;
    const stop = watch(store.value, () => {
      if (rewrites === 0) {
        rewrites += 1;
        store.set(2);
      }
    });
    backend.resolve(backend.calls[0]!, 1);
    await expect(ready).resolves.toBe(2);
    expect(store.value.get()).toBe(2);
    stop();
    graph.dispose();
  });

  it("rejects when later revalidation fails despite a retained committed value", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("retained");
    backend.resolve(backend.calls[0]!, 5);
    await flush();
    const refresh = store.revalidate();
    const failure = new Error("offline");
    backend.reject(backend.calls[1]!, failure);
    await refresh;
    await expect(store.ready).rejects.toBe(failure);
    expect(store.error.get()).toBe(failure);
    expect(store.value.get()).toBe(5);
    graph.dispose();
  });

  it("resolves from adoption without waiting for the superseded request", async () => {
    const sourceBackend = testBackend();
    const source = graphFor(sourceBackend);
    source.api.query("adopt");
    sourceBackend.resolve(sourceBackend.calls[0]!, 11);
    await flush();
    const encoded = snapshot(source);

    const targetBackend = testBackend();
    const target = graphFor(targetBackend);
    const targetStore = target.api.query("adopt");
    const ready = targetStore.ready;
    adopt(target, encoded);
    await expect(ready).resolves.toBe(11);
    source.dispose();
    target.dispose();
  });

  it("rejects immediately when the current request is cancelled, even if the host ignores abort", async () => {
    const backend = testBackend();
    const controller = new AbortController();
    const graph = createGraph({
      contract: defineContract({ namespace: "query-ready", operations: { query: query(backend) } }),
    });
    const store = graph.api.query("cancel", { abortSignal: controller.signal });
    const ready = store.ready;
    controller.abort();
    expect(store.pending.get()).toBe(false);
    expect(store.status.get()).toBe("failed");
    const cancellation = store.error.get();
    expect(cancellation).toMatchObject({ name: "AbortError" });
    await expect(ready).rejects.toBe(cancellation);
    graph.dispose();
  });

  it("rejects an already-aborted caller's readiness without waiting for its host callback", async () => {
    const backend = testBackend();
    const controller = new AbortController();
    controller.abort();
    const graph = createGraph({
      contract: defineContract({ namespace: "query-ready", operations: { query: query(backend) } }),
    });
    const store = graph.api.query("pre-aborted", { abortSignal: controller.signal });
    await expect(store.ready).rejects.toMatchObject({ name: "AbortError" });
    graph.dispose();
  });

  it("rejects held readiness on graph disposal while the host callback hangs", async () => {
    const backend = testBackend();
    const graph = graphFor(backend);
    const store = graph.api.query("dispose");
    const ready = store.ready;
    graph.dispose();
    await expect(ready).rejects.toMatchObject({
      constructor: Fault,
      kind: "disposed",
    });
  });

  it("keeps a held readiness wait across collection until a later member call lands", async () => {
    const backend = testBackend();
    const graph = graphFor(backend, { idleMs: 1 });
    const held = graph.api.query("collect");
    backend.resolve(backend.calls[0]!, 3);
    await flush();
    jest.advanceTimersByTime(2);
    expect(held.value.get()).toBeUndefined();
    const readyAfterCollection = held.ready;

    const reactivated = graph.api.query("collect");
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, 4);
    await expect(readyAfterCollection).resolves.toBe(4);
    await expect(reactivated.ready).resolves.toBe(4);
    graph.dispose();
  });
});
