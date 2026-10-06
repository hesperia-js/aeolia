import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  affects,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createStream,
  defineContract,
  project,
} from "../../src/contract/index.ts";
import { Fault } from "../../src/fault.ts";
import { computed, subscribe, withoutWrites } from "../../src/reactive.ts";
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
  await Promise.resolve();
}

async function nextTurn(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

describe("Graph.reset", () => {
  it("preserves graph and store handles, restores declarations, and accepts fresh undefined data", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.undefined",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number | undefined>("reset.undefined"),
    });
    const declared = createStore({ name: "settings", initial: "default" });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset", operations: { query, declared } }),
    });
    const api = graph.api;
    const declaredStore = graph.store(declared);
    const declaredValue = declaredStore.value;
    const queryStore = graph.api.query("cached");
    const queryValue = queryStore.value;
    const versions: number[] = [];
    const stopVersion = subscribe(graph.resetVersion, (version) => versions.push(version));

    declaredStore.set("changed");
    backend.resolve(backend.calls[0]!, undefined);
    await flush();
    await expect(queryStore.ready).resolves.toBeUndefined();
    expect(queryStore.value.get()).toBeUndefined();

    const stopQuery = subscribe(queryStore.value, () => undefined);
    const reset = graph.reset();
    expect(graph.api).toBe(api);
    expect(graph.resetVersion.get()).toBe(1);
    expect(versions).toEqual([0, 1]);
    expect(graph.store(declared)).toBe(declaredStore);
    expect(declaredStore.value).toBe(declaredValue);
    expect(declaredStore.value.get()).toBe("default");
    expect(queryStore.value).toBe(queryValue);
    expect(queryStore.value.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(2);

    backend.resolve(backend.calls[1]!, undefined);
    await expect(reset).resolves.toBeUndefined();
    await expect(queryStore.ready).resolves.toBeUndefined();
    expect(queryStore.value).toBe(queryValue);

    stopQuery();
    stopVersion();
    graph.dispose();
  });

  it("refreshes a status-only live query and clears an inactive query without fetching it", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.live",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number>("reset.live"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-live", operations: { query } }),
    });
    const live = graph.api.query("live");
    const inactive = graph.api.query("inactive");
    const priorFailure = new Error("prior query failed");
    backend.reject(backend.calls[0]!, priorFailure);
    backend.resolve(backend.calls[1]!, 10);
    await flush();
    expect(live.status.get()).toBe("failed");
    expect(live.error.get()).toBe(priorFailure);
    const stopStatus = subscribe(live.status, () => undefined);

    const reset = graph.reset();
    expect(backend.calls).toHaveLength(3);
    expect(live.value.get()).toBeUndefined();
    expect(live.error.get()).toBeUndefined();
    expect(inactive.value.get()).toBeUndefined();
    expect(live.status.get()).toBe("fetching");
    backend.resolve(backend.calls[2]!, 2);
    await expect(reset).resolves.toBeUndefined();
    expect(live.value.get()).toBe(2);
    expect(backend.calls).toHaveLength(3);

    stopStatus();
    graph.dispose();
  });

  it("refuses reset in a write-restricted scope without superseding active work", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.write-restricted",
      key: () => "reset/write-restricted",
      fetch: backend.respond<void, number>("reset.write-restricted"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-write-restricted", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 5);
    await expect(store.ready).resolves.toBe(5);

    const revalidation = store.revalidate();
    const activeCall = backend.calls[1]!;
    expect(store.status.get()).toBe("revalidating");
    expect(() => withoutWrites(() => graph.reset())).toThrow("write-forbidden");
    expect(graph.resetVersion.get()).toBe(0);
    expect(activeCall.aborted).toBe(false);
    expect(store.value.get()).toBe(5);
    expect(store.status.get()).toBe("revalidating");

    backend.resolve(activeCall, 6);
    await expect(revalidation).resolves.toBeUndefined();
    expect(store.value.get()).toBe(6);
    graph.dispose();
  });

  it("keeps an existing ready wait active across reset", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.ready-waiter",
      key: () => "reset/ready-waiter",
      fetch: backend.respond<void, number>("reset.ready-waiter"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-ready-waiter", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    const ready = store.ready;
    const previous = backend.calls[0]!;

    const reset = graph.reset();
    expect(previous.aborted).toBe(true);
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, 8);
    await expect(reset).resolves.toBeUndefined();
    await expect(ready).resolves.toBe(8);

    graph.dispose();
  });

  it("clears inactive cached data without starting a refresh when no query is live", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.inactive",
      key: () => "reset/inactive",
      fetch: backend.respond<void, number>("reset.inactive"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-inactive", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 7);
    await flush();
    expect(store.value.get()).toBe(7);

    const reset = graph.reset();
    expect(graph.resetVersion.get()).toBe(1);
    expect(store.value.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(1);
    await expect(reset).resolves.toBeUndefined();
    expect(backend.calls).toHaveLength(1);

    graph.dispose();
  });

  it("immediately fetches when a reset-cleared query handle becomes live again", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.reactivate",
      key: () => "reset/reactivate",
      revalidateAfterMs: 10,
      fetch: backend.respond<void, number>("reset.reactivate"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reactivate", operations: { query } }),
    });
    const store = graph.api.query();
    backend.resolve(backend.calls[0]!, 4);
    await flush();
    expect(store.value.get()).toBe(4);

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(store.value.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(1);

    const stop = subscribe(store.value, () => undefined);
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, 5);
    await flush();
    expect(store.value.get()).toBe(5);

    stop();
    graph.dispose();
  });

  it("allows a keyed initial seed after reset only while empty and idle", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.reseed",
      key: (input: string) => `reset/reseed/${input}`,
      fetch: backend.respond<string, number>("reset.reseed"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reseed", operations: { query } }),
    });
    const store = graph.api.query("one", { initial: 3 });
    const previousFetch = store.revalidate();
    backend.resolve(backend.calls[0]!, 4);
    await previousFetch;
    await expect(graph.reset()).resolves.toBeUndefined();

    const reseeded = graph.api.query("one", { initial: 9 });
    expect(reseeded.value.get()).toBe(9);
    expect(backend.calls).toHaveLength(1);

    const liveStop = subscribe(reseeded.status, () => undefined);
    const reset = graph.reset();
    expect(backend.calls).toHaveLength(2);
    expect(() => graph.api.query("one", { initial: 10 })).toThrow(Fault);
    expect(backend.calls[1]!.aborted).toBe(false);
    backend.resolve(backend.calls[1]!, 12);
    await expect(reset).resolves.toBeUndefined();
    expect(reseeded.value.get()).toBe(12);
    expect(() => graph.api.query("one", { initial: 10 })).toThrow(Fault);
    expect(reseeded.value.get()).toBe(12);

    liveStop();
    graph.dispose();
  });

  it("does not wait for a query that becomes live after reset starts", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.late-live",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number>("reset.late-live"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-late-live", operations: { query } }),
    });
    const required = graph.api.query("required");
    let later = graph.api.query("later");
    backend.resolve(backend.calls[0]!, 1);
    backend.resolve(backend.calls[1]!, 2);
    await flush();
    const stopRequired = subscribe(required.status, () => undefined);
    let stopLater: (() => void) | undefined;
    const stopVersion = subscribe(graph.resetVersion, (version) => {
      if (version === 0) return;
      later = graph.api.query("later");
      stopLater = subscribe(later.status, () => undefined);
    });

    let settled = false;
    const outcome = graph.reset().then(
      () => {
        settled = true;
        return { ok: true as const };
      },
      (error: unknown) => {
        settled = true;
        return { ok: false as const, error };
      },
    );
    expect(backend.calls).toHaveLength(4);
    expect(later.status.get()).toBe("fetching");
    expect(settled).toBe(false);
    backend.resolve(backend.calls[3]!, 3);
    expect(await outcome).toEqual({ ok: true });
    expect(settled).toBe(true);
    expect(backend.calls[2]!.aborted).toBe(false);

    backend.resolve(backend.calls[2]!, 4);
    await flush();
    expect(later.value.get()).toBe(4);
    stopRequired();
    stopLater?.();
    stopVersion();
    graph.dispose();
  });

  it("adopts the current same-key refresh after the reset-owned request is superseded", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.adopted-refresh",
      key: () => "reset/adopted-refresh",
      fetch: backend.respond<void, number>("reset.adopted-refresh"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-adopted-refresh", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(() => {
      settled = true;
    });
    const obsolete = backend.calls[1]!;
    const current = store.revalidate();
    expect(backend.calls).toHaveLength(3);
    expect(obsolete.aborted).toBe(true);

    backend.resolve(backend.calls[2]!, 2);
    await current;
    await nextTurn();
    expect(settled).toBe(true);
    await expect(reset).resolves.toBeUndefined();
    expect(store.value.get()).toBe(2);

    backend.resolve(obsolete, 99);
    await flush();
    expect(store.value.get()).toBe(2);
    stop();
    graph.dispose();
  });

  it("does not let an obsolete cooperative abort reject reset ahead of its replacement", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.adopted-abort",
      key: () => "reset/adopted-abort",
      fetch: backend.respond<void, number>("reset.adopted-abort"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-adopted-abort", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const result = graph.reset().then(
      () => {
        settled = true;
        return { ok: true as const };
      },
      (error: unknown) => {
        settled = true;
        return { ok: false as const, error };
      },
    );
    const obsolete = backend.calls[1]!;
    const replacement = store.revalidate();
    backend.reject(obsolete, new DOMException("obsolete fetch was aborted", "AbortError"));
    await flush();
    expect(settled).toBe(false);

    backend.resolve(backend.calls[2]!, 2);
    await expect(result).resolves.toEqual({ ok: true });
    expect(store.value.get()).toBe(2);
    await replacement;
    stop();
    graph.dispose();
  });

  it("does not finish from an obsolete success while its same-key replacement is pending", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.obsolete-success",
      key: () => "reset/obsolete-success",
      fetch: backend.respond<void, number>("reset.obsolete-success"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-obsolete-success", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(() => {
      settled = true;
    });
    const obsolete = backend.calls[1]!;
    store.revalidate();
    const replacement = backend.calls[2]!;
    backend.resolve(obsolete, 99);
    await nextTurn();
    expect(settled).toBe(false);

    backend.resolve(replacement, 2);
    await expect(reset).resolves.toBeUndefined();
    expect(settled).toBe(true);
    expect(store.value.get()).toBe(2);
    stop();
    graph.dispose();
  });

  it("forwards the reset outcome through reentrant same-key replacements", async () => {
    const requests: Array<{
      readonly abortSignal: AbortSignal;
      readonly resolve: (value: number) => void;
      readonly reject: (error: unknown) => void;
    }> = [];
    const query = createQuery({
      name: "reset.reentrant-replacements",
      key: () => "reset/reentrant-replacements",
      fetch: (_input, { abortSignal }) =>
        new Promise<number>((resolve, reject) => {
          requests.push({ abortSignal, resolve, reject });
        }),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-reentrant-replacements",
        operations: { query },
      }),
    });
    const store = graph.api.query(undefined);
    requests[0]!.resolve(1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(() => {
      settled = true;
    });
    const obsolete = requests[1]!;
    let reentrant: Promise<void> | undefined;
    obsolete.abortSignal.addEventListener("abort", () => {
      reentrant = store.revalidate();
    });
    store.revalidate();
    expect(requests).toHaveLength(4);
    expect(reentrant).toBeDefined();
    const reentrantCall = requests[2]!;
    const finalCall = requests[3]!;

    reentrantCall.resolve(3);
    obsolete.resolve(99);
    await nextTurn();
    expect(settled).toBe(false);
    finalCall.resolve(4);
    await expect(reset).resolves.toBeUndefined();
    await expect(reentrant).resolves.toBeUndefined();
    expect(store.value.get()).toBe(4);
    stop();
    graph.dispose();
  });

  it("rejects a sole required query failure with its original reason, including undefined", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.single-failure",
      key: () => "reset/single-failure",
      fetch: backend.respond<void, number>("reset.single-failure"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-single-failure", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    const stop = subscribe(store.status, () => undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();

    const reset = graph.reset().then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(backend.calls).toHaveLength(2);
    backend.reject(backend.calls[1]!, undefined);
    const outcome = await reset;
    expect(outcome).toEqual({ ok: false, error: undefined });
    expect(store.status.get()).toBe("failed");
    expect(store.error.get()).toBeUndefined();

    stop();
    graph.dispose();
  });

  it("waits for every active query outcome and aggregates original failures", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.all",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number>("reset.all"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-all", operations: { query } }),
    });
    const first = graph.api.query("first");
    const second = graph.api.query("second");
    const third = graph.api.query("third");
    backend.resolve(backend.calls[0]!, 1);
    backend.resolve(backend.calls[1]!, 2);
    backend.resolve(backend.calls[2]!, 3);
    await flush();
    const stopFirst = subscribe(first.status, () => undefined);
    const stopSecond = subscribe(second.status, () => undefined);
    const stopThird = subscribe(third.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(
      () => {
        settled = true;
        return undefined;
      },
      (error: unknown) => {
        settled = true;
        throw error;
      },
    );
    const firstFailure = undefined;
    const secondFailure = new Error("second refresh failed");
    backend.reject(backend.calls[3]!, firstFailure);
    backend.resolve(backend.calls[5]!, 4);
    await flush();
    expect(settled).toBe(false);
    expect(third.value.get()).toBe(4);
    backend.reject(backend.calls[4]!, secondFailure);
    const resetError = await reset.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(resetError).toBeInstanceOf(AggregateError);
    expect((resetError as AggregateError).errors).toEqual([firstFailure, secondFailure]);
    expect(third.value.get()).toBe(4);

    stopFirst();
    stopSecond();
    stopThird();
    graph.dispose();
  });

  it("rejects a superseded reset immediately and ignores its late query landing", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.overlap",
      key: () => "reset/one",
      fetch: backend.respond<void, number>("reset.overlap"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-overlap", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    const stop = subscribe(store.status, () => undefined);

    const firstReset = graph.reset();
    const firstResult = firstReset.then(
      () => undefined,
      (error: unknown) => error,
    );
    const secondReset = graph.reset();
    const firstError = await firstResult;
    expect(firstError).toMatchObject({ name: "AbortError" });
    expect(backend.calls[0]!.aborted).toBe(true);
    expect(backend.calls[1]!.aborted).toBe(true);
    expect(backend.calls).toHaveLength(3);

    backend.resolve(backend.calls[0]!, 88);
    backend.resolve(backend.calls[1]!, 99);
    await flush();
    expect(store.value.get()).toBeUndefined();
    backend.resolve(backend.calls[2]!, 3);
    await expect(secondReset).resolves.toBeUndefined();
    expect(store.value.get()).toBe(3);
    expect(graph.resetVersion.get()).toBe(2);

    stop();
    graph.dispose();
  });

  it("ignores a prior-epoch mutation success without retiring new predictions or running effects", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.mutation-query",
      key: (input: { readonly id: string }) => `item/${input.id}`,
      fetch: backend.respond<{ readonly id: string }, number>("reset.mutation-query"),
    });
    const mutation = createMutation({
      name: "reset.mutation",
      affects: [
        affects(query, {
          select: (input: { readonly id: string; readonly amount: number }) => ({ id: input.id }),
          optimistic: (current, input: { readonly id: string; readonly amount: number }) =>
            (current ?? 0) + input.amount,
        }),
      ],
      run: backend.perform<{ readonly id: string; readonly amount: number }, void>(
        "reset.mutation",
      ),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-mutation", operations: { query, mutation } }),
    });
    const store = graph.api.query({ id: "one" });
    const stop = subscribe(store.value, () => undefined);
    backend.resolve(backend.calls[0]!, 10);
    await flush();

    const oldMutation = graph.api.mutation({ id: "one", amount: 1 });
    const oldFailureMutation = graph.api.mutation({ id: "one", amount: 3 });
    const oldMutationCall = backend.calls[1]!;
    const oldFailureCall = backend.calls[2]!;
    const oldFailure = new Error("old mutation failed");
    const oldFailureOutcome = oldFailureMutation.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(store.value.get()).toBe(14);
    const reset = graph.reset();
    expect(oldMutationCall.aborted).toBe(true);
    expect(oldFailureCall.aborted).toBe(true);
    backend.resolve(backend.calls[3]!, 20);
    await expect(reset).resolves.toBeUndefined();

    const newMutation = graph.api.mutation({ id: "one", amount: 2 });
    expect(store.value.get()).toBe(22);
    backend.resolve(oldMutationCall, undefined);
    await oldMutation;
    backend.reject(oldFailureCall, oldFailure);
    expect(await oldFailureOutcome).toBe(oldFailure);
    await flush();
    expect(store.value.get()).toBe(22);
    expect(backend.calls.filter((call) => call.name === "reset.mutation-query")).toHaveLength(2);

    backend.resolve(backend.calls[4]!, undefined);
    await newMutation;
    stop();
    graph.dispose();
  });

  it("does not dispatch a mutation when its optimistic predictor resets the graph", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.reentrant-query",
      key: () => "reset/reentrant",
      fetch: backend.respond<void, number>("reset.reentrant-query"),
    });
    let startReset = (): Promise<void> => Promise.resolve();
    let reset: Promise<void> | undefined;
    const mutation = createMutation({
      name: "reset.reentrant-mutation",
      affects: [
        affects(query, {
          optimistic: (current: number | undefined) => {
            reset ??= startReset();
            return (current ?? 0) + 1;
          },
        }),
      ],
      run: backend.perform<void, void>("reset.reentrant-mutation"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reentrant", operations: { query, mutation } }),
    });
    startReset = () => graph.reset();
    const store = graph.api.query(undefined);
    const stop = subscribe(store.value, () => undefined);
    backend.resolve(backend.calls[0]!, 10);
    await flush();

    const outcome = graph.api.mutation(undefined).then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(await outcome).toMatchObject({ ok: false, error: { name: "AbortError" } });
    expect(backend.calls.filter((call) => call.name === "reset.reentrant-mutation")).toHaveLength(
      0,
    );
    expect(store.value.get()).toBeUndefined();
    expect(reset).toBeDefined();
    backend.resolve(backend.calls[1]!, 20);
    await expect(reset).resolves.toBeUndefined();
    expect(store.value.get()).toBe(20);

    stop();
    graph.dispose();
  });

  it("keeps a mutation promise result when reset aborts its callback signal", async () => {
    const backend = testBackend();
    const mutation = createMutation({
      name: "reset.mutation-result",
      affects: [],
      run: backend.perform<void, string>("reset.mutation-result"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-mutation-result",
        operations: { mutation },
      }),
    });
    const task = graph.api.mutation();
    const call = backend.calls[0]!;

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(call.aborted).toBe(true);
    backend.resolve(call, "saved");
    await expect(task).resolves.toBe("saved");
    graph.dispose();
  });

  it("restarts a live stream and rejects emissions from its retired session", async () => {
    const sessions: Array<{
      readonly abortSignal: AbortSignal;
      readonly pending: Array<(result: IteratorResult<number>) => void>;
    }> = [];
    const stream = createStream({
      name: "reset.stream",
      key: () => "reset/stream",
      open: (_input, { abortSignal }) => {
        const session = {
          abortSignal,
          pending: [] as Array<(result: IteratorResult<number>) => void>,
        };
        sessions.push(session);
        const iterator: AsyncIterator<number> = {
          next: () => new Promise((resolve) => session.pending.push(resolve)),
          return: async () => ({ done: true, value: undefined }),
        };
        return { [Symbol.asyncIterator]: () => iterator };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-stream", operations: { stream } }),
    });
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.value, () => undefined);
    expect(sessions).toHaveLength(1);

    const reset = graph.reset();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]!.abortSignal.aborted).toBe(true);
    expect(store.value.get()).toBeUndefined();
    await expect(reset).resolves.toBeUndefined();

    sessions[0]!.pending[0]!({ done: false, value: 99 });
    await flush();
    expect(store.value.get()).toBeUndefined();
    sessions[1]!.pending[0]!({ done: false, value: 7 });
    await flush();
    expect(store.value.get()).toBe(7);

    stop();
    graph.dispose();
  });

  it("clears retained values and errors from terminal projections without reopening them", async () => {
    const backend = testBackend();
    let resetDuringFinish: Promise<void> | undefined;
    let requestResetDuringFinish = (): void => undefined;
    const completedStream = createStream({
      name: "reset.completed-projection",
      key: () => "reset/completed-projection",
      open: backend.stream<void, number>("reset.completed-projection"),
    });
    const failedStream = createStream({
      name: "reset.failed-projection",
      key: () => "reset/failed-projection",
      open: backend.stream<void, number>("reset.failed-projection"),
    });
    const reentrantOpen = backend.stream<void, number>("reset.reentrant-projection-failure");
    const reentrantStream = createStream({
      name: "reset.reentrant-projection-failure",
      key: () => "reset/reentrant-projection-failure",
      open: (input: void, options) => {
        options.abortSignal?.addEventListener("abort", requestResetDuringFinish, { once: true });
        return reentrantOpen(input, options);
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-terminal-projections",
        operations: { completedStream, failedStream, reentrantStream },
      }),
    });
    requestResetDuringFinish = () => {
      resetDuringFinish = graph.reset();
    };
    const completedReadables = (() => {
      const projection = project(graph, completedStream, undefined, {
        kind: "accumulate",
        max: 4,
      });
      return { value: projection.value, status: projection.status, error: projection.error };
    })();
    const failedReadables = (() => {
      const projection = project(graph, failedStream, undefined, {
        kind: "reduce",
        initial: 10,
        step: (total, item) => total + item,
      });
      return { value: projection.value, status: projection.status, error: projection.error };
    })();
    const waitForStatus = (
      status: typeof completedReadables.status,
      expected: "closed" | "failed",
    ): Promise<void> =>
      new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(status, (value) => {
          if (value !== expected) return;
          stop();
          resolve();
        });
      });
    const failure = new Error("terminal projection source failed");
    const completedStatus = waitForStatus(completedReadables.status, "closed");
    const failedStatus = waitForStatus(failedReadables.status, "failed");

    backend.emit("reset.completed-projection", 2);
    backend.emit("reset.completed-projection", 3);
    backend.end("reset.completed-projection");
    backend.emit("reset.failed-projection", 4);
    backend.end("reset.failed-projection", failure);
    await Promise.all([completedStatus, failedStatus]);
    expect(completedReadables.value.get()).toEqual([2, 3]);
    expect(completedReadables.status.get()).toBe("closed");
    expect(completedReadables.error.get()).toBeUndefined();
    expect(failedReadables.value.get()).toBe(14);
    expect(failedReadables.status.get()).toBe("failed");
    expect(failedReadables.error.get()).toBe(failure);
    expect(backend.calls).toHaveLength(2);

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(completedReadables.value.get()).toEqual([]);
    expect(completedReadables.status.get()).toBe("closed");
    expect(completedReadables.error.get()).toBeUndefined();
    expect(failedReadables.value.get()).toBe(10);
    expect(failedReadables.status.get()).toBe("failed");
    expect(failedReadables.error.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(2);

    const reentrantReadables = (() => {
      const projection = project(graph, reentrantStream, undefined, {
        kind: "reduce",
        initial: 20,
        step: (total, item) => total + item,
      });
      return { value: projection.value, status: projection.status, error: projection.error };
    })();
    const reentrantFailure = new Error("terminal reset should clear this failure");
    const reentrantStatus = waitForStatus(reentrantReadables.status, "failed");
    const previousResetVersion = graph.resetVersion.get();
    backend.emit("reset.reentrant-projection-failure", 5);
    backend.end("reset.reentrant-projection-failure", reentrantFailure);
    await reentrantStatus;
    expect(resetDuringFinish).toBeDefined();
    await expect(resetDuringFinish!).resolves.toBeUndefined();
    expect(graph.resetVersion.get()).toBe(previousResetVersion + 1);
    expect(reentrantReadables.value.get()).toBe(20);
    expect(reentrantReadables.status.get()).toBe("failed");
    expect(reentrantReadables.error.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(3);

    graph.dispose();
  });

  it("restarts observed streams that ended or failed before reset", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "reset.terminal-stream",
      key: () => "reset/terminal-stream",
      open: backend.stream<void, number>("reset.terminal-stream"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-terminal-stream", operations: { stream } }),
    });
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.status, () => undefined);

    backend.emit("reset.terminal-stream", 1);
    await flush();
    backend.end("reset.terminal-stream");
    await flush();
    expect(store.status.get()).toBe("closed");
    expect(store.value.get()).toBe(1);

    const endedReset = graph.reset();
    expect(backend.calls).toHaveLength(2);
    expect(store.status.get()).toBe("opening");
    expect(store.value.get()).toBeUndefined();
    await expect(endedReset).resolves.toBeUndefined();

    const failure = new Error("stream failed before reset");
    backend.end("reset.terminal-stream", failure);
    await flush();
    expect(store.status.get()).toBe("failed");
    expect(store.error.get()).toBe(failure);

    const failedReset = graph.reset();
    expect(backend.calls).toHaveLength(3);
    expect(store.status.get()).toBe("opening");
    expect(store.error.get()).toBeUndefined();
    expect(store.value.get()).toBeUndefined();
    await expect(failedReset).resolves.toBeUndefined();

    stop();
    graph.dispose();
  });

  it("does not create an iterator when stream.open resets its graph", async () => {
    let startReset = (): Promise<void> => Promise.resolve();
    let nestedReset: Promise<void> | undefined;
    let openCalls = 0;
    let iteratorCalls = 0;
    const stream = createStream({
      name: "reset.reentrant-open",
      key: () => "reset/reentrant-open",
      open: () => {
        openCalls += 1;
        if (openCalls === 2) nestedReset = startReset();
        return {
          [Symbol.asyncIterator]: () => {
            iteratorCalls += 1;
            return {
              next: () => new Promise<IteratorResult<number>>(() => undefined),
              return: async () => ({ done: true, value: undefined }),
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reentrant-open", operations: { stream } }),
    });
    startReset = () => graph.reset();
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.status, () => undefined);
    expect(iteratorCalls).toBe(1);

    const firstReset = graph.reset();
    const firstResult = firstReset.then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(await firstResult).toMatchObject({ ok: false, error: { name: "AbortError" } });
    await expect(nestedReset).resolves.toBeUndefined();
    expect(openCalls).toBe(3);
    expect(iteratorCalls).toBe(2);

    stop();
    graph.dispose();
  });

  it("retires an iterator whose factory resets its graph before consumption", async () => {
    let startReset = (): Promise<void> => Promise.resolve();
    let nestedReset: Promise<void> | undefined;
    let openCalls = 0;
    let iteratorCalls = 0;
    let staleIteratorReturns = 0;
    const stream = createStream({
      name: "reset.reentrant-iterator",
      key: () => "reset/reentrant-iterator",
      open: () => {
        openCalls += 1;
        return {
          [Symbol.asyncIterator]: () => {
            iteratorCalls += 1;
            if (iteratorCalls === 2) nestedReset = startReset();
            return {
              next: () => new Promise<IteratorResult<number>>(() => undefined),
              return: async () => {
                staleIteratorReturns += 1;
                return { done: true, value: undefined };
              },
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reentrant-iterator", operations: { stream } }),
    });
    startReset = () => graph.reset();
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.status, () => undefined);

    const firstReset = graph.reset();
    const firstResult = firstReset.then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(await firstResult).toMatchObject({ ok: false, error: { name: "AbortError" } });
    await expect(nestedReset).resolves.toBeUndefined();
    expect(openCalls).toBe(3);
    expect(iteratorCalls).toBe(3);
    expect(staleIteratorReturns).toBe(2);

    stop();
    graph.dispose();
  });

  it("clears and reconnects a shared projection handle through the restarted session", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "reset.projection-stream",
      key: () => "reset/projection",
      open: backend.stream<void, number>("reset.projection-stream"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-projection", operations: { stream } }),
    });
    const source = graph.api.stream(undefined);
    const projection = project(source, { kind: "accumulate", max: 5 });
    backend.emit("reset.projection-stream", 1);
    await flush();
    expect(projection.value.get()).toEqual([1]);

    const reset = graph.reset();
    await expect(reset).resolves.toBeUndefined();
    expect(projection.value.get()).toEqual([]);
    expect(backend.calls).toHaveLength(2);
    backend.emit("reset.projection-stream", 2);
    await flush();
    expect(projection.value.get()).toEqual([2]);

    projection.close();
    graph.dispose();
  });

  it("reopens an independent projection and ignores its retired iterator", async () => {
    const sessions: Array<{
      readonly abortSignal: AbortSignal;
      readonly pending: Array<(result: IteratorResult<number>) => void>;
    }> = [];
    const stream = createStream({
      name: "reset.independent-projection",
      key: () => "reset/independent-projection",
      open: (_input, { abortSignal }) => {
        const session = {
          abortSignal,
          pending: [] as Array<(result: IteratorResult<number>) => void>,
        };
        sessions.push(session);
        const iterator: AsyncIterator<number> = {
          next: () => new Promise((resolve) => session.pending.push(resolve)),
          return: async () => ({ done: true, value: undefined }),
        };
        return { [Symbol.asyncIterator]: () => iterator };
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-independent-projection",
        operations: { stream },
      }),
    });
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 5 });
    expect(sessions).toHaveLength(1);
    sessions[0]!.pending[0]!({ done: false, value: 1 });
    await flush();
    expect(projection.value.get()).toEqual([1]);

    const reset = graph.reset();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]!.abortSignal.aborted).toBe(true);
    expect(projection.status.get()).toBe("open");
    expect(projection.value.get()).toEqual([]);
    await expect(reset).resolves.toBeUndefined();

    expect(sessions[0]!.pending).toHaveLength(2);
    sessions[0]!.pending[1]!({ done: false, value: 99 });
    await flush();
    expect(projection.value.get()).toEqual([]);
    sessions[1]!.pending[0]!({ done: false, value: 2 });
    await flush();
    expect(projection.value.get()).toEqual([2]);

    projection.close();
    graph.dispose();
  });

  it("does not publish a projection reducer result after the reducer resets its graph", async () => {
    const sessions: Array<{
      readonly abortSignal: AbortSignal;
      readonly pending: Array<(result: IteratorResult<number>) => void>;
    }> = [];
    const stream = createStream({
      name: "reset.reentrant-projection",
      key: () => "reset/reentrant-projection",
      open: (_input, { abortSignal }) => {
        const session = {
          abortSignal,
          pending: [] as Array<(result: IteratorResult<number>) => void>,
        };
        sessions.push(session);
        const iterator: AsyncIterator<number> = {
          next: () => new Promise((resolve) => session.pending.push(resolve)),
          return: async () => ({ done: true, value: undefined }),
        };
        return { [Symbol.asyncIterator]: () => iterator };
      },
    });
    let startReset = (): Promise<void> => Promise.resolve();
    let reset: Promise<void> | undefined;
    let resetFromReducer = false;
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-reentrant-projection",
        operations: { stream },
      }),
    });
    startReset = () => graph.reset();
    const projection = project(graph, stream, undefined, {
      kind: "reduce",
      initial: 0,
      step: (sum, item) => {
        if (!resetFromReducer) {
          resetFromReducer = true;
          reset = startReset();
        }
        return sum + item;
      },
    });
    sessions[0]!.pending[0]!({ done: false, value: 5 });
    await flush();
    await expect(reset).resolves.toBeUndefined();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]!.abortSignal.aborted).toBe(true);
    expect(projection.value.get()).toBe(0);

    sessions[1]!.pending[0]!({ done: false, value: 3 });
    await flush();
    expect(projection.value.get()).toBe(3);

    projection.close();
    graph.dispose();
  });

  it("clears a terminal projection when only its status and error readables are held", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "reset.held-terminal-readables",
      key: () => "reset/held-terminal-readables",
      open: backend.stream<void, number>("reset.held-terminal-readables"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-held-terminal-readables",
        operations: { stream },
      }),
    });
    const held = (() => {
      const projection = project(graph, stream, undefined, {
        kind: "reduce",
        initial: 10,
        step: (total, item) => total + item,
      });
      return { status: projection.status, error: projection.error };
    })();
    const failure = new Error("terminal projection failed");

    backend.end("reset.held-terminal-readables", failure);
    await flush();
    expect(held.status.get()).toBe("failed");
    expect(held.error.get()).toBe(failure);

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(held.status.get()).toBe("failed");
    expect(held.error.get()).toBeUndefined();
    graph.dispose();
  });

  it("releases terminal projection registry entries after their readables are collected", async () => {
    const contractModule = new URL("../../src/contract/index.ts", import.meta.url).href;
    const source = `
      const { createGraph, createStream, defineContract, project } = await import(${JSON.stringify(contractModule)});
      const retainedError = new Error("retained terminal projection");
      const endedStream = createStream({
        name: "reset.projection-finalizer-ended",
        key: () => "reset/projection-finalizer-ended",
        open: async function* () {},
      });
      const failedStream = createStream({
        name: "reset.projection-finalizer-failed",
        key: () => "reset/projection-finalizer-failed",
        open: () => { throw retainedError; },
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "reset-projection-finalizer",
          operations: { endedStream, failedStream },
        }),
      });
      const retained = project(graph, failedStream, undefined, { kind: "accumulate", max: 2 });
      if (retained.status.get() !== "failed" || retained.error.get() !== retainedError) {
        throw new Error("retained terminal projection did not fail as expected");
      }
      const registry = graph.__runtime.projections;
      async function makeCloseAndDropProjection() {
        const projection = project(graph, endedStream, undefined, { kind: "accumulate", max: 2 });
        await new Promise((resolve) => setImmediate(resolve));
        if (projection.status.get() !== "closed") throw new Error("projection did not close");
        projection.close();
      }
      try {
        for (let index = 0; index < 24; index += 1) await makeCloseAndDropProjection();
        if (registry.size <= 1) throw new Error("test did not register dropped projections");
        for (let attempt = 0; attempt < 24 && registry.size > 1; attempt += 1) {
          Bun.gc(true);
          await new Promise((resolve) => setImmediate(resolve));
        }
        if (registry.size !== 1) throw new Error("expected one retained terminal projection, found " + registry.size);
        await graph.reset();
        if (retained.status.get() !== "failed" || retained.error.get() !== undefined) {
          throw new Error("reset did not clear the retained terminal projection error");
        }
      } finally {
        graph.dispose();
      }
    `;
    const child = Bun.spawnSync([process.execPath, "-e", source], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const childError = new TextDecoder().decode(child.stderr);
    if (child.exitCode !== 0) throw new Error(childError || `GC child exited ${child.exitCode}`);
  });

  it("does not make a computed that closes a projection depend on resetVersion", async () => {
    const stream = createStream({
      name: "reset.close-tracking",
      key: () => "reset/close-tracking",
      open: () => ({
        [Symbol.asyncIterator]: () => ({
          next: () => new Promise<IteratorResult<number>>(() => undefined),
          return: async () => ({ done: true, value: undefined }),
        }),
      }),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-close-tracking", operations: { stream } }),
    });
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 4 });
    let evaluations = 0;
    const closeAndCount = computed(() => {
      evaluations += 1;
      projection.close();
      return evaluations;
    });

    expect(closeAndCount.get()).toBe(1);
    await graph.reset();
    expect(closeAndCount.get()).toBe(1);
    expect(evaluations).toBe(1);

    graph.dispose();
  });
});
