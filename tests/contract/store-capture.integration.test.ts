import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  affects,
  computed,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createStream,
  Fault,
  observeStore,
  peekStore,
  subscribe,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});
afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function fixture() {
  const backend = testBackend();
  const query = createQuery({
    name: "query",
    key: (key: string) => key,
    fetch: backend.respond<string, number | undefined>("query"),
    revalidateAfterMs: 5,
  });
  const graph = createGraph({
    contract: { namespace: "capture", operations: { query } },
    idleMs: 10,
  });
  const store = graph.api.query("one");
  return { backend, graph, store };
}

it("distinguishes absence from present undefined across native shells and returned wrappers", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "query",
    key: (key: string) => key,
    fetch: backend.respond<string, number | undefined>("query"),
  });
  const stream = createStream({
    name: "stream",
    key: (key: string) => key,
    open: backend.stream<string, number | undefined>("stream"),
  });
  const declared = createStore({ name: "declared", initial: undefined });
  const graph = createGraph({
    contract: { namespace: "presence-capture", operations: { query, stream, declared } },
  });
  try {
    const empty = graph.at("empty");
    expect(peekStore(empty)).toMatchObject({
      kind: "stable",
      present: false,
      identity: empty.identity,
    });
    expect(Object.isFrozen(peekStore(empty))).toBe(true);
    empty.set(undefined);
    expect(peekStore(empty)).toMatchObject({ kind: "stable", present: true, value: undefined });
    const seeded = graph.api.declared();
    expect(peekStore(seeded)).toMatchObject({ kind: "stable", present: true, value: undefined });
    const incoming = graph.api.stream("incoming", { initial: undefined });
    expect(peekStore(incoming)).toMatchObject({ kind: "stable", present: true, value: undefined });
    expect(incoming.pending.get()).toBe(true);
    const requested = graph.api.query("requested");
    const ready = requested.ready;
    backend.resolve(
      backend.calls.find((call) => call.name === "query")!,
      undefined,
    );
    await ready;
    expect(peekStore(requested)).toMatchObject({ kind: "stable", present: true, value: undefined });
    for (const store of [empty, seeded, incoming, requested])
      expect(() => peekStore(Object.create(store))).toThrow(TypeError);
  } finally {
    graph.dispose();
  }
});

it("orders value changes independently of request starts and issue-only updates", async () => {
  const f = fixture();
  try {
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    const lease = observeStore(f.graph, f.store);
    const initial = peekStore(f.store);
    if (initial.kind !== "stable") throw new Error("Expected stable initial capture");
    const request = f.store.revalidate();
    expect(peekStore(f.store)).toStrictEqual(initial);
    lease.reject(Object.freeze({ invalid: true }));
    expect(peekStore(f.store)).toStrictEqual(initial);
    lease.publish(1);
    const equal = peekStore(f.store);
    if (equal.kind !== "stable") throw new Error("Expected stable equal publication");
    expect(equal.identity).toBe(initial.identity);
    expect(equal.valueRevision).toBeGreaterThan(initial.valueRevision);
    expect(lease.issue.get()).toBeUndefined();
    f.backend.resolve(f.backend.calls[1]!, 2);
    await request;
    const landed = peekStore(f.store);
    if (landed.kind !== "stable") throw new Error("Expected stable request landing");
    expect(landed.valueRevision).toBeGreaterThan(equal.valueRevision);
    expect(landed).toMatchObject({ present: true, value: 2 });
  } finally {
    f.graph.dispose();
  }
});

it("returns changed when a prediction dies during the actual folded capture, then reports absence", async () => {
  const backend = testBackend();
  const diagnostics: unknown[] = [];
  const query = createQuery({
    name: "number",
    key: (_input: void) => "number",
    fetch: backend.respond<void, number>("number"),
  });
  let failCapture = false;
  const mutation = createMutation({
    name: "change",
    run: backend.perform<void, void>("change"),
    affects: [
      affects(query, {
        select: () => undefined,
        on: "invalidate",
        optimistic: () => {
          if (failCapture) {
            failCapture = false;
            throw new Error("prediction failed");
          }
          return 3;
        },
      }),
    ],
  });
  const graph = createGraph({
    contract: { namespace: "prediction-capture", operations: { query, mutation } },
    onUnobservedFault: (cause) => {
      diagnostics.push(cause);
    },
  });
  try {
    const store = graph.api.query();
    const run = graph.api.mutation();
    failCapture = true;
    expect(peekStore(store)).toStrictEqual({ kind: "changed" });
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: false });
    expect(store.available.get()).toBe(false);
    expect(diagnostics).toHaveLength(1);
    backend.resolve(backend.calls[1]!, undefined);
    await run;
  } finally {
    graph.dispose();
  }
});

it("does not create an outer reactive dependency or bypass graph affinity", () => {
  const f = fixture();
  const other = fixture();
  try {
    f.store.set(1);
    other.store.set(2);
    let evaluations = 0;
    const captured = computed(() => {
      evaluations += 1;
      return peekStore(f.store);
    });
    const stop = subscribe(captured, () => undefined);
    const original = captured.get();
    f.store.set(3);
    expect(captured.get()).toBe(original);
    expect(evaluations).toBe(1);
    const crossed = computed(() => {
      f.store.value.get();
      return peekStore(other.store);
    });
    expect(() => crossed.get()).toThrow(Fault);
    stop();
  } finally {
    f.graph.dispose();
    other.graph.dispose();
  }
});

it("captures a prediction without a base and loses presence after its rejected mutation", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "number",
    key: (_input: void) => "number",
    fetch: backend.respond<void, number>("number"),
  });
  const mutation = createMutation({
    name: "change",
    run: backend.perform<void, void>("change"),
    affects: [affects(query, { select: () => undefined, on: "invalidate", optimistic: () => 3 })],
  });
  const graph = createGraph({
    contract: { namespace: "prediction-presence", operations: { query, mutation } },
  });
  try {
    const store = graph.api.query();
    const run = graph.api.mutation();
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: true, value: 3 });
    expect(store.available.get()).toBe(true);
    expect(store.status.get()).toBe("fetching");
    const failure = new Error("mutation rejected");
    backend.reject(backend.calls[1]!, failure);
    await expect(run).rejects.toBe(failure);
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: false });
  } finally {
    graph.dispose();
  }
});

it("cannot return a pre-reset value when the authored fold resets its native graph", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "number",
    key: (_input: void) => "number",
    fetch: backend.respond<void, number>("number"),
  });
  let resetCapture = false;
  let reset: Promise<void> | undefined;
  let resetGraph!: () => Promise<void>;
  const mutation = createMutation({
    name: "change",
    run: backend.perform<void, void>("change"),
    affects: [
      affects(query, {
        select: () => undefined,
        on: "invalidate",
        optimistic: () => {
          if (resetCapture) {
            resetCapture = false;
            reset = resetGraph();
          }
          return 3;
        },
      }),
    ],
  });
  const graph = createGraph({
    contract: { namespace: "capture-reset", operations: { query, mutation } },
  });
  resetGraph = () => graph.reset();
  try {
    const store = graph.api.query();
    const run = graph.api.mutation();
    const outcome = run.catch(() => undefined);
    resetCapture = true;
    expect(peekStore(store)).toStrictEqual({ kind: "changed" });
    expect(reset).toBeDefined();
    await reset;
    backend.resolve(backend.calls[1]!, undefined);
    await outcome;
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: false });
  } finally {
    graph.dispose();
  }
});

it("does not cache an obsolete folded value after equality resets and replaces the entry", async () => {
  const backend = testBackend();
  let armed = false;
  let replace!: () => void;
  let reset: Promise<void> | undefined;
  const query = createQuery({
    name: "query",
    key: (_input: void) => "one",
    fetch: backend.respond<void, number>("query"),
    equals: () => {
      if (armed) {
        armed = false;
        replace();
      }
      return false;
    },
  });
  const graph = createGraph({ contract: { namespace: "capture-cache", operations: { query } } });
  try {
    const store = graph.api.query();
    store.set(1);
    peekStore(store);
    const lease = observeStore(graph, store);
    replace = () => {
      reset = graph.reset();
      store.set(7);
    };
    expect(lease.publish(2)).toBe(true);
    armed = true;
    expect(peekStore(store)).toStrictEqual({ kind: "changed" });
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: true, value: 7 });
    await reset;
  } finally {
    graph.dispose();
  }
});

it("capture and active observers do not renew an idle query or revive its collected incarnation", async () => {
  const f = fixture();
  try {
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    const lease = observeStore(f.graph, f.store);
    const activity: boolean[] = [];
    const stop = subscribe(lease.active, (active) => {
      activity.push(active);
    });
    for (let elapsed = 0; elapsed < 10; elapsed++) {
      peekStore(f.store);
      lease.active.get();
      jest.advanceTimersByTime(1);
    }
    expect(activity).toStrictEqual([true, false]);
    expect(lease.publish(9)).toBe(false);
    expect(peekStore(f.store)).toMatchObject({ kind: "stable", present: false });
    expect(() => observeStore(f.graph, f.store)).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(1);
    stop();
  } finally {
    f.graph.dispose();
  }
});

for (const metadata of ["available", "issue"] as const) {
  it(`${metadata} subscriptions retain metadata and join reset without fetching on activation`, async () => {
    const f = fixture();
    try {
      const ready = f.store.ready;
      f.backend.resolve(f.backend.calls[0]!, 1);
      await ready;
      const lease = observeStore(f.graph, f.store);
      const stop =
        metadata === "available"
          ? subscribe(f.store.available, () => undefined)
          : subscribe(lease.issue, () => undefined);
      jest.advanceTimersByTime(20);
      expect(f.backend.calls).toHaveLength(1);
      expect(lease.active.get()).toBe(true);
      const reset = f.graph.reset();
      expect(f.backend.calls).toHaveLength(2);
      f.backend.resolve(f.backend.calls[1]!, 2);
      await reset;
      expect(lease.active.get()).toBe(false);
      stop();
    } finally {
      f.graph.dispose();
    }
  });
}
