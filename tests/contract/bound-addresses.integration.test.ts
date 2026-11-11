import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  bindOperation,
  bindStream,
  createBindingContext,
  createGraph,
  createQuery,
  createStream,
  defineContract,
  Fault,
  inspectBoundAddress,
  storeKey,
  type BoundAddressEmpty,
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

function fixture(options: { readonly holdReturn?: boolean } = {}) {
  const backend = testBackend();
  let keys = 0;
  let opens = 0;
  let beforeKey = (): void => undefined;
  let beforePhysicalReturn = (): void => undefined;
  let firstPullStarted = false;
  const firstPull = Promise.withResolvers<void>();
  const firstValue = Promise.withResolvers<IteratorResult<number | undefined>>();
  const returnStarted = Promise.withResolvers<void>();
  const physicalReturn = Promise.withResolvers<IteratorResult<number | undefined>>();
  const stream = createStream({
    name: "events",
    key: (input: string) => {
      keys += 1;
      beforeKey();
      return input;
    },
    open: (input: string, openOptions) => {
      opens += 1;
      if (options.holdReturn !== true)
        return backend.stream<string, number | undefined>("events")(input, openOptions);
      return {
        [Symbol.asyncIterator]() {
          return {
            next() {
              if (!firstPullStarted) {
                firstPullStarted = true;
                firstPull.resolve();
                return firstValue.promise;
              }
              return new Promise<IteratorResult<number | undefined>>(() => undefined);
            },
            return() {
              returnStarted.resolve();
              beforePhysicalReturn();
              return physicalReturn.promise;
            },
          };
        },
      };
    },
  });
  const query = createQuery({
    name: "read",
    key: (input: string) => input,
    fetch: backend.respond<string, number | undefined>("read"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-addresses", operations: { stream, query } }),
    idleMs: 10,
  });
  const context = createBindingContext(graph);
  const binding = bindStream(graph, stream, graph.api.stream, { context });
  const queries = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
  return {
    backend,
    graph,
    context,
    stream,
    binding,
    queries,
    keys: () => keys,
    opens: () => opens,
    firstPull: () => firstPull.promise,
    deliverFirst(value: number | undefined) {
      firstValue.resolve({ done: false, value });
    },
    returnStarted: () => returnStarted.promise,
    beforePhysicalReturn(callback: () => void) {
      beforePhysicalReturn = callback;
    },
    finishPhysicalReturn() {
      physicalReturn.resolve({ done: true, value: undefined });
    },
    beforeKey: (callback: () => void) => {
      beforeKey = callback;
    },
  };
}

function emptyCondition(
  binding: Parameters<typeof inspectBoundAddress>[0],
  key: string,
): BoundAddressEmpty {
  const inspected = inspectBoundAddress(binding, storeKey(key));
  expect(inspected).toMatchObject({
    kind: "stable",
    state: "empty",
    present: false,
    basePresent: false,
  });
  if (inspected.kind !== "stable" || inspected.state !== "empty")
    throw new Error("Expected an empty native address");
  return inspected.condition;
}

it("inspects missing native addresses without key/open/fetch activity and checks exact provenance", () => {
  const f = fixture();
  try {
    const key = storeKey("Room/é");
    const inspected = inspectBoundAddress(f.binding, key);
    expect(inspected).toMatchObject({
      kind: "stable",
      state: "empty",
      present: false,
      basePresent: false,
    });
    expect(f.keys()).toBe(0);
    expect(f.backend.calls).toHaveLength(0);
    if (inspected.kind !== "stable") throw new Error("Expected stable address");
    expect(inspectBoundAddress(f.binding, key, { expected: inspected.stamp })).toMatchObject({
      state: "empty",
    });
    const other = bindStream(f.graph, f.stream, f.graph.api.stream, {
      context: createBindingContext(f.graph),
    });
    expect(() => inspectBoundAddress(other, key, { expected: inspected.stamp })).toThrow(TypeError);
    expect(() =>
      inspectBoundAddress(f.binding, storeKey("other"), { expected: inspected.stamp }),
    ).toThrow(TypeError);
    expect(() => inspectBoundAddress(Object.create(f.binding), key)).toThrow(TypeError);
    expect(() => inspectBoundAddress(f.binding, "" as never)).toThrow(TypeError);
    expect(() => inspectBoundAddress(f.binding, key, { expected: {} as never })).toThrow(TypeError);
    expect(f.keys()).toBe(0);
    expect(f.backend.calls).toHaveLength(0);
  } finally {
    f.graph.dispose();
  }
});

it.each([false, true])(
  "distinguishes an active empty session from present undefined (initial=%s)",
  async (initial) => {
    const f = fixture();
    try {
      const source = f.binding.member("one", initial ? { initial: undefined } : undefined);
      expect(inspectBoundAddress(f.binding, source.key)).toMatchObject({
        kind: "stable",
        state: "occupied",
        present: initial,
        basePresent: initial,
      });
      f.backend.end("events");
      await f.binding.session(source).detached;
      expect(inspectBoundAddress(f.binding, source.key)).toMatchObject({
        kind: "stable",
        state: initial ? "occupied" : "empty",
        present: initial,
        basePresent: initial,
      });
    } finally {
      f.graph.dispose();
    }
  },
);

it("permanently invalidates an empty condition across an empty opening and one-use admission", async () => {
  const f = fixture();
  try {
    const key = storeKey("one");
    const original = inspectBoundAddress(f.binding, key);
    if (original.kind !== "stable" || original.state !== "empty")
      throw new Error("Expected missing address");
    const a = f.binding.member("one", { expectEmpty: original.condition });
    f.backend.end("events");
    await f.binding.session(a).detached;
    expect(inspectBoundAddress(f.binding, key, { expected: original.stamp })).toEqual({
      kind: "changed",
    });
    expect(() => f.binding.member("one", { expectEmpty: original.condition })).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(1);
    const current = emptyCondition(f.binding, "one");
    const b = f.binding.member("one", { expectEmpty: current });
    expect(f.binding.session(b).identity).not.toBe(f.binding.session(a).identity);
    expect(() => f.binding.member("one", { expectEmpty: current })).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(2);
  } finally {
    f.graph.dispose();
  }
});

it.each(["options", "key"] as const)(
  "rechecks expectEmpty after an authored %s callback acquires the address",
  (phase) => {
    const f = fixture();
    try {
      const condition = emptyCondition(f.binding, "one");
      let once = false;
      let nested: ReturnType<typeof f.binding.member> | undefined;
      const acquire = (): void => {
        if (once) return;
        once = true;
        nested = f.binding.member("one", { initial: 41 });
      };
      if (phase === "key") f.beforeKey(acquire);
      const options = {
        expectEmpty: condition,
        get abortSignal() {
          if (phase === "options") acquire();
          return undefined;
        },
      };
      expect(() => f.binding.member("one", options)).toThrow(TypeError);
      expect(f.backend.calls).toHaveLength(1);
      expect(nested!.value.get()).toBe(41);
      expect(nested!.pending.get()).toBe(true);
      expect(inspectBoundAddress(f.binding, storeKey("one"))).toMatchObject({
        state: "occupied",
        present: true,
      });
    } finally {
      f.graph.dispose();
    }
  },
);

it("reports finite bound query occupation without acquiring or refreshing, and rejects its condition on a stream", async () => {
  const f = fixture();
  try {
    const condition = emptyCondition(f.queries, "one");
    expect(() => f.binding.member("one", { expectEmpty: condition })).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(0);
    const query = f.queries.member("one");
    const pending = inspectBoundAddress(f.queries, query.key);
    expect(pending).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: false,
      basePresent: false,
    });
    expect(f.backend.calls).toHaveLength(1);
    f.backend.resolve(f.backend.pending("read")[0]!, undefined);
    await query.ready;
    expect(inspectBoundAddress(f.queries, query.key)).toMatchObject({
      state: "occupied",
      present: true,
      basePresent: true,
    });
    if (pending.kind !== "stable") throw new Error("Expected finite request address");
    expect(inspectBoundAddress(f.queries, query.key, { expected: pending.stamp })).toEqual({
      kind: "changed",
    });
    expect(f.backend.calls).toHaveLength(1);
  } finally {
    f.graph.dispose();
  }
});

it("leaves unknown raw/query fields ignored while the genuine bound stream validates expectEmpty", async () => {
  const f = fixture();
  try {
    const options = { initial: 3, expectEmpty: Object.freeze({}) };
    const raw = f.graph.api.stream("one", options);
    const query = f.queries.member("one", options);
    expect([raw.value.get(), query.value.get()]).toEqual([3, 3]);
    expect(() => f.binding.member("one", options as never)).toThrow(TypeError);
    expect(f.backend.calls.map((call) => call.name)).toEqual(["events"]);
    const refresh = query.revalidate();
    expect(f.backend.calls.map((call) => call.name)).toEqual(["events", "read"]);
    f.backend.resolve(f.backend.pending("read")[0]!, 4);
    await refresh;
    expect(await query.ready).toBe(4);
  } finally {
    f.graph.dispose();
  }
});

it("invalidates even missing-address stamps on reset and keeps scope disposal in the fault channel", async () => {
  const f = fixture();
  try {
    const key = storeKey("missing");
    const before = inspectBoundAddress(f.binding, key);
    if (before.kind !== "stable" || before.state !== "empty")
      throw new Error("Expected missing address");
    await f.graph.reset();
    expect(inspectBoundAddress(f.binding, key, { expected: before.stamp })).toEqual({
      kind: "changed",
    });
    expect(() => f.binding.member("missing", { expectEmpty: before.condition })).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(0);
    f.context.dispose();
    expect(() => inspectBoundAddress(f.binding, key)).toThrow(Fault);
  } finally {
    f.graph.dispose();
  }
});
