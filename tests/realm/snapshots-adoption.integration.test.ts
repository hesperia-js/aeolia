import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import vm from "node:vm";
import {
  Fault,
  Signal,
  adopt,
  captureReactiveConstruction,
  computed,
  createGraph,
  defineContract,
  createMutation,
  createQuery,
  createStore,
  createStream,
  createPositionedStream,
  capturePositionedStore,
  preparePositionedStoreRestore,
  project,
  affects,
  snapshot,
  observeStore,
  peekStore,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
  signal,
  watch,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";
import type {
  ClosedEncodedValue,
  Contract,
  EncodingId,
  OperationTree,
  RequiredTransferCodec,
  Snapshot,
} from "../../src/index.ts";

const requiredTransferCodec: RequiredTransferCodec = {
  encodeValue: (value) => value as ClosedEncodedValue,
  encodeFailure: (cause) => String(cause),
  decodeValue: (value) => value,
  decodeFailure: (failure) => new Error(String(failure)),
};

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

function queryContract<const T extends OperationTree>(operations: T): Contract<T> {
  return defineContract({ namespace: "realm-tests", operations } as never) as Contract<T>;
}

describe("realm snapshots and adoption", () => {
  it("encodes tagged values, special numbers, and recursively traverses Map keys", async () => {
    const backend = testBackend();
    const sharedDate = new Date("2024-01-02T03:04:05.000Z");
    const map = new Map<unknown, unknown>([[sharedDate, new Set([NaN, Infinity, -Infinity])]]);
    const value: Record<string, unknown> = {
      map,
      date: sharedDate,
      bigint: 12345678901234567890n,
      undef: undefined,
      negativeZero: -0,
    };
    const query = createQuery({
      name: "realm.value",
      key: () => "realm/value",
      fetch: backend.respond("realm.value"),
    });
    const source = createGraph({ contract: queryContract({ query }) });
    source.api.query(undefined, { initial: value });
    const encoded = snapshot(source);
    const root = encoded.entries["realm/value"]!.value as Record<string, unknown>;
    expect(root.date).toEqual({ $ref: "/map/$map/0/0" });
    expect((root.map as { $map: unknown[][] }).$map[0]![0]).toEqual({
      $date: "2024-01-02T03:04:05.000Z",
    });
    expect(root.bigint).toEqual({ $bigint: "12345678901234567890" });
    expect(root.undef).toEqual({ $undefined: true });
    expect(root.negativeZero).toEqual({ $num: "-0" });

    const target = createGraph({ contract: queryContract({ query }) });
    const report = adopt(target, encoded);
    expect(report.adopted.map(String)).toEqual(["realm/value"]);
    const decoded = target.at("realm/value").value.get() as Record<string, unknown>;
    expect(decoded.date).toBeInstanceOf(Date);
    const decodedMap = decoded.map as Map<unknown, unknown>;
    const decodedKey = [...decodedMap.keys()][0]!;
    expect(decodedKey === decoded.date).toBe(true);
    const decodedSet = decodedMap.get(decodedKey) as Set<number>;
    expect(decodedSet).toBeInstanceOf(Set);
    expect([...decodedSet][0]).toBeNaN();
    expect(Object.is(decoded.negativeZero, -0)).toBe(true);
    expect(decoded.bigint).toBe(12345678901234567890n);
    expect(decoded.undef).toBeUndefined();
    source.dispose();
    target.dispose();
  });

  it("round-trips cycles, aliases, sparse arrays, and the $plain escape", () => {
    const backend = testBackend();
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const alias = { value: 7 };
    const dollar: Record<string, unknown> = { $status: "ok" };
    const array = Array.from<unknown>({ length: 2 });
    array[1] = alias;
    const query = createQuery({
      name: "realm.graph",
      key: () => "realm/graph",
      fetch: backend.respond("realm.graph"),
    });
    const source = createGraph({ contract: queryContract({ query }) });
    source.api.query(undefined, { initial: { cyclic, alias, again: alias, dollar, array } });
    const encoded = snapshot(source);
    const root = encoded.entries["realm/graph"]!.value as Record<string, unknown>;
    expect(root.dollar).toEqual({ $plain: { $status: "ok" } });
    expect((root.cyclic as Record<string, unknown>).self).toEqual({ $ref: "/cyclic" });
    const target = createGraph({ contract: queryContract({ query }) });
    adopt(target, encoded);
    const decoded = target.at("realm/graph").value.get() as Record<string, unknown>;
    expect((decoded.cyclic as Record<string, unknown>).self).toBe(decoded.cyclic);
    expect(decoded.alias).toBe(decoded.again);
    const decodedArray = decoded.array as unknown[];
    expect(decodedArray).toHaveLength(2);
    expect(decodedArray[0]).toBeUndefined();
    expect(Object.prototype.hasOwnProperty.call(decodedArray, "0")).toBe(true);
    expect(decodedArray[1]).toBe(decoded.alias);
    source.dispose();
    target.dispose();
  });

  it("handles invalid dates and Date values from another realm", () => {
    const backend = testBackend();
    const foreignDate = vm.runInNewContext("new Date(0)") as Date;
    expect(foreignDate instanceof Date).toBe(false);
    const query = createQuery({
      name: "realm.date",
      key: () => "realm/date",
      fetch: backend.respond("realm.date"),
    });
    const source = createGraph({ contract: queryContract({ query }) });
    source.api.query(undefined, { initial: { foreignDate, invalid: new Date(Number.NaN) } });
    const encoded = snapshot(source);
    expect(encoded.entries["realm/date"]!.value).toEqual({
      foreignDate: { $date: "1970-01-01T00:00:00.000Z" },
      invalid: { $date: null },
    });
    const target = createGraph({ contract: queryContract({ query }) });
    adopt(target, encoded);
    const value = target.at("realm/date").value.get() as Record<string, Date>;
    expect(value.foreignDate).toBeInstanceOf(Date);
    expect(value.foreignDate!.getTime()).toBe(0);
    expect(value.invalid).toBeInstanceOf(Date);
    expect(Number.isNaN(value.invalid!.getTime())).toBe(true);
    source.dispose();
    target.dispose();
  });

  it("does not preserve identity across entries", () => {
    const backend = testBackend();
    const shared = { count: 1 };
    const first = createQuery({
      name: "realm.first",
      key: () => "realm/first",
      fetch: backend.respond("realm.first"),
    });
    const second = createQuery({
      name: "realm.second",
      key: () => "realm/second",
      fetch: backend.respond("realm.second"),
    });
    const source = createGraph({ contract: queryContract({ first, second }) });
    source.api.first(undefined, { initial: shared });
    source.api.second(undefined, { initial: shared });
    const encoded = snapshot(source);
    const target = createGraph({ contract: queryContract({ first, second }) });
    adopt(target, encoded);
    expect(target.at("realm/first").value.get()).not.toBe(target.at("realm/second").value.get());
    source.dispose();
    target.dispose();
  });

  it("keeps own prototype-looking keys and ignores symbol keys", () => {
    const backend = testBackend();
    const value: Record<string, unknown> = {};
    Object.defineProperty(value, "__proto__", {
      value: { safe: true },
      enumerable: true,
      writable: true,
      configurable: true,
    });
    value.visible = "yes";
    value["$tag"] = "literal";
    const dropped = Symbol("dropped");
    (value as Record<PropertyKey, unknown>)[dropped] = "gone";
    const query = createQuery({
      name: "realm.keys",
      key: () => "realm/keys",
      fetch: backend.respond("realm.keys"),
    });
    const source = createGraph({ contract: queryContract({ query }) });
    source.api.query(undefined, { initial: value });
    const encoded = snapshot(source);
    const target = createGraph({ contract: queryContract({ query }) });
    adopt(target, encoded);
    const decoded = target.at("realm/keys").value.get() as Record<string, unknown>;
    expect(Object.keys(decoded)).toEqual(["__proto__", "visible", "$tag"]);
    expect(decoded["__proto__"]).toEqual({ safe: true });
    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
    expect(Object.prototype.hasOwnProperty.call(decoded, "dropped")).toBe(false);
    source.dispose();
    target.dispose();
  });

  it("skips undecodable entries, reports them, and adopts the rest", () => {
    const contract = defineContract({ namespace: "realm-tests", operations: {} });
    const graph = createGraph({ contract });
    const entries: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    entries.bad = { value: { $nope: true }, generation: 0, age: 0 };
    entries.badPayload = { value: { $date: 5 }, generation: 0, age: 0 };
    entries.good = { value: { ok: true }, generation: 0, age: 0 };
    const report = adopt(graph, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries,
    } as never);
    expect(report.adopted.map(String)).toEqual(["good"]);
    expect(report.skipped.map(String)).toEqual(["bad", "badPayload"]);
    expect(graph.at("good").value.get()).toEqual({ ok: true });
    graph.dispose();
  });

  it("rejects unsupported envelope metadata before adopting anything", () => {
    const contract = defineContract({ namespace: "realm-tests", operations: {} });
    const graph = createGraph({ contract });
    const valid: Snapshot = {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1" as EncodingId,
      entries: {},
    };
    for (const candidate of [
      { ...valid, version: 2 },
      { ...valid, encoding: "future/2" },
      { ...valid, namespace: "other" },
    ]) {
      expect(() => adopt(graph, candidate as Snapshot)).toThrow(Fault);
      try {
        adopt(graph, candidate as Snapshot);
      } catch (error) {
        expect((error as Fault).kind).toBe("snapshot");
      }
    }
    expect(Object.keys(snapshot(graph).entries)).toHaveLength(0);
    graph.dispose();
  });

  it("reports malformed age and generation as skipped entries", () => {
    const contract = defineContract({ namespace: "realm-tests", operations: {} });
    const graph = createGraph({ contract });
    const entries: Record<string, unknown> = {
      negativeAge: { value: 1, generation: 0, age: -1 },
      fractionalGeneration: { value: 2, generation: 1.5, age: 0 },
      infinityAge: { value: 3, generation: 0, age: Number.POSITIVE_INFINITY },
      good: { value: 4, generation: 3, age: 0 },
    };
    const report = adopt(graph, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries,
    } as never);
    expect(report.skipped.map(String)).toEqual([
      "negativeAge",
      "fractionalGeneration",
      "infinityAge",
    ]);
    expect(report.adopted.map(String)).toEqual(["good"]);
    expect(graph.at("good").value.get()).toBe(4);
    graph.dispose();
  });

  it("adopts all entries in one internal pass and omits predictions", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "realm.predicted",
      key: () => "realm/predicted",
      fetch: backend.respond("realm.predicted"),
    });
    const mutation = createMutation({
      name: "realm.mutate",
      affects: [
        affects(query, {
          select: () => undefined,
          on: "invalidate",
          optimistic: (current) => ({
            count: ((current as { count: number } | undefined)?.count ?? 0) + 1,
          }),
        }),
      ],
      run: backend.perform("realm.mutate"),
    });
    const source = createGraph({ contract: queryContract({ query, mutation }) });
    const queryStore = source.api.query(undefined, { initial: { count: 0 } });
    const mutationPromise = source.api.mutation(undefined);
    await flush();
    expect(queryStore.value.get()).toEqual({ count: 1 });
    const encoded = snapshot(source);
    expect(encoded.entries["realm/predicted"]!.value).toEqual({ count: 0 });
    const encodedV2 = snapshot(source, { version: 2 });
    expect(encodedV2.entries).toHaveLength(1);
    expect(encodedV2.entries[0]).toMatchObject({
      address: { kind: "raw", key: "realm/predicted" },
      value: { count: 0 },
    });

    const target = createGraph({ contract: queryContract({ query }) });
    const a = target.at("a");
    const b = target.at("b");
    const combined = computed(() => `${String(a.value.get())}:${String(b.value.get())}`);
    let notifications = 0;
    const stop = watch(combined, () => {
      notifications += 1;
    });
    adopt(target, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries: {
        a: { value: 1, generation: 0, age: 0 },
        b: { value: 2, generation: 0, age: 0 },
      },
    } as never);
    expect(notifications).toBe(1);
    stop();
    backend.resolve(
      backend.calls.find((call) => call.name === "realm.mutate")!,
      undefined,
    );
    await mutationPromise;
    source.dispose();
    target.dispose();
  });

  it("uses adopted age for passive freshness and fetches only from a member trigger", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "realm.fresh",
      key: () => "realm/fresh",
      revalidateAfterMs: 100,
      fetch: backend.respond("realm.fresh"),
    });
    const source = createGraph({ contract: queryContract({ query }) });
    source.api.query(undefined, { initial: "server" });
    jest.advanceTimersByTime(50);
    const encoded = snapshot(source);
    expect(encoded.entries["realm/fresh"]!.age).toBe(50);
    const target = createGraph({ contract: queryContract({ query }) });
    adopt(target, encoded);
    expect(backend.calls).toHaveLength(0);
    const queryStore = target.api.query(undefined);
    expect(queryStore.status.get()).toBe("ready");
    const stop = watch(queryStore.value, () => undefined);
    jest.advanceTimersByTime(49);
    expect(backend.calls).toHaveLength(0);
    jest.advanceTimersByTime(1);
    expect(backend.calls).toHaveLength(1);
    expect(queryStore.status.get()).toBe("revalidating");
    stop();
    source.dispose();
    target.dispose();
    await flush();
  });

  it("fetches immediately for an adopted value older than the caller window", () => {
    const backend = testBackend();
    const query = createQuery({
      name: "realm.old",
      key: () => "realm/old",
      fetch: backend.respond("realm.old"),
    });
    const graph = createGraph({ contract: queryContract({ query }) });
    adopt(graph, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries: { "realm/old": { value: "old", generation: 2, age: 100 } },
    } as never);
    const store = graph.api.query(undefined, { revalidateAfterMs: 50 });
    expect(backend.calls).toHaveLength(1);
    expect(store.value.get()).toBe("old");
    expect(store.status.get()).toBe("revalidating");
    graph.dispose();
  });

  it("lets adoption configure an at-reached key before the first query member call", () => {
    const backend = testBackend();
    const query = createQuery({
      name: "realm.configure",
      key: () => "realm/configure",
      fetch: backend.respond("realm.configure"),
    });
    const graph = createGraph({ contract: queryContract({ query }) });
    adopt(graph, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries: { "realm/configure": { value: "adopted", generation: 4, age: 0 } },
    } as never);
    // The stated initial configures the previously unconfigured at-store but
    // must not overwrite a value adoption has already landed.
    const store = graph.api.query(undefined, { initial: "configured" });
    expect(store.value.get()).toBe("adopted");
    expect(store.status.get()).toBe("ready");
    expect(backend.calls).toHaveLength(0);
    graph.dispose();
  });

  it("preserves an adopted value when the first stream member states an initial", () => {
    const backend = testBackend();
    const stream = createStream({
      name: "realm.stream-configure",
      key: () => "realm/stream-configure",
      open: backend.stream<void, string>("realm.stream-configure"),
    });
    const graph = createGraph({ contract: queryContract({ stream }) });
    adopt(graph, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries: {
        "realm/stream-configure": { value: "adopted", generation: 4, age: 0 },
      },
    } as never);

    const store = graph.api.stream(undefined, { initial: "configured" });
    expect(store.value.get()).toBe("adopted");
    expect(store.status.get()).toBe("opening");
    expect(backend.calls).toHaveLength(1);
    graph.dispose();
  });

  it("adoption issues a newer generation and discards an older request", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "realm.race",
      key: () => "realm/race",
      fetch: backend.respond("realm.race"),
    });
    const graph = createGraph({ contract: queryContract({ query }) });
    const store = graph.api.query(undefined);
    const oldCall = backend.calls[0]!;
    adopt(graph, {
      version: 1,
      namespace: "realm-tests",
      encoding: "aeolia-tagged/1",
      entries: { "realm/race": { value: "adopted", generation: 8, age: 0 } },
    } as never);
    backend.resolve(oldCall, "late");
    await flush();
    expect(store.value.get()).toBe("adopted");
    expect(store.error.get()).toBeUndefined();
    graph.dispose();
  });

  it("faults values with no encoding, including accessor throws", () => {
    const backend = testBackend();
    const badFunction = createQuery({
      name: "realm.fn",
      key: () => "realm/fn",
      fetch: backend.respond("realm.fn"),
    });
    const badAccessor = createQuery({
      name: "realm.accessor",
      key: () => "realm/accessor",
      fetch: backend.respond("realm.accessor"),
    });
    const value: Record<string, unknown> = {};
    Object.defineProperty(value, "throws", {
      enumerable: true,
      get: () => {
        throw new Error("getter");
      },
    });
    const graph = createGraph({ contract: queryContract({ badFunction, badAccessor }) });
    graph.api.badFunction(undefined, { initial: () => undefined });
    expect(() => snapshot(graph)).toThrow(Fault);
    try {
      snapshot(graph);
    } catch (error) {
      expect((error as Fault).kind).toBe("encoding");
      expect((error as Fault).involved).toEqual(["realm/fn"]);
    }
    const accessorGraph = createGraph({ contract: queryContract({ badAccessor }) });
    accessorGraph.api.badAccessor(undefined, { initial: value });
    expect(() => snapshot(accessorGraph)).toThrow(Fault);
    try {
      snapshot(accessorGraph);
    } catch (error) {
      expect((error as Fault).kind).toBe("encoding");
      expect((error as Fault).involved).toEqual(["realm/accessor"]);
    }
    graph.dispose();
    accessorGraph.dispose();
  });

  it("faults each unsupported runtime value with its store key", () => {
    class CustomValue {
      readonly value = 1;
    }
    const unsupported: readonly [string, unknown][] = [
      ["function", () => undefined],
      ["promise", Promise.resolve(1)],
      ["weak-map", new WeakMap<object, unknown>()],
      ["class", new CustomValue()],
    ];
    for (const [name, value] of unsupported) {
      const backend = testBackend();
      const query = createQuery({
        name: `realm.unsupported.${name}`,
        key: () => `realm/unsupported/${name}`,
        fetch: backend.respond(`realm.unsupported.${name}`),
      });
      const graph = createGraph({ contract: queryContract({ query }) });
      graph.api.query(undefined, { initial: value });
      try {
        snapshot(graph);
        throw new Error("expected an encoding fault");
      } catch (error) {
        expect(error).toBeInstanceOf(Fault);
        expect((error as Fault).kind).toBe("encoding");
        expect((error as Fault).involved).toEqual([`realm/unsupported/${name}`]);
      }
      graph.dispose();
    }
  });

  it("does not snapshot omitted stores and faults snapshot/adopt after disposal", () => {
    const backend = testBackend();
    const omitted = createStore({ name: "local", initial: 1 });
    const persisted = createStore({ name: "persisted", initial: 3, snapshot: true });
    const query = createQuery({
      name: "realm.omit",
      key: () => "realm/omit",
      fetch: backend.respond("realm.omit"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "realm-tests",
        operations: { omitted, persisted, query },
      }),
    });
    const atOnly = graph.at("realm/at-only");
    atOnly.set("private");
    graph.api.persisted();
    graph.api.query(undefined, { initial: 2 });
    const encoded = snapshot(graph);
    expect(Object.keys(encoded.entries)).toEqual(["persisted", "realm/omit"]);
    graph.dispose();
    expect(() => snapshot(graph)).toThrow("disposed");
    expect(() => adopt(graph, encoded)).toThrow("disposed");
  });
});

it("v1 refuses observation-only authority and preserves current accepted age and eligibility", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "raw",
    key: (_input: void) => "raw",
    fetch: backend.respond<void, number | undefined>("raw"),
  });
  const graph = createGraph({ contract: { namespace: "observed-realm", operations: { query } } });
  try {
    const store = graph.api.query();
    const lease = observeStore(graph, store);
    lease.publish(undefined);
    expect(() => snapshot(graph)).toThrow(Fault);
    lease.publish(2, { transfer: "exclude" });
    expect(Object.keys(snapshot(graph).entries)).toEqual([]);
    expect(snapshot(graph, { version: 2 }).entries).toEqual([]);
    const ready = store.ready;
    backend.resolve(backend.calls[0]!, 3);
    await ready;
    jest.advanceTimersByTime(5);
    lease.publish(4);
    expect(snapshot(graph).entries.raw).toMatchObject({ value: 4, age: 5 });
    lease.publish(5, { transfer: "exclude" });
    expect(Object.keys(snapshot(graph).entries)).toEqual([]);
    expect(snapshot(graph, { version: 2 }).entries).toEqual([]);
    store.set(6);
    expect(snapshot(graph).entries.raw).toMatchObject({ value: 6, age: 0 });
    expect(snapshot(graph, { version: 2 }).entries).toMatchObject([
      { address: { kind: "raw", key: "raw" }, value: 6, landing: { kind: "accepted", age: 0 } },
    ]);
  } finally {
    graph.dispose();
  }
});

it("v1 faults encoding reentry instead of returning old bytes with new observed authority", () => {
  const backend = testBackend();
  const query = createQuery({
    name: "raw",
    key: (_input: void) => "raw",
    fetch: backend.respond<void, { value: number }>("raw"),
  });
  const graph = createGraph({ contract: { namespace: "encoding-reentry", operations: { query } } });
  try {
    const store = graph.api.query();
    const lease = observeStore(graph, store);
    store.set({
      get value() {
        lease.publish({ value: 2 });
        return 1;
      },
    });
    expect(() => snapshot(graph)).toThrow(Fault);
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: true, value: { value: 2 } });
  } finally {
    graph.dispose();
  }
});

it("ordinary v1 adoption advances local visible revision without clearing an observation issue", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "raw",
    key: (_input: void) => "raw",
    fetch: backend.respond<void, number | undefined>("raw"),
  });
  const contract = { namespace: "adopt-observed", operations: { query } };
  const source = createGraph({ contract });
  const target = createGraph({ contract });
  try {
    const original = source.api.query();
    original.set(undefined);
    const store = target.api.query();
    const lease = observeStore(target, store);
    const issue = Object.freeze({ invalid: true });
    lease.reject(issue);
    const before = peekStore(store);
    const report = adopt(target, snapshot(source));
    const after = peekStore(store);
    expect(report.adopted).toEqual([store.key]);
    expect(lease.issue.get()).toBe(issue);
    if (before.kind !== "stable" || after.kind !== "stable")
      throw new Error("Expected stable adoption captures");
    expect(after.valueRevision).toBeGreaterThan(before.valueRevision);
    expect(after).toMatchObject({ present: true, value: undefined });
    expect(await store.ready).toBeUndefined();

    const beforeV2 = peekStore(store);
    const v2Report = adopt(target, snapshot(source, { version: 2 }), {});
    const afterV2 = peekStore(store);
    expect(v2Report.adopted).toMatchObject([{ address: { kind: "raw", key: String(store.key) } }]);
    expect(lease.issue.get()).toBe(issue);
    if (beforeV2.kind !== "stable" || afterV2.kind !== "stable")
      throw new Error("Expected stable v2 adoption captures");
    expect(afterV2.valueRevision).toBeGreaterThan(beforeV2.valueRevision);
    expect(afterV2).toMatchObject({ present: true, value: undefined });
  } finally {
    source.dispose();
    target.dispose();
  }
});

it("ordinary v1 adoption admits a collected entry after a rejected recall and remains collectible", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "query",
    key: (_input: void) => "one",
    fetch: backend.respond<void, number>("query"),
  });
  const contract = { namespace: "adopt-after-rejection", operations: { query } };
  const source = createGraph({ contract });
  const target = createGraph({ contract, idleMs: 10 });
  try {
    const original = source.api.query(undefined, { initial: 9 });
    const store = target.api.query(undefined, { initial: 1 });
    await original.ready;
    await store.ready;
    jest.advanceTimersByTime(11);
    expect(() => target.api.query(undefined, { initial: 2 })).toThrow(Fault);
    expect(() => observeStore(target, store)).toThrow(TypeError);
    expect(adopt(target, snapshot(source)).adopted).toEqual([store.key]);
    const lease = observeStore(target, store);
    expect(await store.ready).toBe(9);
    expect(lease.publish(10)).toBe(true);
    expect(peekStore(store)).toMatchObject({ kind: "stable", present: true, value: 10 });
    jest.advanceTimersByTime(11);
    expect(lease.active.get()).toBe(false);
    expect(backend.calls).toHaveLength(0);
  } finally {
    source.dispose();
    target.dispose();
  }
});

it("v2 preserves an observed undefined landing:none and an accepted age after an observed replacement", async () => {
  const backend = testBackend();
  const key = "realm/observed-v2";
  const query = createQuery({
    name: "realm.observed-v2",
    key: () => key,
    fetch: backend.respond("realm.observed-v2"),
  });
  const contract = queryContract({ query });
  const source = createGraph({ contract });
  const target = createGraph({ contract });
  try {
    const store = source.api.query(undefined);
    const lease = observeStore(source, store);
    lease.publish(undefined);

    const none = snapshot(source, { version: 2 });
    expect(none.entries).toHaveLength(1);
    expect(none.entries[0]).toMatchObject({
      address: { kind: "raw", key },
      value: { $undefined: true },
      landing: { kind: "none" },
    });
    expect(none.entries[0]?.valueRevision).toBeGreaterThan(0);
    adopt(target, JSON.parse(JSON.stringify(none)) as typeof none, {});
    const adopted = target.at(key);
    expect(adopted.available.get()).toBe(true);
    expect(adopted.value.get()).toBeUndefined();

    const ready = store.ready;
    backend.resolve(backend.calls[0]!, "accepted");
    await ready;
    jest.advanceTimersByTime(40);
    lease.publish("observed");

    const accepted = snapshot(source, { version: 2 });
    expect(accepted.entries).toHaveLength(1);
    expect(accepted.entries[0]).toMatchObject({
      address: { kind: "raw", key },
      value: "observed",
      landing: { kind: "accepted", age: 40 },
    });
    adopt(target, JSON.parse(JSON.stringify(accepted)) as typeof accepted, {});
    const adoptedQuery = target.api.query(undefined);
    expect(adoptedQuery.value.get()).toBe("observed");
    const reexported = snapshot(target, { version: 2 });
    expect(reexported.entries[0]).toMatchObject({
      address: { kind: "raw", key },
      value: "observed",
      landing: { kind: "accepted", age: 40 },
    });
  } finally {
    source.dispose();
    target.dispose();
  }
});

it("v2 skips a malformed row independently and rejects duplicate physical addresses before writes", () => {
  const backend = testBackend();
  const query = createQuery({
    name: "realm.v2-preflight",
    key: () => "good",
    fetch: backend.respond("realm.v2-preflight"),
  });
  const contract = queryContract({ query });
  const source = createGraph({ contract });
  const target = createGraph({ contract });
  const duplicateTarget = createGraph({ contract });
  try {
    source.api.query(undefined, { initial: 4 });
    const valid = snapshot(source, { version: 2 });
    const malformedAddress = { kind: "raw", key: "bad" } as const;
    const withMalformed = {
      ...valid,
      entries: [
        ...valid.entries,
        {
          ...valid.entries[0]!,
          address: malformedAddress,
          value: { $unknown: true },
        },
      ],
    };
    const report = adopt(target, withMalformed, {});
    expect(report.adopted.map((row) => row.address)).toEqual([{ kind: "raw", key: "good" }]);
    expect(report.skipped).toHaveLength(1);
    expect(report.skipped[0]?.address).toBe(malformedAddress);
    expect(target.at("good").value.get()).toBe(4);

    const duplicate = {
      ...valid,
      entries: [valid.entries[0]!, valid.entries[0]!],
    } as typeof valid;
    expect(() => adopt(duplicateTarget, duplicate, {})).toThrow(Fault);
    expect(duplicateTarget.at("good").available.get()).toBe(false);
    expect(snapshot(duplicateTarget, { version: 2 }).entries).toHaveLength(0);
  } finally {
    source.dispose();
    target.dispose();
    duplicateTarget.dispose();
  }
});

it.each([false, true])(
  "v2 rejects a duplicate address even when its row has an unsupported field (malformedFirst=%s)",
  (malformedFirst) => {
    const backend = testBackend();
    const query = createQuery({
      name: "realm.v2-malformed-duplicate",
      key: (_input: void) => "duplicate",
      fetch: backend.respond<void, number>("realm.v2-malformed-duplicate"),
    });
    const contract = queryContract({ query });
    const source = createGraph({ contract });
    const target = createGraph({ contract });
    try {
      source.api.query(undefined, { initial: 4 });
      const incumbent = target.api.query(undefined, { initial: 9 });
      const before = peekStore(incumbent);
      const encoded = snapshot(source, { version: 2 });
      const row = encoded.entries[0]!;
      const malformed = { ...row, extra: true };
      const incoming = {
        ...encoded,
        entries: malformedFirst ? [malformed, row] : [row, malformed],
      } as unknown as typeof encoded;

      expect(() => adopt(target, incoming, {})).toThrow(Fault);
      expect(peekStore(incumbent)).toEqual(before);
      expect(incumbent.value.get()).toBe(9);
      expect(backend.calls).toHaveLength(0);
    } finally {
      source.dispose();
      target.dispose();
    }
  },
);

it("refuses generic snapshots for multiple reader-scoped Stores sharing one logical key", async () => {
  type Input = { readonly room: string };
  type Event = { position: number; value: number };
  type Reservation = { input: Input; after: number; seed: unknown };
  const reservations = new WeakMap<object, Reservation>();
  const liveEmitters = new Map<string, (event: Event) => void>();
  const openings: Array<{ room: string; reader: object | undefined }> = [];

  const makeChannel = () => {
    const queued: Event[] = [];
    const waiters: Array<(result: IteratorResult<Event>) => void> = [];
    let ended = false;
    const emit = (event: Event): void => {
      if (ended) return;
      const waiter = waiters.shift();
      if (waiter !== undefined) waiter({ done: false, value: event });
      else queued.push(event);
    };
    const iterable: AsyncIterable<Event> = {
      [Symbol.asyncIterator]() {
        return {
          next: () => {
            const event = queued.shift();
            if (event !== undefined) return Promise.resolve({ done: false as const, value: event });
            if (ended) return Promise.resolve({ done: true as const, value: undefined });
            return new Promise<IteratorResult<Event>>((resolve) => waiters.push(resolve));
          },
          return: () => {
            ended = true;
            for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
            return Promise.resolve({ done: true as const, value: undefined });
          },
        };
      },
    };
    return { iterable, emit };
  };
  const reserve = (input: Input, after: number, seed: unknown): object => {
    const reader = Object.freeze({});
    reservations.set(reader, { input, after, seed });
    return reader;
  };

  const stream = createPositionedStream<Input, number, string, number, object>({
    name: "realm-reader-store",
    key: (input) => input.room,
    source: {
      open: (input, options) => {
        const channel = makeChannel();
        openings.push({ room: input.room, reader: options.reader });
        if (options.reader === undefined) liveEmitters.set(input.room, channel.emit);
        return channel.iterable;
      },
      validateReader: (input, reader, after, checkpoint, seed) => {
        const reservation = reservations.get(reader);
        return (
          checkpoint === undefined &&
          reservation !== undefined &&
          reservation.input === input &&
          reservation.after === after &&
          reservation.seed === seed
        );
      },
      isReaderCurrent: (input, reader, after, seed) => {
        const reservation = reservations.get(reader);
        return (
          reservation !== undefined &&
          reservation.input === input &&
          reservation.after === after &&
          reservation.seed === seed
        );
      },
      acknowledge: () => undefined,
      releaseReader: () => undefined,
    },
  });
  const makeGraph = (namespace: string) =>
    createGraph({ contract: { namespace, operations: { events: stream } } });
  const sourceGraph = makeGraph("realm-reader-source");
  const targetGraph = makeGraph("realm-reader-target");
  const input = Object.freeze({ room: "one" });
  const source = sourceGraph.api.events(input);
  const policy = {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, event: number) => sum + event,
  };
  const projection = project(source, policy);
  let firstDiscard: (() => void) | undefined;
  let secondDiscard: (() => void) | undefined;

  try {
    const folded = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(projection.value, (value) => {
        if (value !== 5) return;
        stop();
        resolve();
      });
    });
    const emit = liveEmitters.get(input.room);
    if (emit === undefined) throw new Error("The source Store did not open");
    emit({ position: 5, value: 5 });
    await folded;

    const checkpoint = capturePositionedStore(source, [projection] as const);
    if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
      throw new Error("The actual source Store has no positioned landing");
    const after: number = checkpoint.position;
    const seed = { after, latest: checkpoint.latest, values: checkpoint.values };
    const firstReader = reserve(input, after, seed);
    const secondReader = reserve(input, after, seed);
    const first = preparePositionedStoreRestore(
      targetGraph.api.events,
      input,
      [policy] as const,
      seed,
      firstReader,
    );
    const second = preparePositionedStoreRestore(
      targetGraph.api.events,
      input,
      [policy] as const,
      seed,
      secondReader,
    );
    firstDiscard = () => first.discard();
    secondDiscard = () => second.discard();

    expect(first.source.identity).not.toBe(second.source.identity);
    expect(openings).toHaveLength(1);
    let v1Fault: unknown;
    try {
      snapshot(targetGraph);
    } catch (error) {
      v1Fault = error;
    }
    expect(v1Fault).toBeInstanceOf(Fault);
    expect(v1Fault).toMatchObject({ kind: "encoding" });

    let v2Fault: unknown;
    try {
      snapshot(targetGraph, { version: 2 });
    } catch (error) {
      v2Fault = error;
    }
    expect(v2Fault).toBeInstanceOf(Fault);
    expect(v2Fault).toMatchObject({ kind: "encoding" });
    expect(openings).toHaveLength(1);

    const ordinary = createStore({ name: "ordinary", initial: 7, snapshot: true });
    const excludedGraph = createGraph({
      contract: {
        namespace: "realm-reader-snapshot-excluded",
        operations: { events: stream, ordinary },
      },
    });
    const ordinaryStore = excludedGraph.api.ordinary();
    const excludedReader = reserve(input, after, seed);
    const excluded = preparePositionedStoreRestore(
      excludedGraph.api.events,
      input,
      [policy] as const,
      seed,
      excludedReader,
      { snapshot: false },
    );
    try {
      const v1 = snapshot(excludedGraph);
      expect(Object.keys(v1.entries)).toEqual([String(ordinaryStore.key)]);
      const v2 = snapshot(excludedGraph, { version: 2 });
      expect(v2.entries).toHaveLength(1);
      expect(openings).toHaveLength(1);
    } finally {
      excluded.discard();
      excludedGraph.dispose();
    }
  } finally {
    firstDiscard?.();
    secondDiscard?.();
    projection.close();
    sourceGraph.dispose();
    targetGraph.dispose();
  }
});
it("refuses a required capture when its codec changes the cached source", () => {
  const graph = createGraph({ contract: defineContract({ namespace: "required-capture-reentry", operations: {} }) });
  const sourceState = signal(1);
  const source = computed(() => sourceState.get() * 2);
  expect(source.get()).toBe(2);
  const construction = captureReactiveConstruction(source);
  let changed = false;
  const codec: RequiredTransferCodec = {
    ...requiredTransferCodec,
    encodeValue(value) {
      if (!changed) {
        changed = true;
        sourceState.set(2);
      }
      return value as ClosedEncodedValue;
    },
  };

  expect(() =>
    prepareRequiredTransferCapture({
      graphs: [graph],
      roots: [{ source, construction }],
      codec,
    }),
  ).toThrow(TypeError);
  expect(source.get()).toBe(4);
  graph.dispose();
});

it("adopts an excluded query cache with mapped State writes before deferred notifications", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "required.read",
    key: (input: string) => input,
    snapshot: false,
    fetch: backend.respond<string, number>("required.read"),
  });
  const contract = queryContract({ query });
  const sourceGraph = createGraph({ contract });
  const targetGraph = createGraph({ contract });
  let capturePlan: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
  let adoptionPlan: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  let discardClaim: (() => void) | undefined;
  let claimRetired: Promise<void> | undefined;
  let capturePublished = false;
  let adoptionPublished = false;
  let sourceRequestResolved = false;
  let targetRequestResolved = false;
  let cleanupWatchers: (() => void) | undefined;
  const notifications: string[] = [];
  const notificationFailure = new Error("required transfer observer failed");
  const valueWatcher = new Signal.subtle.Watcher(() => {
    notifications.push("value");
    throw notificationFailure;
  });
  const mappedWatcher = new Signal.subtle.Watcher(() => notifications.push("mapped"));

  try {
    const sourceStore = sourceGraph.api.query("one");
    const sourceReady = sourceStore.ready;
    const sourceRequest = backend.calls[0]!;
    backend.resolve(sourceRequest, 4);
    sourceRequestResolved = true;
    await sourceReady;

    let sourceRuns = 0;
    const sourceValue = computed(() => {
      sourceRuns += 1;
      return (sourceStore.value.get() ?? 0) * 2;
    });
    expect(sourceValue.get()).toBe(8);
    const sourceConstruction = captureReactiveConstruction(sourceValue);
    capturePlan = prepareRequiredTransferCapture({
      graphs: [sourceGraph],
      roots: [{ source: sourceValue, construction: sourceConstruction }],
      codec: requiredTransferCodec,
    });
    const captured = capturePlan.image;
    expect(captured.stores).toHaveLength(1);
    expect(captured.stores[0]).toMatchObject({ hasValue: false, kind: "query" });
    expect(Object.isFrozen(captured)).toBe(true);
    expect(Object.isFrozen(captured.nodes)).toBe(true);
    const captureCommit = capturePlan.seal();
    if (captureCommit === undefined) throw new Error("The stable required capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capturePlan.retired;

    const claim = preparePassiveTransferClaim(targetGraph, targetGraph.api.query, "one", { snapshot: false });
    discardClaim = () => claim.discard();
    claimRetired = claim.retired;
    expect(backend.calls).toHaveLength(1);
    let targetRuns = 0;
    const targetValue = computed(() => {
      targetRuns += 1;
      return (claim!.store.value.get() ?? 0) * 2;
    });
    const targetConstruction = captureReactiveConstruction(targetValue);
    const mappedState = signal<unknown>("before");
    const mappedConstruction = captureReactiveConstruction(mappedState);
    valueWatcher.watch(targetValue);
    mappedWatcher.watch(mappedState);
    cleanupWatchers = () => {
      valueWatcher.unwatch(targetValue);
      mappedWatcher.unwatch(mappedState);
    };

    const valueNode = captured.nodes.find((node) => node.slot === captured.roots[0]);
    if (valueNode === undefined) throw new Error("The required computed slot is missing");
    adoptionPlan = prepareRequiredTransferAdoption(captured, {
      graphs: [targetGraph],
      sources: [{ slot: captured.stores[0]!.slot, claim }],
      reactive: [{ slot: valueNode.slot, target: targetValue, construction: targetConstruction }],
      codec: requiredTransferCodec,
      mappedStateWrites: [{ target: mappedState, construction: mappedConstruction, value: "published" }],
    });
    const commit = adoptionPlan.seal();
    if (commit === undefined) throw new Error("The stable required adoption did not seal");
    const retirementOutcome = adoptionPlan.retired.then(
      () => undefined,
      (error: unknown) => error,
    );
    commit.publish();
    adoptionPublished = true;
    expect(mappedState.get()).toBe("published");
    expect(targetValue.get()).toBe(8);
    expect(targetRuns).toBe(0);
    expect(notifications).toEqual([]);

    let deliveryError: unknown;
    try {
      commit.notifications.deliver();
    } catch (error) {
      deliveryError = error;
    }
    expect(deliveryError).toBeDefined();
    expect(notifications).toContain("value");
    expect(notifications).toContain("mapped");
    expect(await retirementOutcome).toBe(deliveryError);
    expect(targetValue.get()).toBe(8);
    expect(targetRuns).toBe(0);
    cleanupWatchers();
    cleanupWatchers = undefined;

    const revalidation = claim.store.revalidate();
    const targetReady = claim.store.ready;
    const targetRequest = backend.calls[1]!;
    expect(targetRequest).toBeDefined();
    backend.resolve(targetRequest, 5);
    targetRequestResolved = true;
    await Promise.all([revalidation, targetReady]);
    expect(targetValue.get()).toBe(10);
    expect(targetRuns).toBe(1);
  } finally {
    cleanupWatchers?.();
    if (!adoptionPublished) {
      if (adoptionPlan !== undefined) adoptionPlan.discard();
      else discardClaim?.();
    }
    if (claimRetired !== undefined) await claimRetired.then(() => undefined, () => undefined);
    if (!capturePublished) capturePlan?.discard();
    if (!sourceRequestResolved && backend.calls[0] !== undefined)
      backend.resolve(backend.calls[0], 4);
    if (!targetRequestResolved && backend.calls[1] !== undefined)
      backend.resolve(backend.calls[1], 5);
    if (adoptionPlan !== undefined) await adoptionPlan.retired.then(() => undefined, () => undefined);
    if (capturePlan !== undefined) await capturePlan.retired.then(() => undefined, () => undefined);
    sourceGraph.dispose();
    targetGraph.dispose();
  }
});
it("joins staged observation metadata only through the exact pending lease", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "realm.observation-callers",
    key: (input: string) => input,
    fetch: backend.respond<string, number>("realm.observation-callers"),
  });
  const contract = queryContract({ query });
  const sourceGraph = createGraph({ contract });
  const independentGraph = createGraph({ contract });
  const joinedGraph = createGraph({ contract });
  let capturePlan: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
  let capturePublished = false;
  let independentAdoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  let independentClaims: ReturnType<typeof preparePassiveTransferClaim>[] = [];
  let joinedAdoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  let joinedClaims: ReturnType<typeof preparePassiveTransferClaim>[] = [];
  let joinedPublished = false;
  let sourceRequestResolved = false;
  let revalidationRequestResolved = false;

  try {
    const baseline = sourceGraph.api.query("one");
    const baselineReady = baseline.ready;
    backend.resolve(backend.calls[0]!, 4);
    sourceRequestResolved = true;
    await baselineReady;

    const zeroWindow = sourceGraph.api.query("one", { revalidateAfterMs: 0 });
    const zeroReady = zeroWindow.ready;
    const oneSecond = sourceGraph.api.query("one", { revalidateAfterMs: 1000 });
    const oneSecondReady = oneSecond.ready;
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, 5);
    revalidationRequestResolved = true;
    await Promise.all([zeroReady, oneSecondReady]);
    jest.advanceTimersByTime(1);
    expect(zeroWindow.status.get()).toBe("stale");
    expect(oneSecond.status.get()).toBe("ready");

    const sourceLease = observeStore(sourceGraph, zeroWindow);
    const sourceValue = computed(() =>
      zeroWindow.status.get() === "stale" &&
      oneSecond.status.get() === "ready" &&
      sourceLease.active.get() &&
      sourceLease.issue.get() === undefined,
    );
    expect(sourceValue.get()).toBe(true);
    capturePlan = prepareRequiredTransferCapture({
      graphs: [sourceGraph],
      roots: [{ source: sourceValue, construction: captureReactiveConstruction(sourceValue) }],
      codec: requiredTransferCodec,
    });
    const captured = capturePlan.image;
    expect(captured.stores).toHaveLength(1);
    expect(captured.callers).toHaveLength(2);
    const zeroCaller = captured.callers.find(
      (caller) => caller.window.kind === "finite" && caller.window.milliseconds === 0,
    );
    const oneSecondCaller = captured.callers.find(
      (caller) => caller.window.kind === "finite" && caller.window.milliseconds === 1000,
    );
    if (zeroCaller === undefined || oneSecondCaller === undefined)
      throw new Error("The source caller slots were not captured");
    const physicalSlot = captured.stores[0]!.slot;
    const captureCommit = capturePlan.seal();
    if (captureCommit === undefined) throw new Error("The caller observation capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capturePlan.retired;

    const independentFirst = preparePassiveTransferClaim(
      independentGraph,
      independentGraph.api.query,
      "one",
      { revalidateAfterMs: 0 },
    );
    const independentSecond = preparePassiveTransferClaim(
      independentGraph,
      independentGraph.api.query,
      "one",
      { revalidateAfterMs: 1000 },
    );
    independentClaims = [independentFirst, independentSecond];
    const independentObservationFirst = independentFirst.prepareObservation();
    const independentObservationSecond = independentSecond.prepareObservation();
    expect(independentObservationFirst.lease).not.toBe(independentObservationSecond.lease);
    expect(independentObservationFirst.lease.active.get()).toBe(false);
    expect(independentObservationSecond.lease.active.get()).toBe(false);
    const independentValue = computed(() =>
      independentFirst.store.status.get() === "stale" &&
      independentSecond.store.status.get() === "ready" &&
      independentObservationFirst.lease.active.get() &&
      independentObservationFirst.lease.issue.get() === undefined,
    );
    expect(() => {
      independentAdoption = prepareRequiredTransferAdoption(captured, {
        graphs: [independentGraph],
        sources: [
          { slot: physicalSlot, claim: independentFirst },
          { slot: zeroCaller.slot, claim: independentFirst },
          { slot: oneSecondCaller.slot, claim: independentSecond },
        ],
        reactive: [{
          slot: captured.roots[0]!,
          target: independentValue,
          construction: captureReactiveConstruction(independentValue),
        }],
        codec: requiredTransferCodec,
        mappedStateWrites: [],
      });
    }).toThrow(TypeError);
    independentFirst.discard();
    independentSecond.discard();
    independentClaims = [];
    await Promise.all([independentFirst.retired, independentSecond.retired]);
    expect(independentObservationFirst.lease.active.get()).toBe(false);
    expect(independentObservationSecond.lease.active.get()).toBe(false);

    const joinedFirst = preparePassiveTransferClaim(
      joinedGraph,
      joinedGraph.api.query,
      "one",
      { revalidateAfterMs: 0 },
    );
    const joinedSecond = preparePassiveTransferClaim(
      joinedGraph,
      joinedGraph.api.query,
      "one",
      { revalidateAfterMs: 1000 },
    );
    joinedClaims = [joinedFirst, joinedSecond];
    const joinedObservationFirst = joinedFirst.prepareObservation();
    const joinedObservationSecond = joinedSecond.prepareObservation(joinedObservationFirst.lease);
    expect(joinedObservationSecond.lease).toBe(joinedObservationFirst.lease);
    expect(joinedObservationSecond.retired).toBe(joinedSecond.retired);
    expect(joinedObservationFirst.lease.active.get()).toBe(false);
    expect(backend.calls).toHaveLength(2);

    let joinedRuns = 0;
    const joinedValue = computed(() => {
      joinedRuns += 1;
      return joinedFirst.store.status.get() === "stale" &&
        joinedSecond.store.status.get() === "ready" &&
        joinedObservationFirst.lease.active.get() &&
        joinedObservationFirst.lease.issue.get() === undefined;
    });
    joinedAdoption = prepareRequiredTransferAdoption(captured, {
      graphs: [joinedGraph],
      sources: [
        { slot: physicalSlot, claim: joinedFirst },
        { slot: zeroCaller.slot, claim: joinedFirst },
        { slot: oneSecondCaller.slot, claim: joinedSecond },
      ],
      reactive: [{
        slot: captured.roots[0]!,
        target: joinedValue,
        construction: captureReactiveConstruction(joinedValue),
      }],
      codec: requiredTransferCodec,
      mappedStateWrites: [],
    });
    const joinedCommit = joinedAdoption.seal();
    if (joinedCommit === undefined) throw new Error("The exact-lease caller cohort did not seal");
    joinedCommit.publish();
    joinedPublished = true;
    expect(joinedObservationFirst.lease.active.get()).toBe(true);
    expect(joinedValue.get()).toBe(true);
    expect(joinedRuns).toBe(0);
    joinedCommit.notifications.deliver();
    await Promise.all([joinedAdoption.retired, joinedFirst.retired, joinedSecond.retired]);
    joinedClaims = [];
    expect(joinedObservationSecond.lease.active.get()).toBe(true);
    expect(backend.calls).toHaveLength(2);
  } finally {
    if (!capturePublished) capturePlan?.discard();
    if (capturePlan !== undefined) await capturePlan.retired.then(() => undefined, () => undefined);
    independentAdoption?.discard();
    for (const claim of independentClaims) claim.discard();
    if (independentClaims.length > 0)
      await Promise.all(independentClaims.map((claim) => claim.retired.then(() => undefined, () => undefined)));
    if (!joinedPublished) joinedAdoption?.discard();
    for (const claim of joinedClaims) claim.discard();
    if (joinedClaims.length > 0)
      await Promise.all(joinedClaims.map((claim) => claim.retired.then(() => undefined, () => undefined)));
    if (independentAdoption !== undefined)
      await independentAdoption.retired.then(() => undefined, () => undefined);
    if (joinedAdoption !== undefined)
      await joinedAdoption.retired.then(() => undefined, () => undefined);
    if (!sourceRequestResolved && backend.calls[0] !== undefined)
      backend.resolve(backend.calls[0], 4);
    if (!revalidationRequestResolved && backend.calls[1] !== undefined)
      backend.resolve(backend.calls[1], 5);
    sourceGraph.dispose();
    independentGraph.dispose();
    joinedGraph.dispose();
  }
});

it("publishes a genuine passive observation lease with required adoption", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "realm.observation-transfer",
    key: (input: string) => input,
    fetch: backend.respond<string, number>("realm.observation-transfer"),
  });
  const contract = queryContract({ query });
  const sourceGraph = createGraph({ contract });
  const targetGraph = createGraph({ contract });
  const abandonedGraph = createGraph({ contract });
  let capturePlan: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
  let adoptionPlan: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  let claimDiscard: (() => void) | undefined;
  let claimRetired: Promise<void> | undefined;
  let siblingDiscard: (() => void) | undefined;
  let siblingRetired: Promise<void> | undefined;
  let reuseDiscard: (() => void) | undefined;
  let reuseRetired: Promise<void> | undefined;
  let abandonedDiscard: (() => void) | undefined;
  let abandonedRetired: Promise<void> | undefined;
  let deliverAdoption: (() => void) | undefined;
  let capturePublished = false;
  let adoptionPublished = false;
  let sourceRequestResolved = false;

  try {
    const sourceStore = sourceGraph.api.query("one");
    const sourceReady = sourceStore.ready;
    const sourceRequest = backend.calls[0]!;
    backend.resolve(sourceRequest, 4);
    sourceRequestResolved = true;
    await sourceReady;
    const sourceLease = observeStore(sourceGraph, sourceStore);
    const sourceValue = computed(() =>
      sourceStore.status.get() === "ready" &&
      sourceLease.active.get() &&
      sourceLease.issue.get() === undefined,
    );
    expect(sourceValue.get()).toBe(true);
    const sourceConstruction = captureReactiveConstruction(sourceValue);
    capturePlan = prepareRequiredTransferCapture({
      graphs: [sourceGraph],
      roots: [{ source: sourceValue, construction: sourceConstruction }],
      codec: requiredTransferCodec,
    });
    const captured = capturePlan.image;
    const storeRow = captured.stores[0]!;
    const callerRow = captured.callers[0]!;
    expect(captured.stores).toHaveLength(1);
    expect(captured.callers).toHaveLength(1);
    expect(callerRow.store).toBe(storeRow.slot);
    expect(captured.nodes).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "native",
        store: storeRow.slot,
        caller: callerRow.slot,
        facet: "query-status",
      }),
      expect.objectContaining({ kind: "native", store: storeRow.slot, facet: "observation-activity" }),
      expect.objectContaining({ kind: "native", store: storeRow.slot, facet: "observation-issue" }),
    ]));
    const captureCommit = capturePlan.seal();
    if (captureCommit === undefined) throw new Error("The stable observation capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capturePlan.retired;

    const claim = preparePassiveTransferClaim(targetGraph, targetGraph.api.query, "one");
    claimDiscard = () => claim.discard();
    claimRetired = claim.retired;
    const preparedObservation = claim.prepareObservation();
    expect(preparedObservation.retired).toBe(claim.retired);
    const siblingClaim = preparePassiveTransferClaim(targetGraph, targetGraph.api.query, "one");
    siblingDiscard = () => siblingClaim.discard();
    siblingRetired = siblingClaim.retired;
    const siblingObservation = siblingClaim.prepareObservation();
    expect(siblingObservation.lease).not.toBe(preparedObservation.lease);
    expect(siblingObservation.lease.active.get()).toBe(false);
    expect(backend.calls).toHaveLength(1);
    expect(preparedObservation.lease.active.get()).toBe(false);
    expect(preparedObservation.lease.publish(9)).toBe(false);
    expect(preparedObservation.lease.reject(Object.freeze({ kind: "prepublication" }))).toBe(false);
    expect(preparedObservation.lease.report(new Error("prepublication"))).toBe(false);

    let targetRuns = 0;
    const targetValue = computed(() => {
      targetRuns += 1;
      return claim.store.status.get() === "ready" &&
        preparedObservation.lease.active.get() &&
        preparedObservation.lease.issue.get() === undefined;
    });
    const targetConstruction = captureReactiveConstruction(targetValue);
    const targetRoot = captured.roots[0]!;
    adoptionPlan = prepareRequiredTransferAdoption(captured, {
      graphs: [targetGraph],
      sources: [
        { slot: storeRow.slot, claim },
        ...captured.callers.map((caller) => ({ slot: caller.slot, claim })),
      ],
      reactive: [{ slot: targetRoot, target: targetValue, construction: targetConstruction }],
      codec: requiredTransferCodec,
      mappedStateWrites: [],
    });
    const adoptionCommit = adoptionPlan.seal();
    if (adoptionCommit === undefined) throw new Error("The stable observation adoption did not seal");
    deliverAdoption = () => adoptionCommit.notifications.deliver();
    adoptionCommit.publish();
    adoptionPublished = true;
    expect(preparedObservation.lease.active.get()).toBe(true);
    expect(targetValue.get()).toBe(true);
    expect(targetRuns).toBe(0);
    deliverAdoption();
    deliverAdoption = undefined;
    await preparedObservation.retired;
    expect(backend.calls).toHaveLength(1);
    expect(siblingObservation.lease.active.get()).toBe(false);
    expect(() => prepareRequiredTransferAdoption(captured, {
      graphs: [targetGraph],
      sources: [
        { slot: storeRow.slot, claim: siblingClaim },
        ...captured.callers.map((caller) => ({ slot: caller.slot, claim: siblingClaim })),
      ],
      reactive: [{ slot: captured.roots[0]!, target: targetValue, construction: targetConstruction }],
      codec: requiredTransferCodec,
      mappedStateWrites: [],
    })).toThrow(TypeError);
    siblingObservation.discard();
    siblingDiscard = undefined;
    await siblingRetired;
    expect(preparedObservation.lease.active.get()).toBe(true);

    const reuseClaim = preparePassiveTransferClaim(targetGraph, targetGraph.api.query, "one");
    reuseDiscard = () => reuseClaim.discard();
    reuseRetired = reuseClaim.retired;
    const reusedObservation = reuseClaim.prepareObservation(preparedObservation.lease);
    expect(reusedObservation.lease).toBe(preparedObservation.lease);
    expect(reusedObservation.retired).toBe(reuseClaim.retired);
    reusedObservation.discard();
    reuseDiscard = undefined;
    await reuseRetired;
    expect(preparedObservation.lease.active.get()).toBe(true);

    const abandonedClaim = preparePassiveTransferClaim(abandonedGraph, abandonedGraph.api.query, "one");
    abandonedDiscard = () => abandonedClaim.discard();
    abandonedRetired = abandonedClaim.retired;
    const abandonedObservation = abandonedClaim.prepareObservation();
    expect(abandonedObservation.retired).toBe(abandonedClaim.retired);
    expect(abandonedObservation.lease.active.get()).toBe(false);
    abandonedObservation.lease.dispose();
    expect(abandonedObservation.lease.publish(10)).toBe(false);
    expect(abandonedObservation.lease.active.get()).toBe(false);
    const abandonedValue = computed(() =>
      abandonedClaim.store.status.get() === "ready" &&
      abandonedObservation.lease.active.get() &&
      abandonedObservation.lease.issue.get() === undefined,
    );
    const abandonedConstruction = captureReactiveConstruction(abandonedValue);
    expect(() => prepareRequiredTransferAdoption(captured, {
      graphs: [abandonedGraph],
      sources: [
        { slot: storeRow.slot, claim: abandonedClaim },
        ...captured.callers.map((caller) => ({ slot: caller.slot, claim: abandonedClaim })),
      ],
      reactive: [{ slot: captured.roots[0]!, target: abandonedValue, construction: abandonedConstruction }],
      codec: requiredTransferCodec,
      mappedStateWrites: [],
    })).toThrow(TypeError);
    abandonedObservation.discard();
    abandonedDiscard = undefined;
    await abandonedRetired;
    preparedObservation.lease.dispose();
    expect(preparedObservation.lease.active.get()).toBe(false);
  } finally {
    if (deliverAdoption !== undefined) deliverAdoption();
    if (!adoptionPublished) {
      if (adoptionPlan !== undefined) adoptionPlan.discard();
      else claimDiscard?.();
    }
    siblingDiscard?.();
    reuseDiscard?.();
    abandonedDiscard?.();
    if (claimRetired !== undefined) await claimRetired.then(() => undefined, () => undefined);
    if (siblingRetired !== undefined) await siblingRetired.then(() => undefined, () => undefined);
    if (reuseRetired !== undefined) await reuseRetired.then(() => undefined, () => undefined);
    if (abandonedRetired !== undefined) await abandonedRetired.then(() => undefined, () => undefined);
    if (!capturePublished) capturePlan?.discard();
    if (capturePlan !== undefined) await capturePlan.retired.then(() => undefined, () => undefined);
    if (adoptionPlan !== undefined) await adoptionPlan.retired.then(() => undefined, () => undefined);
    if (!sourceRequestResolved && backend.calls[0] !== undefined)
      backend.resolve(backend.calls[0], 4);
    sourceGraph.dispose();
    targetGraph.dispose();
    abandonedGraph.dispose();
  }
});

