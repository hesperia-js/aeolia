import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import vm from "node:vm";
import {
  Fault,
  adopt,
  computed,
  createGraph,
  defineContract,
  createMutation,
  createQuery,
  createStore,
  createStream,
  affects,
  snapshot,
  watch,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";
import type { Contract, EncodingId, OperationTree, Snapshot } from "../../src/index.ts";

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
