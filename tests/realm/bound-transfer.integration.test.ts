import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  adopt,
  affects,
  bindOperation,
  bindStream,
  createBindingContext,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createStream,
  defineContract,
  Fault,
  observeStore,
  peekStore,
  preparePassiveTransferClaim,
  prepareTransferOperation,
  prepareTransferOperationRetirement,
  project,
  snapshot,
  subscribe,
  type AdoptedEntryV2,
  type Store,
  type StreamSnapshotAddressV2,
  type SnapshotBindingReference,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

interface Input {
  readonly key: string;
}

function makeWorld(
  rawName?: "shared" | "raw",
  queryEquals?: (left: string, right: string) => boolean,
  streamSnapshot = true,
) {
  const backend = testBackend();
  let queryKeys = 0;
  let streamKeys = 0;
  let fetches = 0;
  let opens = 0;
  const query = createQuery({
    name: "lookup",
    key: (input: Input) => {
      queryKeys += 1;
      return input.key;
    },
    revalidateAfterMs: 10_000,
    fetch: async (input: Input) => {
      fetches += 1;
      return `fetch:${input.key}`;
    },
  });
  const stream = createStream({
    name: "updates",
    key: (input: Input) => {
      streamKeys += 1;
      return input.key;
    },
    snapshot: streamSnapshot,
    open: (input: Input, options) => {
      opens += 1;
      return backend.stream<Input, string>("updates")(input, options);
    },
  });
  const contract = defineContract({
    namespace: "bound-transfer",
    operations: { query, stream },
  });
  const graph = createGraph({ contract });
  const rawStore: Store<string> | undefined =
    rawName === undefined
      ? undefined
      : graph.api.query({ key: rawName }, { initial: "raw-initial" });
  queryKeys = 0;
  const context = createBindingContext(graph);
  const queryBinding = bindOperation(graph, query, {
    context,
    profile: {},
    ...(queryEquals === undefined ? {} : { value: { equals: queryEquals } }),
    fetch: async (input: Input) => {
      fetches += 1;
      return `bound:${input.key}`;
    },
  });
  const siblingQueryBinding = bindOperation(graph, query, {
    context: createBindingContext(graph),
    profile: {},
    fetch: async (input: Input) => {
      fetches += 1;
      return `sibling:${input.key}`;
    },
  });
  const streamBinding = bindStream(graph, stream, graph.api.stream, { context });
  return {
    backend,
    graph,
    rawStore,
    queryBinding,
    siblingQueryBinding,
    streamBinding,
    queryKeys: () => queryKeys,
    streamKeys: () => streamKeys,
    fetches: () => fetches,
    opens: () => opens,
  };
}

type World = ReturnType<typeof makeWorld>;

type AdoptedStreamEntry = Extract<AdoptedEntryV2, { readonly address: StreamSnapshotAddressV2 }>;

function isAdoptedStreamEntry(entry: AdoptedEntryV2): entry is AdoptedStreamEntry {
  return entry.address.kind === "stream-binding";
}

function reference(role: string, reverse = false): unknown {
  const shared = { room: "shared" };
  return reverse ? { role, alpha: shared, zeta: shared } : { zeta: shared, alpha: shared, role };
}

async function receiveStreamValue(world: World, input: Input, value: string) {
  const store = world.streamBinding.member(input);
  const received = Promise.withResolvers<void>();
  const stop = subscribe(store, (item) => {
    if (item === value) received.resolve();
  });
  world.backend.emit("updates", value);
  await received.promise;
  stop();
  return store;
}

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

it("round-trips equal-key raw, query, and stream addresses without activating imported bindings", async () => {
  const source = makeWorld("shared");
  const target = makeWorld("shared");
  const input = { key: "shared" };
  const queryReference = reference("query");
  const streamReference = reference("stream");
  let stopSource: (() => void) | undefined;
  let stopProjection: (() => void) | undefined;
  let closeProjection: (() => void) | undefined;
  try {
    source.rawStore!.set("raw-source");
    const sourceQuery = source.queryBinding.member(input);
    expect(await sourceQuery.ready).toBe("bound:shared");
    const sourceStream = source.streamBinding.member(input);
    const sourceValue = Promise.withResolvers<void>();
    stopSource = subscribe(sourceStream, (value) => {
      if (value === "stream-source") sourceValue.resolve();
    });
    source.backend.emit("updates", "stream-source");
    await sourceValue.promise;
    jest.advanceTimersByTime(5);

    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [
        { binding: source.queryBinding, reference: queryReference },
        { binding: source.streamBinding, reference: streamReference },
      ],
    });
    expect(encoded.version).toBe(2);
    expect(encoded.entries).toHaveLength(3);
    expect(encoded.entries.map((entry) => entry.address.kind).sort()).toEqual([
      "binding",
      "raw",
      "stream-binding",
    ]);
    const queryEntry = encoded.entries.find((entry) => entry.address.kind === "binding");
    const streamEntry = encoded.entries.find((entry) => entry.address.kind === "stream-binding");
    const rawEntry = encoded.entries.find((entry) => entry.address.kind === "raw");
    expect(queryEntry?.address).toMatchObject({
      kind: "binding",
      key: "shared",
      reference: {
        alpha: { room: "shared" },
        role: "query",
        zeta: { $ref: "/alpha" },
      },
    });
    expect(streamEntry?.address).toMatchObject({
      kind: "stream-binding",
      key: "shared",
      reference: {
        alpha: { room: "shared" },
        role: "stream",
        zeta: { $ref: "/alpha" },
      },
    });
    expect(queryEntry?.value).toBe("bound:shared");
    expect(rawEntry?.value).toBe("raw-source");
    expect(streamEntry).toMatchObject({
      value: "stream-source",
      landing: { kind: "accepted", age: 5 },
    });
    expect(streamEntry?.valueRevision).toBeGreaterThan(0);

    const wire = JSON.parse(JSON.stringify(encoded)) as typeof encoded;
    const report = adopt(target.graph, wire, {
      bindings: [
        { binding: target.queryBinding, reference: reference("query", true) },
        { binding: target.streamBinding, reference: reference("stream", true) },
      ],
    });
    expect(target.queryKeys()).toBe(0);
    expect(target.streamKeys()).toBe(0);
    expect(target.fetches()).toBe(0);
    expect(target.opens()).toBe(0);
    expect(report.version).toBe(2);
    expect(report.adopted.map((row) => row.address.kind).sort()).toEqual([
      "binding",
      "raw",
      "stream-binding",
    ]);
    const adoptedStream = report.adopted.find(isAdoptedStreamEntry);
    if (adoptedStream === undefined)
      throw new Error("Expected the stream address in the adoption report");
    expect(adoptedStream.valueRevision).toBeGreaterThan(0);
    expect(target.rawStore?.value.get()).toBe("raw-source");

    const targetQuery = target.queryBinding.member(input);
    expect(targetQuery.value.get()).toBe("bound:shared");
    expect(target.fetches()).toBe(0);

    const targetStream = target.streamBinding.member(input, { initial: "target-initial" });
    expect([
      targetStream.available.get(),
      targetStream.value.get(),
      targetStream.pending.get(),
    ]).toEqual([true, "stream-source", true]);
    expect(target.opens()).toBe(1);

    const targetProjection = project(targetStream, { kind: "accumulate", max: 3 });
    closeProjection = () => targetProjection.close();
    expect(targetProjection.value.get()).toEqual([]);
    const delivered = Promise.withResolvers<void>();
    stopProjection = subscribe(targetProjection, (values) => {
      if (values.length === 1 && values[0] === "stream-target") delivered.resolve();
    });
    target.backend.emit("updates", "stream-target");
    await delivered.promise;
    expect(targetProjection.value.get()).toEqual(["stream-target"]);
    expect(targetStream.value.get()).toBe("stream-target");
  } finally {
    stopProjection?.();
    closeProjection?.();
    stopSource?.();
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("restores equal-key query siblings through their distinct profile and context mappings", async () => {
  const source = makeWorld();
  const target = makeWorld();
  const input = { key: "shared" };
  const firstReference = reference("profile-first");
  const secondReference = reference("profile-second");
  try {
    const first = source.queryBinding.member(input);
    const second = source.siblingQueryBinding.member(input);
    expect(await first.ready).toBe("bound:shared");
    expect(await second.ready).toBe("sibling:shared");
    expect(first.key).toBe(second.key);
    expect(first.identity).not.toBe(second.identity);

    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [
        { binding: source.queryBinding, reference: firstReference },
        { binding: source.siblingQueryBinding, reference: secondReference },
      ],
    });
    expect(encoded.entries).toHaveLength(2);
    const report = adopt(target.graph, JSON.parse(JSON.stringify(encoded)) as typeof encoded, {
      bindings: [
        { binding: target.siblingQueryBinding, reference: reference("profile-second", true) },
        { binding: target.queryBinding, reference: reference("profile-first", true) },
      ],
    });
    const adopted = report.adopted.filter((row) => row.address.kind === "binding");
    expect(adopted).toHaveLength(2);
    expect(new Set(adopted.map((row) => row.identity)).size).toBe(2);
    expect(target.queryKeys()).toBe(0);
    expect(target.fetches()).toBe(0);

    const restoredFirst = target.queryBinding.member(input);
    const restoredSecond = target.siblingQueryBinding.member(input);
    expect([restoredFirst.value.get(), restoredSecond.value.get()]).toEqual([
      "bound:shared",
      "sibling:shared",
    ]);
    expect(target.fetches()).toBe(0);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("reports the stream import commit revision when a later public observer writes reentrantly", async () => {
  const source = makeWorld("raw");
  const target = makeWorld("raw");
  const input = { key: "shared" };
  const streamReference = reference("stream");
  let stopRaw: (() => void) | undefined;
  try {
    source.rawStore!.set("raw-source");
    await receiveStreamValue(source, input, "stream-source");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: streamReference }],
    });
    const targetRaw = target.rawStore!;
    const reentered = Promise.withResolvers<void>();
    let targetStream: ReturnType<typeof target.streamBinding.member> | undefined;
    stopRaw = subscribe(targetRaw, (value) => {
      if (value !== "raw-source" || targetStream !== undefined) return;
      targetStream = target.streamBinding.member(input);
      targetStream.set("after-observer");
      reentered.resolve();
    });

    const report = adopt(target.graph, encoded, {
      bindings: [{ binding: target.streamBinding, reference: reference("stream", true) }],
    });
    await reentered.promise;
    const adopted = report.adopted.find(isAdoptedStreamEntry);
    if (adopted === undefined)
      throw new Error("Expected the stream address in the adoption report");
    expect(targetStream).toBeDefined();
    expect(targetStream?.value.get()).toBe("after-observer");
    const after = peekStore(targetStream!);
    if (after.kind !== "stable") throw new Error("Expected the reentrant write to remain current");
    expect(adopted.valueRevision).toBeLessThan(after.valueRevision);
  } finally {
    stopRaw?.();
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("rejects repeated handles and canonical references before running binding callbacks", () => {
  const graph = makeWorld();
  try {
    expect(() =>
      snapshot(graph.graph, {
        version: 2,
        bindings: [
          { binding: graph.queryBinding, reference: reference("first") },
          { binding: graph.queryBinding, reference: reference("second") },
        ],
      }),
    ).toThrow();
    expect(() =>
      snapshot(graph.graph, {
        version: 2,
        bindings: [
          { binding: graph.queryBinding, reference: reference("duplicate") },
          { binding: graph.streamBinding, reference: reference("duplicate", true) },
        ],
      }),
    ).toThrow();
    expect([graph.queryKeys(), graph.streamKeys(), graph.fetches(), graph.opens()]).toEqual([
      0, 0, 0, 0,
    ]);
  } finally {
    graph.graph.dispose();
  }
});

it.each(["write", "reset"] as const)(
  "fences a source stream when a qualified reference getter reenters with %s",
  async (reentry) => {
    const source = makeWorld();
    const input = { key: "shared" };
    let resetting: Promise<void> | undefined;
    try {
      const store = await receiveStreamValue(source, input, "before-encoding");
      const qualified = {
        get role() {
          if (reentry === "write") store.set("after-write");
          else resetting = source.graph.reset();
          return "stream";
        },
      };
      expect(() =>
        snapshot(source.graph, {
          version: 2,
          bindings: [{ binding: source.streamBinding, reference: qualified }],
        }),
      ).toThrow(Fault);
      if (reentry === "write") expect(store.value.get()).toBe("after-write");
      await resetting;
    } finally {
      source.graph.dispose();
    }
  },
);

it("fences a raw source value when its getter writes a successor during v2 encoding", () => {
  const definition = createStore({
    name: "payload",
    initial: { value: "initial" },
    snapshot: true,
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "bound-transfer-value",
      operations: { payload: definition },
    }),
  });
  try {
    const store = graph.api.payload();
    let replace = (): void => undefined;
    const value = Object.defineProperty({}, "value", {
      enumerable: true,
      get() {
        replace();
        return "captured-before-write";
      },
    }) as { value: string };
    replace = () => store.set({ value: "successor" });
    store.set(value);

    expect(() => snapshot(graph, { version: 2 })).toThrow(Fault);
    expect(store.value.get()).toEqual({ value: "successor" });
  } finally {
    graph.dispose();
  }
});

it("does not overwrite a target stream acquired by a value getter during destination decoding", async () => {
  const source = makeWorld();
  const target = makeWorld();
  const input = { key: "shared" };
  let reentrant: ReturnType<typeof target.streamBinding.member> | undefined;
  try {
    await receiveStreamValue(source, input, "imported");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: reference("stream") }],
    });
    const streamRow = encoded.entries[0]!;
    const entry = { ...streamRow };
    Object.defineProperty(entry, "value", {
      enumerable: true,
      get() {
        reentrant = target.streamBinding.member(input);
        return streamRow.value;
      },
    });
    const incoming = { ...encoded, entries: [entry] } as typeof encoded;

    expect(() =>
      adopt(target.graph, incoming, {
        bindings: [{ binding: target.streamBinding, reference: reference("stream", true) }],
      }),
    ).toThrow(Fault);
    expect(reentrant).toBeDefined();
    expect([reentrant?.available.get(), reentrant?.pending.get()]).toEqual([false, true]);
    expect(target.opens()).toBe(1);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("preserves a query binding's completed value and skips a sibling stream opened by its equality callback", async () => {
  const input = { key: "shared" };
  const source = makeWorld();
  let target!: World;
  let openedByEquality: ReturnType<World["streamBinding"]["member"]> | undefined;
  let reentered = false;
  target = makeWorld(undefined, (left, right) => {
    if (!reentered) {
      reentered = true;
      openedByEquality = target.streamBinding.member(input);
    }
    return Object.is(left, right);
  });
  try {
    const query = source.queryBinding.member(input);
    expect(await query.ready).toBe("bound:shared");
    await receiveStreamValue(source, input, "stream-source");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [
        { binding: source.queryBinding, reference: reference("query") },
        { binding: source.streamBinding, reference: reference("stream") },
      ],
    });
    const queryEntry = encoded.entries.find((entry) => entry.address.kind === "binding")!;
    const streamEntry = encoded.entries.find((entry) => entry.address.kind === "stream-binding")!;
    const ordered = { ...encoded, entries: [queryEntry, streamEntry] };
    const existing = target.queryBinding.member(input, { initial: "target-state" });
    const report = adopt(target.graph, ordered, {
      bindings: [
        { binding: target.queryBinding, reference: reference("query", true) },
        { binding: target.streamBinding, reference: reference("stream", true) },
      ],
    });

    expect(existing.value.get()).toBe("bound:shared");
    expect(reentered).toBe(true);
    expect([openedByEquality?.available.get(), openedByEquality?.pending.get()]).toEqual([
      false,
      true,
    ]);
    expect(report.skipped).toMatchObject([{ address: { kind: "stream-binding", key: "shared" } }]);
    expect(target.opens()).toBe(1);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("honors a bound query's snapshot:false policy in v2", () => {
  const query = createQuery({
    name: "excluded",
    key: (_input: void) => "excluded",
    fetch: async () => "unused",
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-transfer-exclusion", operations: { query } }),
  });
  const context = createBindingContext(graph);
  const binding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: async () => "completed",
    value: { snapshot: false },
  });
  try {
    const store = binding.member(undefined, { initial: "private" });
    expect(store.value.get()).toBe("private");
    expect(
      snapshot(graph, { version: 2, bindings: [{ binding, reference: { operation: "excluded" } }] })
        .entries,
    ).toEqual([]);
  } finally {
    graph.dispose();
  }
});

it("keeps the destination stream's snapshot:false policy after importing and claiming a real baseline", () => {
  const source = makeWorld();
  const target = makeWorld(undefined, undefined, false);
  const input = { key: "shared" };
  try {
    source.streamBinding.member(input).set("imported");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: reference("stream") }],
    });
    adopt(target.graph, encoded, {
      bindings: [{ binding: target.streamBinding, reference: reference("stream", true) }],
    });

    const claimed = target.streamBinding.member(input);
    expect([claimed.available.get(), claimed.value.get(), claimed.pending.get()]).toEqual([
      true,
      "imported",
      true,
    ]);
    expect(
      snapshot(target.graph, {
        version: 2,
        bindings: [{ binding: target.streamBinding, reference: reference("stream") }],
      }).entries,
    ).toEqual([]);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("keeps a target observation issue when its completed query binding is adopted in v2", async () => {
  const source = makeWorld();
  const target = makeWorld();
  const input = { key: "shared" };
  const qualified = reference("query");
  try {
    const sourceStore = source.queryBinding.member(input);
    expect(await sourceStore.ready).toBe("bound:shared");
    const targetStore = target.queryBinding.member(input);
    expect(await targetStore.ready).toBe("bound:shared");
    const lease = observeStore(target.graph, targetStore);
    const issue = Object.freeze({ invalid: true });
    lease.reject(issue);
    const before = peekStore(targetStore);
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.queryBinding, reference: qualified }],
    });
    const report = adopt(target.graph, encoded, {
      bindings: [{ binding: target.queryBinding, reference: reference("query", true) }],
    });
    const after = peekStore(targetStore);

    expect(report.adopted).toMatchObject([{ address: { kind: "binding", key: "shared" } }]);
    expect(lease.issue.get()).toBe(issue);
    if (before.kind !== "stable" || after.kind !== "stable")
      throw new Error("Expected stable v2 adoption captures");
    expect(after.valueRevision).toBeGreaterThan(before.valueRevision);
    expect(after).toMatchObject({ present: true, value: "bound:shared" });
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it.each([
  "missing",
  "wrong-kind",
  "foreign",
  "fabricated",
  "inactive",
  "reference",
  "duplicate-handle",
  "duplicate-reference",
] as const)("preflights %s stream mappings before changing a raw sibling", (failure) => {
  const source = makeWorld("shared");
  const target = makeWorld("shared");
  const foreign = makeWorld();
  const input = { key: "shared" };
  const streamReference = reference("stream");
  try {
    source.rawStore!.set("raw-source");
    source.streamBinding.member(input).set("stream-source");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: streamReference }],
    });
    let bindings: SnapshotBindingReference[] = [];
    if (failure === "wrong-kind")
      bindings = [{ binding: target.queryBinding, reference: reference("stream", true) }];
    if (failure === "foreign")
      bindings = [{ binding: foreign.streamBinding, reference: reference("stream", true) }];
    if (failure === "fabricated")
      bindings = [
        {
          binding: Object.create(target.streamBinding) as typeof target.streamBinding,
          reference: reference("stream", true),
        },
      ];
    if (failure === "inactive") {
      target.queryBinding.context.dispose();
      bindings = [{ binding: target.streamBinding, reference: reference("stream", true) }];
    }
    if (failure === "reference")
      bindings = [{ binding: target.streamBinding, reference: reference("other") }];
    if (failure === "duplicate-handle")
      bindings = [
        { binding: target.streamBinding, reference: reference("stream", true) },
        { binding: target.streamBinding, reference: reference("another") },
      ];
    if (failure === "duplicate-reference")
      bindings = [
        { binding: target.streamBinding, reference: reference("same") },
        { binding: target.queryBinding, reference: reference("same", true) },
      ];
    expect(() => adopt(target.graph, encoded, { bindings })).toThrow();
    expect(target.rawStore!.value.get()).toBe("raw-initial");
    expect([target.queryKeys(), target.streamKeys(), target.fetches(), target.opens()]).toEqual([
      0, 0, 0, 0,
    ]);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
    foreign.graph.dispose();
  }
});

it("skips a stream row with landing:none instead of decoding it as a raw address", async () => {
  const source = makeWorld();
  const target = makeWorld();
  const input = { key: "shared" };
  try {
    await receiveStreamValue(source, input, "must-not-import");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: reference("stream") }],
    });
    const original = encoded.entries[0]!;
    const invalidEntry = { ...original, landing: { kind: "none" } };
    const invalid = { ...encoded, entries: [invalidEntry] } as unknown as typeof encoded;
    const report = adopt(target.graph, invalid, {
      bindings: [{ binding: target.streamBinding, reference: reference("stream", true) }],
    });
    expect(report.adopted).toEqual([]);
    expect(report.skipped).toHaveLength(1);
    expect(report.skipped[0]?.address).toBe(original.address);
    const store = target.streamBinding.member(input);
    expect([store.available.get(), store.value.get(), store.pending.get()]).toEqual([
      false,
      undefined,
      true,
    ]);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("rejects unsupported v2 envelope metadata and skips unsafe ordering without writing", () => {
  const source = makeWorld("shared");
  const target = makeWorld("shared");
  try {
    source.rawStore!.set("raw-source");
    const valid = snapshot(source.graph, { version: 2 });
    for (const candidate of [
      { ...valid, version: 1 },
      { ...valid, namespace: "other" },
      { ...valid, encoding: "future/2" },
    ]) {
      expect(() => adopt(target.graph, candidate as unknown as typeof valid, {})).toThrow(Fault);
      expect(target.rawStore!.value.get()).toBe("raw-initial");
    }
    const malformedAddress = valid.entries[0]!.address;
    const invalidOrdering = {
      ...valid,
      entries: valid.entries.map((entry) => ({ ...entry, generation: 1.5 })),
    } as unknown as typeof valid;
    const report = adopt(target.graph, invalidOrdering, {});
    expect(report.adopted).toEqual([]);
    expect(report.skipped).toHaveLength(1);
    expect(report.skipped[0]?.address).toBe(malformedAddress);
    expect(target.rawStore!.value.get()).toBe("raw-initial");
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("rejects duplicate physical stream addresses before writing a valid raw sibling", () => {
  const source = makeWorld("shared");
  const target = makeWorld("shared");
  const input = { key: "shared" };
  try {
    source.rawStore!.set("raw-source");
    source.streamBinding.member(input).set("stream-source");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: reference("stream") }],
    });
    const rawEntry = encoded.entries.find((entry) => entry.address.kind === "raw");
    const streamEntry = encoded.entries.find((entry) => entry.address.kind === "stream-binding");
    if (rawEntry === undefined || streamEntry === undefined)
      throw new Error("Expected both the raw sibling and stream address");
    const duplicate = { ...encoded, entries: [rawEntry, streamEntry, streamEntry] };

    expect(() =>
      adopt(target.graph, duplicate, {
        bindings: [{ binding: target.streamBinding, reference: reference("stream", true) }],
      }),
    ).toThrow(Fault);
    expect(target.rawStore!.value.get()).toBe("raw-initial");
    expect([target.queryKeys(), target.streamKeys(), target.fetches(), target.opens()]).toEqual([
      0, 0, 0, 0,
    ]);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it("preflights local ordering capacity before committing an earlier raw sibling", () => {
  const source = makeWorld("shared");
  const target = makeWorld("shared");
  const input = { key: "shared" };
  try {
    source.rawStore!.set("raw-source");
    source.streamBinding.member(input).set("stream-source");
    const encoded = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.streamBinding, reference: reference("stream") }],
    });
    const entries = encoded.entries.map((entry) =>
      entry.address.kind === "stream-binding"
        ? { ...entry, generation: Number.MAX_SAFE_INTEGER }
        : entry,
    );
    const rawEntry = entries.find((entry) => entry.address.kind === "raw");
    const streamEntry = entries.find((entry) => entry.address.kind === "stream-binding");
    if (rawEntry === undefined || streamEntry === undefined)
      throw new Error("Expected both the raw sibling and stream address");
    const exhausted = { ...encoded, entries: [rawEntry, streamEntry] } as unknown as typeof encoded;

    expect(() =>
      adopt(target.graph, exhausted, {
        bindings: [{ binding: target.streamBinding, reference: reference("stream", true) }],
      }),
    ).toThrow(RangeError);
    expect(target.rawStore!.value.get()).toBe("raw-initial");
    expect([target.queryKeys(), target.streamKeys(), target.fetches(), target.opens()]).toEqual([
      0, 0, 0, 0,
    ]);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it.each(["normal", "last-request", "exhausted"] as const)(
  "keeps native query authority safe after a %s generation import",
  async (mode) => {
    const backend = testBackend();
    const query = createQuery({
      name: "transfer.safe-generation",
      key: (_input: void) => "counter",
      fetch: backend.respond<void, number>("transfer.safe-generation"),
    });
    const contract = defineContract({
      namespace: "transfer-safe-generation",
      operations: { query },
    });
    const source = createGraph({ contract });
    const target = createGraph({ contract });
    const caller = new AbortController();
    const add = caller.signal.addEventListener.bind(caller.signal);
    const remove = caller.signal.removeEventListener.bind(caller.signal);
    let links = 0;
    let unlinks = 0;
    Object.defineProperties(caller.signal, {
      addEventListener: {
        value: (...args: Parameters<AbortSignal["addEventListener"]>) => {
          if (args[0] === "abort") links += 1;
          add(...args);
        },
      },
      removeEventListener: {
        value: (...args: Parameters<AbortSignal["removeEventListener"]>) => {
          if (args[0] === "abort") unlinks += 1;
          remove(...args);
        },
      },
    });
    let refresh: Promise<void> | undefined;
    try {
      source.api.query(undefined, { initial: 4 });
      const handle = target.api.query(undefined, { initial: 0 });
      const encoded = snapshot(source, { version: 2 });
      const generation =
        mode === "normal" ? 20 : Number.MAX_SAFE_INTEGER - (mode === "exhausted" ? 1 : 2);
      const incoming = {
        ...encoded,
        entries: encoded.entries.map((row) => ({ ...row, generation })),
      } as unknown as typeof encoded;
      adopt(target, incoming, {});
      const before = peekStore(handle);
      let refreshThrew = false;
      let refreshError: unknown;
      let writeThrew = false;
      let writeError: unknown;
      try {
        refresh = handle.revalidate({ abortSignal: caller.signal });
      } catch (error) {
        refreshThrew = true;
        refreshError = error;
      }
      try {
        handle.set(100);
      } catch (error) {
        writeThrew = true;
        writeError = error;
      }
      const during = peekStore(handle);
      const dispatched = [...backend.calls];
      // Settle the actual request before assertions, including on the unfixed candidate.
      for (const call of dispatched) backend.resolve(call, 1);
      if (refresh !== undefined) await refresh;

      expect(refreshThrew).toBe(mode === "exhausted");
      if (mode === "exhausted") expect(refreshError).toBeInstanceOf(RangeError);
      expect(writeThrew).toBe(mode !== "normal");
      if (mode !== "normal") expect(writeError).toBeInstanceOf(RangeError);
      expect(dispatched).toHaveLength(mode === "exhausted" ? 0 : 1);
      expect(dispatched.every((call) => !call.aborted && call.settled)).toBe(true);
      expect(links).toBe(mode === "exhausted" ? 0 : 1);
      expect(unlinks).toBe(links);
      if (mode === "exhausted") {
        expect(during).toEqual(before);
        expect(peekStore(handle)).toEqual(before);
      } else {
        expect(during).toMatchObject({ present: true, value: mode === "normal" ? 100 : 4 });
        expect(handle.value.get()).toBe(mode === "normal" ? 100 : 1);
      }
      const current = snapshot(target, { version: 2 }).entries[0]!;
      expect(Number.isSafeInteger(current.generation)).toBe(true);
      expect(Number(current.generation)).toBe(mode === "normal" ? 23 : Number.MAX_SAFE_INTEGER);
    } finally {
      target.dispose();
      source.dispose();
      caller.abort();
    }
  },
);

it("keeps a bound query claim dormant until ordinary post-discard acquisition", async () => {
  const world = makeWorld();
  const input = { key: "passive-query" };
  const beforeKeys = world.queryKeys();
  const controller = new AbortController();
  try {
    const claim = preparePassiveTransferClaim(world.graph, world.queryBinding, input, {
      abortSignal: controller.signal,
    });
    expect(world.queryKeys()).toBe(beforeKeys + 1);
    expect(world.fetches()).toBe(0);
    expect(claim.store.key).toBe("passive-query");
    expect(claim.store.graph).toBe(world.graph.id);
    expect(() => claim.store.value.get()).toThrow(TypeError);
    expect(() => claim.store.status.peek()).toThrow(TypeError);

    controller.abort();
    claim.discard();
    await claim.retired;
    expect(world.fetches()).toBe(0);

    const ordinary = world.queryBinding.member(input);
    expect(world.queryKeys()).toBe(beforeKeys + 2);
    expect(world.fetches()).toBe(1);
    await expect(ordinary.ready).resolves.toBe("bound:passive-query");
  } finally {
    world.graph.dispose();
  }
});

it("does not open a raw stream while preparing or discarding its passive claim", async () => {
  const world = makeWorld();
  const input = { key: "passive-stream" };
  const member = world.graph.api.stream;
  const beforeKeys = world.streamKeys();
  try {
    const claim = preparePassiveTransferClaim(world.graph, member, input);
    expect(world.streamKeys()).toBe(beforeKeys + 1);
    expect(world.opens()).toBe(0);
    expect(claim.store.key).toBe("passive-stream");
    expect(claim.store.graph).toBe(world.graph.id);
    expect(() => claim.store.value.peek()).toThrow(TypeError);
    expect(() => claim.store.status.get()).toThrow(TypeError);
    claim.discard();
    await claim.retired;
    expect(world.opens()).toBe(0);

    world.graph.api.stream(input);
    expect(world.opens()).toBe(1);
  } finally {
    world.graph.dispose();
  }
});

it("joins a bound mutation to the exact native operation role until physical completion", async () => {
  let resolveRun!: (value: void) => void;
  let runs = 0;
  let completion: Promise<void> | undefined;
  let runSettled = false;
  const work = new Promise<void>((resolve) => { resolveRun = resolve; });
  const query = createQuery({
    name: "bound-transfer.work-query",
    key: (input: Input) => input.key,
    fetch: async () => 1,
  });
  const mutation = createMutation({
    name: "bound-transfer.work-mutation",
    run: async () => {
      runs += 1;
      await work;
    },
    affects: [affects(query, { select: (input: Input) => input, on: "invalidate" })],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-transfer-work", operations: { query, mutation } }),
  });
  const input = { key: "one" };
  const context = createBindingContext(graph);
  const mutationBinding = bindOperation(graph, mutation, {
    context,
    profile: {},
    run: async () => {
      runs += 1;
      await work;
    },
  });
  try {
    const queryStore = graph.api.query(input);
    await expect(queryStore.ready).resolves.toBe(1);
    const scope = createTransferScope(graph, graph.api.query, queryStore);
    const pending = prepareTransferOperation([scope], [{
      role: "bound",
      source: mutationBinding,
      input,
    }]);
    const operationCommit = pending.seal();
    if (operationCommit === undefined) throw new Error("The bound mutation operation did not seal");
    operationCommit.publish();
    operationCommit.notifications.deliver();
    await pending.retired;

    completion = mutationBinding.member(input);
    expect(runs).toBe(1);
    expect(() => preparePassiveTransferClaim(graph, graph.api.query, input)).toThrow(TypeError);
    expect(() => prepareTransferOperationRetirement(pending.operation)).toThrow(TypeError);

    resolveRun(undefined);
    await completion;
    runSettled = true;
    const retirement = prepareTransferOperationRetirement(pending.operation);
    const retirementCommit = retirement.seal();
    if (retirementCommit === undefined) throw new Error("The completed bound mutation did not retire");
    retirementCommit.publish();
    retirementCommit.notifications.deliver();
    await retirement.retired;

    const claim = preparePassiveTransferClaim(graph, graph.api.query, input);
    claim.discard();
    await claim.retired;
  } finally {
    if (completion !== undefined && !runSettled) {
      resolveRun(undefined);
      await completion.catch(() => undefined);
    }
    context.dispose();
    graph.dispose();
  }
});
