import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  adopt,
  bindOperation,
  bindStream,
  captureReactiveConstruction,
  computed,
  createBindingContext,
  createGraph,
  createQuery,
  createStream,
  createPositionedStream,
  project,
  capturePositionedStore,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
  preparePositionedStoreRestore,
  defineContract,
  Fault,
  inspectBoundAddress,
  peekStore,
  snapshot,
  storeKey,
  subscribe,
  type AdoptionReportV2,
  type BoundAddressEmpty,
  type StoreIdentity,
  type ValueRevision,
  type PositionedStreamSource,
  type PositionedStoreRestoration,
  type StreamStore,
  type ClosedEncodedValue,
  type RequiredTransferCodec,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

const positionedProviderClockNow = Date.now;

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

const transferReference = { module: "rooms", operation: "updates" };

function importedStream(value: number | undefined) {
  const source = fixture();
  source.binding.member("one", { initial: value });
  const snapshotV2 = snapshot(source.graph, {
    version: 2,
    bindings: [{ binding: source.binding, reference: transferReference }],
  });
  return { source, snapshotV2 };
}

async function transferFromEndedSession(value: number | undefined) {
  const source = fixture();
  let stop: (() => void) | undefined;
  try {
    const store = source.binding.member("one");
    const session = source.binding.session(store);
    const accepted = Promise.withResolvers<void>();
    stop = subscribe(store.available, (available) => {
      if (available) accepted.resolve();
    });
    source.backend.emit("events", value);
    await accepted.promise;
    stop();
    stop = undefined;
    source.backend.end("events");
    const receipt = await session.detached;
    expect(receipt.terminalCapture).toMatchObject({
      kind: "captured",
      capture: { kind: "stable", present: true, value },
    });
    return snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.binding, reference: transferReference }],
    });
  } finally {
    stop?.();
    source.graph.dispose();
  }
}

function streamSnapshot(world: ReturnType<typeof fixture>) {
  return snapshot(world.graph, {
    version: 2,
    bindings: [{ binding: world.binding, reference: transferReference }],
  });
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

it.each([41, undefined])(
  "treats a real adopted %s stream base as occupied and admits it only to the first real session",
  async (value) => {
    const source = importedStream(value);
    const target = fixture();
    try {
      const beforeImport = inspectBoundAddress(target.binding, storeKey("one"));
      if (beforeImport.kind !== "stable" || beforeImport.state !== "empty")
        throw new Error("Expected an empty target address before adoption");

      const report = adopt(target.graph, source.snapshotV2, {
        bindings: [
          { binding: target.binding, reference: { operation: "updates", module: "rooms" } },
        ],
      });
      expect(report.adopted).toMatchObject([
        { address: { kind: "stream-binding", key: "one" }, valueRevision: expect.any(Number) },
      ]);
      expect(inspectBoundAddress(target.binding, storeKey("one"))).toMatchObject({
        kind: "stable",
        state: "occupied",
        present: true,
        basePresent: true,
      });
      expect(() => target.binding.member("one", { expectEmpty: beforeImport.condition })).toThrow(
        TypeError,
      );
      expect(target.backend.calls).toHaveLength(0);

      const first = target.binding.member("one");
      expect([first.available.get(), first.value.get(), first.pending.get()]).toEqual([
        true,
        value,
        true,
      ]);
      target.backend.end("events");
      await target.binding.session(first).detached;

      const second = target.binding.member("one");
      expect([second.available.get(), second.value.get(), second.pending.get()]).toEqual([
        false,
        undefined,
        true,
      ]);
      expect(target.backend.calls).toHaveLength(2);
    } finally {
      source.source.graph.dispose();
      target.graph.dispose();
    }
  },
);

it("rechecks an inspected empty address after a real import reenters through the key callback", () => {
  const source = importedStream(73);
  const target = fixture();
  try {
    const inspected = inspectBoundAddress(target.binding, storeKey("one"));
    if (inspected.kind !== "stable" || inspected.state !== "empty")
      throw new Error("Expected an empty target address before acquisition");
    let imported = false;
    target.beforeKey(() => {
      if (imported) return;
      imported = true;
      adopt(target.graph, source.snapshotV2, {
        bindings: [{ binding: target.binding, reference: transferReference }],
      });
    });

    expect(() => target.binding.member("one", { expectEmpty: inspected.condition })).toThrow(
      TypeError,
    );
    expect(target.backend.calls).toHaveLength(0);
    expect(inspectBoundAddress(target.binding, storeKey("one"))).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });
    const claimed = target.binding.member("one");
    expect(claimed.value.get()).toBe(73);
    expect(target.backend.calls).toHaveLength(1);
  } finally {
    source.source.graph.dispose();
    target.graph.dispose();
  }
});

it("does not revive an empty-address stamp after its imported base is collected", async () => {
  const source = importedStream(19);
  const target = fixture();
  try {
    const beforeImport = inspectBoundAddress(target.binding, storeKey("one"));
    if (beforeImport.kind !== "stable" || beforeImport.state !== "empty")
      throw new Error("Expected an empty target address before adoption");
    adopt(target.graph, source.snapshotV2, {
      bindings: [{ binding: target.binding, reference: transferReference }],
    });
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    jest.advanceTimersByTime(10);

    const afterCollection = inspectBoundAddress(target.binding, storeKey("one"));
    expect(afterCollection).toMatchObject({
      kind: "stable",
      state: "empty",
      present: false,
      basePresent: false,
    });
    expect(
      inspectBoundAddress(target.binding, storeKey("one"), { expected: beforeImport.stamp }),
    ).toEqual({ kind: "changed" });
    expect(() => target.binding.member("one", { expectEmpty: beforeImport.condition })).toThrow(
      TypeError,
    );
    expect(target.backend.calls).toHaveLength(0);
  } finally {
    source.source.graph.dispose();
    target.graph.dispose();
  }
});

it("skips an active empty stream and adopts the same shell after genuine idle collection", async () => {
  const source = importedStream(73);
  const target = fixture();
  try {
    const incoming = JSON.parse(JSON.stringify(source.snapshotV2)) as typeof source.snapshotV2;
    const options = {
      bindings: [{ binding: target.binding, reference: { operation: "updates", module: "rooms" } }],
    };
    const key = storeKey("one");
    const active = target.binding.member("one");
    const session = target.binding.session(active);
    const before = peekStore(active);
    if (before.kind !== "stable") throw new Error("Expected the real active session capture");
    expect(before.present).toBe(false);
    expect(active.pending.get()).toBe(true);
    const occupied = inspectBoundAddress(target.binding, key);
    expect(occupied).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: false,
      basePresent: false,
    });
    if (occupied.kind !== "stable") throw new Error("Expected stable active occupation");
    const counts = [target.keys(), target.opens(), target.backend.calls.length];

    const skipped = adopt(target.graph, incoming, options);

    expect(skipped.adopted).toEqual([]);
    expect(skipped.skipped).toEqual([
      { address: { kind: "stream-binding", key: "one", reference: transferReference } },
    ]);
    expect(peekStore(active)).toEqual(before);
    expect(target.binding.session(active).identity).toBe(session.identity);
    expect(active.pending.get()).toBe(true);
    expect([target.keys(), target.opens(), target.backend.calls.length]).toEqual(counts);
    expect(inspectBoundAddress(target.binding, key, { expected: occupied.stamp })).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: false,
      basePresent: false,
    });

    target.backend.end("events");
    const detached = await session.detached;
    expect(detached.terminalCapture).toEqual({ kind: "captured", capture: before });
    const ended = inspectBoundAddress(target.binding, key);
    if (ended.kind !== "stable" || ended.state !== "empty")
      throw new Error("Expected the ended no-base shell before collection");

    await new Promise<void>((resolve) => queueMicrotask(resolve));
    jest.advanceTimersByTime(10);

    expect(inspectBoundAddress(target.binding, key, { expected: ended.stamp })).toEqual({
      kind: "changed",
    });
    expect(inspectBoundAddress(target.binding, key)).toMatchObject({
      kind: "stable",
      state: "empty",
      present: false,
      basePresent: false,
    });
    const beforeAdoption = [target.keys(), target.opens(), target.backend.calls.length];
    const report = adopt(target.graph, incoming, options);
    expect(report.skipped).toEqual([]);
    expect(report.adopted).toHaveLength(1);
    const imported = report.adopted[0];
    if (
      imported === undefined ||
      imported.address.kind !== "stream-binding" ||
      !("valueRevision" in imported)
    )
      throw new Error("Expected the actual imported stream stamp");
    expect(imported.address).toEqual({
      kind: "stream-binding",
      key: "one",
      reference: transferReference,
    });
    expect(imported.identity).toBe(before.identity);
    expect([target.keys(), target.opens(), target.backend.calls.length]).toEqual(beforeAdoption);
    expect(inspectBoundAddress(target.binding, key)).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });

    const claimed = target.binding.member("one");
    const nextSession = target.binding.session(claimed);
    const capture = peekStore(claimed);
    if (capture.kind !== "stable") throw new Error("Expected the current claimed imported base");
    expect(nextSession.identity).not.toBe(session.identity);
    expect(nextSession.storeIdentity).toBe(imported.identity);
    expect(capture.identity).toBe(imported.identity);
    expect(capture.valueRevision).toBe(imported.valueRevision);
    expect(capture).toMatchObject({ present: true, value: 73 });
    expect([claimed.available.get(), claimed.value.get(), claimed.pending.get()]).toEqual([
      true,
      73,
      true,
    ]);
    expect([target.keys(), target.opens(), target.backend.calls.length]).toEqual([
      beforeAdoption[0]! + 1,
      beforeAdoption[1]! + 1,
      beforeAdoption[2]! + 1,
    ]);
  } finally {
    source.source.graph.dispose();
    target.graph.dispose();
  }
});

it("keeps a missing address stamp current when another key receives an imported stream base", () => {
  const source = fixture();
  const target = fixture();
  try {
    source.binding.member("present", { initial: 5 });
    const transfer = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.binding, reference: transferReference }],
    });
    const missing = inspectBoundAddress(target.binding, storeKey("absent"));
    if (missing.kind !== "stable" || missing.state !== "empty")
      throw new Error("Expected an empty unrelated address");

    adopt(target.graph, transfer, {
      bindings: [{ binding: target.binding, reference: transferReference }],
    });
    expect(
      inspectBoundAddress(target.binding, storeKey("absent"), { expected: missing.stamp }),
    ).toMatchObject({ kind: "stable", state: "empty", present: false, basePresent: false });
    const admitted = target.binding.member("absent", { expectEmpty: missing.condition });
    expect([admitted.available.get(), admitted.pending.get()]).toEqual([false, true]);
    expect(target.backend.calls).toHaveLength(1);
  } finally {
    source.graph.dispose();
    target.graph.dispose();
  }
});

it.each(["retained-value", "retained-undefined", "retiring-prefix", "unclaimed-import"] as const)(
  "skips ordinary adoption into a %s stream address without changing its incumbent",
  async (mode) => {
    const target = fixture({ holdReturn: mode === "retiring-prefix" });
    const key = storeKey("one");
    let stop: (() => void) | undefined;
    let existing: ReturnType<typeof target.binding.member> | undefined;
    let existingSession: ReturnType<typeof target.binding.session> | undefined;
    let importedIdentity: StoreIdentity | undefined;
    let importedRevision: ValueRevision | undefined;
    let snapshotBefore: ReturnType<typeof streamSnapshot>["entries"][number] | undefined;
    let reentrantReport: AdoptionReportV2 | undefined;
    let reentrantThrew = false;
    try {
      const incoming = await transferFromEndedSession(99);
      if (mode === "retained-value" || mode === "retained-undefined") {
        const value = mode === "retained-value" ? 17 : undefined;
        const store = target.binding.member("one");
        existing = store;
        existingSession = target.binding.session(store);
        const accepted = Promise.withResolvers<void>();
        stop = subscribe(store.available, (available) => {
          if (available) accepted.resolve();
        });
        target.backend.emit("events", value);
        await accepted.promise;
        stop();
        stop = undefined;
        target.backend.end("events");
        const receipt = await existingSession.detached;
        expect(receipt.terminalCapture).toMatchObject({
          kind: "captured",
          capture: { kind: "stable", present: true, value },
        });
      } else if (mode === "retiring-prefix") {
        const store = target.binding.member("one");
        existing = store;
        existingSession = target.binding.session(store);
        const accepted = Promise.withResolvers<void>();
        stop = subscribe(store.available, (available) => {
          if (available) accepted.resolve();
        });
        await target.firstPull();
        target.deliverFirst(23);
        await accepted.promise;
        stop();
        stop = undefined;
        snapshotBefore = streamSnapshot(target).entries[0];
        if (snapshotBefore === undefined)
          throw new Error("Expected the retiring stream prefix in its public snapshot");
        target.beforePhysicalReturn(() => {
          try {
            reentrantReport = adopt(target.graph, incoming, {
              bindings: [{ binding: target.binding, reference: transferReference }],
            });
          } catch {
            reentrantThrew = true;
          }
        });
        target.graph.cancelPending();
        await target.returnStarted();
        const receipt = await existingSession.detached;
        expect(receipt.terminalCapture).toMatchObject({
          kind: "captured",
          capture: { kind: "stable", present: true, value: 23 },
        });
      } else {
        const seed = await transferFromEndedSession(41);
        const seeded = adopt(target.graph, seed, {
          bindings: [{ binding: target.binding, reference: transferReference }],
        });
        expect(seeded.adopted).toHaveLength(1);
        const seededRow = seeded.adopted[0];
        if (
          seededRow === undefined ||
          seededRow.address.kind !== "stream-binding" ||
          !("valueRevision" in seededRow)
        )
          throw new Error("Expected an actual stream adoption stamp");
        importedIdentity = seededRow.identity;
        importedRevision = seededRow.valueRevision;
        expect(seededRow.address.key).toBe("one");
        expect(typeof importedIdentity).toBe("object");
        expect(Number.isSafeInteger(importedRevision)).toBe(true);
        expect(target.opens()).toBe(0);
      }

      const before = inspectBoundAddress(target.binding, key);
      if (before.kind !== "stable" || before.state !== "occupied")
        throw new Error("Expected the incumbent stream address to be occupied");
      expect(before).toMatchObject({ present: true, basePresent: true });
      if (mode !== "retiring-prefix") snapshotBefore = streamSnapshot(target).entries[0];
      if (snapshotBefore === undefined)
        throw new Error("Expected the incumbent stream base in its public snapshot");
      const storeIdentity = existingSession?.storeIdentity ?? importedIdentity;
      const opensBefore = target.opens();
      const keysBefore = target.keys();

      expect(reentrantThrew).toBe(false);
      if (mode === "retiring-prefix" && reentrantReport === undefined)
        throw new Error("Expected an adoption report from the actual retirement callout");
      const report =
        reentrantReport ??
        adopt(target.graph, incoming, {
          bindings: [{ binding: target.binding, reference: transferReference }],
        });

      expect(report.adopted).toEqual([]);
      expect(report.skipped).toHaveLength(1);
      expect(report.skipped).toMatchObject([{ address: { kind: "stream-binding", key: "one" } }]);
      expect(target.opens()).toBe(opensBefore);
      expect(target.keys()).toBe(keysBefore);
      const after =
        mode === "retiring-prefix"
          ? inspectBoundAddress(target.binding, key)
          : inspectBoundAddress(target.binding, key, { expected: before.stamp });
      expect(after).toMatchObject({
        kind: "stable",
        state: "occupied",
        present: true,
        basePresent: true,
      });
      if (existing !== undefined && existingSession !== undefined) {
        if (storeIdentity === undefined)
          throw new Error("Expected the incumbent physical identity");
        expect(existing.identity).toBe(storeIdentity);
        expect(target.binding.session(existing).identity).toBe(existingSession.identity);
        const expected =
          mode === "retained-value" ? 17 : mode === "retiring-prefix" ? 23 : undefined;
        expect(existing.value.get()).toBe(expected);
      }
      expect(streamSnapshot(target).entries[0]).toEqual(snapshotBefore);
      if (mode === "unclaimed-import") {
        if (importedIdentity === undefined || importedRevision === undefined)
          throw new Error("Expected the imported physical identity and revision");
        const claimed = target.binding.member("one");
        const claimedSession = target.binding.session(claimed);
        const capture = peekStore(claimed);
        expect(claimed.identity).toBe(importedIdentity);
        expect(claimedSession.storeIdentity).toBe(importedIdentity);
        expect(claimedSession.identity).not.toBe(importedIdentity);
        expect(target.binding.session(claimed).identity).toBe(claimedSession.identity);
        if (capture.kind !== "stable") throw new Error("Expected the current claimed base");
        expect(capture.identity).toBe(importedIdentity);
        expect(capture.valueRevision).toBe(importedRevision);
        expect(capture).toMatchObject({ present: true, value: 41 });
        expect([claimed.available.get(), claimed.value.get(), claimed.pending.get()]).toEqual([
          true,
          41,
          true,
        ]);
        expect(target.opens()).toBe(1);
      }
    } finally {
      stop?.();
      target.finishPhysicalReturn();
      target.graph.dispose();
    }
  },
);

it("keeps same-key positioned claims out of bound addresses until publication", async () => {
  type Input = { readonly room: string };
  type Event = { readonly position: number; readonly value: number };
  type Reservation = {
    readonly input: Input;
    readonly after: number;
    readonly seed: unknown;
    readonly controller: AbortController;
    readonly expiresAt: number;
  };
  const makeProvider = () => {
    const reservations = new WeakMap<object, Reservation>();
    const readers = new WeakMap<object, (event: Event) => void>();
    const releases = new WeakMap<object, number>();
    let emitLive: ((event: Event) => void) | undefined;
    const openings: Array<{ readonly reader: object | undefined }> = [];
    const source: PositionedStreamSource<Input, number, number, object> = {
      open: (_input, options) => {
        const waiters: Array<(result: IteratorResult<Event>) => void> = [];
        const iterable: AsyncIterable<Event> = {
          [Symbol.asyncIterator]() {
            return {
              next: () => new Promise<IteratorResult<Event>>((resolve) => waiters.push(resolve)),
              return: () => {
                for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
                return Promise.resolve({ done: true as const, value: undefined });
              },
            };
          },
        };
        const emit = (event: Event): void => {
          const waiter = waiters.shift();
          if (waiter === undefined)
            throw new Error("The positioned address source has no pending next call");
          waiter({ done: false, value: event });
        };
        openings.push({ reader: options.reader });
        if (options.reader === undefined) emitLive = emit;
        else readers.set(options.reader, emit);
        return iterable;
      },
      readerLifetime: (_input, reader, after, seed) => {
        const reservation = reservations.get(reader);
        if (
          reservation === undefined ||
          reservation.input !== _input ||
          reservation.after !== after ||
          reservation.seed !== seed
        )
          throw new TypeError("The reader lifetime does not match its exact reservation");
        return { signal: reservation.controller.signal, expiresAt: reservation.expiresAt };
      },
      validateReader: (input, reader, after, _checkpoint, seed) => {
        const reservation = reservations.get(reader);
        return (
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
      releaseReader: (_input, reader) => {
        releases.set(reader, (releases.get(reader) ?? 0) + 1);
      },
    };
    return {
      source,
      openings,
      reserve(input: Input, after: number, seed: unknown) {
        const reader = Object.freeze({});
        reservations.set(reader, {
          input,
          after,
          seed,
          controller: new AbortController(),
          expiresAt: positionedProviderClockNow() + 60_000,
        });
        return reader;
      },
      emitLive(event: Event) {
        if (emitLive === undefined) throw new Error("The positioned address source did not open");
        emitLive(event);
      },
      releaseCount(reader: object) {
        return releases.get(reader) ?? 0;
      },
    };
  };
  const providerA = makeProvider();
  const providerB = makeProvider();
  const definitionA = createPositionedStream<Input, number, string, number, object>({
    name: "bound-address-positioned-transfer",
    key: (input) => input.room,
    source: providerA.source,
  });
  const definitionB = createPositionedStream<Input, number, string, number, object>({
    name: "bound-address-positioned-transfer",
    key: (input) => input.room,
    source: providerB.source,
  });
  const namespace = "bound-address-positioned-transfer";
  const sourceGraph = createGraph({
    contract: defineContract({ namespace, operations: { events: definitionA } }),
  });
  const targetGraph = createGraph({
    contract: defineContract({ namespace, operations: { events: definitionB } }),
  });
  const sourceContext = createBindingContext(sourceGraph);
  const targetContext = createBindingContext(targetGraph);
  const sourceBinding = bindStream(sourceGraph, definitionA, sourceGraph.api.events, {
    context: sourceContext,
  });
  const targetBinding = bindStream(targetGraph, definitionB, targetGraph.api.events, {
    context: targetContext,
  });
  const input = Object.freeze({ room: "one" });
  const key = storeKey(input.room);
  const policy = {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, event: number) => sum + event,
  };
  const sourceStore = sourceBinding.member(input);
  const sourceProjection = project(sourceStore, policy);
  let capture: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
  let adoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  let firstClaim:
    | {
        readonly store: StreamStore<number>;
        readonly restoration: PositionedStoreRestoration<number, readonly [number]>;
        readonly retired: Promise<void>;
        discard(): void;
      }
    | undefined;
  let peerClaim: typeof firstClaim;
  let firstReader: object | undefined;
  let peerReader: object | undefined;
  let capturePublished = false;
  let adoptionPublished = false;
  try {
    const landed = new Promise<void>((resolve) => {
      let stop: () => void = () => undefined;
      stop = subscribe(sourceProjection.value, (value) => {
        if (value !== 3) return;
        stop();
        resolve();
      });
    });
    providerA.emitLive({ position: 5, value: 3 });
    await landed;
    const checkpoint = capturePositionedStore(sourceStore, [sourceProjection] as const);
    expect(checkpoint.present).toBe(true);
    if (!checkpoint.present)
      throw new Error("The positioned source event is absent at the captured cut");
    expect(checkpoint.hasPosition).toBe(true);
    expect(checkpoint.position).toBe(5);
    expect(checkpoint.values).toStrictEqual([3]);

    const seed = {
      boundary: { kind: "event" as const, after: 5 },
      base: { present: true as const, value: checkpoint.latest },
      values: checkpoint.values,
    };
    const seedId = "p0-seed";
    const decodedSeeds = new Map<string, unknown>([[seedId, seed]]);
    const codec: RequiredTransferCodec = {
      encodeValue: (value) =>
        value !== null &&
        typeof value === "object" &&
        Object.hasOwn(value, "boundary") &&
        Object.hasOwn(value, "base")
          ? ({ $ref: seedId } as ClosedEncodedValue)
          : (value as ClosedEncodedValue),
      encodeFailure: (cause) => String(cause),
      decodeValue: (value) => {
        if (
          value !== null &&
          typeof value === "object" &&
          Object.hasOwn(value, "$ref") &&
          (value as { readonly $ref?: unknown }).$ref === seedId
        ) {
          const decoded = decodedSeeds.get(seedId);
          if (decoded === undefined) throw new Error("The positioned seed reference is missing");
          return decoded;
        }
        return value;
      },
      decodeFailure: (failure) => new Error(String(failure)),
    };
    const sourceRoot = computed(() => sourceProjection.value.get());
    expect(sourceRoot.get()).toBe(3);
    capture = prepareRequiredTransferCapture({
      graphs: [sourceGraph],
      roots: [{ source: sourceRoot, construction: captureReactiveConstruction(sourceRoot) }],
      positioned: [checkpoint],
      codec,
    });
    const image = capture.image;
    const positionedRows = image.positioned;
    if (positionedRows === undefined) throw new Error("The bound positioned group row was omitted");
    expect(positionedRows).toHaveLength(1);
    expect(positionedRows[0]).toMatchObject({ slot: "p0", store: "s0", seed: { $ref: "p0-seed" } });
    expect(decodedSeeds.get("p0-seed")).toBe(seed);
    const captureCommit = capture.seal();
    if (captureCommit === undefined) throw new Error("The bound positioned capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capture.retired;

    const empty = inspectBoundAddress(targetBinding, key);
    if (empty.kind !== "stable") throw new Error("Expected a stable empty target address");
    expect(empty).toMatchObject({ kind: "stable", state: "empty" });
    const firstReaderLocal = providerB.reserve(input, 5, seed);
    firstReader = firstReaderLocal;
    const first = preparePassiveTransferClaim(targetGraph, targetBinding, input, undefined, {
      policies: [policy] as const,
      seed,
      reader: firstReaderLocal,
    });
    firstClaim = first;
    const peerReaderLocal = providerB.reserve(input, 5, seed);
    peerReader = peerReaderLocal;
    const peer = preparePassiveTransferClaim(targetGraph, targetBinding, input, undefined, {
      policies: [policy] as const,
      seed,
      reader: peerReaderLocal,
    });
    peerClaim = peer;
    expect(first.store.identity).not.toBe(peer.store.identity);
    expect(providerB.openings).toHaveLength(0);
    expect(inspectBoundAddress(targetBinding, key, { expected: empty.stamp })).toMatchObject({
      kind: "stable",
      state: "empty",
    });

    let targetRuns = 0;
    const targetValue = computed(() => {
      targetRuns += 1;
      return first.restoration.projections[0]!.value.get();
    });
    adoption = prepareRequiredTransferAdoption(image, {
      graphs: [targetGraph],
      sources: [{ slot: "s0", claim: first }],
      positioned: [{ slot: "p0", claim: first, seed }],
      reactive: [
        {
          slot: image.roots[0]!,
          target: targetValue,
          construction: captureReactiveConstruction(targetValue),
        },
      ],
      codec,
      mappedStateWrites: [],
    });
    const commit = adoption.seal();
    if (commit === undefined) throw new Error("The bound positioned claim did not seal");
    commit.publish();
    adoptionPublished = true;
    commit.notifications.deliver();
    await Promise.all([adoption.retired, first.retired]);
    expect(targetValue.get()).toBe(3);
    expect(targetRuns).toBe(0);
    const occupied = inspectBoundAddress(targetBinding, key);
    if (occupied.kind !== "stable")
      throw new Error("Expected the published reader address to be stable");
    expect(occupied).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });
    expect(inspectBoundAddress(targetBinding, key, { expected: empty.stamp })).toMatchObject({
      kind: "changed",
    });
    const firstSession = targetBinding.session(first.store);
    expect(firstSession.storeIdentity).toBe(first.store.identity);
    expect(providerB.openings).toHaveLength(0);
    expect(providerB.releaseCount(firstReaderLocal)).toBe(0);

    peer.discard();
    await Promise.all([peer.retired, peer.restoration.retired]);
    expect(providerB.releaseCount(peerReaderLocal)).toBe(1);
    const afterPeerDiscard = inspectBoundAddress(targetBinding, key, { expected: occupied.stamp });
    if (afterPeerDiscard.kind !== "stable")
      throw new Error("Discarding the peer changed the published reader address");
    expect(afterPeerDiscard).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });
    expect(first.store.available.get()).toBe(true);
    expect(first.store.value.get()).toBe(3);
    expect(providerB.releaseCount(firstReaderLocal)).toBe(0);

    first.restoration.discard();
    await first.restoration.retired;
    expect(providerB.releaseCount(firstReaderLocal)).toBe(1);
  } finally {
    if (!capturePublished) capture?.discard();
    if (!adoptionPublished) {
      adoption?.discard();
      firstClaim?.discard();
    } else firstClaim?.restoration.discard();
    peerClaim?.discard();
    sourceProjection.close();
    sourceGraph.dispose();
    targetContext.dispose();
    targetGraph.dispose();
    if (
      firstReader !== undefined &&
      firstClaim === undefined &&
      providerB.releaseCount(firstReader) === 0
    )
      providerB.source.releaseReader(input, firstReader);
    if (
      peerReader !== undefined &&
      peerClaim === undefined &&
      providerB.releaseCount(peerReader) === 0
    )
      providerB.source.releaseReader(input, peerReader);
    if (capture !== undefined)
      await capture.retired.then(
        () => undefined,
        () => undefined,
      );
    if (adoption !== undefined)
      await adoption.retired.then(
        () => undefined,
        () => undefined,
      );
    if (firstClaim !== undefined) {
      await firstClaim.retired.then(
        () => undefined,
        () => undefined,
      );
      await firstClaim.restoration.retired.then(
        () => undefined,
        () => undefined,
      );
    }
    if (peerClaim !== undefined)
      await peerClaim.retired.then(
        () => undefined,
        () => undefined,
      );
  }
});

it("aggregates reader-scoped Stores at their binding address without deleting another key", async () => {
  type Input = { readonly room: string };
  type Event = { position: number; value: number };
  type Reservation = { input: Input; after: number; seed: unknown };
  const reservations = new WeakMap<object, Reservation>();
  const liveEmitters = new Map<string, (event: Event) => void>();
  const releases = new WeakMap<object, number>();
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
  const releaseCount = (reader: object): number => releases.get(reader) ?? 0;
  const releaseReader = (reader: object): void => {
    releases.set(reader, releaseCount(reader) + 1);
  };

  const stream = createPositionedStream<Input, number, string, number, object>({
    name: "addressed-reader-store",
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
      releaseReader: (_input, reader) => releaseReader(reader),
    },
  });
  const sourceGraph = createGraph({
    contract: defineContract({ namespace: "addressed-reader-source", operations: { stream } }),
  });
  const targetGraph = createGraph({
    contract: defineContract({ namespace: "addressed-reader-target", operations: { stream } }),
  });
  const context = createBindingContext(targetGraph);
  const binding = bindStream(targetGraph, stream, targetGraph.api.stream, { context });
  const input = Object.freeze({ room: "one" });
  const otherInput = Object.freeze({ room: "other" });
  const source = sourceGraph.api.stream(input);
  const policy = {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, event: number) => sum + event,
  };
  const projection = project(source, policy);
  let firstReader: object | undefined;
  let secondReader: object | undefined;
  let firstTransferred = false;
  let secondTransferred = false;

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
    if (emit === undefined) throw new Error("The genuine source did not open");
    emit({ position: 1, value: 5 });
    await folded;

    const checkpoint = capturePositionedStore(source, [projection] as const);
    if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
      throw new Error("The genuine source Store has no positioned landing");
    const after: number = checkpoint.position;
    const seed = { after, latest: checkpoint.latest, values: checkpoint.values };

    const before = inspectBoundAddress(binding, storeKey("one"));
    if (before.kind !== "stable" || before.state !== "empty")
      throw new Error("Expected an empty reader address before admission");
    firstReader = reserve(input, after, seed);
    secondReader = reserve(input, after, seed);
    const first = preparePositionedStoreRestore(
      binding,
      input,
      [policy] as const,
      seed,
      firstReader,
    );
    firstTransferred = true;
    const second = preparePositionedStoreRestore(
      binding,
      input,
      [policy] as const,
      seed,
      secondReader,
    );
    secondTransferred = true;
    const firstSession = binding.session(first.source);
    const secondSession = binding.session(second.source);

    expect(openings).toHaveLength(1);
    expect(first.source.identity).not.toBe(second.source.identity);
    expect(inspectBoundAddress(binding, storeKey("one"), { expected: before.stamp })).toEqual({
      kind: "changed",
    });
    expect(inspectBoundAddress(binding, storeKey("one"))).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });
    expect(() => binding.member(input, { expectEmpty: before.condition })).toThrow(TypeError);
    expect(openings).toHaveLength(1);

    const other = binding.member(otherInput);
    const otherSession = binding.session(other);
    expect(openings).toHaveLength(2);
    expect(inspectBoundAddress(binding, storeKey("other"))).toMatchObject({
      kind: "stable",
      state: "occupied",
    });

    first.discard();
    expect(releaseCount(firstReader)).toBe(1);
    expect(inspectBoundAddress(binding, storeKey("one"))).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });
    second.discard();
    expect(releaseCount(secondReader)).toBe(1);
    await Promise.all([firstSession.detached, secondSession.detached]);
    expect(inspectBoundAddress(binding, storeKey("one"))).toMatchObject({
      kind: "stable",
      state: "empty",
      present: false,
      basePresent: false,
    });
    expect(inspectBoundAddress(binding, storeKey("other"))).toMatchObject({
      kind: "stable",
      state: "occupied",
    });
    expect(binding.member(otherInput).identity).toBe(other.identity);
    expect(binding.session(other).identity).toBe(otherSession.identity);
    expect(openings).toHaveLength(2);
  } finally {
    if (firstReader !== undefined && !firstTransferred && releaseCount(firstReader) === 0)
      releaseReader(firstReader);
    if (secondReader !== undefined && !secondTransferred && releaseCount(secondReader) === 0)
      releaseReader(secondReader);
    context.dispose();
    sourceGraph.dispose();
    targetGraph.dispose();
  }
});
