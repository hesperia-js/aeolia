import { expect, jest, test } from "bun:test";
import {
  adopt,
  affects,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createStream,
  createPositionedStream,
  capturePositionedStore,
  isPositionedStoreCheckpointCurrent,
  observePositionedStoreCaptureState,
  preparePositionedStoreRestore,
  positionedFoldReceipt,
  project,
  snapshot,
  subscribe,
  type PositionedStoreCaptureNotice,
  type Readable,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

async function flush() {
  for (let turn = 0; turn < 5; turn++) await Promise.resolve();
}

function fixture(idleMs?: number) {
  const backend = testBackend();
  const stream = createStream({
    name: "values",
    key: (key: string) => `values/${key}`,
    open: backend.stream<string, number | undefined>("values"),
  });
  const graph = createGraph({
    contract: { namespace: "presence", operations: { stream } },
    idleMs,
  });
  return { backend, stream, graph };
}

function waitForCaptureNotice(
  readable: Readable<PositionedStoreCaptureNotice>,
  matches: (notice: PositionedStoreCaptureNotice) => boolean,
): Promise<PositionedStoreCaptureNotice> {
  let stop: (() => void) | undefined;
  const pending = new Promise<PositionedStoreCaptureNotice>((resolve) => {
    stop = subscribe(readable, (notice) => {
      if (matches(notice)) resolve(notice);
    });
  });
  return pending.finally(() => stop?.());
}

function waitForCaptureChange(
  readable: Readable<PositionedStoreCaptureNotice>,
  afterRevision: object,
): Promise<Extract<PositionedStoreCaptureNotice, { readonly kind: "changed" }>> {
  return waitForCaptureNotice(
    readable,
    (notice) => notice.kind !== "changed" || notice.revision !== afterRevision,
  ).then((notice) => {
    if (notice.kind !== "changed") throw new Error("The positioned capture became terminal");
    return notice;
  });
}

test("store subscription skips absence but delivers a real undefined and retained values", async () => {
  const { backend, graph } = fixture();
  const store = graph.api.stream("one");
  const values: Array<number | undefined> = [];
  const raw: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  const stopRaw = subscribe(store.value, (value) => raw.push(value));
  try {
    expect(values).toStrictEqual([]);
    expect(raw).toStrictEqual([undefined]);
    backend.emit("values", undefined);
    await flush();
    expect(values).toStrictEqual([undefined]);
    backend.emit("values", 1);
    await flush();
    backend.emit("values", 1);
    await flush();
    expect(values).toStrictEqual([undefined, 1]);
    const late: Array<number | undefined> = [];
    const stopLate = subscribe(store, (value) => late.push(value));
    expect(late).toStrictEqual([1]);
    stopLate();
    stop();
    backend.emit("values", 2);
    await flush();
    expect(values).toStrictEqual([undefined, 1]);
  } finally {
    stop();
    stopRaw();
    graph.dispose();
  }
});

test("explicit initial undefined is present even while the stream is waiting", () => {
  const { graph } = fixture();
  const store = graph.api.stream("seed", { initial: undefined });
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  expect(values).toStrictEqual([undefined]);
  expect(store.pending.get()).toBe(true);
  stop();
  graph.dispose();
});

test("closing or failing a stream without an emission does not fabricate a value", async () => {
  for (const reason of [undefined, new Error("source failed")]) {
    const { backend, graph } = fixture();
    const store = graph.api.stream("empty");
    const values: Array<number | undefined> = [];
    const stop = subscribe(store, (value) => values.push(value));
    backend.end("values", reason);
    await flush();
    expect(values).toStrictEqual([]);
    expect(store.pending.get()).toBe(false);
    expect(store.status.get()).toBe(reason === undefined ? "closed" : "failed");
    stop();
    graph.dispose();
  }
});

test("query results and direct writes establish presence without placeholder callbacks", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "get",
    key: () => "query",
    fetch: backend.respond<void, number | undefined>("get"),
  });
  const local = createStore({ name: "local", initial: undefined as number | undefined });
  const graph = createGraph({ contract: { namespace: "presence", operations: { query, local } } });
  const store = graph.api.query(undefined);
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  const localValues: Array<number | undefined> = [];
  const stopLocal = subscribe(graph.api.local(), (value) => localValues.push(value));
  try {
    expect(localValues).toStrictEqual([undefined]);
    expect(values).toStrictEqual([]);
    const ready = store.ready;
    backend.resolve(backend.calls[0]!, undefined);
    await ready;
    expect(values).toStrictEqual([undefined]);
    store.set(2);
    store.set(2);
    expect(values).toStrictEqual([undefined, 2]);
  } finally {
    stop();
    stopLocal();
    graph.dispose();
  }
});

test("adoption establishes presence for a held empty stream store", () => {
  const source = fixture();
  const seeded = source.graph.api.stream("adopt", { initial: undefined, snapshot: true });
  const encoded = snapshot(source.graph);
  const target = fixture();
  const store = target.graph.api.stream("adopt", { snapshot: true });
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  expect(values).toStrictEqual([]);
  adopt(target.graph, encoded);
  expect(values).toStrictEqual([undefined]);
  expect(seeded.value.get()).toBeUndefined();
  stop();
  target.graph.dispose();
  source.graph.dispose();
});

test("projections retain seeds and consume repeated source events independently of store equality", async () => {
  const { graph, backend, stream } = fixture();
  const accumulation = project(graph, stream, "projection", { kind: "accumulate", max: 5 });
  const reduction = project(graph, stream, "projection", {
    kind: "reduce",
    initial: 0,
    step: (count) => count + 1,
  });
  const arrays: Array<readonly (number | undefined)[]> = [];
  const counts: number[] = [];
  const stopArray = subscribe(accumulation, (value) => arrays.push(value));
  const stopCount = subscribe(reduction, (value) => counts.push(value));
  try {
    expect(arrays).toStrictEqual([[]]);
    expect(counts).toEqual([0]);
    backend.emit("values", 1);
    await flush();
    backend.emit("values", 1);
    await flush();
    expect(arrays).toStrictEqual([[], [1], [1, 1]]);
    expect(counts).toEqual([0, 1, 2]);
  } finally {
    stopArray();
    stopCount();
    graph.dispose();
  }
});

test("store subscription respects forced same-reference changes and callback writes", () => {
  const item = { count: 0 };
  const local = createStore({ name: "item", initial: item, equals: () => false });
  const graph = createGraph({ contract: { namespace: "presence", operations: { local } } });
  const store = graph.api.local();
  const counts: number[] = [];
  const stop = subscribe(store, (value) => {
    counts.push(value.count);
    if (value.count === 1) store.set({ count: 2 });
  });
  item.count = 1;
  store.set(item);
  expect(counts).toEqual([0, 1, 2]);
  stop();
  store.set({ count: 3 });
  expect(counts).toEqual([0, 1, 2]);
  graph.dispose();
});

test("an empty held store can become present through a direct undefined write", () => {
  const { graph } = fixture();
  const store = graph.api.stream("direct");
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  expect(values).toStrictEqual([]);
  store.set(undefined);
  expect(values).toStrictEqual([undefined]);
  store.set(undefined);
  expect(values).toStrictEqual([undefined]);
  stop();
  graph.dispose();
});

test("store subscriptions retain live streams and release them after unsubscribe or setup failure", async () => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
  const { graph, backend } = fixture(1);
  try {
    const store = graph.api.stream("live");
    const stop = subscribe(store, () => undefined);
    await flush();
    jest.advanceTimersByTime(5);
    expect(backend.calls[0]!.aborted).toBe(false);
    stop();
    await flush();
    jest.advanceTimersByTime(1);
    expect(backend.calls[0]!.aborted).toBe(true);

    const seeded = graph.api.stream("throwing", { initial: 3 });
    const failure = new Error("observer failed");
    expect(() =>
      subscribe(seeded, () => {
        throw failure;
      }),
    ).toThrow(failure);
    await flush();
    jest.advanceTimersByTime(2);
    expect(backend.calls[1]!.aborted).toBe(true);
  } finally {
    graph.dispose();
    jest.clearAllTimers();
    jest.useRealTimers();
  }
});

test("an optimistic value is observable before the first committed query result", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "get",
    key: () => "item",
    fetch: backend.respond<void, number>("get"),
  });
  const mutation = createMutation({
    name: "set",
    run: backend.perform<number, void>("set"),
    affects: [
      affects(query, {
        select: (_value: number) => undefined,
        on: "revalidate",
        optimistic: (_old, value) => value,
      }),
    ],
  });
  const graph = createGraph({
    contract: { namespace: "presence", operations: { query, mutation } },
  });
  const store = graph.api.query(undefined);
  const values: number[] = [];
  const stop = subscribe(store, (value) => values.push(value));
  try {
    const changed = graph.api.mutation(42);
    expect(values).toStrictEqual([42]);
    backend.resolve(backend.calls[1]!, undefined);
    await changed;
    const ready = store.ready;
    backend.resolve(backend.calls[2]!, 42);
    expect(await ready).toBe(42);
    expect(values).toStrictEqual([42]);
  } finally {
    stop();
    graph.dispose();
  }
});

test("a dormant restored Store keeps present undefined and direct writes do not fold events", async () => {
  type Input = { readonly room: string };
  type Event = { position: number; value: number | undefined };
  type Reservation = { input: Input; after: number; seed: unknown };
  const reservations = new WeakMap<object, Reservation>();
  const liveEmitters = new Map<string, (event: Event) => void>();
  const releases = new WeakMap<object, number>();
  const openings: Array<{
    reader: object | undefined;
    after: number | undefined;
    hasAfter: boolean;
  }> = [];

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

  const stream = createPositionedStream<Input, number | undefined, string, number, object>({
    name: "undefined-positioned-store",
    key: (input) => input.room,
    source: {
      open: (input, options) => {
        const channel = makeChannel();
        openings.push({
          reader: options.reader,
          after: options.after,
          hasAfter: Object.hasOwn(options, "after"),
        });
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
  const makeGraph = (namespace: string) =>
    createGraph({ contract: { namespace, operations: { events: stream } } });
  const sourceGraph = makeGraph("undefined-positioned-source");
  const targetGraph = makeGraph("undefined-positioned-target");
  const input = Object.freeze({ room: "one" });
  const source = sourceGraph.api.events(input);
  const projection = project(source, {
    kind: "reduce",
    initial: 0,
    step: (count: number) => count + 1,
  });
  const sourceValues: Array<number | undefined> = [];
  const stopSource = subscribe(source, (value) => sourceValues.push(value));
  let stopRestored: (() => void) | undefined;
  let releaseRestoration: (() => void) | undefined;
  let reader: object | undefined;
  let readerTransferred = false;

  try {
    const folded = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(projection.value, (value) => {
        if (value !== 1) return;
        stop();
        resolve();
      });
    });
    const emit = liveEmitters.get(input.room);
    if (emit === undefined) throw new Error("The actual positioned source did not open");
    emit({ position: 5, value: undefined });
    await folded;

    const checkpoint = capturePositionedStore(source, [projection] as const);
    if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
      throw new Error("The real undefined event did not produce a Store capture");
    const after: number = checkpoint.position;
    expect(Object.hasOwn(checkpoint, "latest")).toBe(true);
    expect(checkpoint.latest).toBeUndefined();
    expect(checkpoint.values).toStrictEqual([1]);
    expect(source.available.get()).toBe(true);
    expect(source.value.get()).toBeUndefined();
    expect(sourceValues).toStrictEqual([undefined]);

    const seed = { after, latest: checkpoint.latest, values: checkpoint.values };
    expect(Object.hasOwn(seed, "latest")).toBe(true);
    reader = reserve(input, after, seed);
    const restoration = preparePositionedStoreRestore(
      targetGraph.api.events,
      input,
      [
        {
          kind: "reduce",
          initial: 0,
          step: (count: number) => count + 1,
        },
      ] as const,
      seed,
      reader,
    );
    readerTransferred = true;
    releaseRestoration = () => restoration.discard();

    expect(openings).toHaveLength(1);
    expect(restoration.source.available.get()).toBe(true);
    expect(restoration.source.value.get()).toBeUndefined();
    expect(restoration.source.status.get()).toBe("empty");
    expect(restoration.projections[0].value.get()).toBe(1);
    const restoredValues: Array<number | undefined> = [];
    stopRestored = subscribe(restoration.source, (value) => restoredValues.push(value));
    expect(restoredValues).toStrictEqual([undefined]);
    expect(openings).toHaveLength(1);

    expect(restoration.activate()).toBe(restoration.source);
    expect(openings).toHaveLength(2);
    expect(openings[1]).toMatchObject({ hasAfter: true, after: 5, reader });

    const receipt = positionedFoldReceipt(projection);
    if (receipt === undefined) throw new Error("The real source fold did not issue a receipt");
    expect(receipt.position).toBe(5);
    expect(isPositionedStoreCheckpointCurrent(checkpoint)).toBe(true);
    source.set(7);
    source.update((current) => (current ?? 0) + 1);
    expect(source.value.get()).toBe(8);
    expect(source.available.get()).toBe(true);
    expect(sourceValues).toStrictEqual([undefined, 7, 8]);
    expect(projection.value.get()).toBe(1);
    expect(positionedFoldReceipt(projection)).toBe(receipt);
    expect(isPositionedStoreCheckpointCurrent(checkpoint)).toBe(false);

    restoration.discard();
    releaseRestoration = undefined;
    expect(releaseCount(reader)).toBe(1);
  } finally {
    stopSource();
    stopRestored?.();
    releaseRestoration?.();
    if (reader !== undefined && !readerTransferred && releaseCount(reader) === 0)
      releaseReader(reader);
    projection.close();
    sourceGraph.dispose();
    targetGraph.dispose();
  }
});

test("capture notices track equal events and prediction retirement without source events", async () => {
  type Input = { readonly room: string };
  type Event = { position: number; value: number };
  const backend = testBackend();
  let emitLive: ((event: Event) => void) | undefined;
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
  const stream = createPositionedStream<Input, number, string, number, object>({
    name: "predicted-positioned-store",
    key: (input) => input.room,
    source: {
      open: () => {
        emitLive = emit;
        return iterable;
      },
      validateReader: () => false,
      isReaderCurrent: () => false,
      acknowledge: () => undefined,
      releaseReader: () => undefined,
    },
  });
  const query = createQuery({
    name: "same-key-query-target",
    key: (input: Input) => input.room,
    fetch: backend.respond<Input, number>("unused-query"),
  });
  const mutation = createMutation({
    name: "equal-prediction",
    run: backend.perform<Input, void>("equal-prediction"),
    affects: [
      affects(query, {
        select: (input: Input) => input,
        on: "revalidate",
        optimistic: () => 5,
      }),
    ],
  });
  const graph = createGraph({
    contract: {
      namespace: "predicted-positioned-store",
      operations: { events: stream, query, mutation },
    },
  });
  const input = Object.freeze({ room: "one" });
  const source = graph.api.events(input);
  const projection = project(source, {
    kind: "reduce",
    initial: 0,
    step: (sum: number, event: number) => sum + event,
  });
  const sourceValues: number[] = [];
  const stopSource = subscribe(source, (value) => sourceValues.push(value));
  let stopCaptureChanges: (() => void) | undefined;

  try {
    const folded = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(projection.value, (value) => {
        if (value !== 5) return;
        stop();
        resolve();
      });
    });
    if (emitLive === undefined) throw new Error("The positioned source did not open");
    const liveEmitter = emitLive;
    liveEmitter({ position: 5, value: 5 });
    await folded;

    const original = capturePositionedStore(source, [projection] as const);
    if (!original.present || !original.hasPosition || typeof original.position !== "number")
      throw new Error("The source did not capture its real event");
    expect(original.resumable).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(original)).toBe(true);
    expect(source.value.get()).toBe(5);
    expect(sourceValues).toStrictEqual([5]);

    const captureChanges = observePositionedStoreCaptureState(original);
    const captureNotices: PositionedStoreCaptureNotice[] = [];
    stopCaptureChanges = subscribe(captureChanges, (notice) => captureNotices.push(notice));
    const initialNotice = captureNotices[0];
    if (initialNotice?.kind !== "changed")
      throw new Error("The authentic capture did not produce its synchronous changed seed");

    const equalEventChange = waitForCaptureChange(captureChanges, initialNotice.revision);
    const secondFold = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(projection.value, (value) => {
        if (value !== 10) return;
        stop();
        resolve();
      });
    });
    liveEmitter({ position: 6, value: 5 });
    const equalNotice = await equalEventChange;
    await secondFold;
    expect(source.value.get()).toBe(5);
    expect(sourceValues).toStrictEqual([5]);
    expect(projection.value.get()).toBe(10);
    const equalEventCut = capturePositionedStore(source, [projection] as const);
    if (!equalEventCut.present) throw new Error("The equal real source Event is missing");
    expect(equalEventCut.latest).toBe(5);
    expect(equalEventCut.position).toBe(6);
    expect(equalEventCut.values).toStrictEqual([10]);
    expect(equalEventCut.resumable).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(equalEventCut)).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(original)).toBe(false);

    const predictionChange = waitForCaptureChange(captureChanges, equalNotice.revision);
    const mutationResult = graph.api.mutation(input);
    expect(source.value.get()).toBe(5);
    expect(projection.value.get()).toBe(10);
    expect(sourceValues).toStrictEqual([5]);
    const predictionNotice = await predictionChange;
    expect(isPositionedStoreCheckpointCurrent(equalEventCut)).toBe(false);
    const predicted = capturePositionedStore(source, [projection] as const);
    if (!predicted.present)
      throw new Error("The real source Event is missing from the prediction cut");
    expect(predicted.latest).toBe(5);
    expect(predicted.position).toBe(6);
    expect(predicted.values).toStrictEqual([10]);
    expect(predicted.resumable).toBe(false);

    const pendingAction = backend.pending("equal-prediction")[0];
    if (pendingAction === undefined) throw new Error("The real mutation callback is not pending");
    const predictionFailure = new Error("prediction rejected without a source event");
    const predictionRetired = waitForCaptureChange(captureChanges, predictionNotice.revision);
    backend.reject(pendingAction, predictionFailure);
    await Promise.all([expect(mutationResult).rejects.toBe(predictionFailure), predictionRetired]);

    expect(source.value.get()).toBe(5);
    expect(projection.value.get()).toBe(10);
    expect(sourceValues).toStrictEqual([5]);
    const retiredWithoutEvent = capturePositionedStore(source, [projection] as const);
    if (!retiredWithoutEvent.present) throw new Error("The retained source Event is missing");
    expect(retiredWithoutEvent.latest).toBe(5);
    expect(retiredWithoutEvent.position).toBe(6);
    expect(retiredWithoutEvent.values).toStrictEqual([10]);
    expect(retiredWithoutEvent.resumable).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(retiredWithoutEvent)).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(predicted)).toBe(false);

    const secondPredictionChange = waitForCaptureChange(captureChanges, predictionRetired.revision);
    const secondMutation = graph.api.mutation(input);
    const secondPredictionNotice = await secondPredictionChange;
    expect(source.value.get()).toBe(5);
    expect(projection.value.get()).toBe(10);
    const secondPendingAction = backend.pending("equal-prediction")[0];
    if (secondPendingAction === undefined)
      throw new Error("The second mutation callback is not pending");

    const eventChange = waitForCaptureChange(captureChanges, secondPredictionNotice.revision);
    const thirdFold = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(projection.value, (value) => {
        if (value !== 12) return;
        stop();
        resolve();
      });
    });
    liveEmitter({ position: 7, value: 2 });
    const eventNotice = await eventChange;
    await thirdFold;
    expect(source.value.get()).toBe(5);
    expect(sourceValues).toStrictEqual([5]);
    expect(projection.value.get()).toBe(12);
    const liveOverlay = capturePositionedStore(source, [projection] as const);
    if (!liveOverlay.present) throw new Error("The second real source Event is missing");
    expect(liveOverlay.latest).toBe(2);
    expect(liveOverlay.position).toBe(7);
    expect(liveOverlay.values).toStrictEqual([12]);
    expect(liveOverlay.resumable).toBe(false);

    const livePredictionRetired = waitForCaptureChange(captureChanges, eventNotice.revision);
    const eventFailure = new Error("event overlay rejected");
    backend.reject(secondPendingAction, eventFailure);
    await Promise.all([expect(secondMutation).rejects.toBe(eventFailure), livePredictionRetired]);

    expect(source.value.get()).toBe(2);
    expect(sourceValues).toStrictEqual([5, 2]);
    expect(projection.value.get()).toBe(12);
    const reconciled = capturePositionedStore(source, [projection] as const);
    if (!reconciled.present) throw new Error("The reconciled source Event is missing");
    expect(reconciled.latest).toBe(2);
    expect(reconciled.position).toBe(7);
    expect(reconciled.values).toStrictEqual([12]);
    expect(reconciled.resumable).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(reconciled)).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(liveOverlay)).toBe(false);
    expect(isPositionedStoreCheckpointCurrent(original)).toBe(false);
    expect(isPositionedStoreCheckpointCurrent(equalEventCut)).toBe(false);
    expect(isPositionedStoreCheckpointCurrent(retiredWithoutEvent)).toBe(false);

    stopCaptureChanges?.();
    stopCaptureChanges = undefined;
    const terminalState = observePositionedStoreCaptureState(reconciled);
    const invalidated = waitForCaptureNotice(
      terminalState,
      (notice) => notice.kind === "invalidated",
    );
    projection.close();
    expect(await invalidated).toStrictEqual({ kind: "invalidated" });
    expect(source.status.get()).toBe("live");
  } finally {
    stopCaptureChanges?.();
    stopSource();
    projection.close();
    graph.dispose();
  }
});

test("an empty store rejects a non-callable observer at registration", () => {
  const { graph } = fixture();
  try {
    const store = graph.api.stream("invalid-observer");
    expect(() => subscribe(store, null as never)).toThrow(TypeError);
  } finally {
    graph.dispose();
  }
});
