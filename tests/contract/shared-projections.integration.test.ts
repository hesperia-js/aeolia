import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  bindStream,
  captureReactiveConstruction,
  computed,
  createBindingContext,
  createGraph,
  createStream,
  createPositionedStream,
  defineContract,
  project,
  subscribe,
  capturePositionedProjection,
  capturePositionedStore,
  isPositionedCheckpointCurrent,
  isPositionedStoreCheckpointCurrent,
  preparePositionedStoreRestore,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
  positionedFoldReceipt,
  acknowledgePositionedFold,
  type PositionedProjectionCheckpoint,
  type Readable,
  type Projection,
  type PositionedStoreRestoration,
  type PositionedFoldReceipt,
  type StreamStore,
  type PositionedStreamSource,
  type RequiredTransferCodec,
  type ClosedEncodedValue,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

const providerClockNow = Date.now;

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});
afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function until<T>(readable: Readable<T>, matches: (value: T) => boolean): Promise<void> {
  const ready = Promise.withResolvers<void>();
  const stop = subscribe(readable, (value) => {
    if (matches(value)) ready.resolve();
  });
  return ready.promise.finally(stop);
}

function setup() {
  const backend = testBackend();
  const stream = createStream({
    name: "shared-events",
    key: () => "shared-events/one",
    open: backend.stream<void, number>("shared-events"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "shared-projections", operations: { stream } }),
    idleMs: 10,
  });
  return { backend, graph, source: graph.api.stream() };
}

it("delivers shared close before nested cancellation without replacing the outer source reason", async () => {
  const backend = testBackend();
  const open = backend.stream<void, number>("nested-cancel");
  let sourceSignal: AbortSignal | undefined;
  let returns = 0;
  const stream = createStream({
    name: "nested-cancel",
    key: () => "nested-cancel/one",
    open: (input: void, options): AsyncIterable<number> => {
      sourceSignal = options.abortSignal;
      const iterable = open(input, options);
      return {
        [Symbol.asyncIterator]() {
          const iterator = iterable[Symbol.asyncIterator]();
          return {
            next: () => iterator.next(),
            return: () => {
              returns += 1;
              return iterator.return!();
            },
          };
        },
      };
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "nested-cancel", operations: { stream } }),
  });
  const source = graph.api.stream(undefined);
  const history = project(source, { kind: "accumulate", max: 3 });
  const outerReason = new Error("outer cancellation");
  const innerReason = new Error("nested cancellation");
  let nestedCalls = 0;
  const stop = subscribe(history.status, (status) => {
    if (status !== "closed") return;
    nestedCalls += 1;
    graph.cancelPending(innerReason);
  });
  try {
    const received = until(history.value, (items) => items.length === 1);
    backend.emit("nested-cancel", 5);
    await received;
    graph.cancelPending(outerReason);
    expect(nestedCalls).toBe(1);
    expect(sourceSignal!.reason).toBe(outerReason);
    expect(returns).toBe(1);
    expect(source.status.get()).toBe("closed");
    expect(history.status.get()).toBe("closed");
    expect(history.error.get()).toBeUndefined();
    expect(history.value.get()).toEqual([5]);
    expect(source.value.get()).toBe(5);
    graph.cancelPending();
    expect(nestedCalls).toBe(1);
    expect(returns).toBe(1);
  } finally {
    stop();
    graph.dispose();
  }
});

it("projects every shared emission without reopening, replaying, or observing direct writes", async () => {
  const { backend, graph, source } = setup();
  try {
    const history = project(source, { kind: "accumulate", max: 10 });
    const count = project(source, { kind: "reduce", initial: 0, step: (n) => n + 1 });
    expect(backend.calls).toHaveLength(1);
    const first = until(history.value, (items) => items.length === 1);
    backend.emit("shared-events", 4);
    await first;
    const joined = project(source, { kind: "accumulate", max: 10 });
    expect(joined.value.get()).toStrictEqual([]);
    const second = until(history.value, (items) => items.length === 2);
    backend.emit("shared-events", 4);
    await second;
    expect(history.value.get()).toStrictEqual([4, 4]);
    expect(joined.value.get()).toStrictEqual([4]);
    expect(count.value.get()).toBe(2);
    source.set(99);
    expect(history.value.get()).toStrictEqual([4, 4]);
    history.close();
    expect(backend.calls[0]!.aborted).toBe(false);
    const third = until(joined.value, (items) => items.length === 2);
    backend.emit("shared-events", 5);
    await third;
    expect(history.value.get()).toStrictEqual([4, 4]);
    expect(joined.value.get()).toStrictEqual([4, 5]);
    expect(count.value.get()).toBe(3);
    expect(backend.calls).toHaveLength(1);
  } finally {
    graph.dispose();
  }
});

it("starts a positioned shared projection at the next event when it joins during delivery", async () => {
  const backend = testBackend();
  const stream = createPositionedStream<void, number, string, number, object>({
    name: "positioned-shared-join",
    key: () => "positioned-shared-join/one",
    source: {
      open: backend.stream<void, { position: number; value: number }>("positioned-shared-join"),
      validateReader: () => true,
      isReaderCurrent: () => true,
      acknowledge: () => undefined,
      releaseReader: () => undefined,
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "positioned-shared-join", operations: { stream } }),
  });
  const source = graph.api.stream();
  const policy = {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, item: number) => sum + item,
  };
  const first = project(source, policy);
  let late: Projection<number> | undefined;
  let lateAtJoin: PositionedProjectionCheckpoint<number, number> | undefined;
  const stop = subscribe(first.value, (value) => {
    if (value !== 1 || late !== undefined) return;
    late = project(source, policy);
    lateAtJoin = capturePositionedProjection<number, number>(late);
  });
  try {
    const firstDelivered = until(first.value, (value) => value === 1);
    backend.emit("positioned-shared-join", { position: 5, value: 1 });
    await firstDelivered;
    expect(lateAtJoin).toMatchObject({
      value: 0,
      hasPosition: false,
      position: undefined,
      revision: 0,
    });
    if (late === undefined || lateAtJoin === undefined)
      throw new Error("The projection did not join during delivery");
    expect(isPositionedCheckpointCurrent(lateAtJoin)).toBe(true);
    expect(capturePositionedProjection<number, number>(first)).toMatchObject({
      value: 1,
      hasPosition: true,
      position: 5,
      revision: 1,
    });
    const secondDelivered = until(late.value, (value) => value === 2);
    backend.emit("positioned-shared-join", { position: 6, value: 2 });
    await secondDelivered;
    expect(first.value.get()).toBe(3);
    expect(late.value.get()).toBe(2);
    expect(capturePositionedProjection<number, number>(late)).toMatchObject({
      value: 2,
      hasPosition: true,
      position: 6,
      revision: 1,
    });
    expect(isPositionedCheckpointCurrent(lateAtJoin)).toBe(false);
    expect(backend.calls).toHaveLength(1);
  } finally {
    stop();
    graph.dispose();
  }
});

it("restores shared folds over one dormant source Store without replaying the latest event", async () => {
  type Event = { position: number; value: number };
  const reservations = new WeakMap<object, { after: number; seed: unknown }>();
  const readerEmitters = new WeakMap<object, (event: Event) => void>();
  const releases = new WeakMap<object, number>();
  const acknowledgements: Array<{ reader: object; receipt: PositionedFoldReceipt<number> }> = [];
  const openings: Array<{
    hasAfter: boolean;
    after: number | undefined;
    hasReader: boolean;
    reader: object | undefined;
    reportGap: () => void;
  }> = [];
  let emitLive: ((event: Event) => void) | undefined;

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

  const reserve = (after: number, seed: unknown): object => {
    const reader = Object.freeze({});
    reservations.set(reader, { after, seed });
    return reader;
  };
  const releaseCount = (reader: object): number => releases.get(reader) ?? 0;
  const releaseReader = (reader: object): void => {
    releases.set(reader, releaseCount(reader) + 1);
  };
  const stream = createPositionedStream<void, number, string, number, object>({
    name: "restored-shared-store",
    key: () => "restored-shared-store/one",
    source: {
      open: (_input, options) => {
        const channel = makeChannel();
        openings.push({
          hasAfter: Object.hasOwn(options, "after"),
          after: options.after,
          hasReader: Object.hasOwn(options, "reader"),
          reader: options.reader,
          reportGap: options.reportGap,
        });
        if (options.reader === undefined) emitLive = channel.emit;
        else readerEmitters.set(options.reader, channel.emit);
        return channel.iterable;
      },
      validateReader: (_input, reader, after, checkpoint, seed) => {
        const reservation = reservations.get(reader);
        return (
          checkpoint === undefined &&
          reservation !== undefined &&
          reservation.after === after &&
          reservation.seed === seed
        );
      },
      isReaderCurrent: (_input, reader, after, seed) => {
        const reservation = reservations.get(reader);
        return (
          reservation !== undefined && reservation.after === after && reservation.seed === seed
        );
      },
      acknowledge: (_input, reader, receipt) => {
        acknowledgements.push({ reader, receipt });
      },
      releaseReader: (_input, reader) => releaseReader(reader),
    },
  });
  const makeGraph = (namespace: string) =>
    createGraph({ contract: defineContract({ namespace, operations: { events: stream } }) });
  const sourceGraph = makeGraph("restored-shared-source");
  const targetGraph = makeGraph("restored-shared-target");
  const source = sourceGraph.api.events();
  const historyPolicy = { kind: "accumulate" as const, max: 10 };
  const totalPolicy = {
    kind: "reduce" as const,
    initial: 10,
    step: (total: number, event: number) => total + event,
  };
  const equalPolicy = {
    kind: "reduce" as const,
    initial: 7,
    step: () => 7,
  };
  const history = project(source, historyPolicy);
  const total = project(source, totalPolicy);
  const unchanged = project(source, equalPolicy);
  let stopRestoredSource: (() => void) | undefined;
  let stopEarlyAcknowledge: (() => void) | undefined;
  let restoredHistory: Projection<readonly number[]> | undefined;
  let restoredTotal: Projection<number> | undefined;
  let restoredEqual: Projection<number> | undefined;
  let later: Projection<number> | undefined;
  let restoration:
    | PositionedStoreRestoration<number, readonly [readonly number[], number, number]>
    | undefined;
  let reader: object | undefined;
  let readerTransferred = false;
  let gapReader: object | undefined;
  let gapReaderTransferred = false;
  let discardGapRestoration: (() => void) | undefined;

  try {
    const folded = until(total.value, (value) => value === 14);
    if (emitLive === undefined) throw new Error("The actual source Store did not open");
    emitLive({ position: 5, value: 4 });
    await folded;

    const checkpoint = capturePositionedStore(source, [history, total, unchanged] as const);
    if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
      throw new Error("The actual source Store did not capture a positioned event");
    const after: number = checkpoint.position;
    expect(checkpoint.sourceIdentity).toBe(source.identity);
    expect(checkpoint.latest).toBe(4);
    expect(checkpoint.values).toStrictEqual([[4], 14, 7]);
    expect(checkpoint.resumable).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(checkpoint)).toBe(true);

    const seed = { after, latest: checkpoint.latest, values: checkpoint.values };
    const readerForRestore = reserve(after, seed);
    reader = readerForRestore;
    const prepared = preparePositionedStoreRestore(
      targetGraph.api.events,
      undefined,
      [historyPolicy, totalPolicy, equalPolicy] as const,
      seed,
      readerForRestore,
    );
    restoration = prepared;
    readerTransferred = true;
    const restoredHistoryProjection = prepared.projections[0];
    const restoredTotalProjection = prepared.projections[1];
    const restoredEqualProjection = prepared.projections[2];
    restoredHistory = restoredHistoryProjection;
    restoredTotal = restoredTotalProjection;
    restoredEqual = restoredEqualProjection;

    expect(openings).toHaveLength(1);
    expect(prepared.source.available.get()).toBe(true);
    expect(prepared.source.value.get()).toBe(4);
    expect(prepared.source.status.get()).toBe("empty");
    expect(restoredHistoryProjection.value.get()).toStrictEqual([4]);
    expect(restoredTotalProjection.value.get()).toBe(14);
    expect(restoredEqualProjection.value.get()).toBe(7);
    const sourceValues: number[] = [];
    stopRestoredSource = subscribe(prepared.source, (value) => sourceValues.push(value));
    const laterProjection = project(prepared.source, {
      kind: "reduce",
      initial: 0,
      step: (sum: number, event: number) => sum + event,
    });
    later = laterProjection;
    expect(laterProjection.value.get()).toBe(0);
    expect(sourceValues).toEqual([4]);
    expect(openings).toHaveLength(1);

    let earlyAcknowledgeAttempted = false;
    let earlyAcknowledgeCount: number | undefined;
    let earlyReceiptAvailable = false;
    let earlyAcknowledgeError: unknown;
    stopEarlyAcknowledge = subscribe(restoredHistoryProjection.value, (items) => {
      if (items.length !== 2 || earlyAcknowledgeAttempted) return;
      earlyAcknowledgeAttempted = true;
      const receipt = positionedFoldReceipt(restoredHistoryProjection);
      if (receipt === undefined) return;
      earlyReceiptAvailable = true;
      try {
        acknowledgePositionedFold(restoredHistoryProjection, receipt);
      } catch (error) {
        earlyAcknowledgeError = error;
      }
      earlyAcknowledgeCount = acknowledgements.length;
    });

    const activated = prepared.activate();
    expect(activated).toBe(prepared.source);
    expect(openings).toHaveLength(2);
    expect(openings[1]).toMatchObject({
      hasAfter: true,
      after: 5,
      hasReader: true,
      reader: readerForRestore,
    });

    const sourceSuffix = until(prepared.source.value, (value) => value === 3);
    const historySuffix = until(restoredHistoryProjection.value, (value) => value.length === 2);
    const totalSuffix = until(restoredTotalProjection.value, (value) => value === 17);
    const laterSuffix = until(laterProjection.value, (value) => value === 3);
    const emitReader = readerEmitters.get(readerForRestore);
    if (emitReader === undefined) throw new Error("The actual reader opening is missing");
    emitReader({ position: 6, value: 3 });
    await Promise.all([sourceSuffix, historySuffix, totalSuffix, laterSuffix]);

    expect(prepared.source.value.get()).toBe(3);
    expect(restoredHistoryProjection.value.get()).toStrictEqual([4, 3]);
    expect(restoredTotalProjection.value.get()).toBe(17);
    expect(restoredEqualProjection.value.get()).toBe(7);
    expect(laterProjection.value.get()).toBe(3);
    expect(sourceValues).toEqual([4, 3]);
    expect(earlyAcknowledgeAttempted).toBe(true);
    expect(earlyReceiptAvailable).toBe(true);
    expect(earlyAcknowledgeCount).toBe(0);
    expect(acknowledgements).toHaveLength(0);
    if (earlyAcknowledgeError !== undefined && !(earlyAcknowledgeError instanceof TypeError))
      throw earlyAcknowledgeError;

    stopEarlyAcknowledge?.();
    stopEarlyAcknowledge = undefined;
    restoredHistoryProjection.close();
    expect(openings).toHaveLength(2);
    expect(releaseCount(readerForRestore)).toBe(0);

    const secondSourceSuffix = until(prepared.source.value, (value) => value === 6);
    const secondTotalSuffix = until(restoredTotalProjection.value, (value) => value === 23);
    const secondLaterSuffix = until(laterProjection.value, (value) => value === 9);
    emitReader({ position: 7, value: 6 });
    await Promise.all([secondSourceSuffix, secondTotalSuffix, secondLaterSuffix]);

    expect(prepared.source.value.get()).toBe(6);
    expect(restoredHistoryProjection.value.get()).toStrictEqual([4, 3]);
    expect(restoredTotalProjection.value.get()).toBe(23);
    expect(restoredEqualProjection.value.get()).toBe(7);
    expect(laterProjection.value.get()).toBe(9);
    expect(openings).toHaveLength(2);
    expect(releaseCount(reader)).toBe(0);

    const finalReceipt = positionedFoldReceipt(restoredEqualProjection);
    if (finalReceipt === undefined) throw new Error("The completed equal fold has no receipt");
    expect(finalReceipt.position).toBe(7);
    acknowledgePositionedFold(restoredEqualProjection, finalReceipt);
    expect(acknowledgements).toHaveLength(1);
    const finalAcknowledgement = acknowledgements[0];
    if (finalAcknowledgement === undefined)
      throw new Error("The completed equal fold was not acknowledged");
    expect(finalAcknowledgement.reader).toBe(reader);
    expect(finalReceipt).toBe(finalAcknowledgement.receipt);

    prepared.discard();
    restoration = undefined;
    expect(releaseCount(readerForRestore)).toBe(1);

    const gapReaderForRestore = reserve(after, seed);
    gapReader = gapReaderForRestore;
    const gapRestoration = preparePositionedStoreRestore(
      targetGraph.api.events,
      undefined,
      [historyPolicy, totalPolicy, equalPolicy] as const,
      seed,
      gapReaderForRestore,
    );
    gapReaderTransferred = true;
    discardGapRestoration = () => gapRestoration.discard();
    expect(openings).toHaveLength(2);
    const gapProjection = gapRestoration.projections[0];
    expect(gapProjection.value.get()).toStrictEqual([4]);
    expect(gapRestoration.activate()).toBe(gapRestoration.source);
    expect(openings).toHaveLength(3);
    const gapOpening = openings[2];
    if (gapOpening === undefined) throw new Error("The restored reader did not open");
    const gapReported = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(gapProjection.status, (status) => {
        if (status !== "failed") return;
        stop();
        resolve();
      });
    });
    gapOpening.reportGap();
    await gapReported;
    expect(gapRestoration.source.status.get()).toBe("failed");
    expect(gapProjection.status.get()).toBe("failed");
    expect(gapRestoration.source.value.get()).toBe(4);
    expect(releaseCount(gapReaderForRestore)).toBe(1);
    expect(openings).toHaveLength(3);
    gapRestoration.discard();
    discardGapRestoration = undefined;
    expect(releaseCount(gapReaderForRestore)).toBe(1);
    expect(openings).toHaveLength(3);
  } finally {
    stopRestoredSource?.();
    stopEarlyAcknowledge?.();
    history.close();
    total.close();
    unchanged.close();
    restoredHistory?.close();
    restoredTotal?.close();
    restoredEqual?.close();
    later?.close();
    restoration?.discard();
    discardGapRestoration?.();
    if (gapReader !== undefined && !gapReaderTransferred && releaseCount(gapReader) === 0)
      releaseReader(gapReader);
    if (reader !== undefined && !readerTransferred && releaseCount(reader) === 0)
      releaseReader(reader);
    sourceGraph.dispose();
    targetGraph.dispose();
  }
});

it("resumes a genuine initial boundary without inventing an Event or fold receipt", async () => {
  type Event = { readonly position: number; readonly value: number };
  type Reservation = {
    readonly input: void;
    readonly after: number;
    readonly seed: unknown;
    readonly controller: AbortController;
    readonly expiresAt: number;
  };
  type Opening = {
    readonly options: object;
    readonly reader: object | undefined;
  };
  const makeProvider = () => {
    const reservations = new WeakMap<object, Reservation>();
    const readerEmitters = new WeakMap<object, (event: Event) => void>();
    const releases = new WeakMap<object, number>();
    const openings: Opening[] = [];
    const boundaries: Array<{ readonly options: object; readonly after: number }> = [];
    const trace: string[] = [];
    const acknowledgements: Array<{
      readonly reader: object;
      readonly receipt: PositionedFoldReceipt<number>;
    }> = [];
    const makeChannel = () => {
      const waiters: Array<(result: IteratorResult<Event>) => void> = [];
      const iterable: AsyncIterable<Event> = {
        [Symbol.asyncIterator]() {
          return {
            next: () => {
              trace.push("pull");
              return new Promise<IteratorResult<Event>>((resolve) => waiters.push(resolve));
            },
            return: () => {
              trace.push("return");
              for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
              return Promise.resolve({ done: true as const, value: undefined });
            },
          };
        },
      };
      return {
        iterable,
        emit(event: Event) {
          const waiter = waiters.shift();
          if (waiter === undefined)
            throw new Error("The positioned reader has no pending next call");
          waiter({ done: false, value: event });
        },
      };
    };
    const source: PositionedStreamSource<void, number, number, object> = {
      open: (_input, options) => {
        trace.push("open");
        const channel = makeChannel();
        openings.push({ options, reader: options.reader });
        if (options.reader !== undefined) readerEmitters.set(options.reader, channel.emit);
        return channel.iterable;
      },
      initialBoundary: (_input, options) => {
        trace.push("boundary");
        const after = Object.hasOwn(options, "after") ? options.after : 5;
        if (typeof after !== "number")
          throw new Error("The genuine initial boundary was not numeric");
        boundaries.push({ options, after });
        return { available: true, after };
      },
      readerLifetime: (_input, reader, after, seed) => {
        const reservation = reservations.get(reader);
        if (
          reservation === undefined ||
          reservation.input !== _input ||
          reservation.after !== after ||
          reservation.seed !== seed
        )
          throw new TypeError("The provider lifetime does not match its exact reservation");
        return { signal: reservation.controller.signal, expiresAt: reservation.expiresAt };
      },
      validateReader: (_input, reader, after, _checkpoint, seed) => {
        const reservation = reservations.get(reader);
        return (
          reservation !== undefined && reservation.after === after && reservation.seed === seed
        );
      },
      isReaderCurrent: (_input, reader, after, seed) => {
        const reservation = reservations.get(reader);
        return (
          reservation !== undefined && reservation.after === after && reservation.seed === seed
        );
      },
      acknowledge: (_input, reader, receipt) => acknowledgements.push({ reader, receipt }),
      releaseReader: (_input, reader) => releases.set(reader, (releases.get(reader) ?? 0) + 1),
    };
    return {
      source,
      openings,
      boundaries,
      trace,
      acknowledgements,
      reserve(after: number, seed: unknown) {
        const reader = Object.freeze({});
        reservations.set(reader, {
          input: undefined,
          after,
          seed,
          controller: new AbortController(),
          expiresAt: providerClockNow() + 60_000,
        });
        return reader;
      },
      emit(reader: object, event: Event) {
        const emit = readerEmitters.get(reader);
        if (emit === undefined) throw new Error("The admitted positioned reader did not open");
        emit(event);
      },
      releaseCount(reader: object) {
        return releases.get(reader) ?? 0;
      },
    };
  };
  const providerA = makeProvider();
  const providerB = makeProvider();
  const definitionA = createPositionedStream<void, number, string, number, object>({
    name: "shared-initial-boundary",
    key: () => "shared-initial-boundary/one",
    snapshot: false,
    source: providerA.source,
  });
  const definitionB = createPositionedStream<void, number, string, number, object>({
    name: "shared-initial-boundary",
    key: () => "shared-initial-boundary/one",
    snapshot: false,
    source: providerB.source,
  });
  const namespace = "shared-initial-boundary";
  const sourceGraph = createGraph({
    contract: defineContract({ namespace, operations: { events: definitionA } }),
  });
  const targetGraph = createGraph({
    contract: defineContract({ namespace, operations: { events: definitionB } }),
  });
  let targetSteps = 0;
  let sourceSteps = 0;
  const policy = {
    kind: "reduce" as const,
    initial: 10,
    step: (sum: number, event: number) => {
      targetSteps += 1;
      return sum + event;
    },
  };
  const sourcePolicy = {
    ...policy,
    step: (sum: number, event: number) => {
      sourceSteps += 1;
      return sum + event;
    },
  };
  let sourceProjection: Projection<number> | undefined;
  let capture: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
  let adoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  let claim:
    | {
        readonly store: StreamStore<number>;
        readonly restoration: PositionedStoreRestoration<number, readonly [number]>;
        readonly retired: Promise<void>;
        discard(): void;
      }
    | undefined;
  let reader: object | undefined;
  let capturePublished = false;
  let adoptionPublished = false;

  try {
    const source = sourceGraph.api.events(undefined);
    sourceProjection = project(source, sourcePolicy);
    expect(providerA.openings).toHaveLength(1);
    expect(providerA.boundaries).toHaveLength(1);
    expect(providerA.boundaries[0]!.options).toBe(providerA.openings[0]!.options);
    expect(providerA.trace.indexOf("boundary")).toBeLessThan(providerA.trace.indexOf("pull"));
    expect(source.available.get()).toBe(false);
    expect(sourceProjection.value.get()).toBe(10);
    expect(sourceSteps).toBe(0);
    expect(positionedFoldReceipt(sourceProjection)).toBeUndefined();
    expect(providerA.acknowledgements).toHaveLength(0);

    const checkpoint = capturePositionedStore(source, [sourceProjection] as const);
    expect(checkpoint.present).toBe(false);
    expect(checkpoint.hasPosition).toBe(false);
    expect(Object.hasOwn(checkpoint, "latest")).toBe(false);
    expect(checkpoint.values).toStrictEqual([10]);
    expect(checkpoint.resumable).toBe(true);
    expect(isPositionedStoreCheckpointCurrent(checkpoint)).toBe(true);

    const seed = {
      boundary: { kind: "initial" as const, after: 5 },
      base: { present: false as const },
      values: [10] as const,
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
    const sourceRoot = computed(() => sourceProjection!.value.get());
    expect(sourceRoot.get()).toBe(10);
    capture = prepareRequiredTransferCapture({
      graphs: [sourceGraph],
      roots: [{ source: sourceRoot, construction: captureReactiveConstruction(sourceRoot) }],
      positioned: [checkpoint],
      codec,
    });
    expect(capture.image.version).toBe(2);
    const positionedRows = capture.image.positioned;
    if (positionedRows === undefined)
      throw new Error("The initial positioned group row was omitted");
    expect(positionedRows).toMatchObject([
      {
        slot: "p0",
        store: "s0",
        projections: 1,
        seed: { $ref: "p0-seed" },
      },
    ]);
    expect(decodedSeeds.get("p0-seed")).toBe(seed);
    expect(decodedSeeds.get("p0-seed")).toBe(seed);
    expect(seed.boundary).toEqual({ kind: "initial", after: 5 });
    expect(seed.base).toEqual({ present: false });
    expect(seed.values).toEqual([10]);
    expect(capture.image.nodes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "native",
          positioned: "p0",
          projection: 0,
          facet: "projection-value",
          hasValue: false,
          hasError: false,
        }),
      ]),
    );
    const captureCommit = capture.seal();
    if (captureCommit === undefined) throw new Error("The genuine initial capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capture.retired;

    const targetReader = providerB.reserve(5, seed);
    reader = targetReader;
    const receiverClaim = preparePassiveTransferClaim(
      targetGraph,
      targetGraph.api.events,
      undefined,
      undefined,
      { policies: [policy] as const, seed, reader: targetReader },
    );
    claim = receiverClaim;
    expect(receiverClaim.restoration.source).toBe(receiverClaim.store);
    expect(receiverClaim.retired).not.toBe(receiverClaim.restoration.retired);
    expect(providerB.openings).toHaveLength(0);
    expect(providerB.acknowledgements).toHaveLength(0);

    let targetRuns = 0;
    const targetValue = computed(() => {
      targetRuns += 1;
      return receiverClaim.restoration.projections[0]!.value.get();
    });
    adoption = prepareRequiredTransferAdoption(capture.image, {
      graphs: [targetGraph],
      sources: [{ slot: "s0", claim: receiverClaim }],
      positioned: [{ slot: "p0", claim: receiverClaim, seed }],
      reactive: [
        {
          slot: capture.image.roots[0]!,
          target: targetValue,
          construction: captureReactiveConstruction(targetValue),
        },
      ],
      codec,
      mappedStateWrites: [],
    });
    const commit = adoption.seal();
    if (commit === undefined) throw new Error("The initial boundary adoption did not seal");
    commit.publish();
    adoptionPublished = true;
    commit.notifications.deliver();
    await Promise.all([adoption.retired, claim.retired]);
    expect(providerB.openings).toHaveLength(0);
    expect(receiverClaim.store.available.get()).toBe(false);
    expect(targetValue.get()).toBe(10);
    expect(targetRuns).toBe(0);
    expect(targetSteps).toBe(0);
    expect(positionedFoldReceipt(receiverClaim.restoration.projections[0]!)).toBeUndefined();
    expect(providerB.acknowledgements).toHaveLength(0);

    expect(receiverClaim.restoration.activate()).toBe(receiverClaim.store);
    expect(providerB.openings).toHaveLength(1);
    expect(providerB.boundaries).toHaveLength(1);
    expect(providerB.boundaries[0]!.options).toBe(providerB.openings[0]!.options);
    expect(providerB.boundaries[0]!.after).toBe(5);
    expect(providerB.trace.indexOf("boundary")).toBeLessThan(providerB.trace.indexOf("pull"));
    expect(providerB.acknowledgements).toHaveLength(0);

    const suffixFold = until(
      receiverClaim.restoration.projections[0]!.value,
      (value) => value === 12,
    );
    providerB.emit(targetReader, { position: 6, value: 2 });
    await suffixFold;
    expect(targetSteps).toBe(1);
    expect(receiverClaim.store.available.get()).toBe(true);
    expect(receiverClaim.store.value.get()).toBe(2);
    const receipt = positionedFoldReceipt(receiverClaim.restoration.projections[0]!);
    if (receipt === undefined) throw new Error("The real suffix did not produce a fold receipt");
    acknowledgePositionedFold(receiverClaim.restoration.projections[0]!, receipt);
    expect(providerB.acknowledgements).toHaveLength(1);
    expect(providerB.acknowledgements[0]!.reader).toBe(reader);
    expect(Object.is(providerB.acknowledgements[0]!.receipt, receipt)).toBe(true);

    receiverClaim.restoration.discard();
    await receiverClaim.restoration.retired;
    expect(providerB.releaseCount(targetReader)).toBe(1);
  } finally {
    if (!capturePublished) capture?.discard();
    if (!adoptionPublished) {
      adoption?.discard();
      claim?.discard();
    } else claim?.restoration.discard();
    sourceProjection?.close();
    sourceGraph.dispose();
    targetGraph.dispose();
    if (reader !== undefined && claim === undefined && providerB.releaseCount(reader) === 0)
      providerB.source.releaseReader(undefined, reader);
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
    if (claim !== undefined) {
      await claim.retired.then(
        () => undefined,
        () => undefined,
      );
      await claim.restoration.retired.then(
        () => undefined,
        () => undefined,
      );
    }
  }
});

it("retains a stream for a projection then collects after its final listener closes", async () => {
  const { backend, graph, source } = setup();
  try {
    const history = project(source, { kind: "accumulate", max: 2, onOverflow: "drop-oldest" });
    await Promise.resolve();
    jest.advanceTimersByTime(50);
    expect(backend.calls[0]!.aborted).toBe(false);
    const stopValue = subscribe(source.value, () => undefined);
    history.close();
    jest.advanceTimersByTime(50);
    expect(backend.calls[0]!.aborted).toBe(false);
    stopValue();
    await Promise.resolve();
    jest.advanceTimersByTime(10);
    expect(backend.calls[0]!.aborted).toBe(true);
  } finally {
    graph.dispose();
  }
});

it("fails independent projection state on overflow or gaps without aborting the shared source", async () => {
  const { backend, graph, source } = setup();
  try {
    const bounded = project(source, { kind: "accumulate", max: 1 });
    const sum = project(source, { kind: "reduce", initial: 0, step: (total, n) => total + n });
    const first = until(sum.value, (value) => value === 2);
    backend.emit("shared-events", 2);
    await first;
    const second = until(sum.value, (value) => value === 5);
    backend.emit("shared-events", 3);
    await second;
    expect(bounded.status.get()).toBe("failed");
    expect(bounded.value.get()).toStrictEqual([2]);
    expect(sum.status.get()).toBe("open");
    expect(backend.calls[0]!.aborted).toBe(false);
    backend.gap("shared-events");
    expect(sum.status.get()).toBe("failed");
    expect(sum.value.get()).toBe(5);
    expect(source.status.get()).toBe("stale");
    expect(backend.calls[0]!.aborted).toBe(false);
  } finally {
    graph.dispose();
  }
});

it("ends projections with their source session and does not reconnect them on reopen", async () => {
  const { backend, graph, source } = setup();
  try {
    const history = project(source, { kind: "accumulate", max: 10 });
    const closed = until(history.status, (status) => status === "closed");
    backend.end("shared-events");
    await closed;
    const reopened = graph.api.stream();
    expect(backend.calls).toHaveLength(2);
    const sum = project(reopened, { kind: "reduce", initial: 0, step: (total, n) => total + n });
    const first = until(sum.value, (value) => value === 1);
    backend.emit("shared-events", 1);
    await first;
    expect(history.value.get()).toStrictEqual([]);
    const reason = new Error("source failed");
    const failed = until(sum.status, (status) => status === "failed");
    backend.end("shared-events", reason);
    await failed;
    expect(sum.error.get()).toBe(reason);
    const late = project(reopened, { kind: "accumulate", max: 2 });
    expect(late.status.get()).toBe("failed");
    expect(late.error.get()).toBe(reason);
  } finally {
    graph.dispose();
  }
});

it.each(["end", "reset"] as const)(
  "keeps bound shared projections on A after %s and starts B explicitly",
  async (mode) => {
    const backend = testBackend();
    const stream = createStream({
      name: "fixed-projection",
      key: () => "one",
      open: backend.stream<void, number>("fixed-projection"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "fixed-projection", operations: { stream } }),
    });
    try {
      const binding = bindStream(graph, stream, graph.api.stream, {
        context: createBindingContext(graph),
      });
      const a = binding.member();
      const history = project(a, { kind: "accumulate", max: 3 });
      const receivedA = until(history.value, (items) => items.length === 1);
      backend.emit("fixed-projection", 5);
      await receivedA;
      let resetting: Promise<void> | undefined;
      if (mode === "reset") resetting = graph.reset();
      else backend.end("fixed-projection");
      await binding.session(a).detached;
      await resetting;
      expect(history.status.get()).toBe("closed");
      expect(backend.calls).toHaveLength(1);
      const prior = mode === "reset" ? [] : [5];
      expect(history.value.get()).toEqual(prior);
      const b = binding.member();
      const next = project(b, { kind: "accumulate", max: 3 });
      const receivedB = until(next.value, (items) => items.length === 1);
      backend.emit("fixed-projection", 9);
      await receivedB;
      expect(next.value.get()).toEqual([9]);
      expect(history.value.get()).toEqual(prior);
      expect(history.status.get()).toBe("closed");
      expect(backend.calls).toHaveLength(2);
      next.close();
    } finally {
      graph.dispose();
    }
  },
);
