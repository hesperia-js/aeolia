import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  Fault,
  captureReactiveConstruction,
  computed,
  createGraph,
  defineContract,
  createStream,
  createPositionedStream,
  createQuery,
  project,
  projectPositioned,
  capturePositionedStore,
  isPositionedStoreCheckpointCurrent,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
  preparePositionedStoreRestore,
  capturePositionedProjection,
  isPositionedCheckpointCurrent,
  preparePositionedRestore,
  preparePositionedSeedRestore,
  positionedFoldReceipt,
  acknowledgePositionedFold,
  subscribe,
  type Graph,
  type AccumulatePolicy,
  type PositionedRestoration,
  type Projection,
  type StreamDefinition,
  type PositionedStreamSource,
  type PositionedStreamDefinition,
  type PositionedStreamEvent,
  type PositionedStreamOpenOptions,
  type PositionedProjectionCheckpoint,
  type PositionedFoldReceipt,
  type PositionedStoreRestoreSeed,
  type PositionedStoreRestoration,
  type StreamStore,
  type ClosedEncodedValue,
  type RequiredTransferCodec,
} from "../../src/index.ts";
import { testBackend, type TestBackend } from "../../src/testing.ts";

const providerClockNow = Date.now;

let ownsFakeTimers = false;
beforeEach(() => {
  jest.useFakeTimers();
  ownsFakeTimers = true;
  jest.setSystemTime(0);
});

afterEach(() => {
  if (ownsFakeTimers) jest.clearAllTimers();
  jest.useRealTimers();
  ownsFakeTimers = false;
});

async function expectPending(promise: Promise<unknown>): Promise<void> {
  jest.useRealTimers();
  ownsFakeTimers = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      promise.then(
        () => "settled",
        () => "settled",
      ),
      new Promise<"pending">((resolve) => {
        timer = setTimeout(() => resolve("pending"), 0);
      }),
    ]);
    expect(result).toBe("pending");
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function streamGraph<T>(stream: StreamDefinition<void, T>): Graph {
  return createGraph({
    contract: defineContract({ namespace: "stream-tests", operations: { stream } }),
    idleMs: 10,
  });
}

function numberStream(backend: TestBackend, name = "numbers"): StreamDefinition<void, number> {
  return createStream({
    name,
    key: () => `${name}/one`,
    open: backend.stream<void, number>(name),
  });
}

function waitForProjectionStatus(
  projection: ReturnType<typeof project>,
  expected: "closed" | "failed",
): Promise<void> {
  return new Promise((resolve) => {
    let stop: (() => void) | undefined;
    let done = false;
    stop = subscribe(projection.status, (status) => {
      if (status === expected) {
        done = true;
        stop?.();
        resolve();
      }
    });
    if (done) stop();
  });
}

type PositionedInput = { readonly room: string };
type PositionedEvent<Value = number> = PositionedStreamEvent<number, Value>;
type PositionedCheckpoint = PositionedProjectionCheckpoint<unknown, number>;

function positionedChannel<Value = number>() {
  const queued: PositionedEvent<Value>[] = [];
  const waiters: Array<(result: IteratorResult<PositionedEvent<Value>>) => void> = [];
  let ended = false;
  const emit = (event: PositionedEvent<Value>): void => {
    const waiter = waiters.shift();
    if (waiter !== undefined) waiter({ done: false, value: event });
    else queued.push(event);
  };
  const source: AsyncIterable<PositionedEvent<Value>> = {
    [Symbol.asyncIterator](): AsyncIterator<PositionedEvent<Value>> {
      return {
        next: () => {
          const event = queued.shift();
          if (event !== undefined) return Promise.resolve({ done: false as const, value: event });
          if (ended) return Promise.resolve({ done: true as const, value: undefined });
          return new Promise<IteratorResult<PositionedEvent<Value>>>((resolve) =>
            waiters.push(resolve),
          );
        },
        return: () => {
          ended = true;
          for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
          return Promise.resolve({ done: true as const, value: undefined });
        },
      };
    },
  };
  return { source, emit };
}

class PositionedProvider<Value = number> implements PositionedStreamSource<
  PositionedInput,
  Value,
  number,
  object
> {
  readonly opens: Array<{
    room: string;
    hasAfter: boolean;
    after: number | undefined;
    hasReader: boolean;
  }> = [];
  readonly currentChecks: Array<{ after: number; seed: unknown }> = [];
  readonly acknowledgements: Array<{ reader: object; receipt: PositionedFoldReceipt<number> }> = [];
  validationOverride: unknown = undefined;
  validationCalls = 0;
  currentCheckHook: (() => void) | undefined = undefined;
  readonly #reservations = new WeakMap<
    object,
    {
      input: PositionedInput;
      after: number;
      checkpoint: PositionedCheckpoint | undefined;
      seed: unknown;
      controller: AbortController;
      expiresAt: number;
    }
  >();
  readonly #readerEmitters = new WeakMap<object, (event: PositionedEvent<Value>) => void>();
  readonly #liveEmitters = new Map<string, (event: PositionedEvent<Value>) => void>();
  readonly #releaseCounts = new WeakMap<object, number>();

  reserve(
    input: PositionedInput,
    after: number,
    checkpoint: PositionedCheckpoint | undefined,
    seed: unknown,
  ): object {
    const reader = Object.freeze({});
    this.#reservations.set(reader, {
      input,
      after,
      checkpoint,
      seed,
      controller: new AbortController(),
      expiresAt: providerClockNow() + 60_000,
    });
    return reader;
  }

  emitLive(input: PositionedInput, event: PositionedEvent<Value>): void {
    const emit = this.#liveEmitters.get(input.room);
    if (emit === undefined) throw new Error("The live provider opening is missing");
    emit(event);
  }

  emitReader(reader: object, event: PositionedEvent<Value>): void {
    const emit = this.#readerEmitters.get(reader);
    if (emit === undefined) throw new Error("The reserved provider opening is missing");
    emit(event);
  }

  releaseCount(reader: object): number {
    return this.#releaseCounts.get(reader) ?? 0;
  }

  open(
    input: PositionedInput,
    options: PositionedStreamOpenOptions<number, object>,
  ): AsyncIterable<PositionedEvent<Value>> {
    const channel = positionedChannel<Value>();
    const hasAfter = Object.hasOwn(options, "after");
    const hasReader = Object.hasOwn(options, "reader");
    this.opens.push({ room: input.room, hasAfter, after: options.after, hasReader });
    if (options.reader !== undefined) this.#readerEmitters.set(options.reader, channel.emit);
    else this.#liveEmitters.set(input.room, channel.emit);
    return channel.source;
  }

  validateReader(
    input: PositionedInput,
    reader: object,
    after: number,
    checkpoint: PositionedCheckpoint | undefined,
    seed: unknown,
  ): boolean {
    this.validationCalls += 1;
    if (this.validationOverride !== undefined) return this.validationOverride as boolean;
    const reservation = this.#reservations.get(reader);
    return (
      reservation !== undefined &&
      reservation.input === input &&
      reservation.after === after &&
      reservation.checkpoint === checkpoint &&
      reservation.seed === seed
    );
  }

  isReaderCurrent(input: PositionedInput, reader: object, after: number, seed: unknown): boolean {
    this.currentChecks.push({ after, seed });
    this.currentCheckHook?.();
    const reservation = this.#reservations.get(reader);
    return (
      reservation !== undefined &&
      reservation.input === input &&
      reservation.after === after &&
      reservation.seed === seed
    );
  }

  acknowledge(
    _input: PositionedInput,
    reader: object,
    receipt: PositionedFoldReceipt<number>,
  ): void {
    this.acknowledgements.push({ reader, receipt });
  }

  readonly readerLifetime = (
    input: PositionedInput,
    reader: object,
    after: number,
    seed: unknown,
  ): { readonly signal: AbortSignal; readonly expiresAt: number } => {
    const reservation = this.#reservations.get(reader);
    if (
      reservation === undefined ||
      reservation.input !== input ||
      reservation.after !== after ||
      reservation.seed !== seed
    )
      throw new TypeError("The positioned lifetime does not match its reader reservation");
    return { signal: reservation.controller.signal, expiresAt: reservation.expiresAt };
  };

  abortReader(reader: object, reason?: unknown): void {
    this.#reservations.get(reader)?.controller.abort(reason);
  }

  releaseReader(_input: PositionedInput, reader: object): void {
    this.#releaseCounts.set(reader, this.releaseCount(reader) + 1);
  }
}

function positionedDefinition(provider: PositionedProvider) {
  return createPositionedStream({
    name: "positioned-events",
    key: (input: PositionedInput) => input.room,
    source: provider,
  });
}

function positionedGraph<Value>(
  stream: PositionedStreamDefinition<PositionedInput, Value, string, number, object>,
  namespace: string,
) {
  return createGraph({
    contract: defineContract({ namespace, operations: { events: stream } }),
    idleMs: 10,
  });
}

type AccumulationRestorePath = "same-runtime" | "fresh-target";

interface AccumulationRestoreFixture {
  readonly provider: PositionedProvider;
  readonly targetGraph: ReturnType<typeof positionedGraph>;
  readonly input: PositionedInput;
  readonly reader: object;
  attempt(): PositionedRestoration<readonly number[]>;
  cleanup(): void;
}

async function createAccumulationRestoreFixture(
  path: AccumulationRestorePath,
  seed: number[],
): Promise<AccumulationRestoreFixture> {
  const provider = new PositionedProvider();
  const stream = positionedDefinition(provider);
  const targetGraph = positionedGraph(stream, "positioned-" + path + "-target");
  const input = Object.freeze({ room: "one" });
  const policy: AccumulatePolicy = { kind: "accumulate", max: 1, onOverflow: "drop-oldest" };
  let sourceGraph: ReturnType<typeof positionedGraph> | undefined;
  let source: Projection<readonly number[]> | undefined;
  let reader: object;
  let attempt: () => PositionedRestoration<readonly number[]>;
  if (path === "same-runtime") {
    sourceGraph = positionedGraph(stream, "positioned-same-runtime-source");
    source = projectPositioned(sourceGraph.api.events, input, policy);
    provider.emitLive(input, { position: 5, value: 1 });
    await flush();
    const checkpoint = capturePositionedProjection<readonly number[], number>(source);
    reader = provider.reserve(input, 5, checkpoint, seed);
    attempt = () =>
      preparePositionedRestore(targetGraph.api.events, input, checkpoint, reader, { value: seed });
  } else {
    reader = provider.reserve(input, 5, undefined, seed);
    attempt = () =>
      preparePositionedSeedRestore(
        targetGraph.api.events,
        input,
        policy,
        { after: 5, value: seed },
        reader,
      );
  }
  return {
    provider,
    targetGraph,
    input,
    reader,
    attempt,
    cleanup() {
      source?.close();
      sourceGraph?.dispose();
      targetGraph.dispose();
      if (provider.releaseCount(reader) === 0) provider.releaseReader(input, reader);
    },
  };
}
describe("stream projections", () => {
  for (const restartFrom of ["query abort", "old iterator cleanup"] as const) {
    it(`preserves a replacement projection generation created during ${restartFrom}`, async () => {
      const sources: Array<{
        signal: AbortSignal;
        read: ReturnType<typeof Promise.withResolvers<IteratorResult<number>>>;
        close: ReturnType<typeof Promise.withResolvers<IteratorResult<number>>>;
        returns: number;
        nextCalls: number;
      }> = [];
      const queryRead = Promise.withResolvers<number>();
      let nestedReset: Promise<void> | undefined;
      let reenter = false;
      const stream = createStream({
        name: "projection-cancel-generation",
        key: () => "projection-cancel-generation/one",
        open: (_input: void, options): AsyncIterable<number> => {
          const source = {
            signal: options.abortSignal,
            read: Promise.withResolvers<IteratorResult<number>>(),
            close: Promise.withResolvers<IteratorResult<number>>(),
            returns: 0,
            nextCalls: 0,
          };
          sources.push(source);
          return {
            [Symbol.asyncIterator]() {
              return {
                next: () => {
                  source.nextCalls += 1;
                  if (source.returns > 0)
                    return Promise.resolve({ done: true as const, value: undefined });
                  if (source.nextCalls > 1)
                    source.read = Promise.withResolvers<IteratorResult<number>>();
                  return source.read.promise;
                },
                return: () => {
                  source.returns += 1;
                  if (reenter && restartFrom === "old iterator cleanup") {
                    reenter = false;
                    nestedReset = graph.reset();
                  }
                  return source.close.promise;
                },
              };
            },
          };
        },
      });
      const query = createQuery({
        name: "projection-cancel-query",
        key: () => "projection-cancel-query/one",
        fetch: (_input: void, options) => {
          options.abortSignal.addEventListener(
            "abort",
            () => {
              if (!reenter || restartFrom !== "query abort") return;
              reenter = false;
              nestedReset = graph.reset();
            },
            { once: true },
          );
          return queryRead.promise;
        },
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "projection-cancel-generation",
          operations: { stream, query },
        }),
      });
      if (restartFrom === "query abort") graph.api.query();
      const projection = project(graph, stream, undefined, { kind: "accumulate", max: 4 });
      try {
        reenter = true;
        if (restartFrom === "query abort") {
          graph.cancelPending(new Error("cancel entry cohort"));
        } else {
          const superseded = graph.reset();
          await expect(superseded).rejects.toMatchObject({ name: "AbortError" });
        }
        expect(nestedReset).toBeDefined();
        await nestedReset;
        expect(sources).toHaveLength(2);
        expect(sources[0]!.signal.aborted).toBe(true);
        expect(sources[0]!.returns).toBe(1);
        expect(sources[1]!.signal.aborted).toBe(false);
        expect(sources[1]!.returns).toBe(0);
        expect(projection.status.get()).toBe("open");

        const oldRead = sources[0]!.read;
        oldRead.resolve({ done: false, value: 99 });
        await oldRead.promise;
        expect(projection.value.get()).toEqual([]);
        const received = Promise.withResolvers<void>();
        const stop = subscribe(projection.value, (items) => {
          if (items.length === 1 && items[0] === 7) received.resolve();
        });
        try {
          sources[1]!.read.resolve({ done: false, value: 7 });
          await received.promise;
        } finally {
          stop();
        }
        expect(projection.value.get()).toEqual([7]);
        expect(sources[1]!.nextCalls).toBe(2);
        const reason = new Error("cancel replacement through its retained registration");
        graph.cancelPending(reason);
        expect(sources[1]!.signal.reason).toBe(reason);
        expect(sources[1]!.returns).toBe(1);
        expect(projection.status.get()).toBe("closed");
        expect(projection.value.get()).toEqual([7]);
        expect(projection.error.get()).toBeUndefined();
        queryRead.resolve(1);
        const retirement = graph.retire();
        await expectPending(retirement);
        expect(sources.map((source) => source.returns)).toEqual([1, 1]);
        sources[1]!.read.resolve({ done: true, value: undefined });
        sources[1]!.close.resolve({ done: true, value: undefined });
        await expectPending(retirement);
        sources[0]!.close.resolve({ done: true, value: undefined });
        await retirement;
      } finally {
        queryRead.resolve(1);
        for (const source of sources) {
          source.read.resolve({ done: true, value: undefined });
          source.close.resolve({ done: true, value: undefined });
        }
        graph.dispose();
      }
      expect(sources.map((source) => source.returns)).toEqual([1, 1]);
    });
  }

  it("opens independently, passes the required key, accumulates, and drops oldest", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "projection");
    const graph = streamGraph(stream);
    const first = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 2,
      onOverflow: "drop-oldest",
    });
    const second = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 2,
      onOverflow: "drop-oldest",
    });

    expect(backend.calls).toHaveLength(2);
    expect(String(backend.calls[0]!.key)).toBe("projection/one");
    expect(String(backend.calls[1]!.key)).toBe("projection/one");
    backend.emit("projection", 1);
    backend.emit("projection", 2);
    await flush();
    expect(first.value.get()).toEqual([1, 2]);
    expect(second.value.get()).toEqual([1, 2]);
    backend.emit("projection", 3);
    await flush();
    expect(first.value.get()).toEqual([2, 3]);
    expect(second.value.get()).toEqual([2, 3]);
    expect(first.status.get()).toBe("open");
    first.close();
    expect(first.status.get()).toBe("closed");
    expect(backend.calls[0]!.aborted).toBe(true);
    second.close();
    graph.dispose();
  });

  it("fails and closes on a gap or overflow while preserving accumulated value", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "projection-failure");
    const graph = streamGraph(stream);
    const gapped = project(graph, stream, undefined, { kind: "accumulate", max: 3 });
    backend.emit("projection-failure", 7);
    await flush();
    backend.gap("projection-failure");
    await flush();
    expect(gapped.status.get()).toBe("failed");
    expect(gapped.value.get()).toEqual([7]);
    expect(backend.calls[0]!.aborted).toBe(true);

    const overflow = project(graph, stream, undefined, { kind: "accumulate", max: 1 });
    backend.emit("projection-failure", 8);
    await flush();
    backend.emit("projection-failure", 9);
    await flush();
    expect(overflow.status.get()).toBe("failed");
    expect(overflow.value.get()).toEqual([8]);
    expect(backend.calls[1]!.aborted).toBe(true);
    graph.dispose();
  });

  it("closes a projection after its source iterator completes normally", async () => {
    const stream = createStream({
      name: "projection-complete",
      key: () => "projection-complete/one",
      open: () =>
        (async function* () {
          yield 2;
          yield 3;
        })(),
    });
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });

    await waitForProjectionStatus(projection, "closed");
    expect(projection.value.get()).toEqual([2, 3]);
    expect(projection.status.get()).toBe("closed");
    expect(projection.error.get()).toBeUndefined();
    graph.dispose();
  });

  it("fails a projection when its source iterator rejects", async () => {
    const failure = new Error("projection iterator failed");
    const stream = createStream({
      name: "projection-iterator-failure",
      key: () => "projection-iterator-failure/one",
      open: () =>
        (async function* () {
          yield 4;
          throw failure;
        })(),
    });
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });

    await waitForProjectionStatus(projection, "failed");
    expect(projection.value.get()).toEqual([4]);
    expect(projection.status.get()).toBe("failed");
    expect(projection.error.get()).toBe(failure);
    graph.dispose();
  });

  it("represents a synchronous projection open throw as a failed projection", () => {
    const reason = new Error("projection cannot open");
    const stream = createStream({
      name: "projection-open-throw",
      key: () => "projection-open-throw/one",
      open: () => {
        throw reason;
      },
    });
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });

    expect(projection.value.get()).toEqual([]);
    expect(projection.status.get()).toBe("failed");
    expect(projection.error.get()).toBe(reason);
    graph.dispose();
  });

  it("rethrows a synchronous projection key throw without opening its source", async () => {
    const reason = new Error("projection key failed");
    let keyCalls = 0;
    let openCalls = 0;
    const stream = createStream({
      name: "projection-key-throw",
      key: () => {
        keyCalls += 1;
        if (keyCalls === 1) throw reason;
        return "projection-key-throw/one";
      },
      open: () => {
        openCalls += 1;
        return (async function* () {})();
      },
    });
    const graph = streamGraph(stream);

    expect(() =>
      project(graph, stream, undefined, {
        kind: "accumulate",
        max: 3,
      }),
    ).toThrow(reason);
    expect(keyCalls).toBe(1);
    expect(openCalls).toBe(0);

    const recovered = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });
    await flush();
    expect(recovered.status.get()).toBe("closed");
    expect(openCalls).toBe(1);
    graph.dispose();
  });

  it("reduces emissions and fails when the reduce step throws", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "reduce");
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "reduce",
      initial: 0,
      step: (accumulator, item) => {
        if (item === 9) throw new Error("bad item");
        return accumulator + item;
      },
    });
    backend.emit("reduce", 2);
    await flush();
    expect(projection.value.get()).toBe(2);
    backend.emit("reduce", 9);
    await flush();
    expect(projection.status.get()).toBe("failed");
    expect(projection.value.get()).toBe(2);
    expect((projection.error.get() as Error).message).toBe("bad item");
    graph.dispose();
  });

  it("rejects invalid accumulation bounds at construction", () => {
    const backend = testBackend();
    const stream = numberStream(backend, "bounds");
    const graph = streamGraph(stream);
    for (const max of [
      0,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      Number.NaN,
      -1,
      1.5,
    ]) {
      try {
        project(graph, stream, undefined, { kind: "accumulate", max });
        throw new Error("project unexpectedly accepted an invalid maximum");
      } catch (error) {
        expect(error).toBeInstanceOf(Fault);
        expect((error as Fault).kind).toBe("contract");
      }
    }
    graph.dispose();
  });

  it("restores a decoded fold into another graph and acknowledges from its original reader boundary", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const sourceGraph = positionedGraph(stream, "positioned-source");
    const targetGraph = positionedGraph(stream, "positioned-target");
    const input = Object.freeze({ room: "one" });
    const policy = {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number, item: number) => sum + item,
    };
    const source = projectPositioned(sourceGraph.api.events, input, policy);
    try {
      expect(provider.opens[0]?.hasAfter).toBe(false);
      provider.emitLive(input, { position: 5, value: 2 });
      await flush();
      const checkpoint = capturePositionedProjection<number, number>(source);
      expect(checkpoint).toMatchObject({
        value: 2,
        hasPosition: true,
        position: 5,
        resumable: true,
      });
      expect(isPositionedCheckpointCurrent(checkpoint)).toBe(true);

      const seed = 20;
      const reader = provider.reserve(input, 5, checkpoint, seed);
      const restore = preparePositionedRestore(targetGraph.api.events, input, checkpoint, reader, {
        value: seed,
      });
      expect(provider.opens).toHaveLength(1);
      expect(restore.projection.value.get()).toBe(seed);
      expect(capturePositionedProjection<number, number>(restore.projection)).toMatchObject({
        value: seed,
        hasPosition: true,
        position: 5,
        resumable: true,
      });

      restore.activate();
      const resumedOpen = provider.opens[1];
      if (resumedOpen === undefined) throw new Error("The admitted reader did not open");
      expect(resumedOpen).toMatchObject({ hasAfter: true, after: 5, hasReader: true });
      provider.emitReader(reader, { position: 6, value: 3 });
      await flush();
      expect(restore.projection.value.get()).toBe(23);
      const receipt = positionedFoldReceipt(restore.projection);
      if (receipt === undefined) throw new Error("The successful fold did not issue a receipt");
      expect(receipt.position).toBe(6);
      acknowledgePositionedFold(restore.projection, receipt);
      expect(provider.currentChecks.length).toBeGreaterThanOrEqual(2);
      expect(provider.currentChecks.every((check) => check.after === 5)).toBe(true);
      expect(provider.acknowledgements[0]?.receipt === receipt).toBe(true);
      restore.discard();
      expect(provider.releaseCount(reader)).toBe(1);
      expect(() => restore.activate()).toThrow(TypeError);
    } finally {
      source.close();
      sourceGraph.dispose();
      targetGraph.dispose();
    }
  });

  it("refuses a non-boolean provider reservation result before issuing a restore", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const sourceGraph = positionedGraph(stream, "positioned-validator-source");
    const targetGraph = positionedGraph(stream, "positioned-validator-target");
    const input = Object.freeze({ room: "one" });
    const source = projectPositioned(sourceGraph.api.events, input, {
      kind: "reduce",
      initial: 0,
      step: (sum, item) => sum + item,
    });
    try {
      provider.emitLive(input, { position: 5, value: 2 });
      await flush();
      const checkpoint = capturePositionedProjection<number, number>(source);
      const reader = provider.reserve(input, 5, checkpoint, 20);
      provider.validationOverride = Promise.resolve(true);
      expect(() =>
        preparePositionedRestore(targetGraph.api.events, input, checkpoint, reader, { value: 20 }),
      ).toThrow(TypeError);
      expect(provider.opens).toHaveLength(1);
      expect(provider.releaseCount(reader)).toBe(0);
    } finally {
      source.close();
      sourceGraph.dispose();
      targetGraph.dispose();
    }
  });

  for (const path of ["same-runtime", "fresh-target"] as const) {
    it("rejects oversized accumulation seeds before provider admission on " + path, async () => {
      const fixture = await createAccumulationRestoreFixture(path, [2, 3]);
      try {
        expect(() => fixture.attempt()).toThrow(TypeError);
        expect(fixture.provider.validationCalls).toBe(0);
        expect(fixture.provider.currentChecks).toHaveLength(0);
        expect(fixture.provider.opens).toHaveLength(path === "same-runtime" ? 1 : 0);
        expect(fixture.provider.releaseCount(fixture.reader)).toBe(0);
      } finally {
        fixture.cleanup();
      }
    });
  }

  for (const path of ["same-runtime", "fresh-target"] as const) {
    it("accepts a borrowed exact-bound seed and applies drop-oldest on " + path, async () => {
      const seed = [2];
      const fixture = await createAccumulationRestoreFixture(path, seed);
      try {
        const restoration = fixture.attempt();
        expect(restoration.projection.value.get()).toBe(seed);
        expect(fixture.provider.opens).toHaveLength(path === "same-runtime" ? 1 : 0);
        restoration.activate();
        const resumedOpen = fixture.provider.opens[fixture.provider.opens.length - 1];
        if (resumedOpen === undefined) throw new Error("The admitted reader did not open");
        expect(resumedOpen).toMatchObject({ hasAfter: true, after: 5, hasReader: true });
        fixture.provider.emitReader(fixture.reader, { position: 6, value: 9 });
        await flush();
        expect(restoration.projection.value.get()).toEqual([9]);
        restoration.discard();
        expect(fixture.provider.releaseCount(fixture.reader)).toBe(1);
      } finally {
        fixture.cleanup();
      }
    });
  }

  for (const path of ["same-runtime", "fresh-target"] as const) {
    it("rechecks borrowed seed bounds after provider callback growth on " + path, async () => {
      const seed = [2];
      const fixture = await createAccumulationRestoreFixture(path, seed);
      fixture.provider.currentCheckHook = () => {
        fixture.provider.currentCheckHook = undefined;
        seed.push(3);
      };
      try {
        expect(() => fixture.attempt()).toThrow(TypeError);
        expect(seed).toEqual([2, 3]);
        expect(fixture.provider.validationCalls).toBeGreaterThan(0);
        expect(fixture.provider.currentChecks.length).toBeGreaterThan(0);
        expect(fixture.provider.opens).toHaveLength(path === "same-runtime" ? 1 : 0);
        expect(fixture.provider.releaseCount(fixture.reader)).toBe(0);
      } finally {
        fixture.cleanup();
      }
    });
  }

  it("rechecks target currentness after a reentrant seed-length read", () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const graph = positionedGraph(stream, "positioned-seed-length-reentry");
    const input = Object.freeze({ room: "one" });
    const policy: AccumulatePolicy = { kind: "accumulate", max: 1, onOverflow: "drop-oldest" };
    let armed = false;
    let invalidatedDuringLengthRead = false;
    const seed = new Proxy([7], {
      get(target, property, receiver) {
        if (property === "length" && armed) {
          armed = false;
          invalidatedDuringLengthRead = true;
          graph.dispose();
          return 1;
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const reader = provider.reserve(input, 5, undefined, seed);
    provider.currentCheckHook = () => {
      armed = true;
    };
    try {
      expect(() =>
        preparePositionedSeedRestore(
          graph.api.events,
          input,
          policy,
          { after: 5, value: seed },
          reader,
        ),
      ).toThrow(TypeError);
      expect(invalidatedDuringLengthRead).toBe(true);
      expect(provider.currentChecks.length).toBeGreaterThan(0);
      expect(provider.opens).toHaveLength(0);
      expect(provider.releaseCount(reader)).toBe(0);
    } finally {
      graph.dispose();
      if (provider.releaseCount(reader) === 0) provider.releaseReader(input, reader);
    }
  });

  it("restores a decoded accumulator into a fresh target provider after source A is gone", async () => {
    const persisted: { after: number; value: { total: number } } = await (async () => {
      const providerA = new PositionedProvider();
      const streamA = positionedDefinition(providerA);
      const graphA = positionedGraph(streamA, "positioned-fresh-a");
      const inputA = Object.freeze({ room: "one" });
      const source = projectPositioned(graphA.api.events, inputA, {
        kind: "reduce" as const,
        initial: { total: 0 },
        step: (accumulator, item) => ({ total: accumulator.total + item }),
      });
      try {
        providerA.emitLive(inputA, { position: 5, value: 2 });
        await flush();
        const checkpoint = capturePositionedProjection<{ total: number }, number>(source);
        if (!checkpoint.hasPosition || checkpoint.position === undefined)
          throw new Error("Source A did not fold a positioned event");
        const record = { after: checkpoint.position, value: { total: checkpoint.value.total } };
        source.close();
        graphA.dispose();
        return record;
      } finally {
        source.close();
        graphA.dispose();
      }
    })();
    const providerB = new PositionedProvider();
    const streamB = positionedDefinition(providerB);
    const graphB = positionedGraph(streamB, "positioned-fresh-b");
    const inputB = Object.freeze({ room: "one" });
    const decodedValue = persisted.value;
    const reader = providerB.reserve(inputB, persisted.after, undefined, decodedValue);
    const policy = {
      kind: "reduce" as const,
      initial: decodedValue,
      step: (accumulator: { total: number }, item: number) => ({ total: accumulator.total + item }),
    };
    try {
      const restore = preparePositionedSeedRestore(
        graphB.api.events,
        inputB,
        policy,
        { after: persisted.after, value: decodedValue },
        reader,
      );
      expect(providerB.opens).toHaveLength(0);
      expect(restore.projection.value.get()).toBe(decodedValue);
      expect(capturePositionedProjection(restore.projection)).toMatchObject({
        value: decodedValue,
        hasPosition: true,
        position: 5,
        revision: 0,
        resumable: true,
      });
      restore.activate();
      const resumedOpen = providerB.opens[0];
      if (resumedOpen === undefined) throw new Error("The target reader did not open");
      expect(resumedOpen).toMatchObject({ hasAfter: true, after: 5, hasReader: true });
      providerB.emitReader(reader, { position: 6, value: 3 });
      await flush();
      expect(restore.projection.value.get()).toEqual({ total: 5 });
      const receipt = positionedFoldReceipt(restore.projection);
      if (receipt === undefined) throw new Error("The target fold did not issue a receipt");
      acknowledgePositionedFold(restore.projection, receipt);
      expect(providerB.currentChecks.length).toBeGreaterThanOrEqual(2);
      expect(providerB.currentChecks.every((check) => check.after === persisted.after)).toBe(true);
      expect(providerB.acknowledgements[0]?.receipt === receipt).toBe(true);
      restore.discard();
      expect(providerB.releaseCount(reader)).toBe(1);
    } finally {
      graphB.dispose();
    }
  });
  it("keeps a mutated accumulator at its last successful position but forbids restore after reducer failure", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const graph = positionedGraph(stream, "positioned-reducer-failure");
    const input = Object.freeze({ room: "one" });
    const failure = new Error("reducer failed after mutation");
    const source = projectPositioned(graph.api.events, input, {
      kind: "reduce" as const,
      initial: { total: 0 },
      step: (accumulator, item) => {
        accumulator.total += item;
        if (item === 9) {
          accumulator.total = 77;
          throw failure;
        }
        return accumulator;
      },
    });
    try {
      provider.emitLive(input, { position: 1, value: 1 });
      await flush();
      const beforeFailure = capturePositionedProjection(source);
      expect(beforeFailure).toMatchObject({ hasPosition: true, position: 1, resumable: true });
      provider.emitLive(input, { position: 2, value: 9 });
      await flush();
      const afterFailure = capturePositionedProjection(source);
      expect(afterFailure.status).toBe("failed");
      expect(afterFailure.error).toBe(failure);
      expect(afterFailure.position).toBe(1);
      expect(afterFailure.value.total).toBe(77);
      expect(afterFailure.resumable).toBe(false);
      expect(() =>
        preparePositionedRestore(
          graph.api.events,
          input,
          afterFailure,
          {},
          { value: afterFailure.value },
        ),
      ).toThrow(TypeError);
      expect(provider.opens).toHaveLength(1);
    } finally {
      source.close();
      graph.dispose();
    }
  });

  it("keeps positioned folds landed across observer failure and equal-value delivery", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const faults: unknown[] = [];
    const graph = createGraph({
      contract: defineContract({
        namespace: "positioned-observer-failure",
        operations: { events: stream },
      }),
      onUnobservedFault: (error) => faults.push(error),
    });
    const input = Object.freeze({ room: "one" });
    const projection = projectPositioned(graph.api.events, input, {
      kind: "reduce" as const,
      initial: 0,
      step: () => 1,
    });
    const notificationFailure = new Error("projection observer failed");
    const stop = subscribe(projection.value, (value) => {
      if (value === 1) throw notificationFailure;
    });
    try {
      provider.emitLive(input, { position: 5, value: 2 });
      await flush();
      expect(projection.status.get()).toBe("open");
      expect(projection.value.get()).toBe(1);
      expect(faults).toEqual([notificationFailure]);
      const first = capturePositionedProjection<number, number>(projection);
      expect(first).toMatchObject({
        value: 1,
        hasPosition: true,
        position: 5,
        revision: 1,
        resumable: true,
      });
      provider.emitLive(input, { position: 6, value: 3 });
      await flush();
      const second = capturePositionedProjection<number, number>(projection);
      expect(second).toMatchObject({
        value: 1,
        hasPosition: true,
        position: 6,
        revision: 2,
        resumable: true,
      });
      expect(isPositionedCheckpointCurrent(first)).toBe(false);
      expect(positionedFoldReceipt(projection)).toMatchObject({ position: 6, revision: 2 });
      expect(faults).toEqual([notificationFailure]);
    } finally {
      stop();
      projection.close();
      graph.dispose();
    }
  });

  it("captures one Store session across equal source and fold values", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const graph = positionedGraph(stream, "positioned-store-equal-fold");
    const input = Object.freeze({ room: "one" });
    const source = graph.api.events(input);
    const equalPolicy = {
      kind: "reduce" as const,
      initial: 10,
      step: () => 10,
    };
    const equalFold = project(source, equalPolicy);
    let progress!: Projection<number>;
    let duringEvent: { resumable: boolean } | undefined;
    progress = project(source, {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number) => {
        duringEvent = capturePositionedStore(source, [equalFold, progress] as const);
        return sum + 1;
      },
    });
    try {
      const firstProgress = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(progress.value, (value) => {
          if (value !== 1) return;
          stop();
          resolve();
        });
      });
      provider.emitLive(input, { position: 5, value: 7 });
      await firstProgress;
      expect(duringEvent?.resumable).toBe(false);

      const first = capturePositionedStore(source, [equalFold, progress] as const);
      if (!first.present || !first.hasPosition || typeof first.position !== "number")
        throw new Error("The completed Store event did not produce a positioned capture");
      expect(first.sourceIdentity).toBe(source.identity);
      expect(first.latest).toBe(7);
      expect(first.position).toBe(5);
      expect(first.values).toStrictEqual([10, 1]);
      expect(first.resumable).toBe(true);
      expect(isPositionedStoreCheckpointCurrent(first)).toBe(true);

      const secondProgress = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(progress.value, (value) => {
          if (value !== 2) return;
          stop();
          resolve();
        });
      });
      provider.emitLive(input, { position: 6, value: 7 });
      await secondProgress;

      const second = capturePositionedStore(source, [equalFold, progress] as const);
      if (!second.present || !second.hasPosition || typeof second.position !== "number")
        throw new Error("The equal-valued event did not advance the Store capture");
      expect(second.sourceIdentity).toBe(source.identity);
      expect(second.sessionIdentity).toBe(first.sessionIdentity);
      expect(second.latest).toBe(first.latest);
      expect(second.position).toBe(6);
      expect(second.values[0]).toBe(first.values[0]);
      expect(second.values[1]).toBe(2);
      expect(isPositionedStoreCheckpointCurrent(first)).toBe(false);
      expect(isPositionedStoreCheckpointCurrent(second)).toBe(true);

      const imitation = Object.create(source) as typeof source;
      expect(() => capturePositionedStore(imitation, [equalFold, progress] as const)).toThrow(
        TypeError,
      );

      const foreignInput = Object.freeze({ room: "foreign" });
      const foreignGraph = positionedGraph(stream, "positioned-store-foreign");
      const foreignSource = foreignGraph.api.events(foreignInput);
      const foreignProjection = project(foreignSource, equalPolicy);
      try {
        expect(() =>
          capturePositionedStore(source, [equalFold, foreignProjection] as const),
        ).toThrow(TypeError);
      } finally {
        foreignProjection.close();
        foreignGraph.dispose();
      }
    } finally {
      equalFold.close();
      progress.close();
      graph.dispose();
    }
  });

  it("preserves omitted and present undefined bases in positioned transfer seeds", async () => {
    const cases = [
      { snapshot: false, event: 17, basePresent: false },
      { snapshot: true, event: undefined, basePresent: true },
    ] as const;

    for (const [index, scenario] of cases.entries()) {
      const provider = new PositionedProvider<number | undefined>();
      const stream = createPositionedStream<
        PositionedInput,
        number | undefined,
        string,
        number,
        object
      >({
        name: "positioned-transfer-base-" + index,
        key: (input) => input.room,
        snapshot: scenario.snapshot,
        source: provider,
      });
      const namespace = "positioned-transfer-base-" + index;
      const sourceGraph = positionedGraph(stream, namespace);
      const targetGraph = positionedGraph(stream, namespace);
      const input = Object.freeze({ room: "one" });
      let sourceSteps = 0;
      let targetSteps = 0;
      let targetRuns = 0;
      const sourcePolicy = {
        kind: "reduce" as const,
        initial: 10,
        step: (sum: number, event: number | undefined) => {
          sourceSteps += 1;
          return sum + (event ?? 0);
        },
      };
      const targetPolicy = {
        kind: "reduce" as const,
        initial: 10,
        step: (sum: number, event: number | undefined) => {
          targetSteps += 1;
          return sum + (event ?? 0);
        },
      };
      let sourceProjection: Projection<number> | undefined;
      let capturePlan: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
      let adoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
      let claim:
        | {
            readonly store: StreamStore<number | undefined>;
            readonly restoration: PositionedStoreRestoration<number | undefined, readonly [number]>;
            readonly retired: Promise<void>;
            discard(): void;
          }
        | undefined;
      let reader: object | undefined;
      let capturePublished = false;
      let adoptionPublished = false;
      try {
        const source = sourceGraph.api.events(input);
        sourceProjection = project(source, sourcePolicy);
        const sourceLanding = new Promise<void>((resolve) => {
          let stop: () => void = () => undefined;
          stop = subscribe(source.available, (available) => {
            if (!available) return;
            stop();
            resolve();
          });
        });
        provider.emitLive(input, { position: 5, value: scenario.event });
        await sourceLanding;
        const sourceValue = scenario.event === undefined ? undefined : scenario.event;
        const checkpoint = capturePositionedStore(source, [sourceProjection] as const);
        expect(checkpoint.present).toBe(true);
        expect(Object.hasOwn(checkpoint, "latest")).toBe(true);
        expect(checkpoint.latest).toBe(sourceValue);
        expect(checkpoint.hasPosition).toBe(true);
        expect(checkpoint.position).toBe(5);
        expect(checkpoint.values).toStrictEqual([10 + (scenario.event ?? 0)]);
        expect(checkpoint.resumable).toBe(true);
        expect(sourceSteps).toBe(1);
        expect(isPositionedStoreCheckpointCurrent(checkpoint)).toBe(true);

        const after = checkpoint.position;
        if (typeof after !== "number")
          throw new Error("The source checkpoint lost its exact event position");
        const base = scenario.basePresent
          ? { present: true as const, value: checkpoint.latest }
          : { present: false as const };
        const seed = {
          boundary: { kind: "event" as const, after },
          base,
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
              if (decoded === undefined)
                throw new Error("The positioned seed reference is missing");
              return decoded;
            }
            return value;
          },
          decodeFailure: (failure) => new Error(String(failure)),
        };
        const sourceRoot = computed(() => sourceProjection!.value.get());
        expect(sourceRoot.get()).toBe(10 + (scenario.event ?? 0));
        capturePlan = prepareRequiredTransferCapture({
          graphs: [sourceGraph],
          roots: [{ source: sourceRoot, construction: captureReactiveConstruction(sourceRoot) }],
          positioned: [checkpoint],
          codec,
        });
        const image = capturePlan.image;
        expect(image.version).toBe(2);
        expect(image.stores).toHaveLength(1);
        const positionedRows = image.positioned;
        if (positionedRows === undefined) throw new Error("The positioned group rows were omitted");
        expect(positionedRows).toHaveLength(1);
        const storeRow = image.stores[0]!;
        const groupRow = positionedRows[0]!;
        expect(storeRow).toMatchObject({ slot: "s0", hasValue: false });
        expect(Object.hasOwn(storeRow, "value")).toBe(false);
        expect(groupRow).toMatchObject({ slot: "p0", store: storeRow.slot, projections: 1 });
        expect(groupRow.seed).toEqual({ $ref: "p0-seed" });
        expect(decodedSeeds.get("p0-seed")).toBe(seed);
        expect(seed.boundary).toEqual({ kind: "event", after: 5 });
        expect(seed.base.present).toBe(scenario.basePresent);
        expect(Object.hasOwn(seed.base, "value")).toBe(scenario.basePresent);
        expect(seed.base.value).toBe(sourceValue);
        expect(seed.values).toStrictEqual([10 + (scenario.event ?? 0)]);
        expect(image.nodes).toEqual(
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

        const captureCommit = capturePlan.seal();
        if (captureCommit === undefined)
          throw new Error("The stable positioned base capture did not seal");
        captureCommit.publish();
        capturePublished = true;
        captureCommit.notifications.deliver();
        await capturePlan.retired;

        const targetReader = provider.reserve(input, after, undefined, seed);
        reader = targetReader;
        const receiverClaim = preparePassiveTransferClaim(
          targetGraph,
          targetGraph.api.events,
          input,
          undefined,
          { policies: [targetPolicy] as const, seed, reader: targetReader },
        );
        claim = receiverClaim;
        expect(receiverClaim.restoration.source).toBe(receiverClaim.store);
        expect(provider.opens).toHaveLength(1);
        const targetProjection = receiverClaim.restoration.projections[0];
        const targetValue = computed(() => {
          targetRuns += 1;
          return targetProjection.value.get();
        });
        adoption = prepareRequiredTransferAdoption(image, {
          graphs: [targetGraph],
          sources: [{ slot: storeRow.slot, claim: receiverClaim }],
          positioned: [{ slot: groupRow.slot, claim: receiverClaim, seed }],
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
        const activeAdoption = adoption;
        const readerSignal = provider.readerLifetime(input, targetReader, after, seed).signal;
        const readerSignalDescriptor = Object.getOwnPropertyDescriptor(readerSignal, "aborted");
        const previousClock = Date.now;
        let shadowedClockReads = 0;
        let shadowedAbortReads = 0;
        let commit: ReturnType<typeof activeAdoption.seal> = undefined;
        try {
          Object.defineProperty(readerSignal, "aborted", {
            configurable: true,
            get() {
              shadowedAbortReads += 1;
              throw new Error("The final native seal read the shadowed reader signal");
            },
          });
          Date.now = () => {
            shadowedClockReads += 1;
            throw new Error("The final native seal read the shadowed clock");
          };
          commit = activeAdoption.seal();
          if (commit !== undefined) {
            commit.publish();
            adoptionPublished = true;
          }
        } finally {
          Date.now = previousClock;
          if (readerSignalDescriptor === undefined) Reflect.deleteProperty(readerSignal, "aborted");
          else Object.defineProperty(readerSignal, "aborted", readerSignalDescriptor);
        }
        expect(shadowedClockReads).toBe(0);
        expect(shadowedAbortReads).toBe(0);
        if (commit === undefined) throw new Error("The positioned base adoption did not seal");
        commit.notifications.deliver();
        await Promise.all([adoption.retired, claim.retired]);

        expect(receiverClaim.store.available.get()).toBe(scenario.basePresent);
        expect(receiverClaim.store.value.get()).toBe(
          scenario.basePresent ? sourceValue : undefined,
        );
        expect(targetValue.get()).toBe(10 + (scenario.event ?? 0));
        expect(targetRuns).toBe(0);
        expect(targetSteps).toBe(0);
        expect(positionedFoldReceipt(targetProjection)).toBeUndefined();
        expect(provider.acknowledgements).toHaveLength(0);

        const targetLanding = new Promise<void>((resolve) => {
          let stop: () => void = () => undefined;
          stop = subscribe(targetProjection.value, (value) => {
            if (value !== 12 + (scenario.event ?? 0)) return;
            stop();
            resolve();
          });
        });
        expect(receiverClaim.restoration.activate()).toBe(receiverClaim.store);
        expect(provider.opens).toHaveLength(2);
        expect(provider.opens[1]).toMatchObject({ hasAfter: true, after: 5, hasReader: true });
        provider.emitReader(targetReader, { position: 6, value: 2 });
        await targetLanding;
        expect(targetSteps).toBe(1);
        expect(receiverClaim.store.available.get()).toBe(true);
        expect(receiverClaim.store.value.get()).toBe(2);
        const receipt = positionedFoldReceipt(targetProjection);
        expect(receipt).toMatchObject({ position: 6 });
        acknowledgePositionedFold(targetProjection, receipt!);
        expect(provider.acknowledgements).toEqual([{ reader, receipt }]);
        receiverClaim.restoration.discard();
        await receiverClaim.restoration.retired;
        expect(provider.releaseCount(targetReader)).toBe(1);
      } finally {
        if (!capturePublished) capturePlan?.discard();
        if (!adoptionPublished) {
          if (adoption !== undefined) adoption.discard();
          claim?.discard();
        } else claim?.restoration.discard();
        sourceProjection?.close();
        sourceGraph.dispose();
        targetGraph.dispose();
        if (reader !== undefined && provider.releaseCount(reader) === 0)
          provider.releaseReader(input, reader);
        if (capturePlan !== undefined)
          await capturePlan.retired.then(
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
    }
  });

  it("rechecks an earlier reader after a later provider callback invalidates it", async () => {
    const sourceProviderA = new PositionedProvider<number>();
    const sourceProviderB = new PositionedProvider<number>();
    const targetProviderA = new PositionedProvider<number>();
    const targetProviderB = new PositionedProvider<number>();
    const sourceStreamA = positionedDefinition(sourceProviderA);
    const sourceStreamB = positionedDefinition(sourceProviderB);
    const targetStreamA = positionedDefinition(targetProviderA);
    const targetStreamB = positionedDefinition(targetProviderB);
    const namespace = "positioned-reader-reentry";
    const sourceGraphA = createGraph({
      contract: defineContract({ namespace, operations: { events: sourceStreamA } }),
    });
    const sourceGraphB = createGraph({
      contract: defineContract({ namespace, operations: { events: sourceStreamB } }),
    });
    const targetGraphA = createGraph({
      contract: defineContract({ namespace, operations: { events: targetStreamA } }),
    });
    const targetGraphB = createGraph({
      contract: defineContract({ namespace, operations: { events: targetStreamB } }),
    });
    const input = Object.freeze({ room: "one" });
    const sourcePolicyA = {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number, event: number) => sum + event,
    };
    const sourcePolicyB = {
      kind: "reduce" as const,
      initial: 10,
      step: (sum: number, event: number) => sum + event,
    };
    const targetPolicyA = {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number, event: number) => sum + event,
    };
    const targetPolicyB = {
      kind: "reduce" as const,
      initial: 10,
      step: (sum: number, event: number) => sum + event,
    };
    const sourceStoreA = sourceGraphA.api.events(input);
    const sourceStoreB = sourceGraphB.api.events(input);
    const sourceProjectionA = project(sourceStoreA, sourcePolicyA);
    const sourceProjectionB = project(sourceStoreB, sourcePolicyB);
    let capture: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
    let adoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
    type PositionedClaim = {
      readonly store: StreamStore<number>;
      readonly restoration: PositionedStoreRestoration<number, readonly [number]>;
      readonly retired: Promise<void>;
      discard(): void;
    };
    let claimA: PositionedClaim | undefined;
    let claimB: PositionedClaim | undefined;
    let readerA: object | undefined;
    let readerB: object | undefined;
    let capturePublished = false;
    try {
      const landedA = until(sourceProjectionA.value, (value) => value === 2);
      const landedB = until(sourceProjectionB.value, (value) => value === 14);
      sourceProviderA.emitLive(input, { position: 5, value: 2 });
      sourceProviderB.emitLive(input, { position: 7, value: 4 });
      await Promise.all([landedA, landedB]);
      const checkpointA = capturePositionedStore(sourceStoreA, [sourceProjectionA] as const);
      const checkpointB = capturePositionedStore(sourceStoreB, [sourceProjectionB] as const);
      expect(checkpointA.position).toBe(5);
      expect(checkpointB.position).toBe(7);
      expect(checkpointA.resumable && checkpointB.resumable).toBe(true);
      const seedA = {
        boundary: { kind: "event" as const, after: 5 },
        base: { present: true as const, value: 2 },
        values: checkpointA.values,
      };
      const seedB = {
        boundary: { kind: "event" as const, after: 7 },
        base: { present: true as const, value: 4 },
        values: checkpointB.values,
      };
      const seedIds = new Map<string, unknown>([
        ["p0-seed", seedA],
        ["p1-seed", seedB],
      ]);
      let encodedGroup = 0;
      const codec: RequiredTransferCodec = {
        encodeValue: (value) => {
          if (
            value !== null &&
            typeof value === "object" &&
            Object.hasOwn(value, "boundary") &&
            Object.hasOwn(value, "base")
          ) {
            const id = encodedGroup++ === 0 ? "p0-seed" : "p1-seed";
            return { $ref: id } as ClosedEncodedValue;
          }
          return value as ClosedEncodedValue;
        },
        encodeFailure: (cause) => String(cause),
        decodeValue: (value) => {
          if (value !== null && typeof value === "object" && Object.hasOwn(value, "$ref")) {
            const id = (value as { readonly $ref?: unknown }).$ref;
            if (typeof id === "string" && seedIds.has(id)) return seedIds.get(id);
          }
          return value;
        },
        decodeFailure: (failure) => new Error(String(failure)),
      };
      const sourceRootA = computed(() => sourceProjectionA.value.get());
      const sourceRootB = computed(() => sourceProjectionB.value.get());
      expect([sourceRootA.get(), sourceRootB.get()]).toEqual([2, 14]);
      capture = prepareRequiredTransferCapture({
        graphs: [sourceGraphA, sourceGraphB],
        roots: [
          { source: sourceRootA, construction: captureReactiveConstruction(sourceRootA) },
          { source: sourceRootB, construction: captureReactiveConstruction(sourceRootB) },
        ],
        positioned: [checkpointA, checkpointB],
        codec,
      });
      const image = capture.image;
      const groups = image.positioned;
      if (groups === undefined || groups.length !== 2)
        throw new Error("Both positioned reader groups were not captured");
      expect(groups.map((group) => group.slot)).toEqual(["p0", "p1"]);
      expect(groups.map((group) => group.seed)).toEqual([{ $ref: "p0-seed" }, { $ref: "p1-seed" }]);
      const sourceRowA = image.stores.find((row) => row.graph === 0);
      const sourceRowB = image.stores.find((row) => row.graph === 1);
      if (sourceRowA === undefined || sourceRowB === undefined)
        throw new Error("The same-key source Stores were not kept independent");
      expect([groups[0]!.store, groups[1]!.store]).toEqual([sourceRowA.slot, sourceRowB.slot]);
      expect(seedIds.get("p0-seed")).toBe(seedA);
      expect(seedIds.get("p1-seed")).toBe(seedB);
      const captureCommit = capture.seal();
      if (captureCommit === undefined)
        throw new Error("The two-reader source capture did not seal");
      captureCommit.publish();
      capturePublished = true;
      captureCommit.notifications.deliver();
      await capture.retired;

      const readerALocal = targetProviderA.reserve(input, 5, undefined, seedA);
      readerA = readerALocal;
      const receiverA = preparePassiveTransferClaim(
        targetGraphA,
        targetGraphA.api.events,
        input,
        undefined,
        { policies: [targetPolicyA] as const, seed: seedA, reader: readerALocal },
      );
      claimA = receiverA;
      const readerBLocal = targetProviderB.reserve(input, 7, undefined, seedB);
      readerB = readerBLocal;
      const receiverB = preparePassiveTransferClaim(
        targetGraphB,
        targetGraphB.api.events,
        input,
        undefined,
        { policies: [targetPolicyB] as const, seed: seedB, reader: readerBLocal },
      );
      claimB = receiverB;
      const lifetimeA = targetProviderA.readerLifetime(input, readerALocal, 5, seedA);
      expect(lifetimeA.signal.aborted).toBe(false);
      expect(targetProviderA.releaseCount(readerALocal)).toBe(0);
      let reentered = false;
      targetProviderB.currentCheckHook = () => {
        if (reentered) return;
        reentered = true;
        targetProviderA.abortReader(readerALocal, new Error("B validation invalidated A"));
      };
      const targetRootA = computed(() => receiverA.restoration.projections[0]!.value.get());
      const targetRootB = computed(() => receiverB.restoration.projections[0]!.value.get());
      let preparationError: unknown;
      try {
        adoption = prepareRequiredTransferAdoption(image, {
          graphs: [targetGraphA, targetGraphB],
          sources: [
            { slot: sourceRowA.slot, claim: receiverA },
            { slot: sourceRowB.slot, claim: receiverB },
          ],
          positioned: [
            { slot: groups[0]!.slot, claim: receiverA, seed: seedA },
            { slot: groups[1]!.slot, claim: receiverB, seed: seedB },
          ],
          reactive: [
            {
              slot: image.roots[0]!,
              target: targetRootA,
              construction: captureReactiveConstruction(targetRootA),
            },
            {
              slot: image.roots[1]!,
              target: targetRootB,
              construction: captureReactiveConstruction(targetRootB),
            },
          ],
          codec,
          mappedStateWrites: [],
        });
      } catch (cause) {
        preparationError = cause;
      }
      expect(reentered).toBe(true);
      expect(targetProviderB.currentChecks.length).toBeGreaterThan(0);
      expect(lifetimeA.signal.aborted).toBe(true);
      expect(targetProviderA.opens).toHaveLength(0);
      expect(targetProviderB.opens).toHaveLength(0);
      if (preparationError !== undefined) {
        expect(preparationError).toBeInstanceOf(TypeError);
      } else {
        if (adoption === undefined) throw new Error("The stale aggregate plan was not retained");
        let commit: ReturnType<typeof adoption.seal> | undefined;
        let sealError: unknown;
        try {
          commit = adoption.seal();
        } catch (cause) {
          sealError = cause;
        }
        expect(commit).toBeUndefined();
        if (sealError !== undefined) expect(sealError).toBeInstanceOf(TypeError);
        adoption.discard();
      }
      expect(() => receiverA.store.value.get()).toThrow(TypeError);
      expect(() => receiverB.store.value.get()).toThrow(TypeError);

      receiverA.discard();
      receiverB.discard();
      await Promise.all([
        receiverA.retired,
        receiverA.restoration.retired,
        receiverB.retired,
        receiverB.restoration.retired,
      ]);
      expect(targetProviderA.releaseCount(readerALocal)).toBe(1);
      expect(targetProviderB.releaseCount(readerBLocal)).toBe(1);
    } finally {
      adoption?.discard();
      claimA?.discard();
      claimB?.discard();
      sourceProjectionA.close();
      sourceProjectionB.close();
      sourceGraphA.dispose();
      sourceGraphB.dispose();
      targetGraphA.dispose();
      targetGraphB.dispose();
      if (
        readerA !== undefined &&
        claimA === undefined &&
        targetProviderA.releaseCount(readerA) === 0
      )
        targetProviderA.source.releaseReader(input, readerA);
      if (
        readerB !== undefined &&
        claimB === undefined &&
        targetProviderB.releaseCount(readerB) === 0
      )
        targetProviderB.source.releaseReader(input, readerB);
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
      if (claimA !== undefined) {
        await claimA.retired.then(
          () => undefined,
          () => undefined,
        );
        await claimA.restoration.retired.then(
          () => undefined,
          () => undefined,
        );
      }
      if (claimB !== undefined) {
        await claimB.retired.then(
          () => undefined,
          () => undefined,
        );
        await claimB.restoration.retired.then(
          () => undefined,
          () => undefined,
        );
      }
    }
  });

  it("marks a genuine Store group nonresumable when a required projection joined after its boundary", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const graph = positionedGraph(stream, "positioned-store-incoherent");
    const input = Object.freeze({ room: "one" });
    const source = graph.api.events(input);
    const firstPolicy = {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number) => sum + 1,
    };
    const first = project(source, firstPolicy);
    let late: Projection<number> | undefined;
    try {
      const firstFold = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(first.value, (value) => {
          if (value !== 1) return;
          stop();
          resolve();
        });
      });
      provider.emitLive(input, { position: 9, value: 2 });
      await firstFold;
      const lateProjection = project(source, firstPolicy);
      late = lateProjection;

      const captured = capturePositionedStore(source, [first, lateProjection] as const);
      expect(captured.present).toBe(true);
      expect(captured.hasPosition).toBe(true);
      expect(captured.resumable).toBe(false);
    } finally {
      first.close();
      late?.close();
      graph.dispose();
    }
  });

  it("rejects a Store restore seed whose own latest event field was removed", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const graph = positionedGraph(stream, "positioned-store-seed-presence");
    const input = Object.freeze({ room: "one" });
    const source = graph.api.events(input);
    const policy = {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number) => sum + 1,
    };
    const projection = project(source, policy);
    let reader: object | undefined;
    try {
      const folded = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(projection.value, (value) => {
          if (value !== 1) return;
          stop();
          resolve();
        });
      });
      provider.emitLive(input, { position: 5, value: 2 });
      await folded;
      const checkpoint = capturePositionedStore(source, [projection] as const);
      if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
        throw new Error("The actual Store event did not yield a position");
      const malformedSeed = {
        after: checkpoint.position,
        latest: checkpoint.latest,
        values: checkpoint.values,
      };
      delete (malformedSeed as { latest?: number }).latest;
      expect(Object.hasOwn(malformedSeed, "latest")).toBe(false);
      reader = provider.reserve(input, checkpoint.position, undefined, malformedSeed);

      expect(() =>
        preparePositionedStoreRestore(
          graph.api.events,
          input,
          [policy] as const,
          malformedSeed,
          reader!,
        ),
      ).toThrow(TypeError);
      expect(provider.opens).toHaveLength(1);
      expect(provider.releaseCount(reader)).toBe(0);
    } finally {
      projection.close();
      graph.dispose();
      if (reader !== undefined && provider.releaseCount(reader) === 0)
        provider.releaseReader(input, reader);
    }
  });

  it("rejects overlong accumulated and wrong-count Store seeds with valid provider reservations", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const sourceGraph = positionedGraph(stream, "positioned-store-seed-source");
    const targetGraph = positionedGraph(stream, "positioned-store-seed-target");
    const input = Object.freeze({ room: "one" });
    const source = sourceGraph.api.events(input);
    const accumulationPolicy: AccumulatePolicy = {
      kind: "accumulate",
      max: 1,
      onOverflow: "drop-oldest",
    };
    const countPolicy = {
      kind: "reduce" as const,
      initial: 0,
      step: (count: number) => count + 1,
    };
    const accumulation = project(source, accumulationPolicy);
    const count = project(source, countPolicy);
    let overlongReader: object | undefined;
    let wrongCountReader: object | undefined;

    try {
      const folded = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(count.value, (value) => {
          if (value !== 1) return;
          stop();
          resolve();
        });
      });
      provider.emitLive(input, { position: 5, value: 2 });
      await folded;

      const checkpoint = capturePositionedStore(source, [accumulation, count] as const);
      if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
        throw new Error("The actual source Store did not capture its positioned event");
      const after: number = checkpoint.position;
      const policies = [accumulationPolicy, countPolicy] as const;

      const overlongSeed = {
        after,
        latest: checkpoint.latest,
        values: [[...checkpoint.values[0], 3], checkpoint.values[1]],
      } as unknown as PositionedStoreRestoreSeed<
        number,
        number,
        readonly [readonly number[], number]
      >;
      overlongReader = provider.reserve(input, after, undefined, overlongSeed);
      expect(() =>
        preparePositionedStoreRestore(
          targetGraph.api.events,
          input,
          policies,
          overlongSeed,
          overlongReader!,
        ),
      ).toThrow(TypeError);
      expect(provider.opens).toHaveLength(1);
      expect(provider.releaseCount(overlongReader)).toBe(0);
      provider.releaseReader(input, overlongReader);
      expect(provider.releaseCount(overlongReader)).toBe(1);

      const wrongCountSeed = {
        after,
        latest: checkpoint.latest,
        values: [checkpoint.values[0]],
      } as unknown as PositionedStoreRestoreSeed<
        number,
        number,
        readonly [readonly number[], number]
      >;
      wrongCountReader = provider.reserve(input, after, undefined, wrongCountSeed);
      expect(() =>
        preparePositionedStoreRestore(
          targetGraph.api.events,
          input,
          policies,
          wrongCountSeed,
          wrongCountReader!,
        ),
      ).toThrow(TypeError);
      expect(provider.opens).toHaveLength(1);
      expect(provider.releaseCount(wrongCountReader)).toBe(0);
      provider.releaseReader(input, wrongCountReader);
      expect(provider.releaseCount(wrongCountReader)).toBe(1);
    } finally {
      accumulation.close();
      count.close();
      sourceGraph.dispose();
      targetGraph.dispose();
      if (overlongReader !== undefined && provider.releaseCount(overlongReader) === 0)
        provider.releaseReader(input, overlongReader);
      if (wrongCountReader !== undefined && provider.releaseCount(wrongCountReader) === 0)
        provider.releaseReader(input, wrongCountReader);
    }
  });

  it("rejects a reader Store equals option that conflicts with its positioned declaration", async () => {
    const provider = new PositionedProvider();
    const declaredEquals = (left: number | undefined, right: number | undefined) =>
      Object.is(left, right);
    const callerEquals = (left: number | undefined, right: number | undefined) => left === right;
    const stream = createPositionedStream<PositionedInput, number, string, number, object>({
      name: "positioned-store-equals-conflict",
      key: (input) => input.room,
      source: provider,
      equals: declaredEquals,
    });
    const sourceGraph = positionedGraph(stream, "positioned-store-equals-source");
    const targetGraph = positionedGraph(stream, "positioned-store-equals-target");
    const input = Object.freeze({ room: "one" });
    const source = sourceGraph.api.events(input);
    const policy = {
      kind: "reduce" as const,
      initial: 0,
      step: (count: number) => count + 1,
    };
    const projection = project(source, policy);
    let reader: object | undefined;

    try {
      const folded = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(projection.value, (value) => {
          if (value !== 1) return;
          stop();
          resolve();
        });
      });
      provider.emitLive(input, { position: 5, value: 2 });
      await folded;

      const checkpoint = capturePositionedStore(source, [projection] as const);
      if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
        throw new Error("The actual source Store did not capture its positioned event");
      const after: number = checkpoint.position;
      const seed = { after, latest: checkpoint.latest, values: checkpoint.values };
      reader = provider.reserve(input, after, undefined, seed);

      expect(() =>
        preparePositionedStoreRestore(
          targetGraph.api.events,
          input,
          [policy] as const,
          seed,
          reader!,
          { equals: callerEquals },
        ),
      ).toThrow(Fault);
      expect(provider.opens).toHaveLength(1);
      expect(provider.releaseCount(reader)).toBe(0);
      provider.releaseReader(input, reader);
      expect(provider.releaseCount(reader)).toBe(1);
    } finally {
      projection.close();
      sourceGraph.dispose();
      targetGraph.dispose();
      if (reader !== undefined && provider.releaseCount(reader) === 0)
        provider.releaseReader(input, reader);
    }
  });

  it("resets a terminal positioned tuple without making its old cursor current", async () => {
    const provider = new PositionedProvider();
    const stream = positionedDefinition(provider);
    const graph = positionedGraph(stream, "positioned-terminal-reset");
    const input = Object.freeze({ room: "one" });
    const projection = projectPositioned(graph.api.events, input, {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number, item: number) => sum + item,
    });
    try {
      provider.emitLive(input, { position: 7, value: 3 });
      await flush();
      const beforeClose = capturePositionedProjection<number, number>(projection);
      projection.close();
      expect(projection.status.get()).toBe("closed");
      expect(projection.value.get()).toBe(3);
      await graph.reset();
      expect(projection.status.get()).toBe("closed");
      expect(projection.value.get()).toBe(0);
      expect(projection.error.get()).toBeUndefined();
      expect(isPositionedCheckpointCurrent(beforeClose)).toBe(false);
      const afterReset = capturePositionedProjection<number, number>(projection);
      expect(afterReset).toMatchObject({
        status: "closed",
        value: 0,
        error: undefined,
        hasPosition: false,
        position: undefined,
        resumable: false,
      });
      expect(isPositionedCheckpointCurrent(afterReset)).toBe(true);
      expect(provider.opens).toHaveLength(1);
    } finally {
      projection.close();
      graph.dispose();
    }
  });

  it("closes projections on graph disposal", () => {
    const backend = testBackend();
    const stream = numberStream(backend, "projection-dispose");
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 2 });
    const call = backend.calls[0]!;
    graph.dispose();
    expect(call.aborted).toBe(true);
    expect(projection.status.get()).toBe("closed");
    projection.close();
  });
});
