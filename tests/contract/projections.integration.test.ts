import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  Fault,
  createGraph,
  defineContract,
  createStream,
  createPositionedStream,
  createQuery,
  project,
  projectPositioned,
  capturePositionedProjection,
  isPositionedCheckpointCurrent,
  preparePositionedRestore,
  preparePositionedSeedRestore,
  positionedFoldReceipt,
  acknowledgePositionedFold,
  subscribe,
  type Graph,
  type StreamDefinition,
  type PositionedStreamSource,
  type PositionedStreamEvent,
  type PositionedStreamOpenOptions,
  type PositionedProjectionCheckpoint,
  type PositionedFoldReceipt,
} from "../../src/index.ts";
import { testBackend, type TestBackend } from "../../src/testing.ts";

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
type PositionedEvent = PositionedStreamEvent<number, number>;
type PositionedCheckpoint = PositionedProjectionCheckpoint<unknown, number>;

function positionedChannel() {
  const queued: PositionedEvent[] = [];
  const waiters: Array<(result: IteratorResult<PositionedEvent>) => void> = [];
  let ended = false;
  const emit = (event: PositionedEvent): void => {
    const waiter = waiters.shift();
    if (waiter !== undefined) waiter({ done: false, value: event });
    else queued.push(event);
  };
  const source: AsyncIterable<PositionedEvent> = {
    [Symbol.asyncIterator](): AsyncIterator<PositionedEvent> {
      return {
        next: () => {
          const event = queued.shift();
          if (event !== undefined) return Promise.resolve({ done: false as const, value: event });
          if (ended) return Promise.resolve({ done: true as const, value: undefined });
          return new Promise<IteratorResult<PositionedEvent>>((resolve) => waiters.push(resolve));
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

class PositionedProvider implements PositionedStreamSource<PositionedInput, number, number, object> {
  readonly opens: Array<{ room: string; hasAfter: boolean; after: number | undefined; hasReader: boolean }> = [];
  readonly currentChecks: Array<{ after: number; seed: unknown }> = [];
  readonly acknowledgements: Array<{ reader: object; receipt: PositionedFoldReceipt<number> }> = [];
  validationOverride: unknown = undefined;
  readonly #reservations = new WeakMap<object, { input: PositionedInput; after: number; checkpoint: PositionedCheckpoint | undefined; seed: unknown }>();
  readonly #readerEmitters = new WeakMap<object, (event: PositionedEvent) => void>();
  readonly #liveEmitters = new Map<string, (event: PositionedEvent) => void>();
  readonly #releaseCounts = new WeakMap<object, number>();

  reserve(input: PositionedInput, after: number, checkpoint: PositionedCheckpoint | undefined, seed: unknown): object {
    const reader = Object.freeze({});
    this.#reservations.set(reader, { input, after, checkpoint, seed });
    return reader;
  }

  emitLive(input: PositionedInput, event: PositionedEvent): void {
    const emit = this.#liveEmitters.get(input.room);
    if (emit === undefined) throw new Error("The live provider opening is missing");
    emit(event);
  }

  emitReader(reader: object, event: PositionedEvent): void {
    const emit = this.#readerEmitters.get(reader);
    if (emit === undefined) throw new Error("The reserved provider opening is missing");
    emit(event);
  }

  releaseCount(reader: object): number {
    return this.#releaseCounts.get(reader) ?? 0;
  }

  open(input: PositionedInput, options: PositionedStreamOpenOptions<number, object>): AsyncIterable<PositionedEvent> {
    const channel = positionedChannel();
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

  isReaderCurrent(
    input: PositionedInput,
    reader: object,
    after: number,
    seed: unknown,
  ): boolean {
    this.currentChecks.push({ after, seed });
    const reservation = this.#reservations.get(reader);
    return (
      reservation !== undefined &&
      reservation.input === input &&
      reservation.after === after &&
      reservation.seed === seed
    );
  }

  acknowledge(_input: PositionedInput, reader: object, receipt: PositionedFoldReceipt<number>): void {
    this.acknowledgements.push({ reader, receipt });
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

function positionedGraph(stream: ReturnType<typeof positionedDefinition>, namespace: string): Graph {
  return createGraph({
    contract: defineContract({ namespace, operations: { events: stream } }),
    idleMs: 10,
  });
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
    const policy = { kind: "reduce" as const, initial: 0, step: (sum: number, item: number) => sum + item };
    const source = projectPositioned(sourceGraph.api.events, input, policy);
    try {
      expect(provider.opens[0]?.hasAfter).toBe(false);
      provider.emitLive(input, { position: 5, value: 2 });
      await flush();
      const checkpoint = capturePositionedProjection<number, number>(source);
      expect(checkpoint).toMatchObject({ value: 2, hasPosition: true, position: 5, resumable: true });
      expect(isPositionedCheckpointCurrent(checkpoint)).toBe(true);

      const seed = 20;
      const reader = provider.reserve(input, 5, checkpoint, seed);
      const restore = preparePositionedRestore(
        targetGraph.api.events,
        input,
        checkpoint,
        reader,
        { value: seed },
      );
      expect(provider.opens).toHaveLength(1);
      expect(restore.projection.value.get()).toBe(seed);
      expect(capturePositionedProjection<number, number>(restore.projection)).toMatchObject({
        value: seed, hasPosition: true, position: 5, resumable: true,
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
      expect(provider.acknowledgements[0]?.receipt).toBe(receipt);
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
      kind: "reduce", initial: 0, step: (sum, item) => sum + item,
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
        value: decodedValue, hasPosition: true, position: 5, revision: 0, resumable: true,
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
      expect(providerB.acknowledgements[0]?.receipt).toBe(receipt);
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
      expect(() => preparePositionedRestore(graph.api.events, input, afterFailure, {}, { value: afterFailure.value })).toThrow(TypeError);
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
      contract: defineContract({ namespace: "positioned-observer-failure", operations: { events: stream } }),
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
      expect(first).toMatchObject({ value: 1, hasPosition: true, position: 5, revision: 1, resumable: true });
      provider.emitLive(input, { position: 6, value: 3 });
      await flush();
      const second = capturePositionedProjection<number, number>(projection);
      expect(second).toMatchObject({ value: 1, hasPosition: true, position: 6, revision: 2, resumable: true });
      expect(isPositionedCheckpointCurrent(first)).toBe(false);
      expect(positionedFoldReceipt(projection)).toMatchObject({ position: 6, revision: 2 });
      expect(faults).toEqual([notificationFailure]);
    } finally {
      stop();
      projection.close();
      graph.dispose();
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
        status: "closed", value: 0, error: undefined, hasPosition: false,
        position: undefined, resumable: false,
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
