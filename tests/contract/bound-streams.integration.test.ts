import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  adopt,
  bindStream,
  createBindingContext,
  createGraph,
  createStream,
  createPositionedStream,
  projectPositioned,
  project,
  capturePositionedStore,
  preparePositionedStoreRestore,
  capturePositionedProjection,
  preparePositionedRestore,
  isPositionedCheckpointCurrent,
  type PositionedRestoration,
  defineContract,
  Fault,
  inspectBoundAddress,
  peekStore,
  snapshot,
  storeKey,
  subscribe,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

type Input = { readonly room: string };
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

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

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

function fixture() {
  const backend = testBackend();
  const signals: AbortSignal[] = [];
  let keyCalls = 0;
  const open = backend.stream<Input, number | undefined>("events");
  const stream = createStream({
    name: "events",
    key: (input: Input) => {
      keyCalls += 1;
      return input.room;
    },
    open: (input, options) => {
      signals.push(options.abortSignal);
      return open(input, options);
    },
  });
  const twin = createStream({
    name: "twin",
    key: (input: Input) => input.room,
    open: backend.stream<Input, number | undefined>("twin"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-streams", operations: { stream, twin } }),
    idleMs: 100,
  });
  const context = createBindingContext(graph);
  const binding = bindStream(graph, stream, graph.api.stream, { context });
  return { backend, stream, twin, graph, context, binding, signals, keyCalls: () => keyCalls };
}

describe("genuine context-bound stream sessions", () => {
  it("registers inertly, checks actual member provenance and isolates equal raw/context keys", () => {
    const f = fixture();
    const foreign = createGraph({
      contract: defineContract({ namespace: "foreign", operations: { stream: f.stream } }),
    });
    try {
      expect(f.keyCalls()).toBe(0);
      expect(f.backend.calls).toHaveLength(0);
      expect(bindStream(f.graph, f.stream, f.graph.api.stream, { context: f.context })).toBe(
        f.binding,
      );
      expect(() =>
        bindStream(f.graph, f.stream, foreign.api.stream, { context: f.context }),
      ).toThrow(TypeError);
      const imitation = (input: Input) => f.graph.api.stream(input);
      expect(() => bindStream(f.graph, f.stream, imitation, { context: f.context })).toThrow(
        TypeError,
      );
      const a = f.binding.member({ room: "Room/é" }, { initial: 1 });
      const joined = f.binding.member({ room: "Room/é" });
      expect(f.binding.session(a)).toBe(f.binding.session(joined));
      const openedSignal = f.signals[0];
      if (openedSignal === undefined) throw new Error("Expected the bound source to open");
      expect(f.binding.session(a).abortSignal).toBe(openedSignal);
      expect(f.backend.calls).toHaveLength(1);
      const other = bindStream(f.graph, f.stream, f.graph.api.stream, {
        context: createBindingContext(f.graph),
      });
      const b = other.member({ room: "Room/é" }, { initial: 2 });
      const twin = bindStream(f.graph, f.twin, f.graph.api.twin, { context: f.context });
      const c = twin.member({ room: "Room/é" }, { initial: 6 });
      expect(c.identity).not.toBe(a.identity);
      expect(() => bindStream(f.graph, f.twin, f.graph.api.stream, { context: f.context })).toThrow(
        TypeError,
      );
      const raw = f.graph.api.stream({ room: "Room/é" }, { initial: 3 });
      const expectedKey = storeKey("Room/é");
      expect([a.key, b.key, raw.key]).toEqual([expectedKey, expectedKey, expectedKey]);
      expect(a.identity).not.toBe(b.identity);
      expect(a.identity).not.toBe(raw.identity);
      f.graph.at("Room/é").set(90);
      a.set(4);
      expect([
        a.value.get(),
        joined.value.get(),
        b.value.get(),
        c.value.get(),
        raw.value.get(),
      ]).toEqual([4, 4, 2, 6, 90]);
      expect(() => f.binding.session(b)).toThrow(TypeError);
      expect(() => f.binding.session(Object.create(a))).toThrow(TypeError);
      expect(Object.getPrototypeOf(a)).toBe(Object.prototype);
    } finally {
      f.graph.dispose();
      foreign.dispose();
    }
  });

  it.each([5, undefined])(
    "seals an actual %s prefix before end and keeps old facets fixed across B",
    async (value) => {
      const f = fixture();
      let stop: (() => void) | undefined;
      try {
        const a = f.binding.member({ room: "one" });
        const ticketA = f.binding.session(a);
        const observed: Array<number | undefined> = [];
        stop = subscribe(a, (item) => observed.push(item));
        f.backend.emit("events", value);
        f.backend.end("events");
        const receipt = await ticketA.detached;
        expect(receipt.session).toBe(ticketA.identity);
        expect(receipt.status).toBe("closed");
        expect(receipt.terminalCapture).toMatchObject({
          kind: "captured",
          capture: { kind: "stable", present: true, value },
        });
        expect(observed).toEqual([value]);
        expect(a.available.get()).toBe(true);
        expect(a.value.get()).toBe(value);
        expect(a.pending.get()).toBe(false);
        expect(a.status.get()).toBe("closed");
        expect(() => peekStore(a)).toThrow(TypeError);
        const b = f.binding.member({ room: "one" });
        const ticketB = f.binding.session(b);
        expect(b.identity).toBe(a.identity);
        expect(ticketB.identity).not.toBe(ticketA.identity);
        expect(f.binding.session(a)).toBe(ticketA);
        expect([b.available.get(), b.value.get(), b.pending.get()]).toEqual([
          false,
          undefined,
          true,
        ]);
        let updater = false;
        expect(() =>
          a.update(() => {
            updater = true;
            return 99;
          }),
        ).toThrow(TypeError);
        expect(updater).toBe(false);
        expect(() => a.set(99)).toThrow(TypeError);
        const landed = Promise.withResolvers<void>();
        const stopB = subscribe(b, (item) => {
          if (item === 7) landed.resolve();
        });
        f.backend.emit("events", 7);
        await landed.promise;
        expect(b.value.get()).toBe(7);
        expect(a.value.get()).toBe(value);
        expect(observed).toEqual([value]);
        stopB();
      } finally {
        stop?.();
        f.graph.dispose();
      }
    },
  );

  it("reports true absence for a source that ended without a value", async () => {
    const f = fixture();
    try {
      const source = f.binding.member({ room: "one" });
      f.backend.end("events");
      const receipt = await f.binding.session(source).detached;
      expect(receipt.terminalCapture).toMatchObject({
        kind: "captured",
        capture: { kind: "stable", present: false },
      });
      expect(source.available.get()).toBe(false);
      expect(source.value.get()).toBeUndefined();
    } finally {
      f.graph.dispose();
    }
  });

  it.each(["open", "iterator", "pre-abort"] as const)(
    "issues total real metadata for synchronous %s retirement",
    async (phase) => {
      let opened = 0;
      const stream = createStream({
        name: "synchronous",
        key: () => "one",
        open: (_input: void): AsyncIterable<number> => {
          opened += 1;
          if (phase === "open") throw undefined;
          return {
            [Symbol.asyncIterator]() {
              throw undefined;
            },
          };
        },
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "synchronous-bound", operations: { stream } }),
      });
      try {
        const binding = bindStream(graph, stream, graph.api.stream, {
          context: createBindingContext(graph),
        });
        const caller = new AbortController();
        const callerReason = new Error("caller cancelled before acquisition");
        if (phase === "pre-abort") caller.abort(callerReason);
        const source = binding.member(undefined, { abortSignal: caller.signal });
        const ticket = binding.session(source);
        const receipt = await ticket.detached;
        expect(receipt.session).toBe(ticket.identity);
        expect(receipt.status).toBe(phase === "pre-abort" ? "closed" : "failed");
        if (phase === "pre-abort") {
          expect(ticket.abortSignal.aborted).toBe(true);
          expect(ticket.abortSignal.reason).toBe(callerReason);
          expect(receipt.reason).toBeUndefined();
        }
        if (phase !== "pre-abort") expect(receipt.reason).toBeUndefined();
        expect(receipt.terminalCapture).toMatchObject({
          kind: "captured",
          capture: { kind: "stable", present: false },
        });
        expect(opened).toBe(phase === "pre-abort" ? 0 : 1);
        expect(source.pending.get()).toBe(false);
      } finally {
        graph.dispose();
      }
    },
  );

  it("forwards a later caller abort reason to the actual source signal while preserving native detachment facts", async () => {
    const f = fixture();
    try {
      const caller = new AbortController();
      const source = f.binding.member({ room: "one" }, { abortSignal: caller.signal });
      const ticket = f.binding.session(source);
      const openedSignal = f.signals[0];
      if (openedSignal === undefined) throw new Error("Expected the bound source to open");
      expect(ticket.abortSignal).toBe(openedSignal);
      expect(ticket.abortSignal.aborted).toBe(false);
      const reason = new Error("caller cancelled an open source");
      caller.abort(reason);
      f.graph.cancelPending(new Error("later graph cancellation"));
      const receipt = await ticket.detached;
      expect(ticket.abortSignal.aborted).toBe(true);
      expect(ticket.abortSignal.reason).toBe(reason);
      expect(f.backend.calls).toHaveLength(1);
      expect(receipt.session).toBe(ticket.identity);
      expect(receipt.status).toBe("closed");
      expect(receipt.reason).toBeUndefined();
      expect(receipt.terminalCapture).toMatchObject({
        kind: "captured",
        capture: { kind: "stable", present: false },
      });
    } finally {
      f.graph.dispose();
    }
  });

  it.each(["reset", "context", "graph"] as const)(
    "keeps the invocation ticket when %s revokes during open",
    async (mode) => {
      let revoke = (): void => undefined;
      let resets: Promise<void> | undefined;
      let opened = 0;
      const stream = createStream({
        name: "revoke-open",
        key: () => "one",
        open: (_input: void): AsyncIterable<number> => {
          opened += 1;
          revoke();
          return {
            async *[Symbol.asyncIterator]() {
              yield 99;
            },
          };
        },
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "revoke-open", operations: { stream } }),
      });
      const context = createBindingContext(graph);
      const binding = bindStream(graph, stream, graph.api.stream, { context });
      revoke = () => {
        if (mode === "reset") resets = graph.reset();
        if (mode === "context") context.dispose();
        if (mode === "graph") graph.dispose();
      };
      try {
        const source = binding.member();
        const ticket = binding.session(source);
        const receipt = await ticket.detached;
        await resets;
        expect(opened).toBe(1);
        expect(receipt.session).toBe(ticket.identity);
        expect(receipt.status).toBe("closed");
        if (mode === "reset") {
          expect(source.available.get()).toBe(false);
          expect(source.status.get()).toBe("closed");
        } else {
          expect(receipt.terminalCapture).toMatchObject({
            kind: "fault",
            cause: { kind: "disposed" },
          });
          expect(() => source.value.get()).toThrow(Fault);
        }
      } finally {
        graph.dispose();
      }
    },
  );

  it("holds B through native retirement while logical detachment does not await physical return", async () => {
    const physical = [
      Promise.withResolvers<IteratorResult<number>>(),
      Promise.withResolvers<IteratorResult<number>>(),
    ];
    const firstNext = Promise.withResolvers<IteratorResult<number>>();
    const secondNext = Promise.withResolvers<IteratorResult<number>>();
    const returned = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    let opens = 0;
    let returnCalls = 0;
    let reentry: unknown;
    let acquire = (): void => undefined;
    const stream = createStream({
      name: "slow-return",
      key: () => "one",
      open: (_input: void): AsyncIterable<number> => {
        opens += 1;
        const own = opens;
        return {
          [Symbol.asyncIterator]() {
            let nextCalls = 0;
            return {
              next: () => {
                nextCalls += 1;
                if (own === 1)
                  return nextCalls === 1
                    ? firstNext.promise
                    : Promise.resolve({ done: true as const, value: undefined });
                return secondNext.promise;
              },
              return: () => {
                returnCalls += 1;
                returned[own - 1]!.resolve();
                try {
                  acquire();
                } catch (cause) {
                  reentry = cause;
                }
                return physical[own - 1]!.promise;
              },
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "slow-return", operations: { stream } }),
    });
    const context = createBindingContext(graph);
    try {
      const binding = bindStream(graph, stream, graph.api.stream, {
        context,
      });
      const a = binding.member(undefined, { initial: 1 });
      acquire = () => {
        binding.member();
      };
      graph.cancelPending();
      await returned[0]!.promise;
      const receipt = await binding.session(a).detached;
      expect(reentry).toBeInstanceOf(TypeError);
      expect(opens).toBe(1);
      expect(receipt.terminalCapture).toMatchObject({
        kind: "captured",
        capture: { present: true, value: 1 },
      });
      const b = binding.member();
      expect(opens).toBe(2);
      expect([a.value.get(), b.value.get(), b.available.get()]).toEqual([1, undefined, false]);
      const delivery = firstNext.promise.then(() => undefined);
      firstNext.resolve({ done: false, value: 77 });
      await delivery;
      expect(b.value.get()).toBeUndefined();
      physical[0]!.resolve({ done: true, value: undefined });
      await physical[0]!.promise;
      expect(a.value.get()).toBe(1);

      const retirement = context.retire();
      expect(context.retire()).toBe(retirement);
      await expectPending(retirement);
      expect(returnCalls).toBe(2);
      secondNext.resolve({ done: true, value: undefined });
      physical[1]!.resolve({ done: true, value: undefined });
      await retirement;
    } finally {
      acquire = () => undefined;
      firstNext.resolve({ done: true, value: undefined });
      secondNext.resolve({ done: true, value: undefined });
      physical[0]!.resolve({ done: true, value: undefined });
      physical[1]!.resolve({ done: true, value: undefined });
      graph.dispose();
    }
  });

  it("retires bound carriers on reset while a live raw source retains native automatic replay", async () => {
    const backend = testBackend();
    const boundDefinition = createStream({
      name: "bound",
      key: () => "same",
      open: backend.stream<void, number>("bound"),
    });
    const rawDefinition = createStream({
      name: "raw",
      key: () => "raw",
      open: backend.stream<void, number>("raw"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-kinds",
        operations: { boundDefinition, rawDefinition },
      }),
    });
    let stopA: (() => void) | undefined;
    let stopRaw: (() => void) | undefined;
    try {
      const binding = bindStream(graph, boundDefinition, graph.api.boundDefinition, {
        context: createBindingContext(graph),
      });
      const a = binding.member();
      const raw = graph.api.rawDefinition();
      stopA = subscribe(a, () => undefined);
      stopRaw = subscribe(raw, () => undefined);
      const resetting = graph.reset();
      await binding.session(a).detached;
      await resetting;
      expect(backend.calls.filter((call) => call.name === "bound")).toHaveLength(1);
      expect(backend.calls.filter((call) => call.name === "raw")).toHaveLength(2);
      expect([a.status.get(), a.pending.get()]).toEqual(["closed", false]);
      const b = binding.member();
      expect(binding.session(b).identity).not.toBe(binding.session(a).identity);
      expect(backend.calls.filter((call) => call.name === "bound")).toHaveLength(2);
    } finally {
      stopA?.();
      stopRaw?.();
      graph.dispose();
    }
  });

  it("disposes only its actual context and exposes a genuine capture fault after scope revocation", async () => {
    const f = fixture();
    try {
      const other = bindStream(f.graph, f.stream, f.graph.api.stream, {
        context: createBindingContext(f.graph),
      });
      const a = f.binding.member({ room: "one" });
      const ticket = f.binding.session(a);
      other.member({ room: "one" });
      f.graph.api.stream({ room: "one" });
      f.context.dispose();
      const receipt = await ticket.detached;
      expect(receipt.terminalCapture).toMatchObject({ kind: "fault", cause: { kind: "disposed" } });
      expect(f.signals.map((signal) => signal.aborted)).toEqual([true, false, false]);
      expect(() => f.binding.member({ room: "one" })).toThrow(Fault);
      expect(() => a.value.get()).toThrow(Fault);
    } finally {
      f.graph.dispose();
    }
  });
});

it("unlinks a real caller signal whose registration completes after reentrant reset", async () => {
  const backend = testBackend();
  const stream = createStream({
    name: "signal-reentry",
    key: () => "one",
    open: backend.stream<void, number>("signal-reentry"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "signal-reentry", operations: { stream } }),
  });
  const caller = new AbortController();
  const originalAdd = caller.signal.addEventListener.bind(caller.signal);
  const originalRemove = caller.signal.removeEventListener.bind(caller.signal);
  const listeners = new Set<EventListenerOrEventListenerObject>();
  let resetting: Promise<void> | undefined;
  Object.defineProperty(caller.signal, "addEventListener", {
    value(
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: boolean | AddEventListenerOptions,
    ) {
      resetting = graph.reset();
      listeners.add(listener);
      originalAdd(type, listener, options);
    },
  });
  Object.defineProperty(caller.signal, "removeEventListener", {
    value(
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: boolean | EventListenerOptions,
    ) {
      listeners.delete(listener);
      originalRemove(type, listener, options);
    },
  });
  try {
    const binding = bindStream(graph, stream, graph.api.stream, {
      context: createBindingContext(graph),
    });
    const a = binding.member(undefined, { abortSignal: caller.signal });
    const ticket = binding.session(a);
    const receipt = await ticket.detached;
    await resetting;
    expect(receipt.session).toBe(ticket.identity);
    expect(receipt.status).toBe("closed");
    expect(listeners.size).toBe(0);
    expect(backend.calls).toHaveLength(0);
    const b = binding.member();
    caller.abort();
    expect(binding.session(b).abortSignal.aborted).toBe(false);
    expect(backend.calls).toHaveLength(1);
  } finally {
    graph.dispose();
  }
});

it.each(["failure", "reset", "idle"] as const)(
  "captures the actual landed prefix before %s retirement clears or revokes A",
  async (mode) => {
    const f = fixture();
    let stop: (() => void) | undefined;
    try {
      const a = f.binding.member({ room: "one" });
      const ticket = f.binding.session(a);
      const landed = Promise.withResolvers<void>();
      stop = subscribe(a, (value) => {
        if (value === 12) landed.resolve();
      });
      f.backend.emit("events", 12);
      await landed.promise;
      stop();
      stop = undefined;
      const reason = new Error("source failure");
      let resetting: Promise<void> | undefined;
      if (mode === "failure") f.backend.end("events", reason);
      if (mode === "reset") resetting = f.graph.reset();
      if (mode === "idle") jest.advanceTimersByTime(100);
      const receipt = await ticket.detached;
      await resetting;
      expect(receipt.status).toBe(
        mode === "failure" ? "failed" : mode === "idle" ? "empty" : "closed",
      );
      if (mode === "failure") {
        expect(receipt.reason).toBe(reason);
        expect(a.error.get()).toBe(reason);
      }
      expect(receipt.terminalCapture).toMatchObject({
        kind: "captured",
        capture: { kind: "stable", present: true, value: 12 },
      });
      expect(a.value.get()).toBe(12);
      const b = f.binding.member({ room: "one" });
      expect(b.available.get()).toBe(false);
      expect(b.value.get()).toBeUndefined();
      expect(b.error.get()).toBeUndefined();
      expect(a.value.get()).toBe(12);
    } finally {
      stop?.();
      f.graph.dispose();
    }
  },
);

const transferReference = { module: "rooms", operation: "events" };

function sourceWithImportedBaseline(value: number) {
  const source = fixture();
  const store = source.binding.member({ room: "one" });
  store.set(value);
  const transfer = snapshot(source.graph, {
    version: 2,
    bindings: [{ binding: source.binding, reference: transferReference }],
  });
  return { source, transfer };
}

function adoptBaseline(
  target: ReturnType<typeof fixture>,
  transfer: ReturnType<typeof sourceWithImportedBaseline>["transfer"],
): void {
  adopt(target.graph, transfer, {
    bindings: [{ binding: target.binding, reference: { operation: "events", module: "rooms" } }],
  });
}

it("leaves an imported baseline claimable after a member precheck fails, then clears it after a real new write", async () => {
  const source = sourceWithImportedBaseline(11);
  const target = fixture();
  let stop: (() => void) | undefined;
  try {
    adoptBaseline(target, source.transfer);
    const precheckFailure = new Error("caller option could not be read");
    const badOptions = {
      get abortSignal(): AbortSignal {
        throw precheckFailure;
      },
    };
    expect(() => target.binding.member({ room: "one" }, badOptions)).toThrow(precheckFailure);
    expect(target.backend.calls).toHaveLength(0);

    const first = target.binding.member({ room: "one" });
    const firstSession = target.binding.session(first);
    expect([first.available.get(), first.value.get(), first.pending.get()]).toEqual([
      true,
      11,
      true,
    ]);
    const delivered = Promise.withResolvers<void>();
    stop = subscribe(first, (value) => {
      if (value === 22) delivered.resolve();
    });
    target.backend.emit("events", 22);
    await delivered.promise;
    expect(first.value.get()).toBe(22);
    target.backend.end("events");
    await firstSession.detached;

    const second = target.binding.member({ room: "one" });
    expect(target.binding.session(second).identity).not.toBe(firstSession.identity);
    expect([second.available.get(), second.value.get(), second.pending.get()]).toEqual([
      false,
      undefined,
      true,
    ]);
    expect(target.backend.calls).toHaveLength(2);
  } finally {
    stop?.();
    source.source.graph.dispose();
    target.graph.dispose();
  }
});

it("does not restore a claimed baseline after the first genuine open fails", async () => {
  const source = sourceWithImportedBaseline(31);
  const backend = testBackend();
  let opens = 0;
  const stream = createStream({
    name: "events",
    key: (input: Input) => input.room,
    open: (input: Input, options) => {
      opens += 1;
      if (opens === 1) throw undefined;
      return backend.stream<Input, number | undefined>("events")(input, options);
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-streams", operations: { stream } }),
    idleMs: 100,
  });
  const context = createBindingContext(graph);
  const binding = bindStream(graph, stream, graph.api.stream, { context });
  try {
    adopt(graph, source.transfer, {
      bindings: [{ binding, reference: transferReference }],
    });
    const first = binding.member({ room: "one" });
    const firstSession = binding.session(first);
    const receipt = await firstSession.detached;
    expect(receipt.status).toBe("failed");
    expect([first.available.get(), first.value.get()]).toEqual([true, 31]);
    expect(opens).toBe(1);

    const second = binding.member({ room: "one" });
    expect(binding.session(second).identity).not.toBe(firstSession.identity);
    expect([second.available.get(), second.value.get(), second.pending.get()]).toEqual([
      false,
      undefined,
      true,
    ]);
    expect(opens).toBe(2);
  } finally {
    source.source.graph.dispose();
    graph.dispose();
  }
});

it.each(["reset", "context"] as const)(
  "invalidates an unclaimed imported baseline after %s",
  async (mode) => {
    const source = sourceWithImportedBaseline(47);
    const target = fixture();
    try {
      adoptBaseline(target, source.transfer);
      if (mode === "reset") await target.graph.reset();
      else target.context.dispose();

      if (mode === "context") {
        expect(() => target.binding.member({ room: "one" })).toThrow(Fault);
        expect(target.backend.calls).toHaveLength(0);
      } else {
        const fresh = target.binding.member({ room: "one" });
        expect([fresh.available.get(), fresh.value.get(), fresh.pending.get()]).toEqual([
          false,
          undefined,
          true,
        ]);
        expect(target.backend.calls).toHaveLength(1);
      }
    } finally {
      source.source.graph.dispose();
      target.graph.dispose();
    }
  },
);

it("refuses an exhausted later stream session before clearing the retained imported base", async () => {
  const source = sourceWithImportedBaseline(53);
  const target = fixture();
  try {
    const incoming = {
      ...source.transfer,
      entries: source.transfer.entries.map((row) => ({
        ...row,
        generation: Number.MAX_SAFE_INTEGER - 2,
      })),
    } as unknown as typeof source.transfer;
    const report = adopt(target.graph, incoming, {
      bindings: [{ binding: target.binding, reference: transferReference }],
    });
    const store = target.binding.member({ room: "one" });
    const session = target.binding.session(store);
    const capture = peekStore(store);
    target.backend.end("events");
    const receipt = await session.detached;
    const key = storeKey("one");
    const before = inspectBoundAddress(target.binding, key);
    if (before.kind !== "stable") throw new Error("Expected a stable retained address");
    const retained = snapshot(target.graph, {
      version: 2,
      bindings: [{ binding: target.binding, reference: transferReference }],
    });

    expect(() => target.binding.member({ room: "one" })).toThrow(RangeError);

    expect(target.backend.calls).toHaveLength(1);
    expect(target.binding.session(store).identity).toBe(session.identity);
    const imported = report.adopted[0];
    if (imported === undefined || imported.address.kind !== "stream-binding")
      throw new Error("Expected the actual stream adoption stamp");
    expect(session.storeIdentity).toBe(imported.identity);
    expect(receipt.terminalCapture).toEqual({ kind: "captured", capture });
    expect(store.value.get()).toBe(53);
    expect(inspectBoundAddress(target.binding, key, { expected: before.stamp })).toMatchObject({
      kind: "stable",
      state: "occupied",
      present: true,
      basePresent: true,
    });
    expect(
      snapshot(target.graph, {
        version: 2,
        bindings: [{ binding: target.binding, reference: transferReference }],
      }),
    ).toEqual(retained);
  } finally {
    source.source.graph.dispose();
    target.graph.dispose();
  }
});

it("keeps same-key restored readers isolated through dormant and active caller abort", async () => {
  type Event = { position: number; value: number };
  type Reservation = { input: Input; after: number; seed: unknown };
  type Opening = {
    hasAfter: boolean;
    after: number | undefined;
    reader: object | undefined;
    abortSignal: AbortSignal;
    returnStarted: Promise<void>;
    finishReturn(): void;
    returnCalls: number;
  };
  const reservations = new WeakMap<object, Reservation>();
  const releases = new WeakMap<object, number>();
  const openings: Opening[] = [];
  let emitLive: ((event: Event) => void) | undefined;

  const makeChannel = (
    opening: Opening,
    returnPromise: Promise<IteratorResult<Event>>,
    markReturnStarted: () => void,
  ) => {
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
            opening.returnCalls += 1;
            markReturnStarted();
            ended = true;
            for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
            return returnPromise;
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
    name: "reader-scoped-store",
    key: (input) => input.room,
    source: {
      open: (input, options) => {
        const returnGate = Promise.withResolvers<IteratorResult<Event>>();
        const returnStarted = Promise.withResolvers<void>();
        const opening: Opening = {
          hasAfter: Object.hasOwn(options, "after"),
          after: options.after,
          reader: options.reader,
          abortSignal: options.abortSignal,
          returnStarted: returnStarted.promise,
          finishReturn: () => returnGate.resolve({ done: true, value: undefined }),
          returnCalls: 0,
        };
        openings.push(opening);
        const channel = makeChannel(opening, returnGate.promise, () => returnStarted.resolve());
        if (options.reader === undefined) emitLive = channel.emit;
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
  const graph = createGraph({
    contract: defineContract({ namespace: "reader-scoped-store", operations: { stream } }),
  });
  const context = createBindingContext(graph);
  const binding = bindStream(graph, stream, graph.api.stream, { context });
  const input = Object.freeze({ room: "one" });
  const policy = {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, event: number) => sum + event,
  };
  const source = binding.member(input);
  const sourceSession = binding.session(source);
  const sourceProjection = project(source, policy);
  const abortA = new AbortController();
  const abortB = new AbortController();
  let readerA: object | undefined;
  let readerB: object | undefined;
  let readerATransferred = false;
  let readerBTransferred = false;

  try {
    const folded = new Promise<void>((resolve) => {
      let stop = (): void => undefined;
      stop = subscribe(sourceProjection.value, (value) => {
        if (value !== 2) return;
        stop();
        resolve();
      });
    });
    if (emitLive === undefined) throw new Error("The bound source did not open");
    emitLive({ position: 5, value: 2 });
    await folded;

    const checkpoint = capturePositionedStore(source, [sourceProjection] as const);
    if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
      throw new Error("The bound source did not capture a positioned landing");
    const after: number = checkpoint.position;
    expect(checkpoint.sourceIdentity).toBe(source.identity);
    expect(checkpoint.sessionIdentity).toBe(sourceSession.identity);
    expect(checkpoint.latest).toBe(2);
    expect(checkpoint.values).toStrictEqual([2]);

    const seed = { after, latest: checkpoint.latest, values: checkpoint.values };
    readerA = reserve(input, after, seed);
    readerB = reserve(input, after, seed);
    const first = preparePositionedStoreRestore(binding, input, [policy] as const, seed, readerA, {
      abortSignal: abortA.signal,
    });
    readerATransferred = true;
    const second = preparePositionedStoreRestore(binding, input, [policy] as const, seed, readerB, {
      abortSignal: abortB.signal,
    });
    readerBTransferred = true;

    expect(openings).toHaveLength(1);
    expect(first.source.identity).not.toBe(second.source.identity);
    expect(first.source.identity).not.toBe(source.identity);
    expect(second.source.identity).not.toBe(source.identity);
    const firstSession = binding.session(first.source);
    const secondSession = binding.session(second.source);
    expect(firstSession.storeIdentity).toBe(first.source.identity);
    expect(secondSession.storeIdentity).toBe(second.source.identity);
    expect(firstSession.identity).not.toBe(secondSession.identity);
    expect(first.source.available.get()).toBe(true);
    expect(first.source.value.get()).toBe(2);
    expect(second.source.available.get()).toBe(true);
    expect(second.source.value.get()).toBe(2);

    const ordinaryJoin = binding.member(input);
    expect(ordinaryJoin.identity).toBe(source.identity);
    expect(binding.session(ordinaryJoin).identity).toBe(sourceSession.identity);
    expect(openings).toHaveLength(1);

    const dormantReason = new Error("dormant reader aborted");
    abortA.abort(dormantReason);
    const firstDetachment = await firstSession.detached;
    expect(firstDetachment.status).toBe("closed");
    expect(firstSession.abortSignal.aborted).toBe(true);
    expect(firstSession.abortSignal.reason).toBe(dormantReason);
    expect(openings).toHaveLength(1);
    expect(releaseCount(readerA)).toBe(1);
    expect(releaseCount(readerB)).toBe(0);
    expect(second.source.status.get()).toBe("empty");
    expect(() => first.activate()).toThrow(TypeError);
    expect(openings).toHaveLength(1);

    expect(second.activate()).toBe(second.source);
    expect(openings).toHaveLength(2);
    expect(openings[1]).toMatchObject({ hasAfter: true, after: 5, reader: readerB });
    expect(openings[1]!.abortSignal).toBe(secondSession.abortSignal);

    const activeReason = new Error("active reader aborted");
    abortB.abort(activeReason);
    const secondDetachment = await secondSession.detached;
    expect(secondDetachment.status).toBe("closed");
    expect(openings[1]!.abortSignal.aborted).toBe(true);
    expect(openings[1]!.abortSignal.reason).toBe(activeReason);
    expect(openings[1]!.returnCalls).toBe(1);
    expect(releaseCount(readerB)).toBe(1);

    const retiring = context.retire();
    expect(context.retire()).toBe(retiring);
    await openings[0]!.returnStarted;
    openings[0]!.finishReturn();
    await expectPending(retiring);

    for (const opening of openings) opening.finishReturn();
    await retiring;
    expect(openings.map((opening) => opening.returnCalls)).toEqual([1, 1]);
    expect(releaseCount(readerA)).toBe(1);
    expect(releaseCount(readerB)).toBe(1);
  } finally {
    for (const opening of openings) opening.finishReturn();
    if (readerA !== undefined && !readerATransferred && releaseCount(readerA) === 0)
      releaseReader(readerA);
    if (readerB !== undefined && !readerBTransferred && releaseCount(readerB) === 0)
      releaseReader(readerB);
    context.dispose();
    graph.dispose();
  }
});

it("revoking a bound context closes its positioned projections and releases dormant readers", async () => {
  const reservations = new WeakMap<
    object,
    { input: Input; after: number; checkpoint: object; seed: unknown }
  >();
  const releaseCounts = new WeakMap<object, number>();
  let liveEmit: ((event: { position: number; value: number }) => void) | undefined;
  let opens = 0;
  const stream = createPositionedStream<Input, number, string, number, object>({
    name: "bound-positioned",
    key: (input: Input) => input.room,
    source: {
      open: () => {
        opens += 1;
        const queued: Array<{ position: number; value: number }> = [];
        const waiters: Array<
          (result: IteratorResult<{ position: number; value: number }>) => void
        > = [];
        liveEmit = (event) => {
          const waiter = waiters.shift();
          if (waiter === undefined) queued.push(event);
          else waiter({ done: false, value: event });
        };
        return {
          [Symbol.asyncIterator]() {
            return {
              next() {
                const event = queued.shift();
                if (event !== undefined)
                  return Promise.resolve({ done: false as const, value: event });
                return new Promise<IteratorResult<{ position: number; value: number }>>((resolve) =>
                  waiters.push(resolve),
                );
              },
              return() {
                return Promise.resolve({ done: true as const, value: undefined });
              },
            };
          },
        };
      },
      validateReader: (input, reader, after, checkpoint, seed) => {
        const reservation = reservations.get(reader);
        return (
          reservation !== undefined &&
          reservation.input === input &&
          reservation.after === after &&
          reservation.checkpoint === checkpoint &&
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
        releaseCounts.set(reader, (releaseCounts.get(reader) ?? 0) + 1);
      },
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-positioned", operations: { stream } }),
  });
  const context = createBindingContext(graph);
  const binding = bindStream(graph, stream, graph.api.stream, { context });
  const input = Object.freeze({ room: "one" });
  const projection = projectPositioned(binding, input, {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, item: number) => sum + item,
  });
  const reserve = (after: number, checkpoint: object, seed: unknown): object => {
    const reader = Object.freeze({});
    reservations.set(reader, { input, after, checkpoint, seed });
    return reader;
  };
  let reader: object | undefined;
  let restore: PositionedRestoration<number> | undefined;
  try {
    const folded = new Promise<void>((resolve) => {
      let stop = () => {};
      stop = subscribe(projection.value, (value) => {
        if (value !== 4) return;
        stop();
        resolve();
      });
    });
    if (liveEmit === undefined) throw new Error("The bound positioned source did not open");
    liveEmit({ position: 2, value: 4 });
    await folded;
    await flush();
    const checkpoint = capturePositionedProjection<number, number>(projection);
    reader = reserve(2, checkpoint, 4);
    restore = preparePositionedRestore(binding, input, checkpoint, reader, { value: 4 });
    expect(opens).toBe(1);
    expect(
      isPositionedCheckpointCurrent(
        capturePositionedProjection<number, number>(restore.projection),
      ),
    ).toBe(true);
    context.dispose();
    expect(projection.status.get()).toBe("closed");
    expect(restore.projection.status.get()).toBe("closed");
    expect(releaseCounts.get(reader)).toBe(1);
    expect(isPositionedCheckpointCurrent(checkpoint)).toBe(false);
    if (liveEmit === undefined) throw new Error("The source emitter was lost");
    liveEmit({ position: 3, value: 8 });
    await flush();
    expect(projection.value.get()).toBe(4);
    expect(restore.projection.value.get()).toBe(4);
    expect(opens).toBe(1);
    restore.discard();
    expect(releaseCounts.get(reader)).toBe(1);
  } finally {
    projection.close();
    restore?.discard();
    context.dispose();
    graph.dispose();
  }
});
