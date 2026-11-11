import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  bindStream,
  createBindingContext,
  createGraph,
  createStream,
  defineContract,
  Fault,
  peekStore,
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
      expect(raw.available.get()).toBe(false);
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
