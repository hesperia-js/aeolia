import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  bindOperation,
  callQueryWithProgress,
  createBindingContext,
  createGraph,
  createQuery,
  defineContract,
  observeQueryProgress,
  queryOptions,
} from "../../src/contract/index.ts";
import { Fault } from "../../src/fault.ts";
import {
  __internal as reactiveInternal,
  computed,
  subscribe,
  withoutWrites,
} from "../../src/reactive.ts";
import { adopt, snapshot } from "../../src/realm.ts";
import {
  captureReactiveConstruction,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
} from "../../src/index.ts";
import type {
  ClosedEncodedValue,
  PassiveTransferClaim,
  QueryProgressCohortEntry,
  QueryStore,
  RequiredTransferCodec,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (reason: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((accept, decline) => {
    resolve = accept;
    reject = decline;
  });
  return { promise, resolve, reject };
}

type PromiseOutcome<T> =
  | { readonly status: "fulfilled"; readonly value: T }
  | { readonly status: "rejected"; readonly reason: unknown };

function observeOutcome<T>(promise: Promise<T>): Promise<PromiseOutcome<T>> {
  return promise.then(
    (value) => ({ status: "fulfilled" as const, value }),
    (reason: unknown) => ({ status: "rejected" as const, reason }),
  );
}

function throwingRemovalSignal(
  controller: AbortController,
  cleanupCause: unknown,
  onRemove?: () => void,
): {
  readonly signal: AbortSignal;
  readonly state: {
    addCalls: number;
    removeCalls: number;
    removedRegisteredListener: boolean;
  };
} {
  const state = { addCalls: 0, removeCalls: 0, removedRegisteredListener: false };
  let registeredListener: EventListenerOrEventListenerObject | null = null;
  const nativeAdd = controller.signal.addEventListener.bind(controller.signal);
  const nativeRemove = controller.signal.removeEventListener.bind(controller.signal);
  const signal = new Proxy(controller.signal, {
    get(target, property) {
      if (property === "addEventListener")
        return (
          type: string,
          listener: EventListenerOrEventListenerObject,
          options?: boolean | AddEventListenerOptions,
        ) => {
          nativeAdd(type, listener, options);
          if (type === "abort") {
            state.addCalls += 1;
            registeredListener = listener;
          }
        };
      if (property === "removeEventListener")
        return (
          type: string,
          listener: EventListenerOrEventListenerObject,
          options?: boolean | EventListenerOptions,
        ) => {
          nativeRemove(type, listener, options);
          if (type !== "abort") return;
          state.removeCalls += 1;
          if (listener !== null && listener === registeredListener)
            state.removedRegisteredListener = true;
          onRemove?.();
          throw cleanupCause;
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { signal, state };
}

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

describe("query progress", () => {
  for (const kind of ["raw", "bound"] as const) {
    it(`enrolls both ${kind} call listeners before synchronous reports`, async () => {
      let calls = 0;
      const query = createQuery<string, number, string, number>({
        name: `cohort.${kind}`,
        key: (input) => input,
        fetch: (_input, options) => {
          calls += 1;
          options.reportProgress(1);
          options.reportProgress(2);
          return Promise.resolve(3);
        },
      });
      const graph = createGraph({
        contract: defineContract({ namespace: `cohort-${kind}`, operations: { query } }),
      });
      const context = createBindingContext(graph);
      const binding = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
      const values: number[][] = [[], []];
      const captures: number[][] = [[], []];
      const stores: Array<QueryStore<number, number> | undefined> = [];
      const receivers: Array<QueryProgressCohortEntry<number, number> | undefined> = [];
      const stops: Array<() => void> = [];
      const entries: QueryProgressCohortEntry<number, number>[] = [0, 1].map((index) => ({
        signal: new AbortController().signal,
        onEvent(event, view) {
          receivers[index] = this;
          stores[index] = view.store;
          stops[index] = view.stop;
          if (event.kind === "report" && event.snapshot.hasValue) {
            values[index]!.push(event.snapshot.value!);
            captures[index]!.push(view.observation.capture().reportRevision);
          }
        },
      }));
      try {
        const store =
          kind === "raw"
            ? callQueryWithProgress(graph.api.query, ["one"], entries)
            : callQueryWithProgress(binding.member, ["one"], entries);
        expect(calls).toBe(1);
        expect(values).toEqual([
          [1, 2],
          [1, 2],
        ]);
        expect(captures).toEqual([
          [1, 2],
          [1, 2],
        ]);
        expect(stores[0]).toBe(store);
        expect(stores[1]).toBe(store);
        expect(receivers[0]).toBe(entries[0]);
        expect(receivers[1]).toBe(entries[1]);
        expect(await store.ready).toBe(3);
      } finally {
        for (const stop of stops) stop();
        graph.dispose();
      }
    });
  }

  it("joins an active delivery at one frontier without replay or duplicate work", async () => {
    const work = deferred<number>();
    let report!: (value: number) => void;
    let calls = 0;
    const query = createQuery<void, number, string, number>({
      name: "cohort.reentrant",
      key: () => "one",
      fetch: (_input, options) => {
        calls += 1;
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-reentrant", operations: { query } }),
    });
    const original = graph.api.query();
    const values: number[][] = [[], []];
    const stops: Array<() => void> = [];
    const entries: QueryProgressCohortEntry<number, number>[] = [0, 1].map((index) => ({
      signal: new AbortController().signal,
      onEvent(event, context) {
        stops[index] = context.stop;
        if (event.snapshot.hasValue) values[index]!.push(event.snapshot.value!);
      },
    }));
    const stopOriginal = observeQueryProgress(original).subscribe((event) => {
      if (event.kind !== "report" || event.snapshot.value !== 1) return;
      callQueryWithProgress(graph.api.query, [], entries);
      report(2);
    });
    try {
      report(0);
      report(1);
      expect(calls).toBe(1);
      expect(values).toEqual([
        [1, 2],
        [1, 2],
      ]);
      work.resolve(3);
      expect(await original.ready).toBe(3);
    } finally {
      stopOriginal();
      for (const stop of stops) stop();
      graph.dispose();
    }
  });

  it("aborts one cohort listener without cancelling the shared query or its peer", async () => {
    const first = new AbortController();
    const second = new AbortController();
    const alreadyAborted = new AbortController();
    alreadyAborted.abort();
    const values: number[][] = [[], [], []];
    const stops: Array<() => void> = [];
    let calls = 0;
    const query = createQuery<void, number, string, number>({
      name: "cohort.abort-one",
      key: () => "one",
      fetch: (_input, options) => {
        calls += 1;
        options.reportProgress(1);
        options.reportProgress(2);
        return Promise.resolve(3);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-abort-one", operations: { query } }),
    });
    const entries: QueryProgressCohortEntry<number, number>[] = [first, second, alreadyAborted].map(
      (controller, index) => ({
        signal: controller.signal,
        onEvent(event, context) {
          stops[index] = context.stop;
          if (event.kind !== "report" || !event.snapshot.hasValue) return;
          values[index]!.push(event.snapshot.value!);
          if (index === 0) first.abort();
        },
      }),
    );
    try {
      const store = callQueryWithProgress(graph.api.query, [], entries);
      expect(await store.ready).toBe(3);
      expect(calls).toBe(1);
      expect(values).toEqual([[1], [1, 2], []]);
    } finally {
      for (const stop of stops) stop();
      graph.dispose();
    }
  });

  it("continues cohort delivery and the shared request when one callback throws", async () => {
    const callbackCause = new Error("cohort callback failed");
    const diagnostics: unknown[] = [];
    const received: number[] = [];
    const stops: Array<() => void> = [];
    const query = createQuery<void, number, string, number>({
      name: "cohort.callback-failure",
      key: () => "one",
      fetch: (_input, options) => {
        options.reportProgress(1);
        options.reportProgress(2);
        return Promise.resolve(3);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-callback-failure", operations: { query } }),
      onUnobservedFault: (cause) => diagnostics.push(cause),
    });
    const entries: QueryProgressCohortEntry<number, number>[] = [
      {
        signal: new AbortController().signal,
        onEvent(event, context) {
          stops[0] = context.stop;
          if (event.kind === "report" && event.snapshot.value === 1) throw callbackCause;
        },
      },
      {
        signal: new AbortController().signal,
        onEvent(event, context) {
          stops[1] = context.stop;
          if (event.kind === "report" && event.snapshot.hasValue)
            received.push(event.snapshot.value!);
        },
      },
    ];
    try {
      const store = callQueryWithProgress(graph.api.query, [], entries);
      expect(await store.ready).toBe(3);
      expect(received).toEqual([1, 2]);
      expect(diagnostics).toContain(callbackCause);
    } finally {
      for (const stop of stops) stop();
      graph.dispose();
    }
  });

  it("does not dispatch a bound query after a cohort callback disposes its context", () => {
    let calls = 0;
    const query = createQuery<string, number, string, number>({
      name: "cohort.revoke-context",
      key: (input) => input,
      fetch: () => {
        calls += 1;
        return Promise.resolve(1);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-revoke-context", operations: { query } }),
    });
    const context = createBindingContext(graph);
    const binding = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
    try {
      expect(() =>
        callQueryWithProgress(
          binding.member,
          ["one"],
          [
            {
              signal: new AbortController().signal,
              onEvent() {
                context.dispose();
              },
            },
          ],
        ),
      ).toThrow("disposed");
      expect(calls).toBe(0);
    } finally {
      graph.dispose();
    }
  });

  it("preserves branded options and second-argument precedence", async () => {
    const values: number[] = [];
    let initial: number | undefined;
    const stops: Array<() => void> = [];
    const entries: QueryProgressCohortEntry<number, number>[] = [
      {
        signal: new AbortController().signal,
        onEvent(event, context) {
          stops[0] = context.stop;
          if (event.kind === "current") initial = context.store.value.get();
          if (event.kind === "report" && event.snapshot.hasValue)
            values.push(event.snapshot.value!);
        },
      },
    ];
    const query = createQuery<void, number, string, number>({
      name: "cohort.options-override",
      acquisition: "fetch",
      key: () => "one",
      fetch: (_input, options) => {
        options.reportProgress(1);
        return Promise.resolve(2);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-options-override", operations: { query } }),
    });
    try {
      const store = callQueryWithProgress(
        graph.api.query,
        [queryOptions({ initial: 7 }), { initial: 9 }],
        entries,
      );
      expect(await store.ready).toBe(2);
      expect(initial).toBe(9);
      expect(values).toEqual([1]);
    } finally {
      for (const stop of stops) stop();
      graph.dispose();
    }
  });

  it("keeps a progressCohort property in ordinary query input data", async () => {
    const received: string[] = [];
    const query = createQuery<{ progressCohort: string }, number, string, number>({
      name: "cohort.input-data",
      key: (input) => input.progressCohort,
      fetch: (input) => {
        received.push(input.progressCohort);
        return Promise.resolve(1);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-input-data", operations: { query } }),
    });
    try {
      const store = callQueryWithProgress(
        graph.api.query,
        [{ progressCohort: "actual-input" }],
        [],
      );
      expect(await store.ready).toBe(1);
      expect(received).toEqual(["actual-input"]);
    } finally {
      graph.dispose();
    }
  });

  it("rejects a wrapper instead of invoking it as a genuine query member", () => {
    let calls = 0;
    const query = createQuery<string, number, string, number>({
      name: "cohort.genuine-member",
      key: (input) => input,
      fetch: () => Promise.resolve(1),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-genuine-member", operations: { query } }),
    });
    const wrapper = (input: string) => {
      calls += 1;
      return graph.api.query(input);
    };
    try {
      expect(() => callQueryWithProgress(wrapper, ["one"], [])).toThrow(TypeError);
      expect(calls).toBe(0);
    } finally {
      graph.dispose();
    }
  });

  it("unwinds a cohort when a signal attaches and throws", () => {
    const controller = new AbortController();
    const registrationCause = new Error("registration failed after attach");
    const cleanupCause = new Error("registered listener removal failed");
    let removals = 0;
    let calls = 0;
    let events = 0;
    const signal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "addEventListener")
          return (
            type: string,
            listener: EventListenerOrEventListenerObject,
            options?: AddEventListenerOptions,
          ) => {
            target.addEventListener(type, listener, options);
            throw registrationCause;
          };
        if (property === "removeEventListener")
          return (type: string, listener: EventListenerOrEventListenerObject) => {
            target.removeEventListener(type, listener);
            removals += 1;
            throw cleanupCause;
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const query = createQuery<void, number, string, number>({
      name: "cohort.registration-failure",
      key: () => "one",
      fetch: () => {
        calls += 1;
        return Promise.resolve(1);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-registration-failure", operations: { query } }),
    });
    try {
      let failure: unknown;
      try {
        callQueryWithProgress(
          graph.api.query,
          [],
          [
            {
              signal: new AbortController().signal,
              onEvent: () => {
                events += 1;
              },
            },
            {
              signal,
              onEvent: () => {
                events += 1;
              },
            },
          ],
        );
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBeInstanceOf(AggregateError);
      if (!(failure instanceof AggregateError))
        throw new Error("Expected registration and cleanup failures");
      expect(failure.errors).toEqual([registrationCause, cleanupCause]);
      expect(removals).toBe(1);
      expect(events).toBe(0);
      expect(calls).toBe(0);
    } finally {
      graph.dispose();
    }
  });
  it("keeps repeated and undefined reports separate from the committed value", async () => {
    const work = deferred<number>();
    let report!: (value: string | undefined) => void;
    const query = createQuery<string, number, string, string | undefined>({
      name: "progress.partial",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-partial", operations: { query } }),
    });
    const store = graph.api.query("one");
    const ready = store.ready;
    const execution = store.progress.get().execution;
    expect(execution).toBeDefined();
    expect(Object.isFrozen(execution)).toBe(true);
    expect(Reflect.ownKeys(execution!)).toEqual([]);

    report(undefined);
    report(undefined);

    expect(store.progress.get()).toMatchObject({
      execution,
      reportRevision: 2,
      hasValue: true,
      value: undefined,
      status: "active",
      error: undefined,
    });
    expect(store.pending.get()).toBe(true);
    expect(store.value.peek()).toBeUndefined();

    work.resolve(7);
    await expect(ready).resolves.toBe(7);

    expect(store.progress.get()).toMatchObject({
      execution,
      reportRevision: 2,
      hasValue: true,
      value: undefined,
      status: "complete",
      error: undefined,
    });
    expect(Object.isFrozen(store.progress.get())).toBe(true);
    expect(store.value.get()).toBe(7);
    expect(store.pending.get()).toBe(false);
    report("late-terminal-report");
    expect(store.progress.get()).toMatchObject({
      execution,
      reportRevision: 2,
      hasValue: true,
      value: undefined,
      status: "complete",
    });
    graph.dispose();
  });

  it("delivers every report when readable propagation coalesces", async () => {
    const work = deferred<number>();
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.ordered-batch",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-ordered-batch", operations: { query } }),
    });
    const store = graph.api.query("one");
    const ready = store.ready;
    const observation = observeQueryProgress(store);
    const events: Array<[string, number, unknown]> = [];
    const readableRevisions: number[] = [];
    const stopEvents = observation.subscribe(({ kind, snapshot }) => {
      events.push([kind, snapshot.reportRevision, snapshot.value]);
    });
    const stopReadable = subscribe(store.progress, (snapshot) => {
      readableRevisions.push(snapshot.reportRevision);
    });

    reactiveInternal.batch(() => {
      report("first");
      report("second");
      report("third");
    });

    expect(events.filter(([kind]) => kind === "report")).toEqual([
      ["report", 1, "first"],
      ["report", 2, "second"],
      ["report", 3, "third"],
    ]);
    expect(readableRevisions).toEqual([0, 3]);
    expect(observation.capture()).toMatchObject({
      reportRevision: 3,
      hasValue: true,
      value: "third",
      status: "active",
    });

    work.resolve(1);
    await expect(ready).resolves.toBe(1);
    stopReadable();
    stopEvents();
    graph.dispose();
  });

  it("queues reentrant reports and current seeds without recursive delivery", async () => {
    const work = deferred<number>();
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.reentrant",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-reentrant", operations: { query } }),
    });
    const store = graph.api.query("one");
    const observation = observeQueryProgress(store);
    const firstEvents: Array<[string, number, unknown]> = [];
    const seededEvents: Array<[string, number, unknown]> = [];
    let depth = 0;
    let maximumDepth = 0;
    let stopSeeded: (() => void) | undefined;
    const stopFirst = observation.subscribe(({ kind, snapshot }) => {
      depth += 1;
      maximumDepth = Math.max(maximumDepth, depth);
      firstEvents.push([kind, snapshot.reportRevision, snapshot.value]);
      if (kind === "report" && snapshot.reportRevision === 1) {
        report("second");
        report("third");
        stopSeeded = observation.subscribe(({ kind: seededKind, snapshot: seeded }) => {
          depth += 1;
          maximumDepth = Math.max(maximumDepth, depth);
          seededEvents.push([seededKind, seeded.reportRevision, seeded.value]);
          depth -= 1;
        });
        expect(observation.capture()).toMatchObject({ reportRevision: 3, value: "third" });
      }
      depth -= 1;
    });

    report("first");

    expect(maximumDepth).toBe(1);
    expect(firstEvents.filter(([kind]) => kind === "report")).toEqual([
      ["report", 1, "first"],
      ["report", 2, "second"],
      ["report", 3, "third"],
    ]);
    expect(seededEvents).toEqual([["current", 3, "third"]]);
    stopSeeded?.();
    stopFirst();
    work.resolve(1);
    await store.ready;
    graph.dispose();
  });

  it("does not deliver queued events to an observer removed during delivery", async () => {
    const work = deferred<number>();
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.unsubscribe-during-delivery",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-unsubscribe", operations: { query } }),
    });
    const store = graph.api.query("one");
    const observation = observeQueryProgress(store);
    const firstReports: number[] = [];
    const removedEvents: string[] = [];
    let stopRemoved = (): void => undefined;
    const stopFirst = observation.subscribe(({ kind, snapshot }) => {
      if (kind !== "report") return;
      firstReports.push(snapshot.reportRevision);
      if (snapshot.reportRevision === 1) {
        stopRemoved();
        report("second");
      }
    });
    stopRemoved = observation.subscribe(({ kind }) => removedEvents.push(kind));

    report("first");

    expect(firstReports).toEqual([1, 2]);
    expect(removedEvents).toEqual(["current"]);
    stopRemoved();
    stopFirst();
    work.resolve(1);
    await store.ready;
    graph.dispose();
  });

  it("isolates observer failures until ordered delivery drains", async () => {
    const work = deferred<number>();
    const observerError = new Error("progress observer failed");
    const diagnostics: Array<{ error: unknown; revisions: number[] }> = [];
    let delivered: number[] = [];
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.observer-fault",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-observer-fault", operations: { query } }),
      onUnobservedFault: (error) => diagnostics.push({ error, revisions: [...delivered] }),
    });
    const store = graph.api.query("one");
    const ready = store.ready;
    const observation = observeQueryProgress(store);
    const directSeedError = new Error("direct seed failed");
    expect(() =>
      observation.subscribe(() => {
        throw directSeedError;
      }),
    ).toThrow(directSeedError);

    const stopThrowing = observation.subscribe(({ kind, snapshot }) => {
      if (kind === "report" && snapshot.reportRevision === 1) {
        report("second");
        throw observerError;
      }
    });
    const stopReceiving = observation.subscribe(({ kind, snapshot }) => {
      if (kind === "report") delivered.push(snapshot.reportRevision);
    });

    expect(() => report("first")).not.toThrow();
    expect(delivered).toEqual([1, 2]);
    expect(diagnostics).toEqual([{ error: observerError, revisions: [1, 2] }]);
    expect(observation.capture()).toMatchObject({ reportRevision: 2, value: "second" });

    work.resolve(1);
    await expect(ready).resolves.toBe(1);
    stopReceiving();
    stopThrowing();
    graph.dispose();
  });

  it("replaces a deferred seed after reentrant execution replacement", async () => {
    const runs: Array<{ work: Deferred<number>; report: (value: string) => void }> = [];
    const query = createQuery<string, number, string, string>({
      name: "progress.reentrant-replacement",
      key: (input) => input,
      fetch: (_input, options) => {
        const work = deferred<number>();
        runs.push({ work, report: options.reportProgress });
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "progress-reentrant-replacement",
        operations: { query },
      }),
    });
    const store = graph.api.query("one");
    const observation = observeQueryProgress(store);
    const reentrantEvents: Array<[string, object | undefined, string]> = [];
    let replacementEvents: Array<[string, object | undefined, string]> = [];
    let replacementReady: Promise<void> | undefined;
    let stopReplacement: (() => void) | undefined;
    const stop = observation.subscribe(({ kind, snapshot }) => {
      if (kind === "report" && snapshot.reportRevision === 1) {
        stopReplacement = observation.subscribe(({ kind: nextKind, snapshot: next }) => {
          replacementEvents.push([nextKind, next.execution, next.status]);
        });
        replacementReady = store.revalidate();
      }
      reentrantEvents.push([kind, snapshot.execution, snapshot.status]);
    });
    const firstExecution = observation.capture().execution;

    runs[0]!.report("first");

    expect(replacementEvents).toEqual([["current", observation.capture().execution, "active"]]);
    expect(observation.capture().execution).not.toBe(firstExecution);
    expect(
      reentrantEvents.some(
        ([, execution, status]) => execution === firstExecution && status === "interrupted",
      ),
    ).toBe(false);
    runs[0]!.work.resolve(1);
    runs[1]!.work.resolve(2);
    await replacementReady;
    stopReplacement?.();
    stop();
    graph.dispose();
  });

  it("delivers reentrant accepted reports before the successful terminal event", async () => {
    const work = deferred<number>();
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.terminal-order",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-terminal-order", operations: { query } }),
    });
    const store = graph.api.query("one");
    const ready = store.ready;
    const observation = observeQueryProgress(store);
    const events: Array<[string, number, string]> = [];
    const stop = observation.subscribe(({ kind, snapshot }) => {
      events.push([kind, snapshot.reportRevision, snapshot.status]);
      if (kind === "report" && snapshot.reportRevision === 1) {
        report("second");
        report("third");
        work.resolve(1);
      }
    });

    report("first");
    await ready;

    expect(events.filter(([kind]) => kind === "report")).toEqual([
      ["report", 1, "active"],
      ["report", 2, "active"],
      ["report", 3, "active"],
    ]);
    expect(events.at(-1)).toEqual(["lifecycle", 3, "complete"]);
    stop();
    graph.dispose();
  });

  it.each(["complete", "failed"] as const)(
    "preserves accepted-report FIFO when a %s terminal is captured reentrantly",
    async (terminal) => {
      const success = { result: "success" } as const;
      const work = deferred<typeof success>();
      const failure = new Error("query failed");
      const nativeThen = Promise.prototype.then;
      let report!: (value: string) => void;
      let forwardSettlement: (() => unknown) | undefined;
      let forwardedDuringReport = false;
      const query = createQuery<string, typeof success, string, string>({
        name: "progress.reentrant-terminal-capture",
        key: (input) => input,
        fetch: (_input, options) => {
          report = options.reportProgress;
          return Object.defineProperty(work.promise, "then", {
            configurable: true,
            value: function (
              this: Promise<typeof success>,
              onFulfilled: ((value: typeof success) => unknown) | null | undefined,
              onRejected: ((reason: unknown) => unknown) | null | undefined,
            ) {
              const forward = <T>(
                callback: ((value: T) => unknown) | null | undefined,
                value: T,
                fallback: () => unknown,
              ): unknown => {
                let forwarded = false;
                let callbackResult: unknown;
                const release = (): unknown => {
                  if (forwarded) return callbackResult;
                  forwarded = true;
                  callbackResult = typeof callback === "function" ? callback(value) : fallback();
                  return callbackResult;
                };
                forwardSettlement = release;
                // The source Promise has settled; forward its outcome during report delivery.
                report("first");
                if (!forwarded) release();
                return callbackResult;
              };

              return Reflect.apply(nativeThen, this, [
                (value: typeof success) => forward(onFulfilled, value, () => value),
                (reason: unknown) =>
                  forward(onRejected, reason, () => {
                    throw reason;
                  }),
              ]);
            },
          });
        },
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "progress-reentrant-terminal-capture",
          operations: { query },
        }),
      });
      const store = graph.api.query("one");
      const ready = store.ready;
      const observation = observeQueryProgress(store);
      const execution = observation.capture().execution;
      type ProgressEventRecord = [string, number, string, unknown, unknown];
      const events: ProgressEventRecord[] = [];
      const deliveredPrefixAtCapture: ProgressEventRecord[][] = [];
      const capturesDuringReport: Array<ReturnType<typeof observation.capture>> = [];
      const stop = observation.subscribe(({ kind, snapshot }) => {
        events.push([
          kind,
          snapshot.reportRevision,
          snapshot.status,
          snapshot.value,
          snapshot.error,
        ]);
        if (kind === "report" && snapshot.reportRevision === 1) {
          report("second");
          report("third");
          const release = forwardSettlement;
          if (release !== undefined) {
            release();
            forwardedDuringReport = true;
          }
          deliveredPrefixAtCapture.push([...events]);
          capturesDuringReport.push(observation.capture());
        }
      });

      if (terminal === "complete") work.resolve(success);
      else work.reject(failure);
      const outcome = await Promise.allSettled([ready]);

      expect(forwardedDuringReport).toBe(true);
      expect(deliveredPrefixAtCapture).toEqual([
        [
          ["current", 0, "active", undefined, undefined],
          ["report", 1, "active", "first", undefined],
        ],
      ]);
      expect(execution).toBeDefined();
      expect(capturesDuringReport).toHaveLength(1);
      expect(capturesDuringReport[0]?.execution).toBe(execution);
      expect(capturesDuringReport[0]).toMatchObject({
        reportRevision: 3,
        hasValue: true,
        value: "third",
        status: terminal,
        error: terminal === "failed" ? failure : undefined,
      });
      expect(capturesDuringReport[0]?.error).toBe(terminal === "failed" ? failure : undefined);
      if (terminal === "complete") {
        expect(outcome[0]?.status).toBe("fulfilled");
        if (outcome[0]?.status === "fulfilled") expect(outcome[0].value).toBe(success);
      } else {
        expect(outcome[0]?.status).toBe("rejected");
        if (outcome[0]?.status === "rejected") expect(outcome[0].reason).toBe(failure);
      }
      expect(events).toEqual([
        ["current", 0, "active", undefined, undefined],
        ["report", 1, "active", "first", undefined],
        ["report", 2, "active", "second", undefined],
        ["report", 3, "active", "third", undefined],
        ["lifecycle", 3, terminal, "third", terminal === "failed" ? failure : undefined],
      ]);
      stop();
      graph.dispose();
    },
  );

  it.each(["complete", "failed"] as const)(
    "terminalizes a %s query execution when caller-listener cleanup throws",
    async (terminal) => {
      type Value = { readonly value: string };
      const initial: Value = { value: "initial" };
      const success: Value = { value: "source success" };
      const sourceCause = new Error("query source failed");
      const cleanupCause = new Error("caller listener removal failed");
      const work = deferred<Value>();
      const sourceOutcome = observeOutcome(work.promise);
      let workSettled = false;
      let fetchCalls = 0;
      let report: ((value: string) => void) | undefined;
      const query = createQuery<string, Value, string, string>({
        name: "progress.cleanup-terminal",
        key: (input) => input,
        fetch: (_input, options) => {
          fetchCalls += 1;
          report = options.reportProgress;
          return work.promise;
        },
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "progress-cleanup-terminal", operations: { query } }),
      });
      const store = graph.api.query("one", { initial });
      const fetchCallsBeforeRevalidation = fetchCalls;
      const controller = new AbortController();
      const callerSignal = throwingRemovalSignal(controller, cleanupCause);
      const observation = observeQueryProgress(store);
      const events: Array<{
        readonly kind: string;
        readonly snapshot: ReturnType<typeof observation.capture>;
      }> = [];
      let stop: () => void = () => undefined;
      let retirementWasCached = false;

      try {
        const continuation = store.revalidate({ abortSignal: callerSignal.signal });
        const continuationOutcome = observeOutcome(continuation);
        const execution = observation.capture().execution;
        stop = observation.subscribe(({ kind, snapshot }) => {
          events.push({ kind, snapshot });
        });
        report?.("partial");
        const beforeSourceSettlement = observation.capture();

        workSettled = true;
        if (terminal === "complete") work.resolve(success);
        else work.reject(sourceCause);
        const [sourceResult, continuationResult] = await Promise.all([
          sourceOutcome,
          continuationOutcome,
        ]);
        const terminalSnapshot = observation.capture();
        const terminalEvents = events.slice();
        const ordinaryValue = store.value.peek();
        const ordinaryError = store.error.peek();
        const registeredListenerWasRemoved = callerSignal.state.removedRegisteredListener;
        const callerSignalWasAborted = controller.signal.aborted;
        stop();

        const retirement = graph.retire();
        const retirementOutcome = await observeOutcome(retirement);
        retirementWasCached = graph.retire() === retirement;

        expect(fetchCallsBeforeRevalidation).toBe(0);
        expect(fetchCalls).toBe(1);
        expect(callerSignal.state.addCalls).toBe(1);
        expect(callerSignal.state.removeCalls).toBe(1);
        expect(registeredListenerWasRemoved).toBe(true);
        expect(callerSignalWasAborted).toBe(false);
        expect(beforeSourceSettlement).toMatchObject({
          execution,
          status: "active",
          reportRevision: 1,
          hasValue: true,
          value: "partial",
          error: undefined,
        });
        expect(sourceResult.status).toBe(terminal === "complete" ? "fulfilled" : "rejected");
        if (sourceResult.status === "fulfilled") expect(sourceResult.value).toBe(success);
        else expect(sourceResult.reason).toBe(sourceCause);
        expect(continuationResult.status).toBe("rejected");
        if (continuationResult.status === "rejected")
          expect(continuationResult.reason).toBe(cleanupCause);
        expect(retirementOutcome.status).toBe("rejected");
        if (retirementOutcome.status === "rejected")
          expect(retirementOutcome.reason).toBe(cleanupCause);
        expect(retirementWasCached).toBe(true);
        expect(ordinaryValue).toBe(initial);
        expect(ordinaryError).toBeUndefined();
        expect(terminalSnapshot).toMatchObject({
          execution,
          reportRevision: 1,
          hasValue: true,
          value: "partial",
        });
        expect(terminalSnapshot.status).toBe(terminal);
        expect(terminalSnapshot.error).toBe(terminal === "failed" ? sourceCause : undefined);
        expect(
          terminalEvents.map(({ kind, snapshot }) => [
            kind,
            snapshot.execution,
            snapshot.reportRevision,
            snapshot.status,
            snapshot.value,
            snapshot.error,
          ]),
        ).toEqual([
          ["current", execution, 0, "active", undefined, undefined],
          ["report", execution, 1, "active", "partial", undefined],
          [
            "lifecycle",
            execution,
            1,
            terminal,
            "partial",
            terminal === "failed" ? sourceCause : undefined,
          ],
        ]);
      } finally {
        if (!workSettled) {
          workSettled = true;
          if (terminal === "complete") work.resolve(success);
          else work.reject(sourceCause);
        }
        stop();
        await graph.retire().then(
          () => undefined,
          () => undefined,
        );
      }
    },
  );

  it("keeps a replacement execution current when caller cleanup reenters revalidation", async () => {
    type Value = { readonly value: string };
    const initial: Value = { value: "initial" };
    const firstResult: Value = { value: "first result" };
    const secondResult: Value = { value: "second result" };
    const cleanupCause = new Error("first caller listener removal failed");
    const firstWork = deferred<Value>();
    const secondWork = deferred<Value>();
    const firstSourceOutcome = observeOutcome(firstWork.promise);
    const secondSourceOutcome = observeOutcome(secondWork.promise);
    let firstWorkSettled = false;
    let secondWorkSettled = false;
    let fetchCalls = 0;
    let firstReport: ((value: string) => void) | undefined;
    let secondReport: ((value: string) => void) | undefined;
    let replacementOutcome: Promise<PromiseOutcome<void>> | undefined;
    let replacementStartCause: unknown;
    let replacementStarts = 0;
    const query = createQuery<string, Value, string, string>({
      name: "progress.cleanup-reentrant-replacement",
      key: (input) => input,
      fetch: (_input, options) => {
        fetchCalls += 1;
        if (fetchCalls === 1) {
          firstReport = options.reportProgress;
          return firstWork.promise;
        }
        secondReport = options.reportProgress;
        return secondWork.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "progress-cleanup-reentrant-replacement",
        operations: { query },
      }),
    });
    const store = graph.api.query("one", { initial });
    let startReplacement = (): void => undefined;
    const controller = new AbortController();
    const callerSignal = throwingRemovalSignal(controller, cleanupCause, () => startReplacement());
    const observation = observeQueryProgress(store);
    const events: Array<{
      readonly kind: string;
      readonly snapshot: ReturnType<typeof observation.capture>;
    }> = [];
    let stop: () => void = () => undefined;
    let retirementWasCached = false;

    try {
      stop = observation.subscribe(({ kind, snapshot }) => {
        events.push({ kind, snapshot });
      });
      startReplacement = () => {
        replacementStarts += 1;
        try {
          replacementOutcome = observeOutcome(store.revalidate());
        } catch (cause) {
          replacementStartCause = cause;
        }
      };
      const firstContinuation = store.revalidate({ abortSignal: callerSignal.signal });
      const firstContinuationOutcome = observeOutcome(firstContinuation);
      const firstExecution = observation.capture().execution;
      firstReport?.("first progress");

      firstWorkSettled = true;
      firstWork.resolve(firstResult);
      const [firstSource, firstContinuationResult] = await Promise.all([
        firstSourceOutcome,
        firstContinuationOutcome,
      ]);
      const replacementBeforeSettlement = observation.capture();
      const eventsBeforeSettlement = events.slice();
      const ordinaryValueBeforeSettlement = store.value.peek();
      const ordinaryErrorBeforeSettlement = store.error.peek();
      firstReport?.("late first progress");
      const afterLateFirstReport = observation.capture();
      const eventsAfterLateReport = events.slice();

      secondReport?.("replacement progress");
      secondWorkSettled = true;
      secondWork.resolve(secondResult);
      const [secondSource, secondContinuationResult] = await Promise.all([
        secondSourceOutcome,
        replacementOutcome ?? Promise.resolve({ status: "missing" as const }),
      ]);
      const finalSnapshot = observation.capture();
      const finalEvents = events.slice();
      const ordinaryValueAfterSettlement = store.value.peek();
      const ordinaryErrorAfterSettlement = store.error.peek();
      const registeredListenerWasRemoved = callerSignal.state.removedRegisteredListener;
      stop();

      const retirement = graph.retire();
      const retirementOutcome = await observeOutcome(retirement);
      retirementWasCached = graph.retire() === retirement;

      expect(callerSignal.state.addCalls).toBe(1);
      expect(callerSignal.state.removeCalls).toBe(1);
      expect(registeredListenerWasRemoved).toBe(true);
      expect(replacementStarts).toBe(1);
      expect(replacementStartCause).toBeUndefined();
      expect(fetchCalls).toBe(2);
      expect(firstExecution).toBeDefined();
      expect(firstSource.status).toBe("fulfilled");
      if (firstSource.status === "fulfilled") expect(firstSource.value).toBe(firstResult);
      expect(firstContinuationResult.status).toBe("rejected");
      if (firstContinuationResult.status === "rejected")
        expect(firstContinuationResult.reason).toBe(cleanupCause);
      expect(replacementBeforeSettlement.execution).toBeDefined();
      expect(replacementBeforeSettlement.execution).not.toBe(firstExecution);
      expect(replacementBeforeSettlement).toMatchObject({
        status: "active",
        reportRevision: 0,
        hasValue: false,
        value: undefined,
        error: undefined,
      });
      expect(afterLateFirstReport).toMatchObject({
        execution: replacementBeforeSettlement.execution,
        status: "active",
        reportRevision: 0,
        hasValue: false,
        value: undefined,
      });
      const replacementActiveIndex = (records: typeof events) =>
        records.findIndex(
          ({ kind, snapshot }) =>
            kind === "lifecycle" &&
            snapshot.execution === replacementBeforeSettlement.execution &&
            snapshot.status === "active",
        );
      const replacementFrontier = replacementActiveIndex(eventsBeforeSettlement);
      expect(replacementFrontier).toBeGreaterThanOrEqual(0);
      const predecessorTerminalEvents = eventsBeforeSettlement
        .slice(0, replacementFrontier)
        .filter(
          ({ kind, snapshot }) =>
            kind === "lifecycle" &&
            snapshot.execution === firstExecution &&
            snapshot.status !== "active",
        );
      expect(predecessorTerminalEvents).toHaveLength(1);
      expect(predecessorTerminalEvents[0]).toMatchObject({
        kind: "lifecycle",
        snapshot: {
          execution: firstExecution,
          reportRevision: 1,
          status: "interrupted",
          hasValue: true,
          value: "first progress",
          error: undefined,
        },
      });
      const oldEventsAfterReplacement = (records: typeof events) => {
        const frontier = replacementActiveIndex(records);
        return records
          .slice(frontier + 1)
          .filter(({ snapshot }) => snapshot.execution === firstExecution);
      };
      expect(oldEventsAfterReplacement(eventsBeforeSettlement)).toEqual([]);
      expect(oldEventsAfterReplacement(eventsAfterLateReport)).toEqual([]);
      expect(oldEventsAfterReplacement(finalEvents)).toEqual([]);
      expect(ordinaryValueBeforeSettlement).toBe(initial);
      expect(ordinaryErrorBeforeSettlement).toBeUndefined();
      expect(secondSource.status).toBe("fulfilled");
      if (secondSource.status === "fulfilled") expect(secondSource.value).toBe(secondResult);
      expect(secondContinuationResult.status).toBe("fulfilled");
      expect(finalSnapshot).toMatchObject({
        execution: replacementBeforeSettlement.execution,
        status: "complete",
        reportRevision: 1,
        hasValue: true,
        value: "replacement progress",
        error: undefined,
      });
      expect(ordinaryValueAfterSettlement).toBe(secondResult);
      expect(ordinaryErrorAfterSettlement).toBeUndefined();
      expect(retirementOutcome.status).toBe("rejected");
      if (retirementOutcome.status === "rejected")
        expect(retirementOutcome.reason).toBe(cleanupCause);
      expect(retirementWasCached).toBe(true);
    } finally {
      if (!firstWorkSettled) {
        firstWorkSettled = true;
        firstWork.resolve(firstResult);
      }
      if (!secondWorkSettled) {
        secondWorkSettled = true;
        secondWork.resolve(secondResult);
      }
      stop();
      await graph.retire().then(
        () => undefined,
        () => undefined,
      );
    }
  });

  it("retains the active request authority through reentrant landing-time writes", async () => {
    const work = deferred<number>();
    let writeDuringLanding = (): void => undefined;
    let wroteDuringEquality = false;
    const query = createQuery({
      name: "progress.landing-write",
      key: (input: string) => input,
      equals(a: number, b: number) {
        if (!wroteDuringEquality && a === 1 && b === 2) {
          wroteDuringEquality = true;
          writeDuringLanding();
        }
        return Object.is(a, b);
      },
      fetch: async () => work.promise,
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-landing-write", operations: { query } }),
    });
    const store = graph.api.query("one", { initial: 1 });
    writeDuringLanding = () => store.set(9);
    const refresh = store.revalidate();
    const observation = observeQueryProgress(store);
    const execution = observation.capture().execution;

    work.resolve(2);
    await refresh;

    expect(wroteDuringEquality).toBe(true);
    expect(store.value.get()).toBe(9);
    expect(observation.capture()).toMatchObject({
      execution,
      status: "interrupted",
      hasValue: false,
      reportRevision: 0,
    });
    graph.dispose();
  });

  it("keeps capture nontracking and faults reads after query disposal", async () => {
    const work = deferred<number>();
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.capture-disposal",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-capture-disposal", operations: { query } }),
    });
    const store = graph.api.query("one");
    const observation = observeQueryProgress(store);
    const capturedRevision = computed(() => observation.capture().reportRevision);
    const revisions: number[] = [];
    const stop = subscribe(capturedRevision, (revision) => revisions.push(revision));
    const beforeForbiddenReport = observation.capture();
    const forbiddenReporter = computed(() => withoutWrites(() => report("forbidden")));
    expect(() => forbiddenReporter.get()).toThrow(Fault);
    expect(observation.capture()).toBe(beforeForbiddenReport);
    report("partial");

    expect(revisions).toEqual([0]);
    expect(observation.capture()).toMatchObject({ reportRevision: 1, value: "partial" });
    stop();
    work.resolve(1);
    await store.ready;
    graph.dispose();

    expect(() => observation.capture()).toThrow();
  });

  it("shares progress for joined calls and rejects reports from a replaced request", async () => {
    const runs: Array<{
      readonly work: Deferred<number>;
      readonly returned: Promise<number>;
      readonly report: (value: string) => void;
    }> = [];
    const query = createQuery<string, number, string, string>({
      name: "progress.replaced",
      key: (input) => input,
      fetch: (_input, options) => {
        const work = deferred<number>();
        const returned = work.promise;
        runs.push({ work, returned, report: options.reportProgress });
        return returned;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-replaced", operations: { query } }),
    });
    const first = graph.api.query("same");
    const joined = graph.api.query("same");
    expect(first.progress).toBe(joined.progress);
    expect(runs).toHaveLength(1);
    const firstExecution = first.progress.get().execution;

    runs[0]!.report("same");
    runs[0]!.report("same");
    expect(first.progress.get().reportRevision).toBe(2);

    const controller = new AbortController();
    const refresh = first.revalidate({ abortSignal: controller.signal });
    expect(runs).toHaveLength(2);
    const replacement = first.progress.get();
    expect(replacement.execution).not.toBe(firstExecution);
    expect(replacement).toMatchObject({ status: "active", reportRevision: 0, hasValue: false });

    runs[0]!.report("late");
    expect(first.progress.get()).toMatchObject({
      execution: replacement.execution,
      status: "active",
      reportRevision: 0,
      hasValue: false,
    });
    runs[1]!.report("current");
    expect(first.progress.get()).toMatchObject({
      execution: replacement.execution,
      status: "active",
      reportRevision: 1,
      hasValue: true,
      value: "current",
    });
    controller.abort();
    expect(first.progress.get()).toMatchObject({
      execution: replacement.execution,
      status: "interrupted",
      reportRevision: 1,
      hasValue: true,
      value: "current",
    });

    runs[0]!.work.resolve(1);
    runs[1]!.work.resolve(2);
    await refresh;
    await runs[0]!.returned;
    expect(first.progress.get()).toMatchObject({
      execution: replacement.execution,
      status: "interrupted",
      reportRevision: 1,
      hasValue: true,
      value: "current",
    });
    graph.dispose();
  });

  it("marks reset progress interrupted and gives a later same-key request a new identity", async () => {
    const runs: Array<{
      readonly work: Deferred<number>;
      readonly returned: Promise<number>;
      readonly report: (value: string) => void;
    }> = [];
    let oldHostSettled = false;
    const query = createQuery<string, number, string, string>({
      name: "progress.reset",
      key: (input) => input,
      fetch: (_input, options) => {
        const work = deferred<number>();
        const isFirstRun = runs.length === 0;
        const returned = work.promise.finally(() => {
          if (isFirstRun) oldHostSettled = true;
        });
        runs.push({ work, returned, report: options.reportProgress });
        return returned;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-reset", operations: { query } }),
    });
    const first = graph.api.query("same");
    const firstExecution = first.progress.get().execution;
    runs[0]!.report("before-reset");

    const reset = graph.reset();
    expect(first.progress.get()).toMatchObject({
      execution: firstExecution,
      status: "interrupted",
      reportRevision: 1,
      hasValue: true,
      value: "before-reset",
    });
    await reset;
    expect(oldHostSettled).toBe(false);

    const replacement = graph.api.query("same");
    const replacementReady = replacement.ready;
    expect(runs).toHaveLength(2);
    const nextExecution = replacement.progress.get().execution;
    expect(nextExecution).not.toBe(firstExecution);
    runs[0]!.report("late-after-reset");
    expect(replacement.progress.get()).toMatchObject({
      execution: nextExecution,
      status: "active",
      reportRevision: 0,
      hasValue: false,
    });
    runs[1]!.report("after-reset");
    runs[0]!.work.resolve(1);
    runs[1]!.work.resolve(2);

    await expect(replacementReady).resolves.toBe(2);
    await runs[0]!.returned;
    expect(oldHostSettled).toBe(true);
    expect(replacement.progress.get()).toMatchObject({
      execution: nextExecution,
      status: "complete",
      reportRevision: 1,
      hasValue: true,
      value: "after-reset",
    });
    graph.dispose();
  });

  it("interrupts progress when a manual write or adoption changes generation", async () => {
    const runs: Array<{
      readonly input: string;
      readonly work: Deferred<number>;
      readonly returned: Promise<number>;
      readonly isSettled: () => boolean;
      readonly report: (value: string) => void;
    }> = [];
    const query = createQuery<string, number, string, string>({
      name: "progress.authority-change",
      key: (input) => input,
      snapshot: true,
      fetch: (input, options) => {
        const work = deferred<number>();
        let settled = false;
        const returned = work.promise.finally(() => {
          settled = true;
        });
        runs.push({
          input,
          work,
          returned,
          isSettled: () => settled,
          report: options.reportProgress,
        });
        return returned;
      },
    });
    const contract = defineContract({
      namespace: "progress-authority-change",
      operations: { query },
    });
    const manualGraph = createGraph({ contract });
    const sourceGraph = createGraph({ contract });
    const targetGraph = createGraph({ contract });
    try {
      const manual = manualGraph.api.query("manual");
      const manualRun = runs.find((run) => run.input === "manual")!;
      const manualExecution = manual.progress.get().execution;
      manualRun.report("before-write");
      manual.set(11);

      expect(manual.value.get()).toBe(11);
      expect(manualRun.isSettled()).toBe(false);
      expect(manual.progress.get()).toMatchObject({
        execution: manualExecution,
        status: "interrupted",
        reportRevision: 1,
        hasValue: true,
        value: "before-write",
        error: undefined,
      });
      manualRun.report("after-write");
      expect(manual.progress.get().reportRevision).toBe(1);

      const source = sourceGraph.api.query("adopt", { initial: 5 });
      await expect(source.ready).resolves.toBe(5);
      const transfer = snapshot(sourceGraph);
      const adopted = targetGraph.api.query("adopt");
      const adoptedRun = runs.find((run) => run.input === "adopt")!;
      const adoptedExecution = adopted.progress.get().execution;
      adoptedRun.report("before-adoption");

      adopt(targetGraph, transfer);

      expect(adopted.value.get()).toBe(5);
      expect(adoptedRun.isSettled()).toBe(false);
      expect(adopted.progress.get()).toMatchObject({
        execution: adoptedExecution,
        status: "interrupted",
        reportRevision: 1,
        hasValue: true,
        value: "before-adoption",
        error: undefined,
      });
      adoptedRun.report("after-adoption");
      expect(adopted.progress.get().reportRevision).toBe(1);
      expect(runs.map((run) => run.input)).toEqual(["manual", "adopt"]);

      manualRun.work.resolve(99);
      adoptedRun.work.resolve(99);
      await Promise.all([manualRun.returned, adoptedRun.returned]);
      expect(manualRun.isSettled()).toBe(true);
      expect(adoptedRun.isSettled()).toBe(true);
      expect(manual.value.get()).toBe(11);
      expect(manual.progress.get()).toMatchObject({
        execution: manualExecution,
        status: "interrupted",
        value: "before-write",
      });
      expect(adopted.value.get()).toBe(5);
      expect(adopted.progress.get()).toMatchObject({
        execution: adoptedExecution,
        status: "interrupted",
        value: "before-adoption",
      });
    } finally {
      for (const run of runs) run.work.resolve(0);
      await Promise.all(runs.map((run) => run.returned));
      manualGraph.dispose();
      sourceGraph.dispose();
      targetGraph.dispose();
    }
  });

  it("shares one bound profile request while isolating other profiles and contexts", async () => {
    type Progress = { readonly value: string };
    const runs: Array<{
      readonly work: Deferred<number>;
      readonly report: (value: Progress) => void;
    }> = [];
    const query = createQuery<string, number, string, Progress>({
      name: "progress.bound",
      key: (input) => input,
      fetch: async () => 0,
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-bound", operations: { query } }),
    });
    const context = createBindingContext(graph);
    const otherContext = createBindingContext(graph);
    const profile = {};
    const otherProfile = {};
    const bind = (owner: typeof context, selectedProfile: object) =>
      bindOperation(graph, query, {
        context: owner,
        profile: selectedProfile,
        fetch: (_input, options) => {
          const work = deferred<number>();
          runs.push({ work, report: options.reportProgress });
          return work.promise;
        },
      });
    const firstBinding = bind(context, profile);
    const otherProfileBinding = bind(context, otherProfile);
    const otherContextBinding = bind(otherContext, profile);
    const first = firstBinding.member("same-key");
    const joined = firstBinding.member("same-key");
    const otherProfileStore = otherProfileBinding.member("same-key");
    const otherContextStore = otherContextBinding.member("same-key");

    expect(runs).toHaveLength(3);
    expect(first.identity).toBe(joined.identity);
    expect(first.progress).toBe(joined.progress);
    expect(first.progress).not.toBe(otherProfileStore.progress);
    expect(first.progress).not.toBe(otherContextStore.progress);
    const firstExecution = first.progress.get().execution;
    const profileExecution = otherProfileStore.progress.get().execution;
    const contextExecution = otherContextStore.progress.get().execution;
    expect(new Set([firstExecution, profileExecution, contextExecution]).size).toBe(3);

    runs[0]!.report({ value: "first-profile" });
    expect(first.progress.get()).toMatchObject({
      reportRevision: 1,
      value: { value: "first-profile" },
    });
    expect(otherProfileStore.progress.get()).toMatchObject({ reportRevision: 0, hasValue: false });
    expect(otherContextStore.progress.get()).toMatchObject({ reportRevision: 0, hasValue: false });
    runs[1]!.report({ value: "other-profile" });
    runs[2]!.report({ value: "other-context" });
    const firstReady = first.ready;
    const joinedReady = joined.ready;
    const otherProfileReady = otherProfileStore.ready;
    const otherContextReady = otherContextStore.ready;
    runs.forEach((run, index) => run.work.resolve(index + 1));

    await expect(
      Promise.all([firstReady, joinedReady, otherProfileReady, otherContextReady]),
    ).resolves.toEqual([1, 1, 2, 3]);
    expect(first.progress.get()).toMatchObject({
      execution: firstExecution,
      status: "complete",
      value: { value: "first-profile" },
    });
    expect(otherProfileStore.progress.get()).toMatchObject({
      execution: profileExecution,
      status: "complete",
      value: { value: "other-profile" },
    });
    expect(otherContextStore.progress.get()).toMatchObject({
      execution: contextExecution,
      status: "complete",
      value: { value: "other-context" },
    });
    graph.dispose();
  });

  it("interrupts before the first report without settling the host promise", async () => {
    const work = deferred<number>();
    let hostPromiseSettled = false;
    let fetchPromise!: Promise<number>;
    const query = createQuery<string, number, string, string>({
      name: "progress.cancelled",
      key: (input) => input,
      fetch: (_input, _options) => {
        fetchPromise = work.promise.finally(() => {
          hostPromiseSettled = true;
        });
        return fetchPromise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-cancelled", operations: { query } }),
    });
    const controller = new AbortController();
    const store = graph.api.query("cancel", { abortSignal: controller.signal });
    const ready = store.ready;
    const execution = store.progress.get().execution;

    controller.abort();

    expect(store.progress.get()).toMatchObject({
      execution,
      status: "interrupted",
      reportRevision: 0,
      hasValue: false,
    });
    const cancellation = store.error.peek();
    expect(store.progress.get().error).toBe(cancellation);
    await expect(ready).rejects.toBe(cancellation);
    expect(hostPromiseSettled).toBe(false);

    work.resolve(9);
    await fetchPromise;
    expect(hostPromiseSettled).toBe(true);
    expect(store.progress.get()).toMatchObject({
      execution,
      status: "interrupted",
      reportRevision: 0,
      hasValue: false,
    });
    expect(store.value.peek()).toBeUndefined();
    graph.dispose();
  });

  it("retains its progress observer without activating freshness or touching the idle window", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "progress.liveness",
      key: () => "one",
      revalidateAfterMs: 5,
      fetch: backend.respond<void, number>("progress.liveness"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-liveness", operations: { query } }),
      idleMs: 10,
    });
    const store = graph.api.query(undefined);
    const ready = store.ready;
    backend.resolve(backend.calls[0]!, 1);
    await expect(ready).resolves.toBe(1);
    const execution = store.progress.get().execution;
    const stop = subscribe(store.progress, () => undefined);

    jest.advanceTimersByTime(16);
    expect(backend.calls).toHaveLength(1);
    expect(store.progress.get()).toMatchObject({ execution, status: "complete" });

    stop();
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    expect(store.progress.get()).toMatchObject({ status: "idle", hasValue: false });
    graph.dispose();
  });

  it("retains through subscriptions but not through capture", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "progress.observation-liveness",
      key: () => "one",
      revalidateAfterMs: 5,
      fetch: backend.respond<void, number>("progress.observation-liveness"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "progress-observation-liveness",
        operations: { query },
      }),
      idleMs: 10,
    });
    const first = graph.api.query(undefined);
    const firstReady = first.ready;
    backend.resolve(backend.calls[0]!, 1);
    await expect(firstReady).resolves.toBe(1);
    const firstObservation = observeQueryProgress(first);
    expect(firstObservation.capture().status).toBe("complete");

    jest.advanceTimersByTime(16);
    expect(firstObservation.capture()).toMatchObject({ status: "idle", hasValue: false });
    expect(backend.calls).toHaveLength(1);

    const second = graph.api.query(undefined);
    const secondReady = second.ready;
    backend.resolve(backend.calls[1]!, 2);
    await expect(secondReady).resolves.toBe(2);
    const secondObservation = observeQueryProgress(second);
    const execution = secondObservation.capture().execution;
    const stop = secondObservation.subscribe(() => undefined);

    jest.advanceTimersByTime(16);
    expect(backend.calls).toHaveLength(2);
    expect(secondObservation.capture()).toMatchObject({ execution, status: "complete" });

    stop();
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    expect(secondObservation.capture()).toMatchObject({ status: "idle", hasValue: false });
    graph.dispose();
  });

  it("retains progress on failure even when the rejection reason is undefined", async () => {
    const work = deferred<number>();
    let report!: (value: string) => void;
    const query = createQuery<string, number, string, string>({
      name: "progress.failure",
      key: (input) => input,
      fetch: (_input, options) => {
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "progress-failure", operations: { query } }),
    });
    const store = graph.api.query("one");
    const ready = store.ready;
    const observation = observeQueryProgress(store);
    const events: Array<[string, number, string]> = [];
    const stop = observation.subscribe(({ kind, snapshot }) => {
      events.push([kind, snapshot.reportRevision, snapshot.status]);
      if (kind === "report" && snapshot.reportRevision === 1) {
        report("second");
        report("third");
        work.reject(undefined);
      }
    });
    report("partial");
    const outcome = await Promise.allSettled([ready]);

    expect(outcome[0]?.status).toBe("rejected");
    if (outcome[0]?.status === "rejected") expect(outcome[0].reason).toBeUndefined();
    expect(store.progress.get()).toMatchObject({
      status: "failed",
      reportRevision: 3,
      hasValue: true,
      value: "third",
      error: undefined,
    });
    expect(events.filter(([kind]) => kind === "report")).toEqual([
      ["report", 1, "active"],
      ["report", 2, "active"],
      ["report", 3, "active"],
    ]);
    expect(events.at(-1)).toEqual(["lifecycle", 3, "failed"]);
    stop();
    graph.dispose();
  });
  it("forwards a cold passive progress facade to the native query progress State after publication", async () => {
    const targetWork = deferred<number>();
    let targetReport: ((value: string) => void) | undefined;
    let targetFetches = 0;
    const sourceQuery = createQuery<string, number, string, string>({
      name: "progress.required-transfer",
      key: (input) => input,
      fetch: async (_input, options) => {
        options.reportProgress("source-one");
        options.reportProgress("source-two");
        return 7;
      },
    });
    const targetQuery = createQuery<string, number, string, string>({
      name: "progress.required-transfer",
      key: (input) => input,
      fetch: (_input, options) => {
        targetFetches += 1;
        targetReport = options.reportProgress;
        return targetWork.promise;
      },
    });
    const sourceGraph = createGraph({
      contract: defineContract({
        namespace: "progress-required-transfer",
        operations: { query: sourceQuery },
      }),
    });
    const targetGraph = createGraph({
      contract: defineContract({
        namespace: "progress-required-transfer",
        operations: { query: targetQuery },
      }),
    });
    let claim: PassiveTransferClaim<typeof targetGraph.api.query> | undefined;
    let stop: (() => void) | undefined;
    let adoptionPublished = false;
    try {
      const sourceStore = sourceGraph.api.query("one");
      await expect(sourceStore.ready).resolves.toBe(7);
      const sourceRoot = computed(() => sourceStore.progress.get().reportRevision);
      expect(sourceRoot.get()).toBe(2);
      const sourceConstruction = captureReactiveConstruction(sourceRoot);
      const capture = prepareRequiredTransferCapture({
        graphs: [sourceGraph],
        roots: [{ source: sourceRoot, construction: sourceConstruction }],
        codec: requiredTransferCodec,
      });
      const captureCommit = capture.seal();
      if (captureCommit === undefined) throw new Error("The progress capture did not seal");
      captureCommit.publish();
      captureCommit.notifications.deliver();
      await capture.retired;

      claim = preparePassiveTransferClaim(targetGraph, targetGraph.api.query, "one");
      expect(targetFetches).toBe(0);
      expect(() => claim!.store.progress.get()).toThrow(TypeError);
      const targetRoot = computed(() => claim!.store.progress.get().reportRevision);
      const targetConstruction = captureReactiveConstruction(targetRoot);
      const rootSlot = capture.image.roots[0]!;
      const sourceSlot = capture.image.stores[0]!.slot;
      const adoption = prepareRequiredTransferAdoption(capture.image, {
        graphs: [targetGraph],
        sources: [{ slot: sourceSlot, claim }],
        reactive: [{ slot: rootSlot, target: targetRoot, construction: targetConstruction }],
        codec: requiredTransferCodec,
        mappedStateWrites: [],
      });
      const adoptionCommit = adoption.seal();
      if (adoptionCommit === undefined)
        throw new Error("The passive progress adoption did not seal");
      adoptionCommit.publish();
      adoptionPublished = true;
      adoptionCommit.notifications.deliver();
      await Promise.all([adoption.retired, claim.retired]);

      const seen: number[] = [];
      stop = subscribe(targetRoot, (revision) => seen.push(revision));
      const revalidation = claim.store.revalidate();
      const ready = claim.store.ready;
      expect(targetFetches).toBe(1);
      if (targetReport === undefined)
        throw new Error("The target query did not receive its progress reporter");
      targetReport("target-report");
      expect(targetRoot.get()).toBe(1);
      expect(seen).toContain(1);
      targetWork.resolve(9);
      await expect(revalidation).resolves.toBeUndefined();
      await expect(ready).resolves.toBe(9);
      expect(claim.store.progress.get()).toMatchObject({ status: "complete", reportRevision: 1 });
    } finally {
      stop?.();
      if (claim !== undefined && !adoptionPublished) claim.discard();
      sourceGraph.dispose();
      targetGraph.dispose();
    }
  });

  it("refuses a bound refresh invalidated by its cached seed and keeps its sibling listener", async () => {
    type Input = { readonly key: string; readonly label: string };
    const work = deferred<number>();
    const input: Input = { key: "shared", label: "A" };
    const fetchInputs: string[] = [];
    let report!: (value: number) => void;
    let workSettled = false;
    const query = createQuery<Input, number, string, number>({
      name: "cohort.bound-seed-refusal",
      acquisition: "fetch",
      key: (value) => value.key,
      fetch: (value, options) => {
        fetchInputs.push(value.label);
        report = options.reportProgress;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-bound-seed-refusal", operations: { query } }),
    });
    const context = createBindingContext(graph);
    const binding = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
    const attemptAuthority = new AbortController();
    const refusal = new Error("bound call lost authority");
    const siblingReports: number[] = [];
    let siblingStop: (() => void) | undefined;
    let attemptStop: (() => void) | undefined;
    let attemptSeeded = false;
    const original = binding.member(input);

    try {
      const sibling = binding.member(input);
      siblingStop = observeQueryProgress(sibling).subscribe((event) => {
        if (event.kind === "report" && event.snapshot.hasValue)
          siblingReports.push(event.snapshot.value!);
      });
      report(1);
      workSettled = true;
      work.resolve(3);
      await expect(original.ready).resolves.toBe(3);
      await expect(sibling.ready).resolves.toBe(3);

      let didThrow = false;
      let failure: unknown;
      try {
        callQueryWithProgress(
          binding.member,
          [input],
          [
            {
              signal: new AbortController().signal,
              onEvent(event, view) {
                attemptStop = view.stop;
                if (event.kind === "current" && event.snapshot.reportRevision === 1) {
                  attemptSeeded = true;
                  attemptAuthority.abort();
                }
              },
            },
          ],
          () => {
            if (attemptAuthority.signal.aborted) throw refusal;
          },
        );
      } catch (cause) {
        didThrow = true;
        failure = cause;
      }

      expect(didThrow).toBe(true);
      expect(failure).toBe(refusal);
      expect(attemptSeeded).toBe(true);
      expect(context.active.get()).toBe(true);
      expect(fetchInputs).toEqual(["A"]);

      expect(siblingReports).toEqual([1]);
      expect(observeQueryProgress(sibling).capture()).toMatchObject({
        status: "complete",
        reportRevision: 1,
        value: 1,
      });
      expect(fetchInputs).toEqual(["A"]);
    } finally {
      if (!workSettled) work.resolve(3);
      attemptStop?.();
      siblingStop?.();
      graph.dispose();
    }
  });

  it("preserves a reentrant same-key query admitted by the authority guard", async () => {
    type Input = { readonly key: string; readonly label: "A" | "B" };
    type Result = { readonly winner: string };
    const work = deferred<Result>();
    const result = Object.freeze({ winner: "B" });
    const inputA: Input = { key: "shared", label: "A" };
    const inputB: Input = { key: "shared", label: "B" };
    const fetchInputs: string[] = [];
    const query = createQuery<Input, Result, string, number>({
      name: "cohort.guard-reentry",
      key: (value) => value.key,
      fetch: (value) => {
        fetchInputs.push(value.label);
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cohort-guard-reentry", operations: { query } }),
    });
    let foreignStore: QueryStore<Result, number> | undefined;
    const refusal = new Error("A lost admission authority");
    let didThrow = false;
    let failure: unknown;

    try {
      try {
        callQueryWithProgress(graph.api.query, [inputA], [], () => {
          foreignStore ??= graph.api.query(inputB);
          throw refusal;
        });
      } catch (cause) {
        didThrow = true;
        failure = cause;
      }

      expect(didThrow).toBe(true);
      expect(failure).toBe(refusal);
      expect(foreignStore).toBeDefined();
      expect(fetchInputs).toEqual(["B"]);
      work.resolve(result);
      await expect(foreignStore!.ready).resolves.toBe(result);
      expect(foreignStore!.progress.get()).toMatchObject({ status: "complete" });
      expect(fetchInputs).toEqual(["B"]);
    } finally {
      graph.dispose();
    }
  });

  it("preserves foreign work started while the caller signal is registered", async () => {
    type Input = { readonly key: string; readonly label: "A" | "B" };
    const inputA: Input = { key: "shared", label: "A" };
    const inputB: Input = { key: "shared", label: "B" };
    const work = deferred<string>();
    const caller = new AbortController();
    const fetchInputs: string[] = [];
    let foreignStore: QueryStore<string, number> | undefined;
    let foreignSignal: AbortSignal | undefined;
    let removes = 0;
    let workSettled = false;
    const query = createQuery<Input, string, string, number>({
      name: "cohort.signal-registration-reentry",
      key: (input) => input.key,
      fetch: (input, options) => {
        fetchInputs.push(input.label);
        foreignSignal = options.abortSignal;
        return work.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "cohort-signal-registration-reentry",
        operations: { query },
      }),
    });
    const signal = new Proxy(caller.signal, {
      get(target, property) {
        if (property === "addEventListener")
          return (
            type: string,
            listener: EventListenerOrEventListenerObject,
            options?: AddEventListenerOptions,
          ) => {
            target.addEventListener(type, listener, options);
            foreignStore ??= graph.api.query(inputB);
          };
        if (property === "removeEventListener")
          return (type: string, listener: EventListenerOrEventListenerObject) => {
            removes += 1;
            target.removeEventListener(type, listener);
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    try {
      let refused = false;
      try {
        callQueryWithProgress(graph.api.query, [inputA, { abortSignal: signal }], [], () => {});
      } catch {
        refused = true;
      }
      expect(refused).toBe(true);
      expect(removes).toBe(1);
      expect(fetchInputs).toEqual(["B"]);
      expect(foreignStore).toBeDefined();
      expect(foreignSignal?.aborted).toBe(false);
      caller.abort();
      expect(foreignSignal?.aborted).toBe(false);
      workSettled = true;
      work.resolve("B result");
      await expect(foreignStore!.ready).resolves.toBe("B result");
      expect(fetchInputs).toEqual(["B"]);
    } finally {
      if (!workSettled) work.resolve("B result");
      graph.dispose();
    }
  });

  it("preserves foreign work started by predecessor interruption before admission", async () => {
    type Input = { readonly key: string; readonly label: "first" | "A" | "B" };
    const firstWork = deferred<string>();
    const foreignWork = deferred<string>();
    const inputFirst: Input = { key: "shared", label: "first" };
    const inputA: Input = { key: "shared", label: "A" };
    const inputB: Input = { key: "shared", label: "B" };
    const fetchInputs: string[] = [];
    let foreignStore: QueryStore<string, number> | undefined;
    let foreignSignal: AbortSignal | undefined;
    let interruptions = 0;
    const query = createQuery<Input, string, string, number>({
      name: "cohort.predecessor-interruption-reentry",
      acquisition: "fetch",
      key: (input) => input.key,
      fetch: (input, options) => {
        fetchInputs.push(input.label);
        if (input.label === "first") {
          options.abortSignal.addEventListener("abort", () => {
            interruptions += 1;
            foreignStore = graph.api.query(inputB);
          });
          return firstWork.promise;
        }
        foreignSignal = options.abortSignal;
        return foreignWork.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "cohort-predecessor-interruption-reentry",
        operations: { query },
      }),
    });
    const first = graph.api.query(inputFirst);
    try {
      first.set("manual value");
      let refused = false;
      try {
        callQueryWithProgress(graph.api.query, [inputA], [], () => {});
      } catch {
        refused = true;
      }
      expect(interruptions).toBe(1);
      expect(refused).toBe(true);
      expect(fetchInputs).toEqual(["first", "B"]);
      expect(foreignStore).toBeDefined();
      expect(foreignSignal?.aborted).toBe(false);
      foreignWork.resolve("B result");
      await expect(foreignStore!.ready).resolves.toBe("B result");
    } finally {
      firstWork.resolve("old result");
      foreignWork.resolve("B result");
      graph.dispose();
    }
  });

  it("keeps an admitted request and its reentrant same-key join after guard invalidation", async () => {
    type Input = { readonly key: string; readonly label: "A" | "B" };
    type Result = { readonly request: number };
    const firstResult = Object.freeze({ request: 1 });
    const admittedResult = Object.freeze({ request: 2 });
    const admittedWork = deferred<Result>();
    const inputA: Input = { key: "shared", label: "A" };
    const fetchInputs: string[] = [];
    let admittedSignal: AbortSignal | undefined;
    let admittedSettled = false;
    const query = createQuery<Input, Result, string, number>({
      name: "cohort.guard-reentry-after-admission",
      acquisition: "fetch",
      key: (value) => value.key,
      fetch: (value, options) => {
        fetchInputs.push(value.label);
        if (fetchInputs.length === 1) return Promise.resolve(firstResult);
        admittedSignal = options.abortSignal;
        return admittedWork.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "cohort-guard-reentry-after-admission",
        operations: { query },
      }),
    });
    const initial = graph.api.query(inputA);
    let foreignStore: QueryStore<Result, number> | undefined;
    let authorized = true;
    let lifecycleObserved = false;
    const refusal = new Error("A authority expired after publication");
    const observation = observeQueryProgress(initial);
    const stop = observation.subscribe((event) => {
      if (event.kind !== "lifecycle" || event.snapshot.status !== "active") return;
      lifecycleObserved = true;
      foreignStore = graph.api.query(inputA);
      authorized = false;
    });

    try {
      await expect(initial.ready).resolves.toBe(firstResult);
      callQueryWithProgress(graph.api.query, [inputA], [], () => {
        if (!authorized) throw refusal;
      });

      expect(lifecycleObserved).toBe(true);
      expect(foreignStore).toBeDefined();
      expect(fetchInputs).toEqual(["A", "A"]);
      expect(admittedSignal?.aborted).toBe(false);

      admittedSettled = true;
      admittedWork.resolve(admittedResult);
      await expect(foreignStore!.ready).resolves.toBe(admittedResult);
      expect(fetchInputs).toEqual(["A", "A"]);
      expect(admittedSignal?.aborted).toBe(false);
    } finally {
      if (!admittedSettled) admittedWork.resolve(admittedResult);
      stop();
      graph.dispose();
    }
  });

  it("retains an undefined authority refusal alongside progress-signal unlink failure", () => {
    const controller = new AbortController();
    const cleanupCause = new Error("progress listener removal failed");
    const signal = throwingRemovalSignal(controller, cleanupCause);
    let current = true;
    let calls = 0;
    let didThrow = false;
    let failure: unknown;
    const query = createQuery<void, number, string, number>({
      name: "cohort.undefined-authority-refusal",
      key: () => "one",
      fetch: () => {
        calls += 1;
        return Promise.resolve(1);
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "cohort-undefined-authority-refusal",
        operations: { query },
      }),
    });

    try {
      try {
        callQueryWithProgress(
          graph.api.query,
          [],
          [
            {
              signal: signal.signal,
              onEvent(event) {
                if (event.kind === "current") current = false;
              },
            },
          ],
          () => {
            if (!current) throw undefined;
          },
        );
      } catch (cause) {
        didThrow = true;
        failure = cause;
      }

      expect(didThrow).toBe(true);
      expect(failure).toBeInstanceOf(AggregateError);
      if (!(failure instanceof AggregateError))
        throw new Error("Expected authority refusal and signal cleanup failures");
      expect(failure.errors).toHaveLength(2);
      expect(failure.errors[0]).toBeUndefined();
      expect(failure.errors[1]).toBe(cleanupCause);
      expect(signal.state.addCalls).toBe(1);
      expect(signal.state.removeCalls).toBe(1);
      expect(signal.state.removedRegisteredListener).toBe(true);
      expect(calls).toBe(0);
    } finally {
      graph.dispose();
    }
  });

  it("retains an undefined authority refusal alongside caller-signal unlink failure", () => {
    const controller = new AbortController();
    const cleanupCause = new Error("caller signal removal failed");
    let current = true;
    let removes = 0;
    let calls = 0;
    const signal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "addEventListener")
          return (
            type: string,
            listener: EventListenerOrEventListenerObject,
            options?: AddEventListenerOptions,
          ) => {
            target.addEventListener(type, listener, options);
            current = false;
          };
        if (property === "removeEventListener")
          return (type: string, listener: EventListenerOrEventListenerObject) => {
            target.removeEventListener(type, listener);
            removes += 1;
            throw cleanupCause;
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const query = createQuery<string, number, string, number>({
      name: "cohort.caller-signal-cleanup",
      key: (input) => input,
      fetch: () => {
        calls += 1;
        return Promise.resolve(1);
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "cohort-caller-signal-cleanup",
        operations: { query },
      }),
    });
    try {
      let didThrow = false;
      let failure: unknown;
      try {
        callQueryWithProgress(graph.api.query, ["one", { abortSignal: signal }], [], () => {
          if (!current) throw undefined;
        });
      } catch (cause) {
        didThrow = true;
        failure = cause;
      }
      expect(didThrow).toBe(true);
      expect(failure).toBeInstanceOf(AggregateError);
      if (!(failure instanceof AggregateError))
        throw new Error("Expected refusal and caller cleanup failures");
      expect(failure.errors).toEqual([undefined, cleanupCause]);
      expect(removes).toBe(1);
      expect(calls).toBe(0);
    } finally {
      graph.dispose();
    }
  });

  it("rejects an asynchronous authority check before query dispatch", () => {
    let calls = 0;
    const query = createQuery<string, number, string, number>({
      name: "cohort.async-authority-check",
      key: (input) => input,
      fetch: () => {
        calls += 1;
        return Promise.resolve(1);
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "cohort-async-authority-check",
        operations: { query },
      }),
    });

    try {
      const shadowed = Object.defineProperty(Promise.resolve(), "then", { value: undefined });
      const observationCause = new Error("authority Promise observation failed");
      const observationFault = Object.defineProperty(Promise.resolve(), "constructor", {
        get: () => {
          throw observationCause;
        },
      });
      for (const guard of [async () => undefined, () => shadowed, () => ({})]) {
        expect(() => callQueryWithProgress(graph.api.query, ["one"], [], guard)).toThrow(TypeError);
      }
      let failure: unknown;
      try {
        callQueryWithProgress(graph.api.query, ["one"], [], () => observationFault);
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBe(observationCause);
      expect(calls).toBe(0);
    } finally {
      graph.dispose();
    }
  });
});
