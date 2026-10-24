import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  bindOperation,
  createBindingContext,
  createGraph,
  createQuery,
  defineContract,
  observeQueryProgress,
} from "../../src/contract/index.ts";
import { Fault } from "../../src/fault.ts";
import {
  __internal as reactiveInternal,
  computed,
  subscribe,
  withoutWrites,
} from "../../src/reactive.ts";
import { adopt, snapshot } from "../../src/realm.ts";
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

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

describe("query progress", () => {
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
});
