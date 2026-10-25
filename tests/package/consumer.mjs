import {
  Fault,
  adopt,
  bindOperation as rootBindOperation,
  bindStream as rootBindStream,
  inspectBoundAddress as rootInspectBoundAddress,
  createGraph,
  createStore,
  defineContract,
  observeQueryProgress as rootObserveQueryProgress,
  peekStore,
  signal,
  snapshot,
  subscribe,
} from "aeolia";
import {
  affects,
  bindOperation,
  createBindingContext,
  createMutation,
  createQuery,
  createStream,
  bindStream,
  inspectBoundAddress,
  observeQueryProgress,
  storeKey,
  predictionFor,
  profileEffect,
} from "aeolia/contract";
import { computed, subscribe as subscribeSignal } from "aeolia/reactive";
import { testBackend } from "aeolia/testing";

function check(condition, message) {
  if (!condition) throw new Error(message);
}

async function expectPending(promise, message) {
  let timer;
  try {
    const result = await Promise.race([
      promise.then(
        () => "settled",
        () => "settled",
      ),
      new Promise((resolve) => {
        timer = setTimeout(() => resolve("pending"), 0);
      }),
    ]);
    check(result === "pending", message);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export async function verifyConsumer() {
  await verifyTransferConsumer();
  await verifyBoundConsumer();
  await verifyBoundStreamConsumer();
  await verifyAcquisitionConsumer();
  const count = signal(2);
  const doubled = computed(() => count.get() * 2);
  const signalValues = [];
  const stopSignal = subscribeSignal(doubled, (value) => signalValues.push(value));
  try {
    count.set(3);
    check(JSON.stringify(signalValues) === "[4,6]", "Cross-entry signal propagation failed");
  } finally {
    stopSignal();
  }

  const backend = testBackend();
  const item = createQuery({
    name: "item.get",
    key: (id) => `items/${id}`,
    fetch: backend.respond("item.get"),
  });
  const save = createMutation({
    name: "item.save",
    affects: [affects(item, { select: (input) => input.id, on: "invalidate" })],
    run: backend.perform("item.save"),
  });
  const unobservedFaults = [];
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-consumer", operations: { item, save } }),
    onUnobservedFault: (error) => {
      unobservedFaults.push(error);
    },
  });
  const pending = (name) => {
    const calls = backend.pending(name);
    check(calls.length === 1, `Expected one pending ${name} call, got ${calls.length}`);
    return calls[0];
  };
  const store = graph.api.item("one");
  const observed = [];
  const stop = subscribe(store, (value) => observed.push(value));
  let retirementCall;
  let retirementCallResolved = false;
  try {
    backend.resolve(pending("item.get"), "initial");
    check((await store.ready) === "initial", "Packed query did not resolve");

    const mutation = graph.api.save({ id: "one" });
    backend.resolve(pending("item.save"), "saved");
    check((await mutation) === "saved", "Packed mutation did not resolve");
    check(store.status.get() === "revalidating", "Mutation did not refresh the observed query");
    backend.resolve(pending("item.get"), "updated");
    check((await store.ready) === "updated", "Refreshed data did not reach the query handle");

    const resetting = graph.reset();
    check(store.value.get() === undefined, "Reset retained the previous identity's data");
    check(graph.resetVersion.get() === 1, "Reset version did not advance");
    backend.resolve(pending("item.get"), "new identity");
    await resetting;
    check(store.value.get() === "new identity", "Reset did not refresh the preserved handle");
    check(
      JSON.stringify(observed) === '["initial","updated","new identity"]',
      "The consumer received the wrong query updates",
    );

    const refreshing = store.revalidate();
    const call = pending("item.get");
    retirementCall = call;
    const disposedRead = store.ready.then(
      () => {
        throw new Error("Disposed query readiness unexpectedly resolved");
      },
      (error) =>
        check(error instanceof Fault && error.kind === "disposed", "Expected a disposal fault"),
    );
    graph.dispose();
    check(call.aborted, "Disposal did not abort the backend request");
    const retirement = graph.retire();
    const retirementOutcome = retirement.then(
      () => ({ status: "fulfilled" }),
      (reason) => ({ status: "rejected", reason }),
    );
    check(graph.retire() === retirement, "Graph retirement did not return its cached promise");
    await disposedRead;
    await expectPending(retirement, "Graph retirement completed before the original fetch");
    backend.resolve(call, "late response");
    retirementCallResolved = true;
    await refreshing;
    const outcome = await retirementOutcome;
    check(
      unobservedFaults.length === 1,
      `Expected one unobserved disposal fault, got ${unobservedFaults.length}`,
    );
    const [disposalFault] = unobservedFaults;
    check(
      disposalFault instanceof Fault &&
        disposalFault.kind === "disposed" &&
        disposalFault.involved.length === 1 &&
        disposalFault.involved[0] === "items/one",
      "Disposal reported an unexpected fault",
    );
    check(
      outcome.status === "rejected",
      "Graph retirement fulfilled despite the live-subscription cleanup failure",
    );
    check(
      outcome.reason === disposalFault,
      "Graph retirement did not reject with the reported disposal fault",
    );
    check(!observed.includes("late response"), "Disposed graph published a late response");
  } finally {
    if (retirementCall !== undefined && !retirementCallResolved)
      backend.resolve(retirementCall, "late response");
    stop();
    graph.dispose();
  }
}

async function verifyAcquisitionConsumer() {
  const backend = testBackend();
  const query = createQuery({
    name: "acquisition.read",
    acquisition: "fetch",
    key: (input) => `acquisition/${input.id}`,
    fetch: backend.respond("acquisition.read"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-acquisition", operations: { query } }),
  });

  try {
    const binding = bindOperation(graph, query, {
      context: createBindingContext(graph),
      profile: {},
      fetch: query.fetch,
    });
    await verifyWarmFetchAcquisition(backend, "raw", 0, () => graph.api.query({ id: "raw" }));
    await verifyWarmFetchAcquisition(backend, "bound", 2, () => binding.member({ id: "bound" }));
  } finally {
    for (const call of backend.pending("acquisition.read"))
      backend.resolve(call, "cleanup");
    graph.dispose();
  }
}

async function verifyWarmFetchAcquisition(backend, label, previousCalls, acquire) {
  const name = "acquisition.read";
  const totalCalls = () => backend.calls.filter((call) => call.name === name).length;
  const initialValue = `${label}:initial`;
  const completedValue = `${label}:completed`;

  const first = acquire();
  const initialCalls = backend.pending(name);
  check(totalCalls() === previousCalls + 1, `The ${label} query did not call its callback`);
  check(initialCalls.length === 1, `The ${label} query did not start one request`);
  backend.resolve(initialCalls[0], initialValue);
  check((await first.ready) === initialValue, `The ${label} query lost its initial value`);

  const refreshing = acquire();
  const pending = backend.pending(name);
  check(totalCalls() === previousCalls + 2, `Warm ${label} acquisition skipped its callback`);
  check(pending.length === 1, `Warm ${label} acquisition did not start one request`);
  check(refreshing.value.get() === initialValue, `Warm ${label} acquisition cleared its landing`);
  check(
    refreshing.pending.get() && refreshing.status.get() === "revalidating",
    `Warm ${label} acquisition did not expose its current request`,
  );

  const joined = acquire();
  check(totalCalls() === previousCalls + 2, `The ${label} query did not join current work`);
  check(
    joined.value.get() === initialValue &&
      joined.pending.get() &&
      joined.status.get() === "revalidating",
    `The joined ${label} query did not retain its landing and request state`,
  );
  await Promise.all([
    expectPending(refreshing.ready, `The ${label} query readiness skipped its current request`),
    expectPending(joined.ready, `The joined ${label} readiness skipped its current request`),
  ]);

  backend.resolve(pending[0], completedValue);
  check(
    (await refreshing.ready) === completedValue && (await joined.ready) === completedValue,
    `The ${label} query did not publish its completed value to every caller`,
  );
}

async function verifyTransferConsumer() {
  const source = createTransferRuntime("source");
  let target;
  let sourceStream;
  let sourceSession;
  let targetQuery;
  let targetStream;
  let targetSession;
  try {
    const sourceRaw = source.graph.api.raw();
    check(
      sourceRaw.value.get().origin === "source",
      "Transfer source raw value was not initialized",
    );
    const sourceQuery = source.queryBinding.member({ key: "shared" });
    await sourceQuery.ready;
    sourceStream = source.streamBinding.member({ key: "shared" });
    sourceSession = source.streamBinding.session(sourceStream);
    const sourceUndefined = waitForValue(
      sourceStream,
      (value) => value === undefined,
      sourceSession,
    );
    source.queue.push(undefined);
    await sourceUndefined;
    check(
      sourceStream.available.get() && sourceStream.value.get() === undefined,
      "Transfer source did not commit a present undefined stream value",
    );

    const wireSnapshot = snapshot(source.graph, {
      version: 2,
      bindings: [
        {
          binding: source.queryBinding,
          reference: { profile: { result: "query" }, operation: source.query.name },
        },
        {
          binding: source.streamBinding,
          reference: { profile: { channel: "events" }, operation: source.stream.name },
        },
      ],
    });
    const transported = JSON.parse(JSON.stringify(wireSnapshot));
    target = createTransferRuntime("target", { deferQuery: true, gateSource: true });
    const report = adopt(target.graph, transported, {
      bindings: [
        {
          binding: target.queryBinding,
          reference: { operation: target.query.name, profile: { result: "query" } },
        },
        {
          binding: target.streamBinding,
          reference: { operation: target.stream.name, profile: { channel: "events" } },
        },
      ],
    });
    check(report.version === 2, "V2 adoption returned the wrong report version");
    check(report.skipped.length === 0, "V2 adoption skipped a valid transfer row");
    check(report.adopted.length === 3, "V2 adoption did not keep raw, query, and stream siblings");
    check(
      target.metrics.queryKeys === 0 &&
        target.metrics.queryFetches === 0 &&
        target.metrics.streamKeys === 0 &&
        target.metrics.streamOpens === 0,
      "V2 adoption ran a key, query fetch, or stream source",
    );

    const rawRow = report.adopted.find(
      (entry) => entry.address.kind === "raw" && entry.address.key === "shared",
    );
    const queryRow = report.adopted.find(
      (entry) => entry.address.kind === "binding" && entry.address.key === "shared",
    );
    const streamRow = report.adopted.find(
      (entry) => entry.address.kind === "stream-binding" && entry.address.key === "shared",
    );
    check(rawRow && queryRow && streamRow, "V2 report lost a full logical address");

    const rawCapture = peekStore(target.graph.api.raw());
    check(
      rawCapture.kind === "stable" &&
        rawCapture.present &&
        rawCapture.identity === rawRow.identity &&
        rawCapture.value.kind === "raw",
      "V2 raw adoption did not match its reported public identity",
    );

    targetQuery = target.queryBinding.member({ key: "shared" });
    const queryCapture = peekStore(targetQuery);
    check(
      queryCapture.kind === "stable" &&
        queryCapture.present &&
        queryCapture.identity === queryRow.identity &&
        queryCapture.value.kind === "query" &&
        queryCapture.value.origin === "source",
      "V2 query adoption did not match its reported public identity and value",
    );
    check(
      target.metrics.queryKeys === 1 && target.metrics.queryFetches === 0,
      "Reading the imported query started a fetch or skipped its authored key",
    );

    targetStream = target.streamBinding.member({ key: "shared" });
    targetSession = target.streamBinding.session(targetStream);
    const streamCapture = peekStore(targetStream);
    check(
      targetStream.pending.get() &&
        targetStream.available.get() &&
        targetStream.value.get() === undefined &&
        streamCapture.kind === "stable" &&
        streamCapture.present &&
        streamCapture.value === undefined,
      "The first imported stream session did not stay pending with present undefined",
    );
    check(
      streamCapture.identity === streamRow.identity &&
        targetSession.storeIdentity === streamRow.identity &&
        streamCapture.valueRevision === streamRow.valueRevision,
      "V2 stream report did not match public capture and session identity",
    );
    check(
      target.metrics.streamKeys === 1 && target.metrics.streamOpens === 1,
      "The first target stream session did not use its real key and source",
    );
    check(
      target.metrics.streamEffects === 0,
      "The gated stream source effect ran before its local identity and revision were checked",
    );

    target.sourceGate.resolve();
    await target.sourceStarted.promise;
    const event = { kind: "event", origin: "target", sequence: 1 };
    const receivedEvent = waitForValue(
      targetStream,
      (value) => value?.kind === "event",
      targetSession,
    );
    target.queue.push(event);
    check((await receivedEvent) === event, "The adopted stream did not accept its next real event");
    target.queue.end();
    const receipt = await targetSession.detached;
    check(
      receipt.terminalCapture.kind === "captured" &&
        receipt.terminalCapture.capture.kind === "stable" &&
        receipt.terminalCapture.capture.present &&
        receipt.terminalCapture.capture.value === event,
      "The adopted stream session did not retain its own terminal capture",
    );
  } finally {
    source.queue.end();
    source.graph.dispose();
    if (sourceSession) await sourceSession.detached;
    if (target) {
      target.sourceGate.resolve();
      target.queue.end();
      if (target.metrics.queryFetches > 0)
        target.queryGate.resolve({ kind: "query", origin: "target" });
      target.graph.dispose();
      if (targetSession) await targetSession.detached;
    }
  }
}

function createTransferRuntime(origin, { deferQuery = false, gateSource = false } = {}) {
  const metrics = {
    queryKeys: 0,
    queryFetches: 0,
    streamKeys: 0,
    streamOpens: 0,
    streamEffects: 0,
  };
  const queue = createAsyncQueue();
  const queryGate = createDeferred();
  const sourceGate = createDeferred();
  const sourceStarted = createDeferred();
  const raw = createStore({
    name: "shared",
    initial: { kind: "raw", origin },
    snapshot: true,
  });
  const query = createQuery({
    name: "transfer.query",
    key: (input) => {
      metrics.queryKeys += 1;
      return input.key;
    },
    fetch: async (input) => {
      metrics.queryFetches += 1;
      if (deferQuery) return queryGate.promise;
      return { kind: "query", origin, key: input.key };
    },
    snapshot: true,
  });
  const stream = createStream({
    name: "transfer.stream",
    key: (input) => {
      metrics.streamKeys += 1;
      return input.key;
    },
    open: () => {
      metrics.streamOpens += 1;
      if (gateSource)
        return {
          async *[Symbol.asyncIterator]() {
            await sourceGate.promise;
            metrics.streamEffects += 1;
            sourceStarted.resolve();
            yield* queue;
          },
        };
      return queue;
    },
    snapshot: true,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-transfer", operations: { raw, query, stream } }),
  });
  const context = createBindingContext(graph);
  const queryBinding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: query.fetch,
    value: { snapshot: true },
  });
  const streamBinding = bindStream(graph, stream, graph.api.stream, { context });
  return {
    graph,
    query,
    stream,
    raw,
    queryBinding,
    streamBinding,
    queue,
    queryGate,
    sourceGate,
    sourceStarted,
    metrics,
  };
}

function createDeferred() {
  let resolve;
  const promise = new Promise((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

function createAsyncQueue() {
  const values = [];
  const waiters = [];
  let ended = false;
  const finish = () => {
    ended = true;
    while (waiters.length > 0) waiters.shift()({ value: undefined, done: true });
  };
  return {
    push(value) {
      const waiter = waiters.shift();
      if (waiter) waiter({ value, done: false });
      else if (!ended) values.push(value);
    },
    end: finish,
    [Symbol.asyncIterator]() {
      return {
        next() {
          if (values.length > 0) return Promise.resolve({ value: values.shift(), done: false });
          if (ended) return Promise.resolve({ value: undefined, done: true });
          return new Promise((resolve) => waiters.push(resolve));
        },
        return() {
          finish();
          return Promise.resolve({ value: undefined, done: true });
        },
      };
    },
  };
}

function waitForValue(store, predicate, session) {
  let stop = () => {};
  let matched = false;
  let resolveValue;
  let rejectValue;
  const result = new Promise((resolve, reject) => {
    resolveValue = resolve;
    rejectValue = reject;
  });
  const unsubscribe = subscribe(store, (value) => {
    if (matched || !predicate(value)) return;
    matched = true;
    stop();
    resolveValue(value);
  });
  stop = unsubscribe;
  if (matched) stop();
  session.detached.then(() => {
    if (matched) return;
    matched = true;
    stop();
    rejectValue(new Error("The source session detached before the expected value"));
  });
  return result;
}

async function verifyBoundConsumer() {
  check(
    bindOperation === rootBindOperation && observeQueryProgress === rootObserveQueryProgress,
    "Binding or progress observer differs across package entries",
  );
  const backend = testBackend();
  let reportNumberProgress;
  const query = createQuery({
    name: "bound.read",
    key: (input) => input.id,
    fetch: backend.respond("bound.read"),
  });
  const mutation = createMutation({
    name: "bound.save",
    run: backend.perform("bound.save"),
    affects: [affects(query, { select: (input) => ({ id: input.id }), on: "revalidate" })],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-bound", operations: { query, mutation } }),
  });
  let originalRead;
  let originalReadResolved = false;
  let stopProgress = () => {};
  let contextRetirement;
  try {
    const context = createBindingContext(graph);
    const numbers = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: (input, call) => {
        reportNumberProgress = call.reportProgress;
        return query.fetch(input, call);
      },
    });
    const labels = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: async (input, call) => "label:" + (await query.fetch(input, call)),
    });
    const number = numbers.member({ id: "one" }, { initial: 1 });
    const label = labels.member({ id: "one" }, { initial: "label:1" });
    const effect = profileEffect(mutation, 0, query, {
      prepare: (phase) =>
        phase.existing(phase.selectedInput).map((target) => phase.prepare(target)),
      settle: (phase) =>
        phase
          .existing(phase.selectedInput)
          .map((target) => phase.selected(target, phase.selectedInput)),
    });
    const save = bindOperation(graph, mutation, {
      context,
      profile: {},
      effects: [effect],
      run: async (input, call) => ({ receipt: await mutation.run(input, call) }),
    });
    const override = predictionFor(mutation, numbers.target(number), () => 9);
    const running = save.member({ id: "one" }, { optimistic: [override] });
    check(
      number.value.get() === 9 && label.value.get() === "label:1",
      "Physical prediction override crossed profiles",
    );
    const pending = backend.pending("bound.save");
    check(pending.length === 1, "Bound mutation did not dispatch exactly once");
    backend.resolve(pending[0], "saved");
    check((await running).receipt === "saved", "Bound completion lost its exact result");
    const refreshes = backend.pending("bound.read");
    check(refreshes.length === 2, "Bound effect did not refresh both existing profiles");
    for (const call of refreshes) backend.resolve(call, 12);
    await Promise.all([number.ready, label.ready]);
    check(
      number.value.get() === 12 && label.value.get() === "label:12",
      "Refresh lost the profile completion callback",
    );

    const siblingContext = createBindingContext(graph);
    const observation = rootObserveQueryProgress(number);
    const progressEvents = [];
    let retirementFromProgress;
    stopProgress = observation.subscribe((event) => {
      progressEvents.push(event);
      if (event.kind !== "report" || retirementFromProgress !== undefined) return;

      let capture;
      try {
        capture = { kind: "captured", snapshot: observation.capture() };
      } catch (error) {
        capture = { kind: "threw", error };
      }

      let result;
      try {
        contextRetirement = context.retire();
        result = { kind: "returned", promise: contextRetirement };
      } catch (error) {
        result = { kind: "threw", error };
      }
      retirementFromProgress = { event, capture, result };
    });

    const revalidation = number.revalidate();
    const revalidationOutcome = revalidation.then(
      (value) => ({ kind: "resolved", value }),
      (error) => ({ kind: "rejected", error }),
    );
    const activeReads = backend.pending("bound.read");
    originalRead = activeReads[0];
    check(activeReads.length === 1, "Progress retirement did not start exactly one original read");
    check(typeof reportNumberProgress === "function", "Bound query did not expose its reporter");

    const disposedReady = number.ready.then(
      () => ({ kind: "resolved" }),
      (error) => ({ kind: "rejected", error }),
    );
    const firstProgress = { source: "installed-bound-query", sequence: 1 };
    reportNumberProgress(firstProgress);

    check(
      retirementFromProgress?.event.snapshot.value === firstProgress &&
        retirementFromProgress.capture.kind === "captured" &&
        retirementFromProgress.capture.snapshot.value === firstProgress,
      "The public progress observer did not capture the accepted bound report",
    );
    check(
      retirementFromProgress.result.kind === "returned" &&
        typeof retirementFromProgress.result.promise?.then === "function",
      "BindingContext.retire did not return from the accepted progress callback",
    );
    check(
      context.retire() === contextRetirement,
      "Binding context retirement did not return its cached promise",
    );
    check(
      context.active.get() === false && originalRead.aborted,
      "Context retirement did not deactivate its context and abort the original request",
    );
    check(
      siblingContext.active.get() === true,
      "Context retirement affected an empty sibling context",
    );

    const readyOutcome = await disposedReady;
    check(
      readyOutcome.kind === "rejected" &&
        readyOutcome.error instanceof Fault &&
        readyOutcome.error.kind === "disposed",
      "A bound query ready read did not retain its disposed Fault after context retirement",
    );
    const acceptedReportCount = progressEvents.filter((event) => event.kind === "report").length;
    check(
      acceptedReportCount === 1,
      "The original bound request delivered an unexpected progress report",
    );
    await expectPending(
      contextRetirement,
      "Binding context retirement completed before its original backend work settled",
    );

    reportNumberProgress({ source: "installed-bound-query", sequence: 2 });
    check(
      progressEvents.filter((event) => event.kind === "report").length === acceptedReportCount,
      "A late report from the retired bound request was delivered",
    );

    backend.resolve(originalRead, 13);
    originalReadResolved = true;
    const [originalRevalidation] = await Promise.all([revalidationOutcome, contextRetirement]);
    check(
      originalRevalidation.kind === "resolved",
      "The original bound revalidation did not resolve after its backend work settled",
    );
    check(
      progressEvents.filter((event) => event.kind === "report").length === acceptedReportCount,
      "The retired bound request delivered progress after its original backend work settled",
    );
  } finally {
    if (originalRead !== undefined && !originalReadResolved) {
      backend.resolve(originalRead, 13);
      originalReadResolved = true;
    }
    stopProgress();
    graph.dispose();
    if (contextRetirement !== undefined) await contextRetirement.catch(() => {});
  }
}

async function verifyBoundStreamConsumer() {
  check(
    bindStream === rootBindStream && inspectBoundAddress === rootInspectBoundAddress,
    "Stream/address constructors differ across package entries",
  );
  const backend = testBackend();
  const stream = createStream({
    name: "packed.events",
    key: (input) => input,
    open: backend.stream("packed.events"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-stream", operations: { stream } }),
  });
  try {
    const binding = bindStream(graph, stream, graph.api.stream, {
      context: createBindingContext(graph),
    });
    const empty = rootInspectBoundAddress(binding, storeKey("one"));
    check(
      empty.kind === "stable" && empty.state === "empty",
      "Packed address did not inspect empty",
    );
    const a = binding.member("one", { expectEmpty: empty.condition });
    const ticket = binding.session(a);
    backend.emit("packed.events", undefined);
    backend.end("packed.events");
    const receipt = await ticket.detached;
    const terminal = receipt.terminalCapture;
    check(
      terminal.kind === "captured" &&
        terminal.capture.kind === "stable" &&
        terminal.capture.present &&
        terminal.capture.value === undefined,
      "Packed stream lost present undefined at native retirement",
    );
    const b = binding.member("one");
    check(
      a.identity === b.identity && ticket.identity !== binding.session(b).identity,
      "Packed stream did not separate native sessions on the physical entry",
    );
    check(
      a.available.get() && !b.available.get() && b.pending.get(),
      "Packed fixed facets followed another session",
    );
    check(
      rootInspectBoundAddress(binding, storeKey("one"), { expected: empty.stamp }).kind ===
        "changed",
      "Packed address stamp survived acquisition",
    );
    let rejected = false;
    try {
      a.set(99);
    } catch (cause) {
      rejected = cause instanceof TypeError;
    }
    check(rejected && !b.available.get(), "Retired packed wrapper wrote into its successor");
  } finally {
    graph.dispose();
  }
}
