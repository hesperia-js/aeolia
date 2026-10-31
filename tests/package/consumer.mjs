import {
  Fault,
  Signal,
  adopt,
  captureReactiveConstruction,
  prepareRequiredTransferCapture,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  bindOperation as rootBindOperation,
  bindStream as rootBindStream,
  inspectBoundAddress as rootInspectBoundAddress,
  createGraph,
  createQuery as rootCreateQuery,
  createPositionedStream,
  project,
  projectPositioned,
  capturePositionedProjection,
  capturePositionedStore,
  isPositionedStoreCheckpointCurrent,
  preparePositionedStoreRestore,
  preparePositionedSeedRestore,
  positionedFoldReceipt,
  acknowledgePositionedFold,
  createStore,
  defineContract,
  observeQueryProgress as rootObserveQueryProgress,
  peekStore,
  signal,
  snapshot,
  subscribe,
  withoutWrites,
} from "aeolia";
import {
  affects,
  createTransferScope,
  prepareTransferOperation,
  prepareTransferScopeSuccessors,
  prepareTransferOperationRetirement,
  bindOperation,
  createBindingContext,
  createGraph as contractCreateGraph,
  createMutation,
  createQuery,
  defineContract as contractDefineContract,
  createStream,
  bindStream,
  inspectBoundAddress,
  observeQueryProgress,
  storeKey,
  predictionFor,
  profileEffect,
} from "aeolia/contract";
import {
  Signal as ReactiveSignal,
  computed,
  signal as reactiveSignal,
  subscribe as subscribeSignal,
} from "aeolia/reactive";
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

async function verifyPositionedRequiredTransferConsumer() {
  const providerA = new ConsumerPositionedSource({ initialBoundary: true, after: 5 });
  const providerB = new ConsumerPositionedSource({ initialBoundary: true, readerLifetime: true });
  let targetSteps = 0;
  const policy = {
    kind: "reduce",
    initial: 10,
    step: (total, event) => {
      targetSteps += 1;
      return total + event;
    },
  };
  let sourceSteps = 0;
  const sourcePolicy = {
    ...policy,
    step: (total, event) => {
      sourceSteps += 1;
      return total + event;
    },
  };
  const definitionA = createPositionedStream({
    name: "positioned.required-transfer",
    key: (input) => input.id,
    snapshot: false,
    source: providerA,
  });
  const definitionB = createPositionedStream({
    name: "positioned.required-transfer",
    key: (input) => input.id,
    snapshot: false,
    source: providerB,
  });
  const inputA = { id: "one" };
  const inputB = { id: "one" };
  const graphA = createGraph({
    contract: defineContract({
      namespace: "positioned-required-transfer",
      operations: { events: definitionA },
    }),
  });
  const graphB = createGraph({
    contract: defineContract({
      namespace: "positioned-required-transfer",
      operations: { events: definitionB },
    }),
  });
  let projectionA;
  let sourceRoot;
  let capture;
  let claim;
  let adoption;
  let published = false;
  let reader;
  try {
    const sourceA = graphA.api.events(inputA);
    projectionA = project(sourceA, sourcePolicy);
    check(providerA.opens.length === 1, "The source Store did not open exactly once");
    check(
      providerA.initialBoundaries.length === 1,
      "The source did not report its genuine initial boundary",
    );
    check(
      providerA.initialBoundaries[0].options === providerA.opens[0].options,
      "The source boundary used a different opening options object",
    );
    check(
      providerA.opens[0].hasAfter === false && providerA.opens[0].hasReader === false,
      "The initial source opening was not unreserved",
    );
    check(sourceA.available.get() === false, "An empty initial boundary created a Store landing");
    check(projectionA.value.get() === 10 && sourceSteps === 0, "The initial anchor ran a reducer");
    check(
      positionedFoldReceipt(projectionA) === undefined && providerA.acknowledgements.length === 0,
      "The initial anchor created or acknowledged an Event fold receipt",
    );

    const checkpoint = capturePositionedStore(sourceA, [projectionA]);
    check(
      checkpoint.present === false &&
        checkpoint.hasPosition === false &&
        !Object.hasOwn(checkpoint, "latest") &&
        checkpoint.resumable === true,
      "The genuine initial boundary was not captured separately from source presence",
    );
    sourceRoot = computed(() => projectionA.value.get());
    check(sourceRoot.get() === 10, "The source root did not read its authored initial fold value");
    const decodedSeed = {
      boundary: { kind: "initial", after: 5 },
      base: { present: false },
      values: [10],
    };
    const seedId = "p0-seed";
    const decodedSeeds = new Map([[seedId, decodedSeed]]);
    const codec = {
      encodeValue: (value) =>
        value !== null &&
        typeof value === "object" &&
        Object.hasOwn(value, "boundary") &&
        Object.hasOwn(value, "base")
          ? { $ref: seedId }
          : value,
      encodeFailure: (cause) => String(cause),
      decodeValue: (value) => {
        if (
          value !== null &&
          typeof value === "object" &&
          Object.hasOwn(value, "$ref") &&
          value.$ref === seedId
        ) {
          const decoded = decodedSeeds.get(seedId);
          check(decoded !== undefined, "The positioned seed reference is missing");
          return decoded;
        }
        return value;
      },
      decodeFailure: (failure) => new Error(String(failure)),
    };
    capture = prepareRequiredTransferCapture({
      graphs: [graphA],
      roots: [{ source: sourceRoot, construction: captureReactiveConstruction(sourceRoot) }],
      positioned: [checkpoint],
      codec,
    });
    check(capture.image.version === 2, "The positioned capture did not use image version 2");
    check(
      capture.image.stores[0].hasValue === false,
      "The snapshot:false base was copied into the Store row",
    );
    const positionedRows = capture.image.positioned;
    check(
      Array.isArray(positionedRows) && positionedRows.length === 1,
      "The positioned group row was omitted",
    );
    const group = positionedRows[0];
    check(
      JSON.stringify(group.seed) === JSON.stringify({ $ref: seedId }),
      "The group seed did not use its stable codec reference",
    );
    check(
      decodedSeeds.get(seedId) === decodedSeed,
      "The codec arena lost the exact decoded seed object",
    );
    check(
      group.slot === "p0" && group.store === "s0" && group.projections === 1,
      "The positioned group did not preserve its Store and projection ordinal",
    );
    check(
      decodedSeed.boundary.kind === "initial" &&
        decodedSeed.boundary.after === 5 &&
        decodedSeed.base.present === false &&
        decodedSeed.values[0] === 10 &&
        !Object.hasOwn(decodedSeed, "latest"),
      "The decoded initial seed fabricated a source event or base value",
    );
    check(
      capture.image.nodes.some(
        (node) =>
          node.positioned === "p0" &&
          node.projection === 0 &&
          node.facet === "projection-value" &&
          node.kind === "native" &&
          node.hasValue === false &&
          node.hasError === false,
      ),
      "The cached fold did not retain its native projection tuple mapping",
    );

    reader = providerB.reserve(inputB, 5, undefined, decodedSeed);
    claim = preparePassiveTransferClaim(graphB, graphB.api.events, inputB, undefined, {
      policies: [policy],
      seed: decodedSeed,
      reader,
    });
    check(
      claim.restoration.source === claim.store,
      "The positioned claim did not own its restoration Store",
    );
    check(providerB.opens.length === 0, "Claim preparation opened the target source");
    check(providerB.lifetimeCalls.length === 1, "The target reader lifetime was not captured");
    check(
      claim.retired !== claim.restoration.retired,
      "Claim registration retirement replaced the restoration resource completion",
    );

    let targetRuns = 0;
    const targetValue = computed(() => {
      targetRuns += 1;
      return claim.restoration.projections[0].value.get();
    });
    adoption = prepareRequiredTransferAdoption(capture.image, {
      graphs: [graphB],
      sources: [{ slot: "s0", claim }],
      positioned: [{ slot: "p0", claim, seed: decodedSeed }],
      reactive: [
        {
          slot: capture.image.roots[0],
          target: targetValue,
          construction: captureReactiveConstruction(targetValue),
        },
      ],
      codec,
      mappedStateWrites: [],
    });
    const commit = adoption.seal();
    check(commit !== undefined, "The initial-boundary adoption did not seal");
    commit.publish();
    published = true;
    commit.notifications.deliver();
    await Promise.all([capture.retired, claim.retired, adoption.retired]);
    check(providerB.opens.length === 0, "Publication opened the target source early");
    check(claim.store.available.get() === false, "An omitted base became a target Store landing");
    check(targetValue.get() === 10 && targetRuns === 0, "The cached initial fold was not adopted");
    check(targetSteps === 0, "Adoption replayed a source event through the reducer");
    check(
      positionedFoldReceipt(claim.restoration.projections[0]) === undefined &&
        providerB.acknowledgements.length === 0,
      "The target initial boundary fabricated or acknowledged an Event fold receipt",
    );

    check(
      claim.restoration.activate() === claim.store,
      "Activation returned a different native Store",
    );
    check(providerB.opens.length === 1, "Activation did not open the target reader once");
    check(
      providerB.initialBoundaries[0].options === providerB.opens[0].options &&
        providerB.opens[0].hasAfter &&
        providerB.opens[0].after === 5 &&
        providerB.opens[0].hasReader,
      "The target initial boundary did not keep its original reader opening",
    );
    check(
      providerB.acknowledgements.length === 0,
      "Opening the initial boundary acknowledged a fold",
    );

    const folded = waitForPositionedValue(
      claim.restoration.projections[0],
      (value) => value === 12,
    );
    providerB.emitReader(reader, { position: 6, value: 2 });
    await folded;
    check(targetSteps === 1, "The first suffix event did not fold exactly once");
    check(
      claim.store.available.get() && claim.store.value.get() === 2,
      "The first suffix did not create the source landing",
    );
    const receipt = positionedFoldReceipt(claim.restoration.projections[0]);
    check(
      receipt !== undefined && receipt.position === 6,
      "The suffix did not create a native fold receipt",
    );
    acknowledgePositionedFold(claim.restoration.projections[0], receipt);
    check(
      providerB.acknowledgements.length === 1 && providerB.acknowledgements[0].receipt === receipt,
      "The first suffix fold did not acknowledge its exact native receipt",
    );
    claim.restoration.discard();
    await claim.restoration.retired;
    check(providerB.releaseCount(reader) === 1, "The restoration did not release its reader once");
  } finally {
    if (!published) {
      adoption?.discard();
      claim?.discard();
    } else claim?.restoration.discard();
    projectionA?.close();
    graphA.dispose();
    graphB.dispose();
    if (capture !== undefined) await capture.retired.catch(() => {});
    if (adoption !== undefined) await adoption.retired.catch(() => {});
    if (claim !== undefined) await claim.retired.catch(() => {});
    if (claim !== undefined) await claim.restoration.retired.catch(() => {});
    if (reader !== undefined && claim === undefined && providerB.releaseCount(reader) === 0)
      providerB.releaseReader(inputB, reader);
  }
}

const stateUpdateDiagnostic = "AEOLIA-DEV-STATE-UPDATE: update() expects next to be a function";

function verifyPublicEntryIdentity() {
  check(Signal === ReactiveSignal, "The root and reactive Signal namespaces differ");
  check(Signal.State === ReactiveSignal.State, "The root and reactive State classes differ");
  check(Signal.isState === ReactiveSignal.isState, "The root and reactive State guards differ");
  check(
    Signal.isComputed === ReactiveSignal.isComputed,
    "The root and reactive Computed guards differ",
  );
  check(signal === reactiveSignal, "The root and reactive signal factories differ");
  check(createGraph === contractCreateGraph, "The root and contract graph factories differ");
  check(
    defineContract === contractDefineContract,
    "The root and contract declaration factories differ",
  );
  check(rootCreateQuery === createQuery, "The root and contract query factories differ");
}

function verifyStateUpdateConsumer(development) {
  const updateSource = Function.prototype.toString.call(Signal.State.prototype.update);
  check(
    !updateSource.includes("__AEOLIA_DEV__"),
    "The private development marker reached State.update",
  );
  if (development) {
    check(
      updateSource.includes(stateUpdateDiagnostic),
      "The development State.update implementation was not selected",
    );
    check(updateSource.includes("typeof"), "The development updater predicate was not emitted");
  } else {
    check(
      !updateSource.includes(stateUpdateDiagnostic),
      "The default State.update implementation retained the diagnostic",
    );
    check(
      !updateSource.includes("typeof"),
      "The default State.update implementation retained the development predicate",
    );
  }

  const foreignReceiverError = (() => {
    try {
      Signal.State.prototype.update.call({}, null);
    } catch (error) {
      return error;
    }
    return undefined;
  })();
  check(foreignReceiverError instanceof TypeError, "State.update accepted a foreign receiver");
  check(
    !foreignReceiverError.message.startsWith(stateUpdateDiagnostic),
    "The development updater diagnostic replaced native receiver validation",
  );

  const computedReceiver = new Signal.Computed(() => 1);
  const computedReceiverError = (() => {
    try {
      Signal.State.prototype.update.call(computedReceiver, null);
    } catch (error) {
      return error;
    }
    return undefined;
  })();
  check(computedReceiverError instanceof TypeError, "State.update accepted a Computed receiver");
  check(
    !computedReceiverError.message.startsWith(stateUpdateDiagnostic),
    "The development updater diagnostic replaced native State-kind validation",
  );

  const state = new Signal.State(1);
  let updaterCalls = 0;
  state.update((current) => {
    updaterCalls += 1;
    check(current === 1, "State.update received the wrong current value");
    return 2;
  });
  check(updaterCalls === 1 && state.get() === 2, "A valid updater did not run exactly once");

  const updaterFailure = new Error("installed updater failure");
  for (const thrownCause of [updaterFailure, undefined]) {
    let throwingUpdaterCalls = 0;
    let updaterThrew = false;
    let caughtUpdaterFailure;
    try {
      state.update(() => {
        throwingUpdaterCalls += 1;
        throw thrownCause;
      });
    } catch (error) {
      updaterThrew = true;
      caughtUpdaterFailure = error;
    }
    check(updaterThrew, "State.update did not surface the updater exception");
    check(throwingUpdaterCalls === 1, "A throwing updater did not run exactly once");
    check(
      Object.is(caughtUpdaterFailure, thrownCause),
      "State.update changed the updater exception identity",
    );
    check(state.get() === 2, "A throwing updater changed the State value");
  }

  let invalidUpdaterError;
  try {
    state.update(null);
  } catch (error) {
    invalidUpdaterError = error;
  }
  check(invalidUpdaterError instanceof TypeError, "An invalid updater did not throw TypeError");
  check(state.get() === 2, "An invalid updater changed the State value");
  if (development)
    check(
      invalidUpdaterError.message === stateUpdateDiagnostic,
      "Development used the wrong State.update diagnostic",
    );
  else
    check(
      !invalidUpdaterError.message.startsWith("AEOLIA-DEV-STATE-UPDATE:"),
      "The default State.update path exposed the development diagnostic",
    );

  let readonlyUpdaterCalls = 0;
  let readonlyError;
  try {
    withoutWrites(() =>
      state.update((current) => {
        readonlyUpdaterCalls += 1;
        return current + 1;
      }),
    );
  } catch (error) {
    readonlyError = error;
  }
  check(
    readonlyError instanceof Fault && readonlyError.kind === "write-forbidden",
    "A read-only scope did not reject State.update first",
  );
  check(
    readonlyUpdaterCalls === 0 && state.get() === 2,
    "A read-only updater ran or changed State",
  );

  let readonlyInvalidError;
  try {
    withoutWrites(() => state.update(null));
  } catch (error) {
    readonlyInvalidError = error;
  }
  check(
    readonlyInvalidError instanceof Fault && readonlyInvalidError.kind === "write-forbidden",
    "The development updater diagnostic replaced read-only write refusal",
  );
  check(state.get() === 2, "A read-only malformed updater changed the State value");

  let currentFunctionCalls = 0;
  let nextFunctionCalls = 0;
  const currentFunction = () => {
    currentFunctionCalls += 1;
    return "current";
  };
  const nextFunction = () => {
    nextFunctionCalls += 1;
    return "next";
  };
  const functionState = new Signal.State(currentFunction);
  let functionStateUpdaterCalls = 0;
  functionState.update((current) => {
    functionStateUpdaterCalls += 1;
    check(current === currentFunction, "State.update changed a function-valued T");
    return nextFunction;
  });
  check(
    functionStateUpdaterCalls === 1 && functionState.get() === nextFunction,
    "State.update rejected a function-valued State value",
  );
  check(
    currentFunctionCalls === 0 && nextFunctionCalls === 0,
    "State.update invoked a function-valued state or result",
  );
}

function verifyPublicGraphIdentity(graph) {
  check(
    Signal.isComputed(graph.resetVersion),
    "The contract Graph used another reactive runtime instance",
  );
}

export async function verifyConsumer(development = false) {
  verifyPublicEntryIdentity();
  verifyStateUpdateConsumer(development);
  await verifyRequiredTransferConsumer();
  await verifyTransferConsumer();
  await verifyBoundConsumer();
  await verifyBoundStreamConsumer();
  await verifyAcquisitionConsumer();
  await verifyPositionedConsumer();
  await verifyPositionedStoreConsumer();
  await verifyPositionedRequiredTransferConsumer();
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
  const graph = contractCreateGraph({
    contract: contractDefineContract({ namespace: "packed-consumer", operations: { item, save } }),
    onUnobservedFault: (error) => {
      unobservedFaults.push(error);
    },
  });
  verifyPublicGraphIdentity(graph);
  const pending = (name) => {
    const calls = backend.pending(name);
    check(calls.length === 1, `Expected one pending ${name} call, got ${calls.length}`);
    return calls[0];
  };
  const store = graph.api.item("one");
  const observed = [];
  const reactiveObserved = [];
  const stop = subscribe(store, (value) => observed.push(value));
  const stopReactive = subscribeSignal(store, (value) => reactiveObserved.push(value));
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
      JSON.stringify(observed) === '["initial","updated","new identity"]' &&
        JSON.stringify(reactiveObserved) === '["initial","updated","new identity"]',
      "The consumer entry points received different query updates",
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
    stopReactive();
    graph.dispose();
  }
}

function positionedChannel() {
  const queued = [];
  const waiters = [];
  let ended = false;
  return {
    source: {
      [Symbol.asyncIterator]() {
        return {
          next() {
            const event = queued.shift();
            if (event !== undefined) return Promise.resolve({ done: false, value: event });
            if (ended) return Promise.resolve({ done: true, value: undefined });
            return new Promise((resolve) => waiters.push(resolve));
          },
          return() {
            ended = true;
            for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
            return Promise.resolve({ done: true, value: undefined });
          },
        };
      },
    },
    emit(event) {
      const waiter = waiters.shift();
      if (waiter === undefined) queued.push(event);
      else waiter({ done: false, value: event });
    },
  };
}

class ConsumerPositionedSource {
  #reservations = new WeakMap();
  #readerEmitters = new WeakMap();
  #liveEmitters = new Map();
  #releaseCounts = new WeakMap();
  opens = [];
  acknowledgements = [];
  initialBoundaries = [];
  lifetimeCalls = [];

  constructor({ initialBoundary = false, readerLifetime = false, after = 5 } = {}) {
    if (initialBoundary) {
      this.initialBoundary = (input, options) => {
        this.initialBoundaries.push({ input, options });
        const boundary = Object.hasOwn(options, "after") ? options.after : after;
        return { available: true, after: boundary };
      };
    }
    if (readerLifetime) {
      this.readerLifetime = (input, reader, after, seed) => {
        const reservation = this.#reservations.get(reader);
        check(
          reservation !== undefined &&
            reservation.input === input &&
            reservation.after === after &&
            reservation.seed === seed,
          "The positioned lifetime did not match its exact reader reservation",
        );
        this.lifetimeCalls.push({ input, reader, after, seed });
        return {
          signal: reservation.controller.signal,
          expiresAt: reservation.expiresAt,
        };
      };
    }
  }

  reserve(input, after, checkpoint, seed) {
    const reader = Object.freeze({});
    this.#reservations.set(reader, {
      input,
      after,
      checkpoint,
      seed,
      controller: new AbortController(),
      expiresAt: Date.now() + 60_000,
    });
    return reader;
  }

  emitLive(input, event) {
    const emit = this.#liveEmitters.get(input.id);
    check(emit !== undefined, "The positioned live provider opening is missing");
    emit(event);
  }

  emitReader(reader, event) {
    const emit = this.#readerEmitters.get(reader);
    check(emit !== undefined, "The positioned reserved reader opening is missing");
    emit(event);
  }

  open(input, options) {
    const channel = positionedChannel();
    const hasAfter = Object.hasOwn(options, "after");
    const hasReader = Object.hasOwn(options, "reader");
    this.opens.push({ input, hasAfter, after: options.after, hasReader, options });
    if (options.reader !== undefined) this.#readerEmitters.set(options.reader, channel.emit);
    else this.#liveEmitters.set(input.id, channel.emit);
    return channel.source;
  }

  validateReader(input, reader, after, checkpoint, seed) {
    const reservation = this.#reservations.get(reader);
    return (
      reservation !== undefined &&
      reservation.input === input &&
      reservation.after === after &&
      reservation.checkpoint === checkpoint &&
      reservation.seed === seed
    );
  }

  isReaderCurrent(input, reader, after, seed) {
    const reservation = this.#reservations.get(reader);
    return (
      reservation !== undefined &&
      reservation.input === input &&
      reservation.after === after &&
      reservation.seed === seed
    );
  }

  acknowledge(_input, reader, receipt) {
    this.acknowledgements.push({ reader, receipt });
  }

  releaseReader(_input, reader) {
    this.#releaseCounts.set(reader, this.releaseCount(reader) + 1);
  }

  releaseCount(reader) {
    return this.#releaseCounts.get(reader) ?? 0;
  }
}

function waitForPositionedValue(projection, predicate) {
  let stop = () => {};
  let matched = false;
  let resolveValue;
  const result = new Promise((resolve) => {
    resolveValue = resolve;
  });
  const unsubscribe = subscribe(projection.value, (value) => {
    if (matched || !predicate(value)) return;
    matched = true;
    stop();
    resolveValue(value);
  });
  stop = unsubscribe;
  if (matched) stop();
  return result;
}

async function verifyPositionedConsumer() {
  const persisted = await (async () => {
    const providerA = new ConsumerPositionedSource();
    const definitionA = createPositionedStream({
      name: "positioned.consumer",
      key: (input) => input.id,
      source: providerA,
    });
    const graphA = createGraph({
      contract: defineContract({
        namespace: "positioned-consumer-a",
        operations: { events: definitionA },
      }),
    });
    const inputA = { id: "one" };
    const source = projectPositioned(graphA.api.events, inputA, {
      kind: "reduce",
      initial: { total: 0 },
      step: (accumulator, event) => ({ total: accumulator.total + event }),
    });
    try {
      check(
        providerA.opens.length === 1 && !providerA.opens[0].hasAfter,
        "A live positioned open included a restore boundary",
      );
      const firstFold = waitForPositionedValue(source, (value) => value.total === 2);
      providerA.emitLive(inputA, { position: 5, value: 2 });
      await firstFold;
      const checkpoint = capturePositionedProjection(source);
      check(
        checkpoint.hasPosition && checkpoint.position === 5 && checkpoint.resumable,
        "A did not capture its successful positioned fold",
      );
      return { after: checkpoint.position, value: { total: checkpoint.value.total } };
    } finally {
      source.close();
      graphA.dispose();
    }
  })();

  const providerB = new ConsumerPositionedSource();
  const definitionB = createPositionedStream({
    name: "positioned.consumer",
    key: (input) => input.id,
    source: providerB,
  });
  const graphB = createGraph({
    contract: defineContract({
      namespace: "positioned-consumer-b",
      operations: { events: definitionB },
    }),
  });
  const inputB = { id: "one" };
  const seed = persisted.value;
  const reader = providerB.reserve(inputB, persisted.after, undefined, seed);
  let restore;
  try {
    restore = preparePositionedSeedRestore(
      graphB.api.events,
      inputB,
      {
        kind: "reduce",
        initial: seed,
        step: (accumulator, event) => ({ total: accumulator.total + event }),
      },
      { after: persisted.after, value: seed },
      reader,
    );
    check(providerB.opens.length === 0, "Dormant B opened before activation");
    check(
      restore.projection.value.get() === seed,
      "B did not install the decoded accumulator seed",
    );
    const resumedFold = waitForPositionedValue(restore.projection, (value) => value.total === 5);
    restore.activate();
    check(
      providerB.opens.length === 1 &&
        providerB.opens[0].hasAfter &&
        providerB.opens[0].after === 5 &&
        providerB.opens[0].hasReader,
      "B did not open after its admitted boundary",
    );
    providerB.emitReader(reader, { position: 6, value: 3 });
    await resumedFold;
    const receipt = positionedFoldReceipt(restore.projection);
    check(
      receipt !== undefined && receipt.position === 6,
      "B did not issue a successful-fold receipt",
    );
    acknowledgePositionedFold(restore.projection, receipt);
    check(
      providerB.acknowledgements.length === 1 && providerB.acknowledgements[0].receipt === receipt,
      "B did not acknowledge the native receipt",
    );
    restore.discard();
    check(providerB.releaseCount(reader) === 1, "B did not release its reader exactly once");
  } finally {
    restore?.discard();
    graphB.dispose();
  }
}
function waitForPositionedStoreValue(source, predicate) {
  let stop = () => {};
  let matched = false;
  let resolveValue;
  const result = new Promise((resolve) => {
    resolveValue = resolve;
  });
  const unsubscribe = subscribe(source, (value) => {
    if (matched || !predicate(value)) return;
    matched = true;
    stop();
    resolveValue(value);
  });
  stop = unsubscribe;
  if (matched) stop();
  return result;
}

async function verifyPositionedStoreConsumer() {
  const providerA = new ConsumerPositionedSource();
  const definitionA = createPositionedStream({
    name: "positioned.store.consumer",
    key: (input) => input.id,
    source: providerA,
  });
  const graphA = createGraph({
    contract: defineContract({
      namespace: "positioned-store-consumer-a",
      operations: { events: definitionA },
    }),
  });
  const inputA = { id: "one" };
  let sourceA;
  const sourceValuesA = [];
  let stopSourceA = () => {};
  let totalStepsA = 0;
  let countStepsA = 0;
  let totalA;
  let countA;
  let graphB;
  let restoration;
  let stopSourceB = () => {};
  try {
    sourceA = graphA.api.events(inputA);
    stopSourceA = subscribe(sourceA, (value) => sourceValuesA.push(value));
    totalA = project(sourceA, {
      kind: "reduce",
      initial: 0,
      step: (total, event) => {
        totalStepsA += 1;
        return total + (event ?? 0);
      },
    });
    countA = project(sourceA, {
      kind: "reduce",
      initial: 0,
      step: (count) => {
        countStepsA += 1;
        return count + 1;
      },
    });
    check(
      providerA.opens.length === 1 && !providerA.opens[0].hasAfter && !providerA.opens[0].hasReader,
      "The live positioned Store did not open one unreserved source",
    );

    const firstSourceValue = waitForPositionedStoreValue(sourceA, (value) => value === 2);
    const firstTotal = waitForPositionedValue(totalA, (value) => value === 2);
    const firstCount = waitForPositionedValue(countA, (value) => value === 1);
    providerA.emitLive(inputA, { position: 2, value: 2 });
    await Promise.all([firstSourceValue, firstTotal, firstCount]);

    const secondSourceValue = waitForPositionedStoreValue(sourceA, (value) => value === 5);
    const secondTotal = waitForPositionedValue(totalA, (value) => value === 7);
    const secondCount = waitForPositionedValue(countA, (value) => value === 2);
    providerA.emitLive(inputA, { position: 5, value: 5 });
    await Promise.all([secondSourceValue, secondTotal, secondCount]);

    const undefinedSourceValue = waitForPositionedStoreValue(
      sourceA,
      (value) => value === undefined,
    );
    const thirdCount = waitForPositionedValue(countA, (value) => value === 3);
    providerA.emitLive(inputA, { position: 7, value: undefined });
    await Promise.all([undefinedSourceValue, thirdCount]);

    check(
      sourceA.available.get() &&
        sourceA.value.get() === undefined &&
        sourceValuesA.length === 3 &&
        sourceValuesA[0] === 2 &&
        sourceValuesA[1] === 5 &&
        sourceValuesA[2] === undefined,
      "The source Store did not retain and publish its present undefined event",
    );

    const checkpoint = capturePositionedStore(sourceA, [totalA, countA]);
    check(
      checkpoint.present &&
        Object.hasOwn(checkpoint, "latest") &&
        checkpoint.latest === undefined &&
        checkpoint.hasPosition &&
        checkpoint.position === 7 &&
        checkpoint.resumable &&
        checkpoint.sessionIdentity !== undefined &&
        checkpoint.sourceIdentity === sourceA.identity,
      "The positioned Store capture lost its actual source event or session facts",
    );
    check(
      checkpoint.values[0] === 7 &&
        checkpoint.values[1] === 3 &&
        totalStepsA === 3 &&
        countStepsA === 3,
      "The captured fold tuple did not match the source's completed event cut",
    );
    check(
      isPositionedStoreCheckpointCurrent(checkpoint),
      "The source Store checkpoint was stale before transfer",
    );

    const encoded = JSON.stringify({
      after: checkpoint.position,
      latest:
        checkpoint.latest === undefined
          ? { kind: "undefined" }
          : { kind: "value", value: checkpoint.latest },
      values: checkpoint.values,
    });
    const decoded = JSON.parse(encoded);
    check(
      Number.isSafeInteger(decoded.after),
      "The provider codec did not recognize the captured position",
    );
    const seed = {
      after: decoded.after,
      latest: decoded.latest.kind === "undefined" ? undefined : decoded.latest.value,
      values: decoded.values,
    };
    check(
      Object.hasOwn(seed, "latest") && seed.latest === undefined,
      "The provider codec did not preserve a present undefined latest event",
    );
    check(
      isPositionedStoreCheckpointCurrent(checkpoint),
      "The source Store checkpoint changed while the codec copied its values",
    );

    const providerB = new ConsumerPositionedSource();
    const definitionB = createPositionedStream({
      name: "positioned.store.consumer",
      key: (input) => input.id,
      source: providerB,
    });
    graphB = createGraph({
      contract: defineContract({
        namespace: "positioned-store-consumer-b",
        operations: { events: definitionB },
      }),
    });
    const inputB = { id: "one" };
    const readerB = providerB.reserve(inputB, seed.after, undefined, seed);
    let totalStepsB = 0;
    let countStepsB = 0;
    let equalityCallsB = 0;
    const policiesB = [
      {
        kind: "reduce",
        initial: 0,
        step: (total, event) => {
          totalStepsB += 1;
          return total + (event ?? 0);
        },
      },
      {
        kind: "reduce",
        initial: 0,
        step: (count) => {
          countStepsB += 1;
          return count + 1;
        },
      },
    ];
    const targetOptionsB = {
      equals: (left, right) => {
        equalityCallsB += 1;
        if (left === undefined || right === undefined) return left === right;
        return Math.floor(left) === Math.floor(right);
      },
    };
    restoration = preparePositionedStoreRestore(
      graphB.api.events,
      inputB,
      policiesB,
      seed,
      readerB,
      targetOptionsB,
    );
    const sourceB = restoration.source;
    check(
      providerB.opens.length === 0 &&
        sourceB.status.get() === "empty" &&
        !sourceB.pending.get() &&
        sourceB.available.get() &&
        sourceB.value.get() === undefined &&
        restoration.projections[0].value.get() === 7 &&
        restoration.projections[1].value.get() === 3 &&
        totalStepsB === 0 &&
        countStepsB === 0,
      "The dormant Store restore opened its source or refolded the transferred prefix",
    );

    const sourceValuesB = [];
    stopSourceB = subscribe(sourceB, (value) => sourceValuesB.push(value));
    check(
      sourceValuesB.length === 1 &&
        sourceValuesB[0] === undefined &&
        providerB.opens.length === 0 &&
        sourceB.status.get() === "empty" &&
        !sourceB.pending.get(),
      "Subscribing to the dormant Store opened it or lost its present seed",
    );

    let lateSteps = 0;
    const lateProjection = project(sourceB, {
      kind: "reduce",
      initial: 100,
      step: (total, event) => {
        lateSteps += 1;
        return total + (event ?? 0);
      },
    });
    check(
      lateProjection.value.get() === 100 && lateSteps === 0 && providerB.opens.length === 0,
      "A later shared projection replayed the Store's restored prefix",
    );

    check(
      restoration.activate() === sourceB &&
        sourceB.status.get() === "opening" &&
        sourceB.pending.get() &&
        providerB.opens.length === 1 &&
        providerB.opens[0].hasAfter &&
        providerB.opens[0].after === seed.after &&
        providerB.opens[0].hasReader,
      "The restored Store did not activate its admitted reader exactly once",
    );

    const suffixSourceValue = waitForPositionedStoreValue(sourceB, (value) => value === 4);
    const suffixCount = waitForPositionedValue(restoration.projections[1], (value) => value === 4);
    providerB.emitReader(readerB, { position: 8, value: 4 });
    await Promise.all([suffixSourceValue, suffixCount]);
    check(
      sourceValuesB.length === 2 &&
        sourceValuesB[0] === undefined &&
        sourceValuesB[1] === 4 &&
        sourceB.value.get() === 4 &&
        sourceB.status.get() === "live" &&
        !sourceB.pending.get() &&
        restoration.projections[0].value.get() === 11 &&
        restoration.projections[1].value.get() === 4 &&
        lateProjection.value.get() === 104 &&
        totalStepsB === 1 &&
        countStepsB === 1 &&
        lateSteps === 1,
      "The restored Store or its projections did not consume only the real suffix",
    );

    const equalityCallsBeforeWrite = equalityCallsB;
    sourceB.set(4.25);
    check(
      equalityCallsB > equalityCallsBeforeWrite &&
        sourceB.value.get() === 4 &&
        sourceValuesB.length === 2 &&
        totalStepsB === 1 &&
        countStepsB === 1 &&
        lateProjection.value.get() === 104,
      "The restored Store did not retain its target equality for direct writes",
    );

    restoration.discard();
    check(
      providerB.releaseCount(readerB) === 1,
      "The restored Store did not release its reader once",
    );
  } finally {
    stopSourceA();
    stopSourceB();
    restoration?.discard();
    const sourceRetirement = graphA.retire();
    const targetRetirement = graphB?.retire();
    await Promise.all([
      sourceRetirement,
      ...(targetRetirement === undefined ? [] : [targetRetirement]),
    ]);
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
    for (const call of backend.pending("acquisition.read")) backend.resolve(call, "cleanup");
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

async function verifyRequiredTransferConsumer() {
  const backend = testBackend();
  const query = createQuery({
    name: "required.query",
    key: (input) => input,
    fetch: backend.respond("required.query"),
  });
  const mutate = createMutation({
    name: "required.mutate",
    run: backend.perform("required.mutate"),
    affects: [],
  });
  const graphContract = defineContract({
    namespace: "packed-required-transfer",
    operations: { query, mutate },
  });
  const sourceGraph = createGraph({ contract: graphContract });
  const targetGraph = createGraph({ contract: graphContract });
  const codec = {
    encodeValue: (value, witness) => {
      if (witness.store !== undefined)
        check(
          witness.store === sourceStore,
          "Required source witness lost the exact caller Store wrapper",
        );
      return value;
    },
    encodeFailure: (cause) => String(cause),
    decodeValue: (value, witness) => {
      if (witness.store !== undefined)
        check(
          witness.store === claim.store,
          "Required target witness lost the exact passive claim wrapper",
        );
      return value;
    },
    decodeFailure: (failure) => new Error(String(failure)),
  };
  let capture;
  let claim;
  let sourceStore;
  let adoption;
  let targetGraphSuccessor;
  let operationPlan;
  let mutationResult;
  let mutationCall;
  let mutationResolved = false;
  let capturePublished = false;
  let adoptionPublished = false;
  try {
    sourceStore = sourceGraph.api.query("one", { revalidateAfterMs: 60000.5 });
    const sourceReady = sourceStore.ready;
    backend.resolve(backend.pending("required.query")[0], "source value");
    await sourceReady;
    const sourceValue = computed(() =>
      sourceStore.status.get() === "ready" ? sourceStore.value.get() + "!" : "",
    );
    check(sourceValue.get() === "source value!", "Required source cache was not initialized");
    capture = prepareRequiredTransferCapture({
      graphs: [sourceGraph],
      roots: [{ source: sourceValue, construction: captureReactiveConstruction(sourceValue) }],
      codec,
    });
    const image = capture.image;
    const rootSlot = image.roots[0];
    const nativeSource = image.stores[0];
    const sourceCaller = image.callers[0];
    check(
      rootSlot !== undefined && nativeSource !== undefined && sourceCaller !== undefined,
      "Required image omitted its source or caller slots",
    );
    check(
      sourceCaller.store === nativeSource.slot,
      "Required caller was detached from its physical source slot",
    );
    check(
      sourceCaller.window.kind === "finite" && sourceCaller.window.milliseconds === 60000.5,
      "Required caller lost its normalized freshness window",
    );
    check(sourceCaller.status === "ready", "Required caller did not retain settled source status");
    const sourceStatusNode = image.nodes.find(
      (node) =>
        node.kind === "native" &&
        node.facet === "query-status" &&
        node.caller === sourceCaller.slot,
    );
    check(
      sourceStatusNode !== undefined && sourceStatusNode.store === nativeSource.slot,
      "Required status facet lost its exact caller slot",
    );
    const physicalWitness = capture.sources.find((witness) => witness.slot === nativeSource.slot);
    const callerWitness = capture.sources.find((witness) => witness.slot === sourceCaller.slot);
    check(
      physicalWitness?.store === sourceStore,
      "Physical source witness lost the actual acquisition wrapper",
    );
    check(
      callerWitness?.physical === nativeSource.slot &&
        callerWitness.readable === sourceStore.status &&
        callerWitness.store === sourceStore,
      "Caller witness lost its exact status wrapper or physical source",
    );
    const captureCommit = capture.seal();
    check(captureCommit !== undefined, "Required capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capture.retired;

    claim = preparePassiveTransferClaim(targetGraph, targetGraph.api.query, "one", {
      revalidateAfterMs: 60000.5,
    });
    const targetValue = computed(() =>
      claim.store.status.get() === "ready" ? claim.store.value.get() + "!" : "",
    );
    const targetConstruction = captureReactiveConstruction(targetValue);
    adoption = prepareRequiredTransferAdoption(image, {
      graphs: [targetGraph],
      sources: [
        { slot: nativeSource.slot, claim },
        ...image.callers.map((caller) => ({ slot: caller.slot, claim })),
      ],
      reactive: [{ slot: rootSlot, target: targetValue, construction: targetConstruction }],
      codec,
      mappedStateWrites: [],
    });
    const adoptionCommit = adoption.seal();
    check(adoptionCommit !== undefined, "Required adoption did not seal");
    adoptionCommit.publish();
    adoptionPublished = true;
    check(
      targetValue.get() === "source value!",
      "Required cache was not installed in the target graph",
    );
    adoptionCommit.notifications.deliver();
    await adoption.retired;

    const targetStore = targetGraph.api.query("one");
    const scope = createTransferScope(targetGraph, targetGraph.api.query, targetStore);
    const carrier = Object.freeze({ request: "consumer" });
    operationPlan = prepareTransferOperation(
      [scope],
      [{ role: "raw", source: targetGraph.api.mutate, input: carrier }],
    );
    const operationCommit = operationPlan.seal();
    check(operationCommit !== undefined, "Transfer operation did not seal");
    operationCommit.publish();
    operationCommit.notifications.deliver();
    mutationResult = targetGraph.api.mutate(carrier);
    mutationCall = backend.pending("required.mutate")[0];
    check(mutationCall.input === carrier, "Transfer dispatch did not preserve its exact carrier");
    backend.resolve(mutationCall, "mutated");
    mutationResolved = true;
    check((await mutationResult) === "mutated", "The original mutation result changed");

    targetGraphSuccessor = createGraph({ contract: graphContract });
    const successorPlan = prepareTransferScopeSuccessors([
      {
        operation: operationPlan.operation,
        predecessor: scope,
        graph: targetGraphSuccessor,
        source: targetGraphSuccessor.api.query,
        input: "one",
      },
    ]);
    const successorCommit = successorPlan.seal();
    check(
      successorCommit !== undefined && successorPlan.scopes.length === 1,
      "Transfer successor did not seal",
    );
    successorCommit.publish();
    successorCommit.notifications.deliver();
    await successorPlan.retired;

    const retirement = prepareTransferOperationRetirement(operationPlan.operation);
    const retirementCommit = retirement.seal();
    check(retirementCommit !== undefined, "Transfer retirement did not seal");
    retirementCommit.publish();
    retirementCommit.notifications.deliver();
    await retirement.retired;
  } finally {
    if (mutationCall !== undefined && mutationResult !== undefined && !mutationResolved) {
      backend.resolve(mutationCall, "mutated");
      await mutationResult.then(
        () => undefined,
        () => undefined,
      );
    }
    if (!adoptionPublished) adoption?.discard();
    if (!capturePublished) capture?.discard();
    if (adoption !== undefined) await adoption.retired.catch(() => {});
    if (claim !== undefined && !adoptionPublished) await claim.retired.catch(() => {});
    if (capture !== undefined) await capture.retired.catch(() => {});
    sourceGraph.dispose();
    targetGraph.dispose();
    targetGraphSuccessor?.dispose();
  }
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
