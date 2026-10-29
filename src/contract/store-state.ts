import type {
  Generation,
  QueryDefinition,
  StoreDefinition,
  NativeStreamDefinition,
  ValueOptions,
} from "./types.ts";
import { Fault } from "../fault.ts";
import { nextValueRevision } from "./identity.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import type {
  BoundStreamSessionRuntime,
  Prediction,
  StoreRuntime,
  StreamSession,
} from "./runtime.ts";
import { scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import { writeAllStatuses } from "./status.ts";
import { assertStoreOpen, graphFault } from "./faults.ts";

import { beginStoreAddress, changeBoundAddress, endStoreAddress } from "./registry.ts";

/** @internal Checks the exact next authority without changing native state. */
export function nextGeneration(current: number, minimum = current): Generation {
  if (
    !Number.isSafeInteger(current) ||
    current < 0 ||
    !Number.isSafeInteger(minimum) ||
    minimum < 0 ||
    Math.max(current, minimum) >= Number.MAX_SAFE_INTEGER
  )
    throw new RangeError("Store generation exhausted");
  return (Math.max(current, minimum) + 1) as Generation;
}

export function issue<T>(runtime: StoreRuntime<T>, minimum = runtime.generation): Generation {
  const previousGeneration = runtime.generation;
  const owner = runtime.queryProgressAuthority;
  const activeProgressRequest = owner?.current.status === "active" ? owner.request : undefined;
  const request = activeProgressRequest ?? runtime.activeRequest;
  const generation = nextGeneration(runtime.generation, minimum);
  runtime.generation = generation;
  if (
    request !== undefined &&
    !request.superseded &&
    request.generation === previousGeneration &&
    owner?.current.execution === request.progressIdentity &&
    owner.current.status === "active"
  )
    runtime.queryProgressAuthorityChange?.(request);
  return generation;
}

export function recordLanding<T>(
  runtime: StoreRuntime<T>,
  value: T,
  landingGeneration?: number,
  expectedSession?: BoundStreamSessionRuntime<T>,
  expectedPositionedSession?: StreamSession<T>,
): void {
  assertStoreOpen(runtime);
  const revision = nextValueRevision(runtime.valueRevision);
  const addressWrite = beginStoreAddress(runtime);
  try {
    if (runtime.incarnation === undefined) runtime.incarnation = {};
    const incarnation = runtime.incarnation;
    const generation = runtime.generation;
    const epoch = runtime.graph.__runtime.resetEpoch;
    const previousRevision = runtime.valueRevision;
    reactiveInternal.batch(() => {
      const landed = reactiveInternal.guardedWrite(
        runtime.committed,
        value,
        () =>
          !runtime.disposed &&
          !runtime.graph.__runtime.disposed &&
          runtime.incarnation === incarnation &&
          runtime.generation === generation &&
          runtime.valueRevision === previousRevision &&
          runtime.graph.__runtime.resetEpoch === epoch &&
          (expectedSession === undefined ||
            (expectedSession.phase === "active" &&
              !expectedSession.session.ended &&
              runtime.stream?.token === expectedSession.session.token &&
              runtime.incarnation === expectedSession.incarnation)) &&
          (expectedPositionedSession === undefined ||
            (runtime.stream === expectedPositionedSession &&
              !expectedPositionedSession.ended &&
              runtime.streamToken === expectedPositionedSession.token &&
              expectedPositionedSession.resetEpoch === epoch)),
        () => {
          runtime.valueRevision = revision;
          runtime.importedBaseline = undefined;
          runtime.baseAvailable = true;
          runtime.hasAcceptedLanding = true;
          runtime.transferEligible = true;
          runtime.transitioning = false;
          runtime.lastLandingAt = Date.now();
          runtime.failing = false;
          runtime.invalidated = false;
          runtime.refreshOnActivation = false;
          runtime.dropped = false;
          const positioned = runtime.stream?.positioned;
          if (positioned !== undefined) {
            positioned.baseRevision = revision;
            if (expectedPositionedSession === runtime.stream && positioned.dispatch !== undefined) {
              positioned.dispatch.landingRevision = revision;
              positioned.diverged = !expectedPositionedSession.hasPosition;
            } else if (
              expectedPositionedSession === runtime.stream &&
              positioned.phase === "dormant" &&
              positioned.reader !== undefined
            )
              positioned.diverged = false;
            else positioned.diverged = true;
          }
          runtime.errorCell.set(undefined);
          runtime.presence.set(true);
        },
      );
      if (!landed) return;
      if (
        expectedSession?.phase === "active" &&
        !expectedSession.session.ended &&
        runtime.stream?.token === expectedSession.session.token
      )
        expectedSession.lifecycle.update((version) => version + 1);
      touch(runtime);
      writeAllStatuses(runtime, Date.now(), landingGeneration);
    });
  } finally {
    endStoreAddress(runtime, addressWrite);
  }
}

/** Commit only observed value facts; accepted request authority stays untouched. */
export function publishObservation<T>(
  runtime: StoreRuntime<T>,
  value: T,
  eligible: boolean,
  current: () => boolean,
): boolean {
  const before = runtime.valueRevision;
  const next = nextValueRevision(before);
  const addressWrite = beginStoreAddress(runtime);
  try {
    let committed = false;
    reactiveInternal.batch(() => {
      reactiveInternal.guardedWrite(
        runtime.committed,
        value,
        () => current() && runtime.valueRevision === before,
        () => {
          runtime.valueRevision = next;
          runtime.baseAvailable = true;
          runtime.transferEligible = eligible;
          runtime.presence.set(true);
          runtime.observation!.issue.set(undefined);
          committed = true;
        },
      );
    });
    return committed;
  } finally {
    endStoreAddress(runtime, addressWrite);
  }
}

export type KeyedDefinition<T> =
  | QueryDefinition<any, T, any, any>
  | NativeStreamDefinition<any, T, any, any, any>;

export function optionConflict(
  runtime: StoreRuntime<unknown>,
  option: "initial" | "equals" | "snapshot",
): Fault {
  return new Fault("contract", [runtime.key as string, option]);
}

export function configureDeclaredStore<T>(
  runtime: StoreRuntime<T>,
  definition: StoreDefinition<T>,
): void {
  if (runtime.configured) return;
  if (!runtime.baseAvailable) nextValueRevision(runtime.valueRevision);
  runtime.configured = true;
  runtime.declared = true;
  runtime.initial = definition.initial;
  runtime.initialSpecified = true;
  runtime.equals = definition.equals ?? Object.is;
  runtime.equalsSpecified = definition.equals != null;
  runtime.snapshot = definition.snapshot ?? false;
  runtime.snapshotSpecified = definition.snapshot != null;
  if (!runtime.baseAvailable) {
    recordLanding(runtime, definition.initial);
  }
  touch(runtime);
}

export function configureKeyedValue<T>(
  runtime: StoreRuntime<T>,
  definition: KeyedDefinition<T>,
  options: ValueOptions<T> | undefined,
  expectedSession?: BoundStreamSessionRuntime<T>,
  validateOnly = false,
  // Dormant transfer installs initial policy without publishing an initial value.
  seed = true,
): void {
  const initialStated = options != null && Object.prototype.hasOwnProperty.call(options, "initial");
  const callerEqualsStated = options?.equals != null;
  const definitionEqualsStated = definition.equals != null;
  const callerSnapshotStated = options?.snapshot != null;
  const definitionSnapshotStated = definition.snapshot != null;
  const equalsStated = callerEqualsStated || definitionEqualsStated;
  const snapshotStated = callerSnapshotStated || definitionSnapshotStated;
  const initial = initialStated ? options?.initial : undefined;
  const equals = options?.equals ?? definition.equals;
  const snapshot = options?.snapshot ?? definition.snapshot ?? true;

  if (callerEqualsStated && definitionEqualsStated && options?.equals !== definition.equals) {
    throw optionConflict(runtime as StoreRuntime<unknown>, "equals");
  }
  if (
    callerSnapshotStated &&
    definitionSnapshotStated &&
    !Object.is(options?.snapshot, definition.snapshot)
  ) {
    throw optionConflict(runtime as StoreRuntime<unknown>, "snapshot");
  }

  if (!runtime.configured) {
    if (seed && initialStated && !runtime.baseAvailable) nextValueRevision(runtime.valueRevision);
    if (validateOnly) return;
    runtime.configured = true;
    runtime.declared = false;
    runtime.initial = initial;
    runtime.initialSpecified = initialStated;
    runtime.equals = equals ?? Object.is;
    runtime.equalsSpecified = equalsStated;
    runtime.snapshot = snapshot;
    runtime.snapshotSpecified = snapshotStated;
    if (seed && initialStated && !runtime.baseAvailable) {
      recordLanding(runtime, initial as T, undefined, expectedSession);
    }
    touch(runtime);
    return;
  }
  if (initialStated && runtime.initialSpecified && !Object.is(runtime.initial, initial))
    throw optionConflict(runtime as StoreRuntime<unknown>, "initial");
  const configureInitial = initialStated && !runtime.initialSpecified;
  const seedInitial =
    seed &&
    configureInitial &&
    !runtime.baseAvailable &&
    (runtime.generation === 0 || runtime.graph.__runtime.resetEpoch > 0);
  if (configureInitial) {
    const state = runtime.graph.__runtime;
    const resetRequest = runtime.activeRequest;
    const reseedAfterReset =
      state.resetEpoch > 0 &&
      (runtime.baseAvailable ||
        (resetRequest !== undefined &&
          !resetRequest.settled &&
          resetRequest.resetEpoch === state.resetEpoch));
    if (reseedAfterReset) throw optionConflict(runtime as StoreRuntime<unknown>, "initial");
  }
  if (equalsStated && runtime.equalsSpecified && runtime.equals !== (equals ?? Object.is))
    throw optionConflict(runtime as StoreRuntime<unknown>, "equals");
  if (snapshotStated && runtime.snapshotSpecified && runtime.snapshot !== snapshot)
    throw optionConflict(runtime as StoreRuntime<unknown>, "snapshot");
  if (seedInitial) nextValueRevision(runtime.valueRevision);
  if (validateOnly) return;
  if (equalsStated && !runtime.equalsSpecified) {
    runtime.equals = equals ?? Object.is;
    runtime.equalsSpecified = true;
  }
  if (snapshotStated && !runtime.snapshotSpecified) {
    runtime.snapshot = snapshot;
    runtime.snapshotSpecified = true;
  }
  if (configureInitial) {
    runtime.initial = initial;
    runtime.initialSpecified = true;
    if (seedInitial) {
      recordLanding(runtime, initial as T, undefined, expectedSession);
      runtime.refreshOnActivation = false;
    }
  }
  touch(runtime);
}

export function predictionFault<T>(runtime: StoreRuntime<T>): Fault {
  return new Fault("prediction", [runtime.key as string]);
}

export function preparePrediction<T>(
  runtime: StoreRuntime<T>,
  predictor: (current: T | undefined, input: unknown) => T,
  input: unknown,
  isCurrent: () => boolean = () => true,
): Prediction<T> | undefined {
  try {
    const current = runtime.value.peek();
    if (!isCurrent()) return undefined;
    reactiveInternal.withPredictionReadRefusal(() => predictor(current, input));
  } catch (error) {
    if (!isCurrent()) return undefined;
    graphFault(
      runtime.graph.__runtime,
      error instanceof Fault && error.kind === "prediction" ? error : predictionFault(runtime),
    );
    return undefined;
  }
  if (!isCurrent()) return undefined;
  const prediction: Prediction<T> = {
    id: runtime.graph.__runtime.nextPredictionId++,
    succeeded: false,
    dead: false,
    reported: false,
    apply(value: T | undefined): T | undefined {
      if (prediction.dead || !isCurrent()) return value;
      let next: T;
      try {
        next = reactiveInternal.withPredictionReadRefusal(() => predictor(value, input));
      } catch (error) {
        if (!isCurrent() || prediction.dead) return value;
        const revision = nextValueRevision(runtime.valueRevision);
        changeBoundAddress(runtime);
        prediction.dead = true;
        runtime.valueRevision = revision;
        if (!prediction.reported) {
          prediction.reported = true;
          graphFault(
            runtime.graph.__runtime,
            error instanceof Fault && error.kind === "prediction"
              ? error
              : predictionFault(runtime),
          );
        }
        return value;
      }
      return isCurrent() && !prediction.dead ? next : value;
    },
  };
  return prediction;
}

export function markPredictionSucceeded<T>(runtime: StoreRuntime<T>, id: number): void {
  const prediction = runtime.predictionStack.peek().find((entry) => entry.id === id);
  if (prediction !== undefined && !prediction.dead) prediction.succeeded = true;
}

export function retirePredictions<T>(runtime: StoreRuntime<T>, ids: readonly number[]): void {
  if (ids.length === 0) return;
  const covered = new Set(ids);
  const current = runtime.predictionStack.peek();
  const next = current.filter(
    (prediction) => !(prediction.succeeded && covered.has(prediction.id)),
  );
  if (next.length === current.length) return;
  const revision = nextValueRevision(runtime.valueRevision);
  changeBoundAddress(runtime);
  runtime.valueRevision = revision;
  runtime.predictionStack.set(next);
  updateCollectionCandidate(runtime);
  scheduleSweep(runtime.graph.__runtime);
}

export function pushPrediction<T>(runtime: StoreRuntime<T>, prediction: Prediction<T>): boolean {
  const live = runtime.predictionStack.peek().filter((entry) => !entry.dead);
  if (live.length >= runtime.graph.__runtime.maxPredictions) {
    graphFault(runtime.graph.__runtime, predictionFault(runtime));
    return false;
  }
  const revision = nextValueRevision(runtime.valueRevision);
  changeBoundAddress(runtime);
  reactiveInternal.batch(() => {
    runtime.valueRevision = revision;
    runtime.predictionStack.set([...live, prediction]);
    if (!runtime.baseAvailable) runtime.presence.set(true);
  });
  touch(runtime);
  return true;
}

export function removePrediction<T>(runtime: StoreRuntime<T>, id: number): void {
  const current = runtime.predictionStack.peek();
  const next = current.filter((entry) => entry.id !== id && !entry.dead);
  if (next.length !== current.length) {
    const revision = nextValueRevision(runtime.valueRevision);
    reactiveInternal.batch(() => {
      runtime.valueRevision = revision;
      if (!runtime.baseAvailable && next.length === 0) runtime.presence.set(false);
      runtime.predictionStack.set(next);
    });
    updateCollectionCandidate(runtime);
    scheduleSweep(runtime.graph.__runtime);
  }
}
