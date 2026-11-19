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
import { noteTransferChange } from "./query-policy.ts";

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
  noteTransferChange(runtime);
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

/** Prepare a V1 adoption authority change and its real progress interruption. */
export function prepareAdoptionIssue<T>(
  runtime: StoreRuntime<T>,
  minimum: number,
): {
  publish(): void;
  notify(): void;
} {
  const generation = nextGeneration(runtime.generation, minimum);
  const owner = runtime.queryProgressAuthority;
  const active = owner?.current.status === "active" ? owner.request : undefined;
  const request = active ?? runtime.activeRequest;
  const interrupt =
    request !== undefined &&
    !request.superseded &&
    request.generation === runtime.generation &&
    owner?.current.execution === request.progressIdentity &&
    owner.current.status === "active"
      ? request
      : undefined;
  return {
    publish: () => {
      noteTransferChange(runtime);
      runtime.generation = generation;
    },
    notify: () => {
      if (interrupt !== undefined) runtime.queryProgressAuthorityChange?.(interrupt);
    },
  };
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
  noteTransferChange(runtime);
  if (runtime.binding === undefined && runtime.stream?.positioned === undefined) {
    const state = runtime.graph.__runtime;
    const epoch = state.resetEpoch;
    const generation = runtime.generation;
    const incarnation = runtime.incarnation;
    const current = (): boolean =>
      !state.disposed &&
      !runtime.disposed &&
      state.resetEpoch === epoch &&
      runtime.generation === generation &&
      runtime.incarnation === incarnation &&
      runtime.valueRevision === revision &&
      (expectedPositionedSession === undefined ||
        (runtime.stream === expectedPositionedSession &&
          !expectedPositionedSession.ended &&
          runtime.streamToken === expectedPositionedSession.token &&
          expectedPositionedSession.resetEpoch === epoch));
    runtime.valueRevision = revision;
    runtime.lastLandingAt = Date.now();
    runtime.failing = false;
    runtime.invalidated = false;
    runtime.refreshOnActivation = false;
    runtime.dropped = false;
    runtime.transferEligible = true;
    const errors: unknown[] = [];
    try {
      runtime.errorCell.set(undefined);
    } catch (error) {
      errors.push(error);
    }
    let landed = false;
    try {
      landed = reactiveInternal.guardedWrite(runtime.committed, value, current, () => undefined);
    } catch (error) {
      errors.push(error);
    }
    if (!landed && errors.length === 0) return;
    if (!current()) {
      if (errors.length === 1) throw errors[0];
      if (errors.length > 1) throw new AggregateError(errors, "Store landing propagation failed");
      return;
    }
    runtime.baseAvailable = true;
    runtime.hasAcceptedLanding = true;
    try {
      runtime.presence.set(true);
    } catch (error) {
      errors.push(error);
    }
    touch(runtime);
    try {
      writeAllStatuses(runtime, Date.now(), landingGeneration);
    } catch (error) {
      errors.push(error);
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, "Store landing propagation failed");
    return;
  }
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
          runtime.binding?.context.disposed !== true &&
          runtime.incarnation === incarnation &&
          runtime.generation === generation &&
          runtime.valueRevision === previousRevision &&
          runtime.graph.__runtime.resetEpoch === epoch &&
          (expectedSession === undefined ||
            (expectedSession.phase === "active" &&
              !expectedSession.session.ended &&
              runtime.stream?.token === expectedSession.session.token &&
              runtime.incarnation === expectedSession.incarnation &&
              expectedSession.binding.context.disposed !== true)) &&
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
            if (
              expectedPositionedSession !== undefined &&
              expectedPositionedSession === runtime.stream &&
              positioned.dispatch !== undefined
            ) {
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

export function publishObservation<T>(
  runtime: StoreRuntime<T>,
  value: T,
  eligible: boolean,
  current: () => boolean,
): boolean {
  const before = runtime.valueRevision;
  const next = nextValueRevision(before);
  noteTransferChange(runtime);
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
  noteTransferChange(runtime);
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
    if (runtime.binding === undefined) {
      runtime.valueRevision = nextValueRevision(runtime.valueRevision);
      runtime.baseAvailable = true;
      runtime.hasAcceptedLanding = true;
      runtime.lastLandingAt = Date.now();
      runtime.committed.set(definition.initial);
      runtime.presence.set(true);
    } else {
      recordLanding(runtime, definition.initial);
    }
  }
  touch(runtime);
}

function seedInitialValue<T>(
  runtime: StoreRuntime<T>,
  initial: T,
  expectedSession?: BoundStreamSessionRuntime<T>,
): void {
  if (runtime.binding !== undefined) {
    recordLanding(runtime, initial, undefined, expectedSession);
    return;
  }
  runtime.valueRevision = nextValueRevision(runtime.valueRevision);
  runtime.lastLandingAt = Date.now();
  runtime.committed.set(initial);
  runtime.baseAvailable = true;
  runtime.hasAcceptedLanding = true;
  runtime.presence.set(true);
}

export function configureKeyedValue<T>(
  runtime: StoreRuntime<T>,
  definition: KeyedDefinition<T>,
  options: ValueOptions<T> | undefined,
  expectedSession?: BoundStreamSessionRuntime<T>,
  validateOnly = false,
  // Dormant transfer installs initial policy without publishing an initial value.
  seed = true,
  afterRead?: () => void,
  touchCollection = true,
): void {
  const initialStated = options != null && Object.prototype.hasOwnProperty.call(options, "initial");
  afterRead?.();
  const callerEqualsStated = options?.equals != null;
  afterRead?.();
  const definitionEqualsStated = definition.equals != null;
  afterRead?.();
  const callerSnapshotStated = options?.snapshot != null;
  afterRead?.();
  const definitionSnapshotStated = definition.snapshot != null;
  afterRead?.();
  const equalsStated = callerEqualsStated || definitionEqualsStated;
  const snapshotStated = callerSnapshotStated || definitionSnapshotStated;
  const initial = initialStated ? options?.initial : undefined;
  afterRead?.();
  const callerEquals = options?.equals;
  afterRead?.();
  const defaultEquals = callerEquals == null ? definition.equals : undefined;
  afterRead?.();
  const equals = callerEquals ?? defaultEquals;
  const callerSnapshot = options?.snapshot;
  afterRead?.();
  const defaultSnapshot = callerSnapshot == null ? definition.snapshot : undefined;
  afterRead?.();
  const snapshot = callerSnapshot ?? defaultSnapshot ?? true;

  if (callerEqualsStated && definitionEqualsStated) {
    const caller = options?.equals;
    afterRead?.();
    const declared = definition.equals;
    afterRead?.();
    if (caller !== declared) throw optionConflict(runtime as StoreRuntime<unknown>, "equals");
  }
  if (callerSnapshotStated && definitionSnapshotStated) {
    const caller = options?.snapshot;
    afterRead?.();
    const declared = definition.snapshot;
    afterRead?.();
    if (!Object.is(caller, declared))
      throw optionConflict(runtime as StoreRuntime<unknown>, "snapshot");
  }

  if (!runtime.configured) {
    if (seed && initialStated && !runtime.baseAvailable) nextValueRevision(runtime.valueRevision);
    if (validateOnly) return;
    noteTransferChange(runtime);
    runtime.configured = true;
    runtime.declared = false;
    runtime.initial = initial;
    runtime.initialSpecified = initialStated;
    runtime.equals = equals ?? Object.is;
    runtime.equalsSpecified = equalsStated;
    runtime.snapshot = snapshot;
    runtime.snapshotSpecified = snapshotStated;
    if (seed && initialStated && !runtime.baseAvailable) {
      seedInitialValue(runtime, initial as T, expectedSession);
    }
    if (touchCollection) touch(runtime);
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
  if (
    (equalsStated && !runtime.equalsSpecified) ||
    (snapshotStated && !runtime.snapshotSpecified) ||
    configureInitial
  )
    noteTransferChange(runtime);
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
      seedInitialValue(runtime, initial as T, expectedSession);
      runtime.refreshOnActivation = false;
    }
  }
  if (touchCollection) touch(runtime);
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
        noteTransferChange(runtime);
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
  if (prediction !== undefined && !prediction.dead && !prediction.succeeded) {
    noteTransferChange(runtime);
    prediction.succeeded = true;
  }
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
  noteTransferChange(runtime);
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
  noteTransferChange(runtime);
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
    noteTransferChange(runtime);
    reactiveInternal.batch(() => {
      runtime.valueRevision = revision;
      if (!runtime.baseAvailable && next.length === 0) runtime.presence.set(false);
      runtime.predictionStack.set(next);
    });
    updateCollectionCandidate(runtime);
    scheduleSweep(runtime.graph.__runtime);
  }
}
