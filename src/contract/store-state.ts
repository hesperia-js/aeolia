import type {
  Generation,
  QueryDefinition,
  StoreDefinition,
  StreamDefinition,
  ValueOptions,
} from "./types.ts";
import { Fault } from "../fault.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import type { Prediction, StoreRuntime } from "./runtime.ts";
import { scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import { writeAllStatuses } from "./status.ts";
import { assertStoreOpen, graphFault } from "./faults.ts";

export function issue<T>(runtime: StoreRuntime<T>): Generation {
  runtime.generation += 1;
  return runtime.generation as Generation;
}

export function recordLanding<T>(
  runtime: StoreRuntime<T>,
  value: T,
  landingGeneration?: number,
): void {
  assertStoreOpen(runtime);
  runtime.lastLandingAt = Date.now();
  runtime.failing = false;
  runtime.invalidated = false;
  runtime.dropped = false;
  const errors: unknown[] = [];
  try {
    runtime.errorCell.set(undefined);
  } catch (error) {
    errors.push(error);
  }
  try {
    runtime.committed.set(value);
  } catch (error) {
    errors.push(error);
  }
  runtime.hasCommitted = true;
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
}

export type KeyedDefinition<T> = QueryDefinition<any, T, any> | StreamDefinition<any, T, any>;

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
  runtime.configured = true;
  runtime.declared = true;
  runtime.initial = definition.initial;
  runtime.initialSpecified = true;
  runtime.equals = definition.equals ?? Object.is;
  runtime.equalsSpecified = definition.equals != null;
  runtime.snapshot = definition.snapshot ?? false;
  runtime.snapshotSpecified = definition.snapshot != null;
  if (!runtime.hasCommitted) {
    runtime.hasCommitted = true;
    runtime.lastLandingAt = Date.now();
    runtime.committed.set(definition.initial);
    runtime.presence.set(true);
  }
  touch(runtime);
}

export function configureKeyedValue<T>(
  runtime: StoreRuntime<T>,
  definition: KeyedDefinition<T>,
  options: ValueOptions<T> | undefined,
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
    runtime.configured = true;
    runtime.declared = false;
    runtime.initial = initial;
    runtime.initialSpecified = initialStated;
    runtime.equals = equals ?? Object.is;
    runtime.equalsSpecified = equalsStated;
    runtime.snapshot = snapshot;
    runtime.snapshotSpecified = snapshotStated;
    if (initialStated && !runtime.hasCommitted) {
      runtime.lastLandingAt = Date.now();
      runtime.committed.set(initial);
      runtime.hasCommitted = true;
      runtime.presence.set(true);
    }
    touch(runtime);
    return;
  }
  if (initialStated && runtime.initialSpecified && !Object.is(runtime.initial, initial))
    throw optionConflict(runtime as StoreRuntime<unknown>, "initial");
  if (initialStated && !runtime.initialSpecified) {
    runtime.initial = initial;
    runtime.initialSpecified = true;
    if (!runtime.hasCommitted && runtime.generation === 0) {
      runtime.lastLandingAt = Date.now();
      runtime.committed.set(initial);
      runtime.hasCommitted = true;
      runtime.presence.set(true);
    }
  }
  if (equalsStated && runtime.equalsSpecified && runtime.equals !== (equals ?? Object.is))
    throw optionConflict(runtime as StoreRuntime<unknown>, "equals");
  if (equalsStated && !runtime.equalsSpecified) {
    runtime.equals = equals ?? Object.is;
    runtime.equalsSpecified = true;
  }
  if (snapshotStated && runtime.snapshotSpecified && runtime.snapshot !== snapshot)
    throw optionConflict(runtime as StoreRuntime<unknown>, "snapshot");
  if (snapshotStated && !runtime.snapshotSpecified) {
    runtime.snapshot = snapshot;
    runtime.snapshotSpecified = true;
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
): Prediction<T> | undefined {
  try {
    const current = runtime.value.peek();
    reactiveInternal.withPredictionReadRefusal(() => predictor(current, input));
  } catch (error) {
    graphFault(
      runtime.graph.__runtime,
      error instanceof Fault && error.kind === "prediction" ? error : predictionFault(runtime),
    );
    return undefined;
  }
  const prediction: Prediction<T> = {
    id: runtime.graph.__runtime.nextPredictionId++,
    succeeded: false,
    dead: false,
    reported: false,
    apply(value: T | undefined): T | undefined {
      if (prediction.dead) return value;
      try {
        return reactiveInternal.withPredictionReadRefusal(() => predictor(value, input));
      } catch (error) {
        prediction.dead = true;
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
  runtime.predictionStack.set([...live, prediction]);
  if (!runtime.hasCommitted) runtime.presence.set(true);
  touch(runtime);
  return true;
}

export function removePrediction<T>(runtime: StoreRuntime<T>, id: number): void {
  const current = runtime.predictionStack.peek();
  const next = current.filter((entry) => entry.id !== id && !entry.dead);
  if (next.length !== current.length) {
    if (!runtime.hasCommitted && next.length === 0) runtime.presence.set(false);
    runtime.predictionStack.set(next);
    updateCollectionCandidate(runtime);
    scheduleSweep(runtime.graph.__runtime);
  }
}
