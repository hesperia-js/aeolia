import type { Affected, MutationDefinition, MutationOptions } from "./types.ts";
import { Fault } from "../fault.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { addSignalAbort, isObject } from "../utils.ts";
import type { PhysicalWork } from "./retirement.ts";
import {
  beginPhysicalWork,
  canReplacePhysicalCarrier,
  createPhysicalCompletion,
  finishPhysicalWork,
  normalizePhysicalPromise,
  physicalCleanup,
} from "./retirement.ts";
import { storeKey } from "./identity.ts";
import type { GraphRuntimeState, Prediction, StoreRuntime } from "./runtime.ts";
import { AbortError, assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { callerForSource, startQuery, invalidateStore } from "./query-request.ts";
import {
  markPredictionSucceeded,
  preparePrediction,
  pushPrediction,
  removePrediction,
} from "./store-state.ts";

const nativeApply = Reflect.apply;

interface MutationOutput<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T | PromiseLike<T>) => void;
  readonly reject: (reason: unknown) => void;
  acknowledged: boolean;
}

function createMutationOutput<T>(physical: PhysicalWork): MutationOutput<T> {
  let output: MutationOutput<T>;
  const { promise, resolve, reject } = createPhysicalCompletion<T>(() => {
    if (output.acknowledged) finishPhysicalWork(physical);
  });
  output = { promise, resolve, reject, acknowledged: true };
  physical.original = promise;
  physical.continuation = promise;
  return output;
}

export interface MutationTarget {
  readonly affected: Affected<any>;
  readonly key: ReturnType<typeof storeKey>;
  readonly queryInput: unknown;
  readonly runtime?: StoreRuntime<unknown>;
}

function isCurrentMutationEpoch(state: GraphRuntimeState<any>, resetEpoch: number): boolean {
  return !state.disposed && state.resetEpoch === resetEpoch;
}

function resolveMutationTarget(
  state: GraphRuntimeState<any>,
  affected: Affected<any>,
  input: unknown,
  resetEpoch = state.resetEpoch,
): MutationTarget | undefined {
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
  const select = affected.select;
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
  const queryInput = nativeApply(select, affected, [input]);
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
  const query = affected.query;
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
  const keyForInput = query.key;
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
  const key = storeKey(nativeApply(keyForInput, query, [queryInput]));
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
  const runtime = state.stores.get(String(key));
  return {
    affected,
    key,
    queryInput,
    runtime: runtime?.dropped === true ? undefined : runtime,
  };
}

function applyMutationEffects(
  state: GraphRuntimeState<any>,
  definition: MutationDefinition<any, any>,
  input: unknown,
  resetEpoch = state.resetEpoch,
): void {
  for (const affected of definition.affects) {
    if (!isCurrentMutationEpoch(state, resetEpoch)) return;
    let target: MutationTarget;
    try {
      const resolved = resolveMutationTarget(state, affected, input, resetEpoch);
      if (resolved === undefined) return;
      target = resolved;
    } catch {
      if (!isCurrentMutationEpoch(state, resetEpoch)) return;
      graphFault(state, new Fault("contract", [affected.query.name]));
      continue;
    }
    const runtime = target.runtime;
    if (runtime === undefined || runtime.disposed) continue;
    if (affected.on === "invalidate") {
      invalidateStore(runtime);
      if (!runtime.valueLive) continue;
    }
    try {
      const caller = callerForSource(runtime, {
        query: target.affected.query,
        input: target.queryInput,
      });
      const recovering =
        runtime.recoveryDisarmed || runtime.activeRequest?.automaticRecovery === true;
      void startQuery(runtime, caller, true, undefined, recovering);
    } catch (error) {
      reportContinuationError(state, error);
    }
  }
}

export function runMutation<I, R>(
  state: GraphRuntimeState<any>,
  definition: MutationDefinition<I, R>,
  input: I,
  options: MutationOptions | undefined,
): Promise<R> {
  assertGraphOpen(state);
  const acquisition = beginPhysicalWork(state.retirement, "acquisition");
  let output: MutationOutput<R>;
  try {
    output = createMutationOutput<R>(acquisition);
  } catch (error) {
    finishPhysicalWork(acquisition);
    throw error;
  }
  let sourceStarted = false;
  const superseded = (): Promise<R> => {
    output.reject(new AbortError("The mutation was superseded by a graph reset."));
    return output.promise;
  };
  try {
    const resetEpoch = state.resetEpoch;
    const targets: MutationTarget[] = [];
    for (const affected of definition.affects) {
      const target = resolveMutationTarget(state, affected, input, resetEpoch);
      if (target === undefined) return superseded();
      targets.push(target);
    }
    const prepared: Array<{
      readonly runtime: StoreRuntime<unknown>;
      readonly prediction: Prediction<unknown>;
    }> = [];
    for (const target of targets) {
      if (target.runtime === undefined) continue;
      const declared = target.affected.optimistic as
        | ((current: unknown, input: unknown) => unknown)
        | undefined;
      if (!isCurrentMutationEpoch(state, resetEpoch)) return superseded();
      const optimistic = options?.optimistic;
      if (!isCurrentMutationEpoch(state, resetEpoch)) return superseded();
      let supplied: ((current: unknown, input: unknown) => unknown) | undefined;
      if (optimistic !== undefined) {
        const get = optimistic.get;
        if (!isCurrentMutationEpoch(state, resetEpoch)) return superseded();
        supplied = nativeApply(get, optimistic, [target.key]);
      }
      const predictor = (supplied ?? declared) as
        | ((current: unknown, input: unknown) => unknown)
        | undefined;
      if (!isCurrentMutationEpoch(state, resetEpoch)) return superseded();
      if (predictor === undefined) continue;
      const prediction = preparePrediction(target.runtime, predictor, input, () =>
        isCurrentMutationEpoch(state, resetEpoch),
      );
      if (!isCurrentMutationEpoch(state, resetEpoch)) return superseded();
      if (prediction != null) prepared.push({ runtime: target.runtime, prediction });
    }
    const predictions: Array<{ readonly runtime: StoreRuntime<unknown>; readonly id: number }> = [];
    reactiveInternal.batch(() => {
      for (const job of prepared) {
        if (pushPrediction(job.runtime, job.prediction))
          predictions.push({ runtime: job.runtime, id: job.prediction.id });
        else if (!isCurrentMutationEpoch(state, resetEpoch)) return;
      }
    });
    if (!isCurrentMutationEpoch(state, resetEpoch)) return superseded();

    const controller = new AbortController();
    let removeAbort: () => void;
    try {
      removeAbort = addSignalAbort(
        options?.abortSignal,
        controller,
        (remove) => physicalCleanup(state.retirement, remove),
        () => isCurrentMutationEpoch(state, resetEpoch),
      );
    } catch (error) {
      if (isCurrentMutationEpoch(state, resetEpoch)) {
        try {
          physicalCleanup(state.retirement, () =>
            reactiveInternal.batch(() => {
              for (const prediction of predictions) {
                if (!isCurrentMutationEpoch(state, resetEpoch)) return;
                removePrediction(prediction.runtime, prediction.id);
              }
            }),
          );
        } catch {
          // Registration keeps its original error; retirement records cleanup failure.
        }
      }
      throw error;
    }
    if (!isCurrentMutationEpoch(state, resetEpoch)) {
      physicalCleanup(state.retirement, removeAbort);
      return superseded();
    }
    state.activeControllers.add(controller);
    let runResult: unknown;
    let runReturned = false;
    let returned: Promise<R>;
    try {
      const run = definition.run;
      if (!isCurrentMutationEpoch(state, resetEpoch)) {
        physicalCleanup(state.retirement, removeAbort);
        state.activeControllers.delete(controller);
        return superseded();
      }
      sourceStarted = true;
      runResult = nativeApply(run, definition, [
        input,
        { abortSignal: controller.signal, graph: state.graph.id },
      ]);
      runReturned = true;
      acquisition.original = runResult;
      returned = normalizePhysicalPromise(runResult as R | PromiseLike<R>);
    } catch (error) {
      if (runReturned && isObject(runResult)) output.acknowledged = false;
      const rejected = createPhysicalCompletion<R>();
      rejected.reject(error);
      returned = rejected.promise;
    }
    const observer = returned.then;
    const nativeObserver = canReplacePhysicalCarrier(returned, observer);
    if (!nativeObserver) output.acknowledged = false;
    const succeeded = (result: R): R | void => {
      try {
        try {
          physicalCleanup(state.retirement, removeAbort);
        } finally {
          state.activeControllers.delete(controller);
        }
        if (isCurrentMutationEpoch(state, resetEpoch)) {
          try {
            reactiveInternal.batch(() => {
              for (const prediction of predictions) {
                markPredictionSucceeded(prediction.runtime, prediction.id);
              }
              applyMutationEffects(state, definition, input, resetEpoch);
            });
          } catch (error) {
            if (isCurrentMutationEpoch(state, resetEpoch)) reportContinuationError(state, error);
          }
        }
        if (nativeObserver) output.resolve(result);
        else return result;
      } catch (error) {
        if (nativeObserver) output.reject(error);
        else throw error;
      }
    };
    const failed = (error: unknown): Promise<never> | void => {
      try {
        try {
          physicalCleanup(state.retirement, removeAbort);
        } finally {
          state.activeControllers.delete(controller);
        }
        if (isCurrentMutationEpoch(state, resetEpoch)) {
          try {
            reactiveInternal.batch(() => {
              for (const prediction of predictions) {
                removePrediction(prediction.runtime, prediction.id);
              }
            });
          } catch (cleanupError) {
            if (isCurrentMutationEpoch(state, resetEpoch))
              reportContinuationError(state, cleanupError);
          }
        }
        if (nativeObserver) output.reject(error);
        else return Promise.reject(error);
      } catch (failure) {
        if (nativeObserver) output.reject(failure);
        else throw failure;
      }
    };
    const completion = nativeApply(observer, returned, [succeeded, failed]) as Promise<R>;
    if (!nativeObserver) {
      acquisition.continuation = completion;
      output.resolve(undefined as R);
      return completion;
    }
    return output.promise;
  } catch (error) {
    if (sourceStarted) output.acknowledged = false;
    output.reject(error);
    if (!sourceStarted) finishPhysicalWork(acquisition);
    throw error;
  }
}
