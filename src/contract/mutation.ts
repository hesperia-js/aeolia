import type { Affected, MutationDefinition, MutationOptions } from "./types.ts";
import { Fault } from "../fault.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { addSignalAbort } from "../utils.ts";
import { storeKey } from "./identity.ts";
import type { GraphRuntimeState, Prediction, StoreRuntime } from "./runtime.ts";
import { assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { callerForSource, startQuery, invalidateStore } from "./query-request.ts";
import {
  markPredictionSucceeded,
  preparePrediction,
  pushPrediction,
  removePrediction,
} from "./store-state.ts";

export interface MutationTarget {
  readonly affected: Affected<any>;
  readonly key: ReturnType<typeof storeKey>;
  readonly queryInput: unknown;
  readonly runtime?: StoreRuntime<unknown>;
}

export function resolveMutationTarget(
  state: GraphRuntimeState<any>,
  affected: Affected<any>,
  input: unknown,
): MutationTarget {
  const queryInput = affected.select(input);
  const key = storeKey(affected.query.key(queryInput));
  const runtime = state.stores.get(String(key));
  return {
    affected,
    key,
    queryInput,
    runtime: runtime?.dropped === true ? undefined : runtime,
  };
}

export function applyMutationEffects(
  state: GraphRuntimeState<any>,
  definition: MutationDefinition<any, any>,
  input: unknown,
): void {
  for (const affected of definition.affects) {
    let target: MutationTarget;
    try {
      target = resolveMutationTarget(state, affected, input);
    } catch {
      graphFault(state, new Fault("contract", [affected.query.name]));
      continue;
    }
    const runtime = target.runtime;
    if (runtime === undefined || runtime.disposed) continue;
    if (affected.on === "invalidate") {
      invalidateStore(runtime);
      continue;
    }
    try {
      const caller = callerForSource(runtime, {
        query: target.affected.query,
        input: target.queryInput,
      });
      void startQuery(runtime, caller, true);
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
  const targets = definition.affects.map((affected) =>
    resolveMutationTarget(state, affected, input),
  );
  const prepared: Array<{
    readonly runtime: StoreRuntime<unknown>;
    readonly prediction: Prediction<unknown>;
  }> = [];
  for (const target of targets) {
    if (target.runtime === undefined) continue;
    const declared = target.affected.optimistic as
      | ((current: unknown, input: unknown) => unknown)
      | undefined;
    const predictor = (options?.optimistic?.get(target.key) ?? declared) as
      | ((current: unknown, input: unknown) => unknown)
      | undefined;
    if (predictor === undefined) continue;
    const prediction = preparePrediction(target.runtime, predictor, input);
    if (prediction != null) prepared.push({ runtime: target.runtime, prediction });
  }
  const predictions: Array<{ readonly runtime: StoreRuntime<unknown>; readonly id: number }> = [];
  reactiveInternal.batch(() => {
    for (const job of prepared)
      if (pushPrediction(job.runtime, job.prediction))
        predictions.push({ runtime: job.runtime, id: job.prediction.id });
  });

  const controller = new AbortController();
  const removeAbort = addSignalAbort(options?.abortSignal, controller);
  state.activeControllers.add(controller);
  let returned: Promise<R>;
  try {
    returned = Promise.resolve(
      definition.run(input, { abortSignal: controller.signal, graph: state.graph.id }),
    );
  } catch (error) {
    returned = Promise.reject(error);
  }
  return returned.then(
    (result) => {
      removeAbort();
      state.activeControllers.delete(controller);
      try {
        reactiveInternal.batch(() => {
          for (const prediction of predictions)
            markPredictionSucceeded(prediction.runtime, prediction.id);
          if (!state.disposed) applyMutationEffects(state, definition, input);
        });
      } catch (error) {
        reportContinuationError(state, error);
      }
      return result;
    },
    (error) => {
      removeAbort();
      state.activeControllers.delete(controller);
      try {
        reactiveInternal.batch(() => {
          for (const prediction of predictions) removePrediction(prediction.runtime, prediction.id);
        });
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
      return Promise.reject(error);
    },
  );
}
