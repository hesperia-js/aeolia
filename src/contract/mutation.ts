import type { Affected, MutationDefinition, MutationOptions } from "./types.ts";
import { Fault } from "../fault.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { isObject } from "../utils.ts";
import type { ExistingTarget, TypedTarget } from "./query-binding.ts";
import type {
  PreparedEffect,
  PrepareEffectContext,
  SettledEffect,
  SettleEffectContext,
} from "./mutation-binding.ts";
import {
  assertBindingContextOpen,
  captureBoundProducer,
  captureQueryTarget,
  effectInstructionByHandle,
  isQueryTargetCurrent,
  queryTargetByHandle,
  targetPredictionByHandle,
} from "./registry.ts";
import { storeKey } from "./identity.ts";
import type {
  BoundProducerRuntime,
  EffectInstructionRuntime,
  EffectPhaseRuntime,
  GraphRuntimeState,
  MutationBindingRuntime,
  Prediction,
  ProfileEffectRuntime,
  QueryTargetRuntime,
  StoreRuntime,
} from "./runtime.ts";
import { AbortError, assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { callerForSource, startQuery, invalidateStore } from "./query-request.ts";
import {
  markPredictionSucceeded,
  preparePrediction,
  pushPrediction,
  removePrediction,
} from "./store-state.ts";
import type { PhysicalWork } from "./retirement.ts";
import {
  beginPhysicalWork,
  canReplacePhysicalCarrier,
  continuePhysicalPromise,
  createPhysicalCompletion,
  createPhysicalPromise,
  finishPhysicalWork,
  normalizePhysicalPromise,
  physicalCleanup,
} from "./retirement.ts";

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

function linkMutationAbort(
  state: GraphRuntimeState<any>,
  signal: AbortSignal | undefined,
  controller: AbortController,
): () => void {
  if (signal === undefined) return () => undefined;
  const abort = (): void => controller.abort(signal.reason);
  const remove = (): void => signal.removeEventListener("abort", abort);
  try {
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
    return remove;
  } catch (error) {
    try {
      physicalCleanup(state.retirement, remove);
    } catch (cleanupError) {
      reportContinuationError(state, cleanupError);
    }
    throw error;
  }
}

function resolveMutationTarget(
  state: GraphRuntimeState<any>,
  affected: Affected<any>,
  input: unknown,
  resetEpoch = state.resetEpoch,
): MutationTarget | undefined {
  if (!isCurrentMutationEpoch(state, resetEpoch)) return undefined;
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
      const predictor = (options?.optimistic?.get(target.key) ?? declared) as
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
    const removeAbort = linkMutationAbort(state, options?.abortSignal, controller);
    if (!isCurrentMutationEpoch(state, resetEpoch)) {
      physicalCleanup(state.retirement, removeAbort);
      return superseded();
    }
    state.activeControllers.add(controller);
    let runResult: unknown;
    let runReturned = false;
    let returned: Promise<R>;
    sourceStarted = true;
    try {
      runResult = definition.run(input, { abortSignal: controller.signal, graph: state.graph.id });
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

export interface BoundMutationAttempt {
  readonly physical: PhysicalWork;
  readonly outputPhysical: PhysicalWork;
  readonly output: MutationOutput<unknown>;
  readonly binding: MutationBindingRuntime;
  readonly controller: AbortController;
  readonly epoch: number;
  readonly predictions: Array<{ readonly target: QueryTargetRuntime; readonly id: number }>;
  stage: "prepare" | "run" | "settle" | "finished";
  unacknowledged: boolean;
  outputStarted: boolean;
  removeAbort?: () => void;
}

function retainUnacknowledgedMutation(attempt: BoundMutationAttempt): void {
  attempt.unacknowledged = true;
  attempt.output.acknowledged = false;
}

/** @internal Enroll before option accessors, selectors, resolvers or signal linking. */
export function beginBoundMutation(binding: MutationBindingRuntime): BoundMutationAttempt {
  assertBindingContextOpen(binding.context);
  const physical = beginPhysicalWork(binding.context.retirement, "acquisition");
  const outputPhysical = beginPhysicalWork(binding.context.retirement, "mutation");
  let output: MutationOutput<unknown> | undefined;
  try {
    output = createMutationOutput(outputPhysical);
    const controller = new AbortController();
    const attempt: BoundMutationAttempt = {
      physical,
      outputPhysical,
      output,
      binding,
      controller,
      epoch: binding.context.state.resetEpoch,
      predictions: [],
      stage: "prepare",
      unacknowledged: false,
      outputStarted: false,
    };
    binding.context.mutationControllers.add(controller);
    binding.context.state.activeControllers.add(controller);
    return attempt;
  } catch (error) {
    output?.reject(error);
    if (output === undefined) finishPhysicalWork(outputPhysical);
    finishPhysicalWork(physical);
    throw error;
  }
}

/** @internal Release both native controller owners without aborting successful work. */
export function finishBoundMutation(attempt: BoundMutationAttempt): void {
  if (attempt.stage === "finished") return;
  attempt.stage = "finished";
  attempt.binding.context.mutationControllers.delete(attempt.controller);
  attempt.binding.context.state.activeControllers.delete(attempt.controller);
  try {
    physicalCleanup(attempt.binding.context.retirement, () => attempt.removeAbort?.());
  } catch (error) {
    reportContinuationError(attempt.binding.context.state, error);
  } finally {
    attempt.removeAbort = undefined;
    if (!attempt.outputStarted) attempt.output.resolve(undefined);
    if (!attempt.unacknowledged) finishPhysicalWork(attempt.physical);
  }
}

function currentBoundLifetime(attempt: BoundMutationAttempt): boolean {
  return (
    !attempt.binding.context.disposed &&
    isCurrentMutationEpoch(attempt.binding.context.state, attempt.epoch)
  );
}

function assertBoundPhase(attempt: BoundMutationAttempt): void {
  if (
    attempt.stage === "finished" ||
    !currentBoundLifetime(attempt) ||
    (attempt.stage === "prepare" && attempt.controller.signal.aborted)
  )
    throw new AbortError("The bound mutation phase was superseded or canceled.");
}

function targetOwnedByPhase(phase: EffectPhaseRuntime, value: object): QueryTargetRuntime {
  const target = queryTargetByHandle.get(value);
  if (
    target === undefined ||
    target.binding.context !== phase.binding.context ||
    target.binding.definition !== phase.effect.query ||
    phase.members.get(target.runtime)?.has(target.incarnation) !== true
  )
    throw new TypeError("Effect target was not discovered by this phase");
  return target;
}

function effectPhase(
  attempt: BoundMutationAttempt,
  effect: ProfileEffectRuntime,
  input: unknown,
  kind: "prepare" | "settle",
): { readonly phase: EffectPhaseRuntime; readonly result: unknown } {
  assertBoundPhase(attempt);
  const selectedInput = effect.affected.select(input);
  assertBoundPhase(attempt);
  const phase: EffectPhaseRuntime = {
    binding: attempt.binding,
    effect,
    kind,
    members: new Map(),
    active: true,
  };
  const assertActive = (): void => {
    assertBoundPhase(attempt);
    if (!phase.active || attempt.stage !== kind)
      throw new TypeError("Effect phase has already completed");
  };
  const instruction = (
    value: ExistingTarget<any>,
    instructionKind: EffectInstructionRuntime["kind"],
    selected?: unknown,
    prediction?: BoundProducerRuntime,
  ): object => {
    assertActive();
    const target = targetOwnedByPhase(phase, value);
    const handle = Object.freeze({});
    effectInstructionByHandle.set(handle, {
      phase,
      target,
      kind: instructionKind,
      input: selected,
      prediction,
    });
    return handle;
  };
  const common = {
    input,
    selectedInput,
    signal: attempt.controller.signal,
    existing(queryInput: unknown): readonly ExistingTarget<any>[] {
      assertActive();
      const key = storeKey(effect.query.key(queryInput));
      assertActive();
      const targets: ExistingTarget<any>[] = [];
      for (const binding of phase.binding.context.bindings.get(effect.query)?.values() ?? []) {
        const runtime = binding.stores.get(String(key));
        if (
          runtime === undefined ||
          runtime.disposed ||
          runtime.dropped ||
          runtime.incarnation === undefined
        )
          continue;
        const ticket = captureQueryTarget(binding, runtime);
        if (ticket === undefined) continue;
        let incarnations = phase.members.get(runtime);
        if (incarnations === undefined) {
          incarnations = new Set();
          phase.members.set(runtime, incarnations);
        }
        incarnations.add(runtime.incarnation);
        targets.push(ticket);
      }
      return Object.freeze(targets);
    },
  };
  const prepare: PrepareEffectContext<any, any> = Object.freeze({
    ...common,
    prepare: (target: ExistingTarget<any>) => instruction(target, "prepare") as PreparedEffect,
    original: (target: TypedTarget<any, any>) => instruction(target, "original") as PreparedEffect,
    predict<V>(
      target: TypedTarget<any, V>,
      producer: (value: NoInfer<V> | undefined, input: unknown) => NoInfer<V>,
    ): PreparedEffect {
      assertActive();
      targetOwnedByPhase(phase, target);
      const prediction = captureBoundProducer(attempt.binding.definition, target, producer);
      assertActive();
      return instruction(target, "predict", undefined, prediction) as PreparedEffect;
    },
  });
  const settle: SettleEffectContext<any, any> = Object.freeze({
    ...common,
    selected: (target: ExistingTarget<any>, queryInput: unknown) =>
      instruction(target, "selected", queryInput) as SettledEffect,
    current: (target: ExistingTarget<any>) => instruction(target, "current") as SettledEffect,
  });
  try {
    const result = kind === "prepare" ? effect.prepare(prepare) : effect.settle(settle);
    return { phase, result };
  } catch (error) {
    phase.active = false;
    throw error;
  }
}

function phaseInstructions(
  attempt: BoundMutationAttempt,
  phase: EffectPhaseRuntime,
  result: unknown,
): readonly EffectInstructionRuntime[] {
  try {
    assertBoundPhase(attempt);
    if (!Array.isArray(result))
      throw new TypeError("Effect resolver must return an instruction array");
    const instructions: EffectInstructionRuntime[] = [];
    for (const value of result) {
      const checked = effectInstructionByHandle.get(value);
      if (
        checked === undefined ||
        checked.phase !== phase ||
        (phase.kind === "prepare"
          ? checked.kind === "selected" || checked.kind === "current"
          : checked.kind !== "selected" && checked.kind !== "current")
      )
        throw new TypeError("Effect resolver returned a foreign or wrong-phase instruction");
      instructions.push(checked);
    }
    assertBoundPhase(attempt);
    return instructions;
  } finally {
    phase.active = false;
  }
}

// Capture then once: getters and thenable invocation are authored authority boundaries.
function afterPhase<T>(
  attempt: BoundMutationAttempt,
  value: unknown,
  next: (value: unknown) => T | Promise<T>,
): T | Promise<T> {
  const owner = attempt.binding.context.retirement;
  const physical = beginPhysicalWork(owner, "mutation-phase");
  let retained = false;
  let thenCaptured = false;
  let thenCallable = false;
  let invoked = false;
  const retainUnacknowledgedOriginal = (): void => {
    if (!isObject(value)) return;
    retained = true;
    retainUnacknowledgedMutation(attempt);
    // Revoked thenables cannot gain new callouts to manufacture an acknowledgement.
    physical.original = value;
  };
  try {
    const then: unknown = isObject(value) ? Reflect.get(value, "then") : undefined;
    thenCaptured = true;
    thenCallable = typeof then === "function";
    assertBoundPhase(attempt);
    if (typeof then !== "function") return next(value);
    let removeWait = (): void => undefined;
    const pending = createPhysicalPromise<{ readonly value: unknown }>((resolve, reject) => {
      if (attempt.stage === "prepare" && attempt.controller.signal.aborted) {
        retainUnacknowledgedOriginal();
        reject(new AbortError("Mutation preparation was canceled."));
        return;
      }
      const aborted = (): void => reject(new AbortError("Mutation preparation was canceled."));
      try {
        if (attempt.stage === "prepare") {
          removeWait = () => attempt.controller.signal.removeEventListener("abort", aborted);
          attempt.controller.signal.addEventListener("abort", aborted, { once: true });
        }
        assertBoundPhase(attempt);
      } catch (error) {
        retainUnacknowledgedOriginal();
        reject(error);
        return;
      }
      physical.original = value;
      const original = createPhysicalPromise<unknown>((resolveOriginal, rejectOriginal) => {
        try {
          invoked = true;
          then.call(value, resolveOriginal, rejectOriginal);
        } catch (error) {
          rejectOriginal(error);
        }
      });
      retained = true;
      const continuation = continuePhysicalPromise<unknown, void>(original, (certified) => {
        if (!certified) retainUnacknowledgedMutation(attempt);
        return {
          fulfilled: (result) => {
            try {
              resolve({ value: result });
            } finally {
              if (certified) finishPhysicalWork(physical);
            }
          },
          rejected: (error) => {
            try {
              reject(error);
            } finally {
              if (certified) finishPhysicalWork(physical);
            }
          },
        };
      });
      if (physical.active) physical.continuation = continuation.promise;
    });
    // Cancellation ends this wait; the invoked original keeps its separate physical owner.
    return continuePhysicalPromise<{ readonly value: unknown }, T>(pending, (certified) => {
      if (!certified) retainUnacknowledgedMutation(attempt);
      return {
        fulfilled: ({ value: result }) => {
          physicalCleanup(owner, removeWait);
          assertBoundPhase(attempt);
          return next(result);
        },
        rejected: (error) => {
          physicalCleanup(owner, removeWait);
          throw error;
        },
      };
    }).promise;
  } catch (error) {
    if (!invoked && (!thenCaptured || thenCallable)) retainUnacknowledgedOriginal();
    throw error;
  } finally {
    if (!retained) finishPhysicalWork(physical);
  }
}

function prepareBoundEffects(
  attempt: BoundMutationAttempt,
  input: unknown,
  index: number,
  prepared: EffectInstructionRuntime[],
): readonly EffectInstructionRuntime[] | Promise<readonly EffectInstructionRuntime[]> {
  for (let position = index; position < attempt.binding.effects.length; position += 1) {
    const effect = attempt.binding.effects[position]!;
    const { phase, result } = effectPhase(attempt, effect, input, "prepare");
    const resolved = afterPhase(attempt, result, (value) =>
      phaseInstructions(attempt, phase, value),
    );
    if (resolved instanceof Promise)
      return continuePhysicalPromise<
        readonly EffectInstructionRuntime[],
        readonly EffectInstructionRuntime[]
      >(resolved, (certified) => {
        if (!certified) retainUnacknowledgedMutation(attempt);
        return {
          fulfilled: (instructions) => {
            prepared.push(...instructions);
            return prepareBoundEffects(attempt, input, position + 1, prepared);
          },
        };
      }).promise;
    prepared.push(...resolved);
  }
  return prepared;
}

function removeBoundPredictions(attempt: BoundMutationAttempt): void {
  reactiveInternal.batch(() => {
    for (const prediction of attempt.predictions) {
      if (!isQueryTargetCurrent(prediction.target)) continue;
      try {
        removePrediction(prediction.target.runtime, prediction.id);
      } catch (error) {
        reportContinuationError(attempt.binding.context.state, error);
      }
    }
  });
}

function publishBoundPredictions(
  attempt: BoundMutationAttempt,
  input: unknown,
  instructions: readonly EffectInstructionRuntime[],
  overrides: ReadonlyMap<object, BoundProducerRuntime>,
): void {
  const jobs: Array<{
    readonly target: QueryTargetRuntime;
    readonly prediction: Prediction<unknown>;
  }> = [];
  for (const instruction of instructions) {
    assertBoundPhase(attempt);
    const target = instruction.target;
    if (!isQueryTargetCurrent(target)) continue;
    const override = overrides.get(target.runtime.identity);
    const explicit =
      override !== undefined && override.target.incarnation === target.incarnation
        ? override
        : undefined;
    const original =
      instruction.kind === "original" ? instruction.phase.effect.affected.optimistic : undefined;
    const producer = explicit?.producer ?? instruction.prediction?.producer ?? original;
    if (producer === undefined) continue;
    const prediction = preparePrediction(
      target.runtime,
      producer,
      input,
      () =>
        currentBoundLifetime(attempt) &&
        isQueryTargetCurrent(target) &&
        (attempt.stage !== "prepare" || !attempt.controller.signal.aborted),
    );
    assertBoundPhase(attempt);
    if (prediction !== undefined) jobs.push({ target, prediction });
  }
  reactiveInternal.batch(() => {
    for (const job of jobs) {
      assertBoundPhase(attempt);
      if (!isQueryTargetCurrent(job.target)) continue;
      attempt.predictions.push({ target: job.target, id: job.prediction.id });
      pushPrediction(job.target.runtime, job.prediction);
      assertBoundPhase(attempt);
    }
  });
  assertBoundPhase(attempt);
}

function applyBoundEffect(
  attempt: BoundMutationAttempt,
  instruction: EffectInstructionRuntime,
): void {
  assertBoundPhase(attempt);
  const target = instruction.target;
  if (!isQueryTargetCurrent(target)) return;
  const runtime = target.runtime;
  if (instruction.kind === "selected") {
    const key = storeKey(instruction.phase.effect.query.key(instruction.input));
    assertBoundPhase(attempt);
    if (!isQueryTargetCurrent(target)) return;
    if (key !== runtime.key)
      throw new TypeError("Selected effect input does not match its target key");
  }
  if (instruction.phase.effect.affected.on === "invalidate") {
    invalidateStore(runtime);
    assertBoundPhase(attempt);
    if (!isQueryTargetCurrent(target) || !runtime.valueLive) return;
  }
  const source = runtime.source;
  if (source === undefined) return;
  const caller = callerForSource(runtime, {
    query: source.query,
    input: instruction.kind === "selected" ? instruction.input : source.input,
  });
  assertBoundPhase(attempt);
  if (!isQueryTargetCurrent(target)) return;
  const recovering = runtime.recoveryDisarmed || runtime.activeRequest?.automaticRecovery === true;
  void startQuery(runtime, caller, true, undefined, recovering, target.incarnation);
}

function settleBoundEffects(
  attempt: BoundMutationAttempt,
  input: unknown,
  index = 0,
): void | Promise<void> {
  for (let position = index; position < attempt.binding.effects.length; position += 1) {
    if (!currentBoundLifetime(attempt)) return;
    let resolved:
      | readonly EffectInstructionRuntime[]
      | Promise<readonly EffectInstructionRuntime[]>;
    try {
      const { phase, result } = effectPhase(
        attempt,
        attempt.binding.effects[position]!,
        input,
        "settle",
      );
      resolved = afterPhase(attempt, result, (value) => phaseInstructions(attempt, phase, value));
    } catch (error) {
      if (currentBoundLifetime(attempt))
        reportContinuationError(attempt.binding.context.state, error);
      continue;
    }
    const apply = (instructions: readonly EffectInstructionRuntime[]): void => {
      for (const instruction of instructions) {
        if (!currentBoundLifetime(attempt)) return;
        try {
          applyBoundEffect(attempt, instruction);
        } catch (error) {
          if (currentBoundLifetime(attempt))
            reportContinuationError(attempt.binding.context.state, error);
        }
      }
    };
    if (resolved instanceof Promise) {
      const applied = continuePhysicalPromise<readonly EffectInstructionRuntime[], void>(
        resolved,
        (certified) => {
          if (!certified) retainUnacknowledgedMutation(attempt);
          return {
            fulfilled: apply,
            rejected: (error) => {
              if (currentBoundLifetime(attempt))
                reportContinuationError(attempt.binding.context.state, error);
            },
          };
        },
      ).promise;
      return continuePhysicalPromise<void, void>(applied, (certified) => {
        if (!certified) retainUnacknowledgedMutation(attempt);
        return { fulfilled: () => settleBoundEffects(attempt, input, position + 1) };
      }).promise;
    }
    apply(resolved);
  }
}

/** @internal Executes only instructions validated by the genuine native phase owners. */
export function runBoundMutation(
  attempt: BoundMutationAttempt,
  input: unknown,
  signal: AbortSignal | undefined,
  suppliedOverrides: readonly unknown[],
): Promise<unknown> {
  const overrides = new Map<object, BoundProducerRuntime>();
  for (const value of suppliedOverrides) {
    const checked = targetPredictionByHandle.get(value as object);
    if (
      checked === undefined ||
      checked.mutation !== attempt.binding.definition ||
      checked.target.binding.context !== attempt.binding.context
    )
      throw new TypeError("Mutation override requires its exact mutation and context");
    const identity = checked.target.runtime.identity;
    if (overrides.has(identity))
      throw new TypeError("Duplicate prediction override for one store identity");
    overrides.set(identity, checked);
  }
  attempt.outputStarted = true;
  const cleanFailedMutation = (): void => {
    try {
      removeBoundPredictions(attempt);
    } catch (cleanupError) {
      reportContinuationError(attempt.binding.context.state, cleanupError);
    } finally {
      finishBoundMutation(attempt);
    }
  };
  const fail = (error: unknown): void => {
    try {
      cleanFailedMutation();
      attempt.output.reject(error);
    } catch (failure) {
      attempt.output.reject(failure);
    }
  };
  try {
    assertBoundPhase(attempt);
    if (signal !== undefined) {
      const abort = (): void => attempt.controller.abort();
      attempt.removeAbort = () => signal.removeEventListener("abort", abort);
      const aborted = signal.aborted;
      assertBoundPhase(attempt);
      if (aborted) attempt.controller.abort();
      else signal.addEventListener("abort", abort, { once: true });
    }
    assertBoundPhase(attempt);
    const preparation = prepareBoundEffects(attempt, input, 0, []);
    const dispatch = (instructions: readonly EffectInstructionRuntime[]): Promise<unknown> => {
      publishBoundPredictions(attempt, input, instructions, overrides);
      attempt.stage = "run";
      let returned: unknown;
      let runReturned = false;
      try {
        returned = attempt.binding.run(input, {
          graph: attempt.binding.context.state.graph.id,
          abortSignal: attempt.controller.signal,
        });
        runReturned = true;
        attempt.physical.original = returned;
      } catch (error) {
        returned = Promise.reject(error);
      }
      try {
        const output = normalizePhysicalPromise(returned);
        const completion = continuePhysicalPromise(output, (certified) => {
          if (!certified) retainUnacknowledgedMutation(attempt);
          return {
            fulfilled: (result) => {
              attempt.stage = "settle";
              if (!currentBoundLifetime(attempt)) return result;
              for (const prediction of attempt.predictions) {
                if (!currentBoundLifetime(attempt)) return result;
                if (!isQueryTargetCurrent(prediction.target)) continue;
                try {
                  markPredictionSucceeded(prediction.target.runtime, prediction.id);
                } catch (error) {
                  reportContinuationError(attempt.binding.context.state, error);
                }
              }
              const settlement = settleBoundEffects(attempt, input);
              return settlement instanceof Promise
                ? continuePhysicalPromise<void, unknown>(settlement, (settlementCertified) => {
                    if (!settlementCertified) retainUnacknowledgedMutation(attempt);
                    return { fulfilled: () => result };
                  }).promise
                : result;
            },
          };
        });
        return completion.promise;
      } catch (error) {
        if (runReturned && isObject(returned)) retainUnacknowledgedMutation(attempt);
        throw error;
      }
    };
    const running =
      preparation instanceof Promise
        ? continuePhysicalPromise<readonly EffectInstructionRuntime[], unknown>(
            preparation,
            (certified) => {
              if (!certified) retainUnacknowledgedMutation(attempt);
              return { fulfilled: dispatch };
            },
          ).promise
        : dispatch(preparation);
    try {
      let replaceFinalCarrier = false;
      const final = continuePhysicalPromise(running, (certified) => {
        if (!certified) retainUnacknowledgedMutation(attempt);
        const nativeObserver = certified && !attempt.unacknowledged;
        replaceFinalCarrier = nativeObserver;
        const succeeded = (result: unknown): unknown => {
          if (!nativeObserver) {
            finishBoundMutation(attempt);
            return result;
          }
          try {
            finishBoundMutation(attempt);
            attempt.output.resolve(result);
          } catch (error) {
            attempt.output.reject(error);
          }
        };
        const failed = (error: unknown): Promise<never> | void => {
          if (nativeObserver) fail(error);
          else {
            cleanFailedMutation();
            return Promise.reject(error);
          }
        };
        return { fulfilled: succeeded, rejected: failed };
      });
      const completion = final.promise;
      if (attempt.physical.active) attempt.physical.continuation = completion;
      if (!replaceFinalCarrier) {
        attempt.outputPhysical.original = completion;
        attempt.output.resolve(undefined);
        return completion;
      }
      return attempt.output.promise;
    } catch (error) {
      if (attempt.stage !== "prepare" && isObject(attempt.physical.original))
        retainUnacknowledgedMutation(attempt);
      throw error;
    }
  } catch (error) {
    fail(error);
    return attempt.output.promise;
  }
}
