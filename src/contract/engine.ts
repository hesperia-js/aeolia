import type {
  Contract,
  ContractApi,
  Graph,
  GraphOptions,
  MutationDefinition,
  MutationOptions,
  OperationTree,
  QueryDefinition,
  Store,
  StoreAt,
  StoreDefinition,
  StoreOptions,
  StreamDefinition,
  ValueOptions,
  StoreIdentity,
  ValueRevision,
} from "./types.ts";
import {
  hasMutationOptionFields,
  hasStoreOptionFields,
  isMutationOptions,
  isStoreOptions,
  isCancellationOptions,
  captureQueryAcquisition,
  isCreateQueryInputWithAcquisition,
  isStoreDefinition,
  isOperationDefinition,
} from "./types.ts";
import { validateContract } from "./validation.ts";
import { isObject } from "../utils.ts";
import { nextValueRevision, storeKey } from "./identity.ts";
import { retireObservation } from "./observation-state.ts";
import { Fault, __registerFaultChannel, __unregisterFaultChannel } from "../fault.ts";
import type { Unsubscribe } from "../fault.ts";
import { __internal as reactiveInternal, computed, signal } from "../reactive.ts";
import {
  allocateGraphId,
  graphRuntimeByGraph,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
  rawStreamMemberByFunction,
  queryBindingByHandle,
  streamBindingByHandle,
  assertBindingContextOpen,
  beginBindingAddress,
  endBindingAddress,
  bindingAddress,
} from "./registry.ts";
import type {
  BindingContextRuntime,
  GraphDisposalProgress,
  GraphRuntime,
  GraphRuntimeState,
  RealmStoreState,
  StoreRuntime,
  StoreBindingRuntime,
} from "./runtime.ts";
import { collectDeclaredStores, finiteBound, graphIdleMs } from "./runtime-validation.ts";
import { AbortError, assertGraphOpen, reportContinuationError } from "./faults.ts";
import {
  armCollectionTimer,
  collectExpired,
  endStream,
  touch,
  updateCollectionCandidate,
} from "./collection.ts";
import { getOrCreateStore } from "./store.ts";
import { makeQueryStore } from "./query.ts";
import { runMutation } from "./mutation.ts";
import { streamMember } from "./stream.ts";
import { armStoreTimer, finishRequest } from "./query-request.ts";
import { hasQueryOptionsBrand } from "./query-options.ts";
import { configureKeyedValue, issue, nextGeneration } from "./store-state.ts";
import { boundAddressOccupied } from "./binding-address.ts";
import { writeAllStatuses } from "./status.ts";
import { resetGraph } from "./reset.ts";
import { retireQueryProgress } from "./query-progress.ts";
import {
  beginPhysicalWork,
  createRetirementOwner,
  finishPhysicalWork,
  physicalCleanup,
  recordCleanupFailure,
  retireOwner,
  type PhysicalWork,
} from "./retirement.ts";

/** @internal Default keyed-store retention period, in milliseconds. */
export const DEFAULT_IDLE_MS = 300_000;

/** @internal Default maximum number of simultaneous predictions on one store. */
export const DEFAULT_MAX_PREDICTIONS = 64;

function realmStores(graph: Graph): readonly RealmStoreState[] {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  return [...state.stores.values()]
    .filter((runtime) => !runtime.disposed)
    .map((runtime) => {
      const incarnation = runtime.incarnation;
      const revision = runtime.valueRevision;
      const generation = runtime.generation;
      const epoch = state.resetEpoch;
      const binding = runtime.binding;
      const session = runtime.stream;
      const ended = session?.ended;
      const streamToken = runtime.streamToken;
      const retirement = runtime.boundRetirement;
      const snapshot = runtime.snapshot;
      const equals = runtime.equals;
      const available = runtime.baseAvailable;
      const accepted = runtime.hasAcceptedLanding;
      const eligible = runtime.transferEligible;
      return {
        bound: runtime.binding !== undefined,
        binding,
        identity: runtime.identity,
        valueRevision: revision,
        key: runtime.key,
        snapshot: runtime.snapshot,
        baseAvailable: runtime.baseAvailable,
        hasAcceptedLanding: runtime.hasAcceptedLanding,
        transferEligible: runtime.transferEligible,
        current: () =>
          !state.disposed &&
          !runtime.disposed &&
          !runtime.transitioning &&
          binding?.context.disposed !== true &&
          runtime.stream === session &&
          runtime.stream?.ended === ended &&
          runtime.streamToken === streamToken &&
          runtime.boundRetirement === retirement &&
          retirement === undefined &&
          runtime.snapshot === snapshot &&
          runtime.equals === equals &&
          runtime.baseAvailable === available &&
          runtime.hasAcceptedLanding === accepted &&
          runtime.transferEligible === eligible &&
          runtime.incarnation === incarnation &&
          runtime.valueRevision === revision &&
          runtime.generation === generation &&
          state.resetEpoch === epoch,
        generation: runtime.generation,
        ...(runtime.lastLandingAt === undefined ? {} : { lastLandingAt: runtime.lastLandingAt }),
        committedValue: () => reactiveInternal.signalValue(runtime.committed),
      };
    });
}

function realmNamespace(graph: Graph): string {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  return state.namespace;
}

function realmBinding(graph: Graph, handle: object): StoreBindingRuntime {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  const binding = queryBindingByHandle.get(handle) ?? streamBindingByHandle.get(handle);
  if (binding === undefined || binding.context.state !== state)
    throw new TypeError("Transfer requires a genuine binding on this exact graph");
  assertBindingContextOpen(binding.context);
  return binding;
}

/** Native graph/reset authority, including captures with no selected entries. */
function realmCurrent(graph: Graph): () => boolean {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  const epoch = state.resetEpoch;
  return () => !state.disposed && state.resetEpoch === epoch;
}

function realmPreparationCurrent(graph: Graph): () => boolean {
  const current = realmCurrent(graph);
  const state = graphRuntimeByGraph.get(graph)!;
  const stores = [...state.stores.values()].map((runtime) => {
    const incarnation = runtime.incarnation;
    const revision = runtime.valueRevision;
    const generation = runtime.generation;
    const stream = runtime.stream;
    const ended = stream?.ended;
    const transitioning = runtime.transitioning;
    const disposed = runtime.disposed;
    const retirement = runtime.boundRetirement;
    const snapshot = runtime.snapshot;
    const equals = runtime.equals;
    const available = runtime.baseAvailable;
    const eligible = runtime.transferEligible;
    const accepted = runtime.hasAcceptedLanding;
    const landing = runtime.lastLandingAt;
    return () =>
      runtime.incarnation === incarnation &&
      runtime.valueRevision === revision &&
      runtime.generation === generation &&
      runtime.stream === stream &&
      stream?.ended === ended &&
      runtime.transitioning === transitioning &&
      runtime.disposed === disposed &&
      runtime.boundRetirement === retirement &&
      runtime.snapshot === snapshot &&
      runtime.equals === equals &&
      runtime.baseAvailable === available &&
      runtime.transferEligible === eligible &&
      runtime.hasAcceptedLanding === accepted &&
      runtime.lastLandingAt === landing;
  });
  const count = state.stores.size;
  return () => current() && state.stores.size === count && stores.every((current) => current());
}

/** Captures a destination before decoding can invoke authored accessors. */
function prepareRealmAdoption(graph: Graph, key: string, binding?: StoreBindingRuntime) {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  if (binding !== undefined) assertBindingContextOpen(binding.context);
  const lookup = (): StoreRuntime<unknown> | undefined =>
    binding === undefined ? state.stores.get(key) : binding.stores.get(key);
  const previous = lookup();
  const incarnation = previous?.incarnation;
  const revision = previous?.valueRevision;
  const generation = previous?.generation;
  const session = previous?.stream;
  const ended = session?.ended;
  const request = previous?.activeRequest;
  const requests = previous?.requests.size;
  const snapshot = previous?.snapshot;
  const equals = previous?.equals;
  const retirement = previous?.boundRetirement;
  const transitioning = previous?.transitioning;
  const epoch = state.resetEpoch;
  const address = binding === undefined ? undefined : bindingAddress(binding, key);
  const guard = address?.guard;
  const current = (): boolean =>
    !state.disposed &&
    binding?.context.disposed !== true &&
    state.resetEpoch === epoch &&
    lookup() === previous &&
    (address === undefined || address.guard === guard) &&
    (previous === undefined ||
      (!previous.disposed &&
        previous.transitioning === transitioning &&
        previous.incarnation === incarnation &&
        previous.valueRevision === revision &&
        previous.generation === generation &&
        previous.stream === session &&
        previous.stream?.ended === ended &&
        previous.activeRequest === request &&
        previous.requests.size === requests &&
        previous.snapshot === snapshot &&
        previous.equals === equals &&
        previous.boundRetirement === retirement));
  return {
    current,
    validate(foreignGeneration: number): void {
      if (binding?.kind === "stream" && boundAddressOccupied(binding, key)) return;
      if (!current()) throw new Fault("snapshot", [key]);
      const baseRevision =
        previous?.valueRevision ??
        ((binding === undefined && state.declaredStores.has(key) ? 1 : 0) as ValueRevision);
      nextValueRevision(baseRevision);
      nextGeneration(previous?.generation ?? 0, foreignGeneration);
    },
    adopt(
      value: unknown,
      foreignGeneration: number,
      landingAt: number | undefined,
    ): { readonly identity: StoreIdentity; readonly valueRevision: ValueRevision } | undefined {
      if (!current()) {
        if (binding?.kind === "stream") return undefined;
        throw new Fault("snapshot", [key]);
      }
      if (binding?.kind === "stream" && boundAddressOccupied(binding, key)) return undefined;
      const operation = beginBindingAddress(binding, key);
      try {
        const definition = binding === undefined ? state.declaredStores.get(key) : undefined;
        const runtime = getOrCreateStore(state, storeKey(key), definition, binding, false);
        if (
          state.disposed ||
          state.resetEpoch !== epoch ||
          binding?.context.disposed === true ||
          (previous !== undefined
            ? runtime !== previous ||
              runtime.incarnation !== incarnation ||
              runtime.valueRevision !== revision ||
              runtime.generation !== generation
            : definition === undefined
              ? runtime.incarnation !== undefined ||
                runtime.valueRevision !== 0 ||
                runtime.generation !== 0 ||
                runtime.baseAvailable
              : runtime.generation !== 0 || runtime.valueRevision !== 1 || !runtime.declared)
        )
          throw new Fault("snapshot", [key]);
        const before = runtime.valueRevision;
        const beforeIncarnation = runtime.incarnation;
        const beforeGeneration = runtime.generation;
        const beforeSession = runtime.stream;
        const beforeGuard = address?.guard;
        const owns = (expectedGeneration = beforeGeneration): boolean =>
          !state.disposed &&
          !runtime.disposed &&
          binding?.context.disposed !== true &&
          state.resetEpoch === epoch &&
          lookup() === runtime &&
          runtime.incarnation === beforeIncarnation &&
          runtime.valueRevision === before &&
          runtime.generation === expectedGeneration &&
          runtime.stream === beforeSession &&
          runtime.boundRetirement === undefined &&
          (address === undefined ||
            (address.guard === beforeGuard &&
              address.operations.size === 1 &&
              address.operations.has(operation!)));
        if (binding !== undefined) {
          const policy = binding.kind === "query" ? binding.execution : binding.definition;
          configureKeyedValue(
            runtime,
            policy,
            binding.kind === "query" ? binding.value : undefined,
            undefined,
            false,
            false,
          );
        }
        if (!owns()) throw new Fault("snapshot", [key]);
        const effectiveSnapshot = runtime.snapshot;
        const effectiveEquals = runtime.equals;
        if (
          binding?.kind === "stream" &&
          (runtime.baseAvailable ||
            runtime.requests.size > 0 ||
            (runtime.stream !== undefined && !runtime.stream.ended))
        )
          return undefined;
        const next = nextValueRevision(before);
        const adoptedGeneration = nextGeneration(runtime.generation, foreignGeneration);
        reactiveInternal.assertWritesAllowed(runtime.committed);
        runtime.generation = adoptedGeneration;
        let stamp:
          | { readonly identity: StoreIdentity; readonly valueRevision: ValueRevision }
          | undefined;
        const landed = reactiveInternal.guardedWrite(
          runtime.committed,
          value,
          () =>
            owns(adoptedGeneration) &&
            runtime.snapshot === effectiveSnapshot &&
            runtime.equals === effectiveEquals,
          () => {
            if (runtime.incarnation === undefined) runtime.incarnation = {};
            runtime.valueRevision = next;
            runtime.baseAvailable = true;
            runtime.hasAcceptedLanding = landingAt !== undefined;
            runtime.lastLandingAt = landingAt;
            runtime.transferEligible = true;
            runtime.transitioning = false;
            runtime.dropped = false;
            runtime.failing = false;
            runtime.invalidated = false;
            runtime.importedBaseline =
              binding?.kind === "stream"
                ? {
                    incarnation: runtime.incarnation,
                    resetEpoch: epoch,
                    generation: adoptedGeneration,
                    valueRevision: next,
                  }
                : undefined;
            stamp = Object.freeze({ identity: runtime.identity, valueRevision: next });
            runtime.errorCell.set(undefined);
            runtime.presence.set(true);
          },
        );
        if (!landed || stamp === undefined) throw new Fault("snapshot", [key]);
        touch(runtime);
        writeAllStatuses(runtime);
        armStoreTimer(runtime);
        return stamp;
      } finally {
        endBindingAddress(binding, key, operation);
      }
    },
  };
}

function adoptRealmEntry(
  graph: Graph,
  key: string,
  value: unknown,
  foreignGeneration: number,
  landingAt: number,
): void {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  nextGeneration(0, foreignGeneration);
  const definition = state.declaredStores.get(key) as StoreDefinition<unknown> | undefined;
  const runtime = getOrCreateStore(state, storeKey(key), definition, undefined, false);
  const revision = nextValueRevision(runtime.valueRevision);
  reactiveInternal.assertWritesAllowed(runtime.committed);
  issue(runtime, foreignGeneration);
  const generation = runtime.generation;
  const incarnation = runtime.incarnation;
  const epoch = state.resetEpoch;
  const previousRevision = runtime.valueRevision;
  const landed = reactiveInternal.guardedWrite(
    runtime.committed,
    value,
    () =>
      !state.disposed &&
      !runtime.disposed &&
      runtime.incarnation === incarnation &&
      state.resetEpoch === epoch &&
      runtime.valueRevision === previousRevision &&
      runtime.generation === generation,
    () => {
      if (runtime.incarnation === undefined) runtime.incarnation = {};
      runtime.valueRevision = revision;
      runtime.lastLandingAt = landingAt;
      runtime.failing = false;
      runtime.invalidated = false;
      runtime.baseAvailable = true;
      runtime.hasAcceptedLanding = true;
      runtime.transferEligible = true;
      runtime.transitioning = false;
      runtime.errorCell.set(undefined);
      runtime.presence.set(true);
      runtime.dropped = false;
    },
  );
  if (!landed) throw new Fault("snapshot", [key]);
  touch(runtime);
  writeAllStatuses(runtime);
  armStoreTimer(runtime);
}

function buildApi<C extends Contract>(state: GraphRuntimeState<C>, tree: OperationTree): unknown {
  const api: Record<string, unknown> = {};
  for (const [property, value] of Object.entries(tree)) {
    if (isStoreDefinition(value)) {
      api[property] = () => getOrCreateStore(state, storeKey(value.name), value).store;
    } else if (isObject(value) && "kind" in value && value.kind === "query") {
      const acquisition = captureQueryAcquisition(value);
      if (isCreateQueryInputWithAcquisition(value, acquisition) && value.kind === "query") {
        const definition = value as QueryDefinition<unknown, unknown, any, any>;
        state.queryDefinitions.set(definition, acquisition);
        api[property] = (...args: [input?: unknown, options?: StoreOptions<unknown>]) => {
          assertGraphOpen(state);
          const physical = beginPhysicalWork(state.retirement, "acquisition");
          try {
            const [input, options] = args;
            const call = normalizeQueryCall(input, options, args.length > 1);
            return makeQueryStore(
              state,
              definition,
              call.input,
              call.options,
              acquisition,
            );
          } finally {
            finishPhysicalWork(physical);
          }
        };
      } else if (value !== null && typeof value === "object") {
        api[property] = buildApi(state, value as OperationTree);
      }
    } else if (isOperationDefinition(value)) {
      if (value.kind === "mutation") {
        state.mutationDefinitions.add(value);
        api[property] = (...args: [input?: unknown, options?: MutationOptions]) => {
          assertGraphOpen(state);
          const physical = beginPhysicalWork(state.retirement, "acquisition");
          try {
            const [input, options] = args;
            const call = normalizeMutationCall(input, options, args.length > 1);
            return runMutation(
              state,
              value as MutationDefinition<unknown, unknown>,
              call.input,
              call.options,
            );
          } finally {
            finishPhysicalWork(physical);
          }
        };
      } else {
        const definition = value as StreamDefinition<unknown, unknown, any>;
        const member = (input: unknown, options?: ValueOptions<unknown>) =>
          streamMember(state, definition, input, options);
        rawStreamMemberByFunction.set(member, { state, definition });
        api[property] = member;
      }
    } else if (value !== null && typeof value === "object")
      api[property] = buildApi(state, value as OperationTree);
  }
  return Object.freeze(api);
}

/** @internal Shared raw and bound query argument normalization. */
export function normalizeQueryCall(
  input: unknown,
  options: unknown,
  hasSecondArgument: boolean,
): {
  readonly input: unknown;
  readonly options?: StoreOptions<unknown>;
} {
  let callInput = input;
  let callOptions: unknown = options;
  if (hasQueryOptionsBrand(input)) {
    if (
      options !== undefined &&
      (!isObject(options) || typeof options === "function" || Array.isArray(options))
    )
      throw new TypeError("Invalid query options.");
    callInput = undefined;
    callOptions = { ...input, ...options };
  } else if (!hasSecondArgument && hasStoreOptionFields(input)) {
    callInput = undefined;
    callOptions = input;
  }
  if (callOptions === undefined) return { input: callInput };
  if (!isStoreOptions(callOptions)) throw new TypeError("Invalid query options.");
  return { input: callInput, options: callOptions };
}

/** @internal Shared argument shape rules; each execution route validates its own options. */
export function normalizeMutationArguments(
  input: unknown,
  options: unknown,
  hasSecondArgument: boolean,
): { readonly input: unknown; readonly options?: unknown } {
  let callInput = input;
  let callOptions = options;
  if (hasQueryOptionsBrand(input)) {
    if (
      options !== undefined &&
      (!isObject(options) || typeof options === "function" || Array.isArray(options))
    )
      throw new TypeError("Invalid mutation options.");
    callInput = undefined;
    callOptions = { ...input, ...options };
  } else if (!hasSecondArgument && hasMutationOptionFields(input)) {
    callInput = undefined;
    callOptions = input;
  }
  return { input: callInput, options: callOptions };
}

function normalizeMutationCall(
  input: unknown,
  options: MutationOptions | undefined,
  hasSecondArgument: boolean,
): { readonly input: unknown; readonly options?: MutationOptions } {
  const call = normalizeMutationArguments(input, options, hasSecondArgument);
  if (call.options === undefined) return { input: call.input };
  if (!isMutationOptions(call.options)) throw new TypeError("Invalid mutation options.");
  return { input: call.input, options: call.options };
}

function cancelPending<C extends Contract>(state: GraphRuntimeState<C>, reason?: unknown): void {
  if (state.disposed) return;
  const cancellation = reason === undefined ? new AbortError("Graph work was canceled.") : reason;
  const operation = state.resetOperation;
  const controllers = [...state.activeControllers];
  const streams = [...state.stores.values()].flatMap((runtime) => {
    const session = runtime.stream;
    return session !== undefined && !session.ended ? [{ runtime, session }] : [];
  });
  const projections = [...state.projections].flatMap((projection) => {
    const cancel = projection.captureCancellation();
    return cancel === undefined ? [] : [cancel];
  });
  const streamControllers = new Set(streams.map(({ session }) => session.controller));
  const failures: unknown[] = [];
  const attempt = (action: () => void): void => {
    try {
      action();
    } catch (error) {
      failures.push(error);
    }
  };
  if (operation !== undefined) {
    if (state.resetOperation === operation) state.resetOperation = undefined;
    attempt(() => operation.reject(cancellation));
    attempt(() => operation.abort(cancellation));
  }
  for (const controller of controllers)
    if (!streamControllers.has(controller)) attempt(() => controller.abort(cancellation));
  for (const { runtime, session } of streams)
    attempt(() => {
      endStream(runtime, session, "closed", undefined, { abortReason: cancellation });
    });
  for (const cancel of projections) attempt(() => cancel(cancellation));
  if (failures.length > 0)
    throw new AggregateError(failures, "Several graph cancellation operations failed");
}

/** @internal Revokes a binding branch through the native graph/store owners. */
export function disposeBindingContextRuntime(
  context: BindingContextRuntime,
  physical?: PhysicalWork,
): void {
  if (context.disposed) return;
  reactiveInternal.assertWritesAllowed(context.activeCell);
  const work = physical ?? beginPhysicalWork(context.retirement, "cleanup");
  try {
    disposeBindingContext(context);
  } catch (cause) {
    recordCleanupFailure(work, cause);
    throw cause;
  } finally {
    finishPhysicalWork(work);
  }
}

/** @internal Genuine context closure, including work removed by prior disposal. */
export function retireBindingContextRuntime(context: BindingContextRuntime): Promise<void> {
  return retireOwner(context.retirement, (work) => disposeBindingContextRuntime(context, work));
}

function disposeBindingContext(context: BindingContextRuntime): void {
  const state = context.state;
  const stores = [
    ...[...context.bindings.values()].flatMap((profiles) =>
      [...profiles.values()].flatMap((binding) => [...binding.stores.values()]),
    ),
    ...[...context.streamBindings.values()].flatMap((members) =>
      [...members.values()].flatMap((binding) => [...binding.stores.values()]),
    ),
  ];
  const revisions = stores.map((runtime) =>
    state.disposed ? runtime.valueRevision : nextValueRevision(runtime.valueRevision),
  );
  const mutationControllers = [...context.mutationControllers];
  context.disposed = true;
  for (const profiles of context.bindings.values())
    for (const binding of profiles.values()) binding.addresses.clear();
  for (const members of context.streamBindings.values())
    for (const binding of members.values()) binding.addresses.clear();
  context.mutationControllers.clear();
  for (const controller of mutationControllers) state.activeControllers.delete(controller);
  state.bindingContexts.delete(context);
  for (const runtime of stores) {
    retireQueryProgress(runtime as StoreRuntime<unknown>);
    runtime.disposed = true;
    runtime.incarnation = undefined;
    state.stores.delete(runtime.identity);
    updateCollectionCandidate(runtime);
  }
  armCollectionTimer(state);
  const attempt = (action: () => void, alreadyOwned = false): void => {
    try {
      if (alreadyOwned) action();
      else physicalCleanup(context.retirement, action);
    } catch (error) {
      reportContinuationError(state, error);
    }
  };
  attempt(() =>
    reactiveInternal.batch(() => {
      for (const [index, runtime] of stores.entries()) {
        runtime.valueRevision = revisions[index]!;
        retireObservation(runtime);
      }
      context.activeCell.set(false);
    }),
  );
  for (const controller of mutationControllers)
    attempt(() => controller.abort(new Fault("disposed", [String(state.graph.id)])));
  for (const runtime of stores) {
    const session = runtime.stream;
    if (session !== undefined && runtime.binding?.kind === "stream")
      attempt(() => {
        endStream(runtime, session, "closed", new Fault("disposed", [String(state.graph.id)]));
      }, true);
    if (runtime.timer !== undefined) clearTimeout(runtime.timer);
    runtime.timer = undefined;
    runtime.source = undefined;
    runtime.onReadableActivation = undefined;
    runtime.refreshOnActivation = false;
    runtime.baseAvailable = false;
    runtime.hasAcceptedLanding = false;
    runtime.transferEligible = true;
    for (const request of [...runtime.requests]) {
      const reason = new Fault("disposed", [String(runtime.key)]);
      request.superseded = true;
      state.activeControllers.delete(request.controller);
      attempt(() => {
        finishRequest(runtime, request);
      }, true);
      if (!request.outcomeReported) {
        request.outcomeReported = true;
        request.resolveOutcome({ ok: false, error: reason });
      }
      attempt(() => request.controller.abort(reason));
    }
    runtime.requests.clear();
    runtime.activeRequest = undefined;
    for (const prediction of reactiveInternal.signalValue(runtime.predictionStack))
      prediction.dead = true;
    attempt(() => runtime.predictionStack.set([]));
    attempt(() => runtime.committed.set(undefined));
    attempt(() => runtime.presence.set(false));
    attempt(() => runtime.errorCell.set(undefined));
    attempt(() => runtime.lifecycle.update((value) => value + 1));
    for (const stop of [...runtime.livenessStops, ...runtime.readStops]) attempt(stop);
    runtime.livenessStops.length = 0;
    runtime.readStops.length = 0;
    runtime.callers.length = 0;
  }
}

function createGraphDisposalProgress<C extends Contract>(
  state: GraphRuntimeState<C>,
): GraphDisposalProgress {
  return {
    phase: "observation",
    revisions: new Map(
      [...state.stores.values()].map(
        (runtime) => [runtime, nextValueRevision(runtime.valueRevision)] as const,
      ),
    ),
    contextIndex: 0,
    resetOperationCaptured: false,
    resetRejectAttempted: false,
    resetAbortAttempted: false,
    stopAbortCaptured: false,
    stopAbortAttempted: false,
    collectionTimerCaptured: false,
    collectionTimerAttempted: false,
    projectionIndex: 0,
    storeIndex: 0,
    controllerIndex: 0,
    faultChannelUnregistered: false,
    faultObserversCleared: false,
    active: false,
    complete: false,
  };
}

function releaseGraphDisposalProgress(progress: GraphDisposalProgress): void {
  progress.revisions.clear();
  progress.contexts = undefined;
  progress.resetOperation = undefined;
  progress.stopAbort = undefined;
  progress.collectionTimer = undefined;
  progress.projections = undefined;
  progress.stores = undefined;
  progress.controllers = undefined;
  progress.phase = "complete";
  progress.complete = true;
}

function disposeGraph<C extends Contract>(
  state: GraphRuntimeState<C>,
  physical?: PhysicalWork,
): void {
  if (physical === undefined && state.disposed) return;
  let progress = state.disposal;
  if (progress?.complete) return;
  if (progress?.active) {
    if (physical !== undefined && progress.resumeWork === undefined)
      progress.resumeWork = beginPhysicalWork(state.retirement, "cleanup");
    return;
  }
  const work = physical ?? beginPhysicalWork(state.retirement, "cleanup");
  try {
    if (progress === undefined) {
      progress = createGraphDisposalProgress(state);
      state.disposal = progress;
      state.disposed = true;
    }
    progress.active = true;
    disposeGraphContents(state, progress, physical !== undefined);
  } catch (cause) {
    recordCleanupFailure(work, cause);
    throw cause;
  } finally {
    if (progress !== undefined) {
      progress.active = false;
      const resumeWork = progress.resumeWork;
      if (resumeWork !== undefined) {
        progress.resumeWork = undefined;
        if (!progress.complete) {
          progress.active = true;
          try {
            disposeGraphContents(state, progress, true);
          } catch (cause) {
            recordCleanupFailure(resumeWork, cause);
            reportContinuationError(state, cause);
          } finally {
            progress.active = false;
          }
        }
        finishPhysicalWork(resumeWork);
      }
    }
    finishPhysicalWork(work);
  }
}

function disposeGraphContents<C extends Contract>(
  state: GraphRuntimeState<C>,
  progress: GraphDisposalProgress,
  retiring: boolean,
): void {
  const attempt = (action: () => void, alreadyOwned = false): void => {
    if (!retiring) {
      action();
      return;
    }
    try {
      if (alreadyOwned) action();
      else physicalCleanup(state.retirement, action);
    } catch (cause) {
      reportContinuationError(state, cause);
    }
  };

  while (!progress.complete) {
    switch (progress.phase) {
      case "observation": {
        progress.phase = "contexts";
        try {
          physicalCleanup(state.retirement, () =>
            reactiveInternal.batch(() => {
              for (const [runtime, revision] of progress.revisions) {
                runtime.valueRevision = revision;
                retireObservation(runtime);
              }
            }),
          );
        } catch (error) {
          reportContinuationError(state, error);
        }
        break;
      }
      case "contexts": {
        progress.contexts ??= [...state.bindingContexts];
        const context = progress.contexts[progress.contextIndex];
        if (context !== undefined) {
          if (!context.disposed) reactiveInternal.assertWritesAllowed(context.activeCell);
          progress.contextIndex += 1;
          attempt(() => disposeBindingContextRuntime(context), true);
          break;
        }
        progress.contexts = undefined;
        progress.phase = "reset-reject";
        break;
      }
      case "reset-reject": {
        if (!progress.resetOperationCaptured) {
          progress.resetOperation = state.resetOperation;
          progress.resetOperationCaptured = true;
        }
        if (!progress.resetRejectAttempted) {
          progress.resetRejectAttempted = true;
          const operation = progress.resetOperation;
          if (operation !== undefined) attempt(() => operation.reject(new Fault("disposed", [])));
        }
        progress.phase = "reset-abort";
        break;
      }
      case "reset-abort": {
        if (!progress.resetAbortAttempted) {
          progress.resetAbortAttempted = true;
          const operation = progress.resetOperation;
          if (operation !== undefined) attempt(() => operation.abort(new Fault("disposed", [])));
        }
        state.resetOperation = undefined;
        progress.resetOperation = undefined;
        progress.phase = "lifetime";
        break;
      }
      case "lifetime": {
        if (!progress.stopAbortCaptured) {
          progress.stopAbort = state.stopAbort;
          progress.stopAbortCaptured = true;
        }
        if (!progress.stopAbortAttempted) {
          progress.stopAbortAttempted = true;
          const stop = progress.stopAbort;
          state.stopAbort = undefined;
          if (stop !== undefined) attempt(stop);
        }
        progress.stopAbort = undefined;
        progress.phase = "collection";
        break;
      }
      case "collection": {
        if (!progress.collectionTimerCaptured) {
          progress.collectionTimer = state.collectionTimer;
          progress.collectionTimerCaptured = true;
        }
        if (!progress.collectionTimerAttempted) {
          progress.collectionTimerAttempted = true;
          const timer = progress.collectionTimer;
          if (timer != null) attempt(() => clearTimeout(timer));
        }
        state.collectionTimer = undefined;
        state.collectionHeap.length = 0;
        progress.collectionTimer = undefined;
        progress.phase = "projections";
        break;
      }
      case "projections": {
        progress.projections ??= Array.from(state.projections);
        const projection = progress.projections[progress.projectionIndex];
        if (projection !== undefined) {
          progress.projectionIndex += 1;
          try {
            projection.dispose();
          } catch (error) {
            reportContinuationError(state, error);
          }
          break;
        }
        state.projections.clear();
        progress.projections = undefined;
        progress.phase = "stores";
        break;
      }
      case "stores": {
        progress.stores ??= Array.from(state.stores.values(), (runtime) => ({
          runtime,
          timerAttempted: false,
          stopsCaptured: false,
          livenessIndex: 0,
          readIndex: 0,
          stopsCleared: false,
          streamCaptured: false,
          streamAttempted: false,
          publicationAttempted: false,
        }));
        const store = progress.stores[progress.storeIndex];
        if (store === undefined) {
          progress.stores = undefined;
          progress.phase = "controllers";
          break;
        }
        const runtime = store.runtime;
        if (!store.timerAttempted) {
          store.timerAttempted = true;
          const timer = runtime.timer;
          if (timer != null) attempt(() => clearTimeout(timer));
        }
        runtime.timer = undefined;
        if (!store.stopsCaptured) {
          store.livenessStops = [...runtime.livenessStops];
          store.readStops = [...runtime.readStops];
          store.stopsCaptured = true;
        }
        const livenessStop = store.livenessStops?.[store.livenessIndex];
        if (livenessStop !== undefined) {
          store.livenessIndex += 1;
          attempt(livenessStop);
          break;
        }
        const readStop = store.readStops?.[store.readIndex];
        if (readStop !== undefined) {
          store.readIndex += 1;
          attempt(readStop);
          break;
        }
        if (!store.stopsCleared) {
          store.stopsCleared = true;
          runtime.livenessStops.length = 0;
          runtime.readStops.length = 0;
          store.livenessStops = undefined;
          store.readStops = undefined;
        }
        if (!store.streamCaptured) {
          store.stream = runtime.stream;
          store.streamCaptured = true;
        }
        if (!store.streamAttempted) {
          store.streamAttempted = true;
          const stream = store.stream;
          if (stream != null && !stream.ended) {
            try {
              endStream(runtime, stream, "closed");
            } catch (error) {
              reportContinuationError(state, error);
            }
          }
        }
        store.stream = undefined;
        retireQueryProgress(runtime as StoreRuntime<unknown>);
        runtime.disposed = true;
        runtime.incarnation = undefined;
        runtime.collectionIndex = -1;
        if (!store.publicationAttempted) {
          store.publicationAttempted = true;
          try {
            physicalCleanup(runtime.binding?.context.retirement ?? state.retirement, () =>
              reactiveInternal.batch(() => {
                runtime.presence.set(false);
                runtime.lifecycle.update((value) => value + 1);
              }),
            );
          } catch (error) {
            reportContinuationError(state, error);
          }
        }
        progress.storeIndex += 1;
        break;
      }
      case "controllers": {
        progress.controllers ??= Array.from(state.activeControllers);
        const controller = progress.controllers[progress.controllerIndex];
        if (controller !== undefined) {
          progress.controllerIndex += 1;
          attempt(() => controller.abort());
          break;
        }
        state.activeControllers.clear();
        progress.controllers = undefined;
        progress.phase = "fault-channel";
        break;
      }
      case "fault-channel": {
        if (!progress.faultChannelUnregistered) {
          progress.faultChannelUnregistered = true;
          attempt(() => __unregisterFaultChannel(state.graph));
        }
        progress.phase = "fault-observers";
        break;
      }
      case "fault-observers": {
        if (!progress.faultObserversCleared) {
          progress.faultObserversCleared = true;
          state.faultObservers.clear();
        }
        releaseGraphDisposalProgress(progress);
        break;
      }
      case "complete":
        releaseGraphDisposalProgress(progress);
        break;
    }
  }
}

/**
 * Creates an isolated state graph for a contract.
 *
 * @param options - Contract, lifetime signal, and resource bounds for the graph.
 * @returns A graph whose typed API is derived from `options.contract`.
 * @throws A {@link Fault} If a numeric resource bound is invalid.
 * @throws TypeError if the contract, cancellation signal, or diagnostic callback is malformed.
 */
export function createGraph<C extends Contract>(options: GraphOptions<C>): Graph<C> {
  if (!isCancellationOptions(options)) throw new TypeError("Invalid graph cancellation options.");
  if (options.onUnobservedFault !== undefined && typeof options.onUnobservedFault !== "function")
    throw new TypeError("Graph onUnobservedFault must be a function.");
  validateContract(options.contract);
  const idleMs = graphIdleMs(options.idleMs === undefined ? DEFAULT_IDLE_MS : options.idleMs);
  const maxPredictions = finiteBound(
    options.maxPredictions === undefined ? DEFAULT_MAX_PREDICTIONS : options.maxPredictions,
    "maxPredictions",
  );
  const id = allocateGraphId();
  const graph = {} as GraphRuntime<C>;
  const state: GraphRuntimeState<C> = {
    retirement: createRetirementOwner(),
    graph,
    namespace: options.contract.namespace,
    idleMs,
    maxPredictions,
    ...(options.onUnobservedFault === undefined
      ? {}
      : { onUnobservedFault: options.onUnobservedFault }),
    stores: new Map(),
    queryDefinitions: new Map(),
    mutationDefinitions: new Set(),
    bindingContexts: new Set(),
    declaredStores: new Map(),
    faultObservers: new Set(),
    activeControllers: new Set(),
    projections: new Set(),
    collectionHeap: [],
    sweepScheduled: false,
    nextPredictionId: 0,
    resetEpoch: 0,
    resetVersion: signal(0),
    disposed: false,
  };
  collectDeclaredStores(options.contract.operations, state.declaredStores);
  const graphStore = <T, N extends string>(definition: StoreDefinition<T, N>): Store<T> =>
    getOrCreateStore(state, storeKey(definition.name), definition).store;
  const graphAt = <K extends string>(key: K): StoreAt<C, K> => {
    assertGraphOpen(state);
    const branded = storeKey(key);
    const definition = state.declaredStores.get(key) as StoreDefinition<unknown> | undefined;
    return getOrCreateStore(state, branded, definition).store as StoreAt<C, K>;
  };
  const onFault = (observer: (fault: Fault) => void): Unsubscribe => {
    assertGraphOpen(state);
    state.faultObservers.add(observer);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      state.faultObservers.delete(observer);
    };
  };
  Object.assign(graph, {
    __runtime: state,
    id,
    api: buildApi(state, options.contract.operations) as ContractApi<C>,
    resetVersion: computed(() => state.resetVersion.get(), { label: `graph:${id}:reset-version` }),
    reset: () => resetGraph(state),
    cancelPending: (reason?: unknown) => cancelPending(state, reason),
    store: graphStore,
    at: graphAt,
    dispose: () => disposeGraph(state),
    retire: () =>
      retireOwner(state.retirement, (work) => {
        if (!state.disposed) {
          reactiveInternal.assertWritesAllowed(state.resetVersion);
          for (const context of state.bindingContexts)
            reactiveInternal.assertWritesAllowed(context.activeCell);
        }
        disposeGraph(state, work);
      }),
  });
  graphRuntimeByGraph.set(graph, state);
  __registerFaultChannel(graph, onFault);
  const acquisition = beginPhysicalWork(state.retirement, "acquisition");
  try {
    const abortSignal = options.abortSignal;
    if (abortSignal != null) {
      if (abortSignal.aborted || state.disposed) disposeGraph(state);
      else {
        const abort = (): void => disposeGraph(state);
        const stop = (): void => abortSignal.removeEventListener("abort", abort);
        state.stopAbort = stop;
        try {
          abortSignal.addEventListener("abort", abort, { once: true });
        } catch (cause) {
          state.stopAbort = undefined;
          try {
            physicalCleanup(state.retirement, stop);
          } catch (cleanupCause) {
            reportContinuationError(state, cleanupCause);
          }
          throw cause;
        }
        if (state.disposed) {
          state.stopAbort = undefined;
          physicalCleanup(state.retirement, stop);
        }
      }
    }
    return graph;
  } finally {
    finishPhysicalWork(acquisition);
  }
}

/** @internal Integration surface shared by Aeolia's graph-adjacent modules. */
export const __internal = Object.freeze({
  runtimeOf(store: Store<unknown>): StoreRuntime<unknown> {
    const runtime = runtimeForStore(store);
    if (runtime !== undefined) return runtime;
    throw new TypeError("The value is not an Aeolia graph store");
  },
  issueGeneration: issue,
  collectExpired,
  touch,
  registerProjection,
  reportContinuationError: reportGraphContinuationError,
  realmStores,
  realmNamespace,
  adoptRealmEntry,
  realmBinding,
  prepareRealmAdoption,
  realmPreparationCurrent,
  realmCurrent,
});
