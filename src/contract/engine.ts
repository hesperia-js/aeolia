import type {
  Contract,
  ContractApi,
  Graph,
  GraphOptions,
  MutationDefinition,
  MutationOptions,
  OperationTree,
  QueryDefinition,
  QueryStore,
  QueryProgressSnapshot,
  Store,
  StreamStore,
  StoreAt,
  StoreDefinition,
  StoreOptions,
  PositionedStreamDefinition,
  PositionedStoreRestoreSeed,
  PositionedStoreRestoration,
  NativeStreamDefinition,
  StoreIdentity,
  StoreKey,
  Readable,
  ValueRevision,
  ValueOptions,
} from "./types.ts";
import {
  hasMutationOptionFields,
  hasStoreOptionFields,
  isMutationOptions,
  isStoreOptions,
  isValueOptions,
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
import type { Computed, WritableSignal } from "../reactive.ts";
import { __internal as reactiveInternal, computed, signal } from "../reactive.ts";
import {
  allocateGraphId,
  graphRuntimeByGraph,
  registerDormantStoreHandle,
  assertStoreHandlePublished,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
  runtimeByStore,
  rawStreamMemberByFunction,
  positionedStreamSourceByDefinition,
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
  QueryBindingRuntime,
  QueryCaller,
  ObservationState,
  StreamBindingRuntime,
  GraphProjection,
  BindingAddressRuntime,
} from "./runtime.ts";
import { collectDeclaredStores, finiteBound, graphIdleMs } from "./runtime-validation.ts";
import {
  AbortError,
  assertGraphOpen,
  assertStoreOpen,
  graphFault,
  reportContinuationError,
} from "./faults.ts";
import {
  beginTransferAdmission,
  finishTransferAdmission,
  registerTransferGraph,
  registerTransferSource,
  registerTransferReadable,
  registerTransferStoreHandle,
  registerTransferCaller,
  unregisterTransferReadable,
  transferEpochCurrent,
  transferGraph,
  transferSource,
  transferStoreAdmissible,
  inheritTransferCoverage,
  noteTransferChange,
  prepareTransferChange,
} from "./query-policy.ts";
import type { TransferAdmission, TransferSourceRegistration } from "./query-policy.ts";
import {
  armCollectionTimer,
  collectExpired,
  endStream,
  discardPositionedStoreSession,
  touch,
  updateCollectionCandidate,
} from "./collection.ts";
import {
  admitDetachedStore,
  getOrCreateStore,
  makeStore,
  observeStoreReadable,
  storeEquals,
} from "./store.ts";
import { addCaller, createQueryRuntimeActivation, makeQueryStore, ready } from "./query.ts";
import { runMutation } from "./mutation.ts";
import { createStreamRuntimeActivation, streamMember } from "./stream.ts";
import {
  preparePositionedStoreTransfer,
  type PreparedPositionedStoreTransfer,
} from "./projection.ts";
import type { ProjectionPolicy } from "./projection.ts";
import { asWindow, createCaller, isCurrentRequest, queryIsStale } from "./status.ts";
import { prepareQueryProgressCell } from "./query-progress.ts";
import type { PreparedQueryProgressCell } from "./query-progress.ts";
import { prepareObservationTransfer } from "./observation.ts";
import type { ObservationLease, PreparedObservationTransfer } from "./observation.ts";
import { storeSubscribe } from "../subscribe.ts";
import { armStoreTimer, finishRequest, startQuery } from "./query-request.ts";
import { hasQueryOptionsBrand } from "./query-options.ts";
import { configureKeyedValue, issue, nextGeneration } from "./store-state.ts";
import { boundAddressOccupied } from "./binding-address.ts";
import { writeAllStatuses } from "./status.ts";
import { resetGraph } from "./reset.ts";
import { retireQueryProgress } from "./query-progress.ts";
import {
  beginPhysicalWork,
  createPhysicalCompletion,
  createRetirementOwner,
  finishPhysicalWork,
  physicalCleanup,
  recordCleanupFailure,
  retireOwner,
  type PhysicalWork,
  readerLifetimeNow,
  observeReaderCompletion,
  observeUnreturnedReaderRetirement,
} from "./retirement.ts";

/** @internal Default keyed-store retention period, in milliseconds. */
export const DEFAULT_IDLE_MS = 300_000;

/** @internal Default maximum number of simultaneous predictions on one store. */
export const DEFAULT_MAX_PREDICTIONS = 64;

function realmStores(graph: Graph): readonly RealmStoreState[] {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  const graphStores = [...state.stores.values()];
  const unsupportedReader = graphStores.find(
    (runtime) =>
      !runtime.disposed &&
      runtime.readerScope !== undefined &&
      runtime.snapshot &&
      runtime.baseAvailable &&
      runtime.transferEligible,
  );
  if (unsupportedReader !== undefined) throw new Fault("encoding", [String(unsupportedReader.key)]);
  return graphStores
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
      const member = () => getOrCreateStore(state, storeKey(value.name), value).store;
      registerTransferSource(member, { graph: state.graph, kind: "store", definition: value });
      api[property] = member;
    } else if (isObject(value) && "kind" in value && value.kind === "query") {
      const acquisition = captureQueryAcquisition(value);
      if (isCreateQueryInputWithAcquisition(value, acquisition) && value.kind === "query") {
        const definition = value as QueryDefinition<unknown, unknown, any, any>;
        state.queryDefinitions.set(definition, acquisition);
        const member = (...args: [input?: unknown, options?: StoreOptions<unknown>]) => {
          assertGraphOpen(state);
          const physical = beginPhysicalWork(state.retirement, "acquisition");
          let admission: TransferAdmission | undefined;
          const finishAdmission = (): void => {
            if (admission === undefined) return;
            const pending = admission;
            admission = undefined;
            finishTransferAdmission(pending);
          };
          try {
            const [input, options] = args;
            admission = beginTransferAdmission(state, definition, undefined, input);
            const call = normalizeQueryCall(input, options, args.length > 1);
            finishAdmission();
            return makeQueryStore(state, definition, call.input, call.options, acquisition);
          } finally {
            try {
              finishAdmission();
            } finally {
              finishPhysicalWork(physical);
            }
          }
        };
        registerTransferSource(member, { graph: state.graph, kind: "query", definition });
        api[property] = member;
      } else if (value !== null && typeof value === "object") {
        api[property] = buildApi(state, value as OperationTree);
      }
    } else if (isOperationDefinition(value)) {
      if (value.kind === "mutation") {
        state.mutationDefinitions.add(value);
        const definition = value as MutationDefinition<unknown, unknown>;
        const member = (...args: [input?: unknown, options?: MutationOptions]) => {
          assertGraphOpen(state);
          const physical = beginPhysicalWork(state.retirement, "acquisition");
          let admission: TransferAdmission | undefined;
          const finishAdmission = (): void => {
            if (admission === undefined) return;
            const pending = admission;
            admission = undefined;
            finishTransferAdmission(pending);
          };
          try {
            const [input, options] = args;
            admission = beginTransferAdmission(state, definition, undefined, input);
            const call = normalizeMutationCall(input, options, args.length > 1);
            finishAdmission();
            return runMutation(state, definition, call.input, call.options, member);
          } finally {
            try {
              finishAdmission();
            } finally {
              finishPhysicalWork(physical);
            }
          }
        };
        registerTransferSource(member, { graph: state.graph, kind: "mutation", definition });
        api[property] = member;
      } else {
        const definition = value as NativeStreamDefinition<unknown, unknown, any, any, any>;
        if (definition.kind === "positioned-stream") {
          if (!positionedStreamSourceByDefinition.has(definition))
            throw new TypeError("Positioned stream must be created by createPositionedStream");
          state.positionedDefinitions.add(
            definition as PositionedStreamDefinition<any, any, any, any, any>,
          );
        }
        const member = (input: unknown, options?: ValueOptions<unknown>) =>
          streamMember(state, definition, input, options, undefined, member);
        rawStreamMemberByFunction.set(member, { state, definition });
        registerTransferSource(member, { graph: state.graph, kind: "stream", definition });
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

function closePendingPositionedStores(owner: GraphRuntimeState<any> | BindingContextRuntime): void {
  const pending = owner.pendingPositionedStores;
  if (pending === undefined) return;
  const failures: unknown[] = [];
  for (const runtime of [...pending]) {
    const session = runtime.stream;
    if (session?.positioned?.preparation?.published !== false) continue;
    try {
      discardPositionedStoreSession(runtime, session);
    } catch (cause) {
      failures.push(cause);
    }
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1)
    throw new AggregateError(failures, "Pending positioned reader cleanup failed");
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
  attempt(() => closePendingPositionedStores(state));
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
    ...new Set([
      ...[...context.bindings.values()].flatMap((profiles) =>
        [...profiles.values()].flatMap((binding) => [...binding.stores.values()]),
      ),
      ...[...context.streamBindings.values()].flatMap((members) =>
        [...members.values()].flatMap((binding) => [
          ...binding.stores.values(),
          ...(binding.readerStores ?? []),
        ]),
      ),
    ]),
  ];
  const revisions = stores.map((runtime) =>
    state.disposed ? runtime.valueRevision : nextValueRevision(runtime.valueRevision),
  );
  const mutationControllers = [...context.mutationControllers];
  const positionedProjections = [...(context.positionedProjections ?? [])];
  context.positionedProjections?.clear();
  context.disposed = true;
  closePendingPositionedStores(context);
  for (const profiles of context.bindings.values())
    for (const binding of profiles.values()) binding.addresses.clear();
  for (const members of context.streamBindings.values())
    for (const binding of members.values()) {
      binding.addresses.clear();
      binding.readerStores?.clear();
    }
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
  for (const closeProjection of positionedProjections) attempt(closeProjection);
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
      closePendingPositionedStores(state);
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
    positionedDefinitions: new Set(),
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
  Object.assign(graph, { __runtime: state, id });
  graphRuntimeByGraph.set(graph, state);
  registerTransferGraph(graph, state);
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
  const api = buildApi(state, options.contract.operations) as ContractApi<C>;
  Object.assign(graph, {
    api,
    resetVersion: computed(() => state.resetVersion.get(), { label: `graph:${id}:reset-version` }),
    reset: () => {
      const before = state.resetEpoch;
      try {
        return resetGraph(state);
      } finally {
        if (state.resetEpoch !== before) {
          try {
            closePendingPositionedStores(state);
          } catch (cause) {
            reportContinuationError(state, cause);
          }
        }
      }
    },
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

export interface PassiveNativeFacet {
  readonly target: Computed<unknown>;
  readonly source: Readable<unknown>;
}

interface PreparedNativePassiveTransferClaim {
  readonly kind: "query" | "stream" | "store";
  readonly runtime: StoreRuntime<unknown>;
  readonly policy: PassiveQueryPolicy;
  readonly compare?: (left: unknown, right: unknown) => boolean;
  readonly store: Store<unknown>;
  readonly caller?: QueryCaller<unknown>;
  readonly progressCell?: WritableSignal<QueryProgressSnapshot<unknown>>;
  readonly facets: readonly PassiveNativeFacet[];
  readonly existing: boolean;
  readonly incarnation: object;
  prepareObservation(
    existingLease?: ObservationLease<unknown>,
  ): PreparedObservationTransfer<unknown>;
  observation(): PreparedObservationTransfer<unknown> | undefined;
  readonly current: () => boolean;
  readonly positioned?: PreparedPositionedStoreTransfer;
  readonly restoration?: PositionedStoreRestoration<unknown, readonly unknown[]>;
  currentAt?(now: number): boolean;
  discard(): void;
  readonly retired: Promise<void>;
}

export interface PreparedNativePassiveTransferClaims {
  current(now?: number): boolean;
  publish(): void;
  deliver(): void;
}

interface PassiveQueryPolicy {
  readonly configured: boolean;
  readonly declared: boolean;
  readonly initial: unknown;
  readonly initialSpecified: boolean;
  readonly equals: (left: unknown, right: unknown) => boolean;
  readonly equalsSpecified: boolean;
  readonly snapshot: boolean;
  readonly snapshotSpecified: boolean;
}

interface PassiveQueryConfiguration {
  readonly before: PassiveQueryPolicy;
  readonly after: PassiveQueryPolicy;
  readonly compare: (left: unknown, right: unknown) => boolean;
  readonly current: () => boolean;
  publish(): void;
}

interface PassiveQueryClaimRecord {
  readonly state: GraphRuntimeState<any>;
  readonly binding?: QueryBindingRuntime;
  readonly key: StoreKey;
  readonly runtime: StoreRuntime<unknown>;
  readonly runtimeReservation?: PassiveRuntimeReservation;
  readonly store: Store<unknown>;
  readonly caller?: QueryCaller<unknown>;
  readonly progress?: PreparedQueryProgressCell;
  observation?: PreparedObservationTransfer<unknown>;
  observationLease?: ObservationLease<unknown>;
  observationGroup?: PassiveObservationGroup;
  observationDemand?: PassiveObservationDemand;
  observationPrepared: boolean;
  readonly incarnationBefore: object | undefined;
  readonly incarnationAfter: object;
  readonly droppedBefore: boolean;
  readonly detached: boolean;
  readonly graphStoresBefore: GraphRuntimeState<any>["stores"];
  readonly bindingStoresBefore?: QueryBindingRuntime["stores"];
  readonly callerRowsBefore: StoreRuntime<unknown>["callers"];
  readonly graphEntryBefore: StoreRuntime<unknown> | undefined;
  readonly bindingEntryBefore: StoreRuntime<unknown> | undefined;
  readonly addressRecord?: ReturnType<typeof bindingAddress>;
  readonly addressGuard?: object;
  readonly resetEpoch: number;
  readonly graphCurrent: () => boolean;
  readonly bindingCurrent: () => boolean;
  readonly runtimeCurrent: () => boolean;
  readonly configuration?: PassiveQueryConfiguration;
  readonly policy: PassiveQueryPolicy;
  readonly sourceBefore: StoreRuntime<unknown>["source"];
  readonly streamSourceBefore: StoreRuntime<unknown>["streamSource"];
  readonly activationBefore: StoreRuntime<unknown>["onReadableActivation"];
  readonly sourceAfter?: NonNullable<StoreRuntime<unknown>["source"]>;
  readonly streamSourceAfter?: NonNullable<StoreRuntime<unknown>["streamSource"]>;
  readonly activationAfter?: () => void;
  readonly claimReadStops: readonly Unsubscribe[];
  readonly claimLivenessStops: readonly Unsubscribe[];
  readonly facets: readonly PassiveNativeFacet[];
  readonly claimObservations: readonly {
    readonly readable: Readable<unknown>;
    readonly observation: {
      readonly isValue?: boolean;
      readonly armsFreshness?: boolean;
      readonly metadata?: boolean;
    };
  }[];
  readonly retired: Promise<void>;
  readonly resolveRetired: () => void;
  readonly rejectRetired: (cause: unknown) => void;
  readonly releasePrepared: () => void;
  published: boolean;
  discarded: boolean;
  inAggregate: boolean;
  current(): boolean;
  setPublished(): void;
  discard(): void;
}
interface PendingPassiveValue<T> {
  readonly value: T;
  refs: number;
}
interface PendingPassiveRuntime {
  readonly runtime: StoreRuntime<unknown>;
  refs: number;
}
interface PassiveRuntimeReservation {
  readonly runtime: StoreRuntime<unknown>;
  readonly detached: boolean;
  current(): boolean;
  release(): void;
}
interface PassiveObservationDemand {
  readonly record: PassiveQueryClaimRecord;
  active: boolean;
}
interface PassiveObservationGroup {
  readonly runtime: StoreRuntime<unknown>;
  readonly incarnation: object;
  readonly demands: PassiveObservationDemand[];
  lease?: ObservationLease<unknown>;
  observation?: PreparedObservationTransfer<unknown>;
  inAggregate: boolean;
  published: boolean;
  discarded: boolean;
}
const passiveObservationGroups = new WeakMap<object, PassiveObservationGroup>();
const passiveQueryRuntimes = new WeakMap<object, Map<string, PendingPassiveRuntime>>();
const passiveQueryClaims = new WeakMap<object, PassiveQueryClaimRecord>();
const passiveQueryProgressCells = new WeakMap<
  object,
  PendingPassiveValue<PreparedQueryProgressCell>
>();
const passiveQueryIncarnations = new WeakMap<object, PendingPassiveValue<object>>();

function reservePassiveProgressCell(runtime: StoreRuntime<unknown>): {
  readonly value: PreparedQueryProgressCell;
  release(): void;
} {
  if (runtime.queryProgressCell !== undefined) {
    passiveQueryProgressCells.delete(runtime);
    return { value: prepareQueryProgressCell(runtime), release: () => undefined };
  }
  let pending = passiveQueryProgressCells.get(runtime);
  if (pending === undefined) {
    pending = { value: prepareQueryProgressCell(runtime), refs: 0 };
    passiveQueryProgressCells.set(runtime, pending);
  }
  pending.refs += 1;
  let released = false;
  return {
    value: pending.value,
    release: () => {
      if (released) return;
      released = true;
      pending!.refs -= 1;
      if (pending!.refs === 0) {
        if (passiveQueryProgressCells.get(runtime) === pending)
          passiveQueryProgressCells.delete(runtime);
        pending!.value.discard();
      }
    },
  };
}

function reservePassiveIncarnation(runtime: StoreRuntime<unknown>): {
  readonly value: object;
  release(): void;
} {
  if (runtime.incarnation !== undefined && !runtime.dropped)
    return { value: runtime.incarnation, release: () => undefined };
  let pending = passiveQueryIncarnations.get(runtime);
  if (pending === undefined) {
    pending = { value: {}, refs: 0 };
    passiveQueryIncarnations.set(runtime, pending);
  }
  pending.refs += 1;
  let released = false;
  return {
    value: pending.value,
    release: () => {
      if (released) return;
      released = true;
      pending!.refs -= 1;
      if (pending!.refs === 0 && passiveQueryIncarnations.get(runtime) === pending)
        passiveQueryIncarnations.delete(runtime);
    },
  };
}

function releasePreparedPassiveRows(rows: readonly (() => void)[]): void {
  const failures: unknown[] = [];
  for (let index = 0; index < rows.length; index += 1) {
    try {
      rows[index]!();
    } catch (cause) {
      failures.push(cause);
    }
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, "Passive claim cleanup failed");
}

function passiveQueryPolicy(runtime: StoreRuntime<unknown>): PassiveQueryPolicy {
  return {
    configured: runtime.configured,
    declared: runtime.declared,
    initial: runtime.initial,
    initialSpecified: runtime.initialSpecified,
    equals: runtime.equals,
    equalsSpecified: runtime.equalsSpecified,
    snapshot: runtime.snapshot,
    snapshotSpecified: runtime.snapshotSpecified,
  };
}

function samePassiveQueryPolicy(
  runtime: StoreRuntime<unknown>,
  policy: PassiveQueryPolicy,
): boolean {
  return (
    runtime.configured === policy.configured &&
    runtime.declared === policy.declared &&
    Object.is(runtime.initial, policy.initial) &&
    runtime.initialSpecified === policy.initialSpecified &&
    runtime.equals === policy.equals &&
    runtime.equalsSpecified === policy.equalsSpecified &&
    runtime.snapshot === policy.snapshot &&
    runtime.snapshotSpecified === policy.snapshotSpecified
  );
}

function samePassiveQueryPolicies(left: PassiveQueryPolicy, right: PassiveQueryPolicy): boolean {
  return (
    left.configured === right.configured &&
    left.declared === right.declared &&
    Object.is(left.initial, right.initial) &&
    left.initialSpecified === right.initialSpecified &&
    left.equals === right.equals &&
    left.equalsSpecified === right.equalsSpecified &&
    left.snapshot === right.snapshot &&
    left.snapshotSpecified === right.snapshotSpecified
  );
}

function writePassiveQueryPolicy(runtime: StoreRuntime<unknown>, policy: PassiveQueryPolicy): void {
  runtime.configured = policy.configured;
  runtime.declared = policy.declared;
  runtime.initial = policy.initial;
  runtime.initialSpecified = policy.initialSpecified;
  runtime.equals = policy.equals;
  runtime.equalsSpecified = policy.equalsSpecified;
  runtime.snapshot = policy.snapshot;
  runtime.snapshotSpecified = policy.snapshotSpecified;
}

function preparePassiveQueryConfiguration(
  runtime: StoreRuntime<unknown>,
  query:
    | QueryDefinition<any, unknown, any, any>
    | NativeStreamDefinition<any, unknown, any, any, any>,
  options: ValueOptions<unknown> | undefined,
): PassiveQueryConfiguration {
  const transition = prepareTransferChange(runtime);
  const before = passiveQueryPolicy(runtime);
  const shadow = { ...runtime } as StoreRuntime<unknown>;
  configureKeyedValue(shadow, query, options, undefined, false, false, false);
  const after = passiveQueryPolicy(shadow);
  const changed = !samePassiveQueryPolicies(before, after);
  let applied = false;
  const isCurrent = (): boolean =>
    (applied ? transition.afterCurrent() : transition.current()) &&
    samePassiveQueryPolicy(runtime, applied ? after : before);
  const compare = (left: unknown, right: unknown): boolean => {
    if (!isCurrent()) return true;
    const equal = storeEquals(shadow, left, right);
    return isCurrent() ? equal : true;
  };
  return {
    before,
    after,
    compare,
    current: isCurrent,
    publish: () => {
      if (!changed || applied) return;
      transition.publish();
      writePassiveQueryPolicy(runtime, after);
      applied = true;
    },
  };
}
function preparePassiveDeclaredStoreConfiguration(
  runtime: StoreRuntime<unknown>,
  definition: StoreDefinition<unknown>,
): PassiveQueryConfiguration {
  const transition = prepareTransferChange(runtime);
  const before = passiveQueryPolicy(runtime);
  const shadow = { ...runtime } as StoreRuntime<unknown>;
  if (!shadow.configured) {
    shadow.configured = true;
    shadow.declared = true;
    shadow.initial = definition.initial;
    shadow.initialSpecified = true;
    shadow.equals = definition.equals ?? Object.is;
    shadow.equalsSpecified = definition.equals != null;
    shadow.snapshot = definition.snapshot ?? false;
    shadow.snapshotSpecified = definition.snapshot != null;
  }
  const after = passiveQueryPolicy(shadow);
  const changed = !samePassiveQueryPolicies(before, after);
  let applied = false;
  const isCurrent = (): boolean =>
    (applied ? transition.afterCurrent() : transition.current()) &&
    samePassiveQueryPolicy(runtime, applied ? after : before);
  const compare = (left: unknown, right: unknown): boolean => {
    if (!isCurrent()) return true;
    const equal = storeEquals(shadow, left, right);
    return isCurrent() ? equal : true;
  };
  return {
    before,
    after,
    compare,
    current: isCurrent,
    publish: () => {
      if (!changed || applied) return;
      transition.publish();
      writePassiveQueryPolicy(runtime, after);
      applied = true;
    },
  };
}
function passiveQueryRuntime(
  state: GraphRuntimeState<any>,
  binding: QueryBindingRuntime | undefined,
  key: StoreKey,
): StoreRuntime<unknown> | undefined {
  return binding === undefined
    ? (state.stores.get(String(key)) as StoreRuntime<unknown> | undefined)
    : binding.stores.get(String(key));
}

function reservePassiveQueryRuntime(
  state: GraphRuntimeState<any>,
  binding: QueryBindingRuntime | undefined,
  key: StoreKey,
): PassiveRuntimeReservation {
  const physical = passiveQueryRuntime(state, binding, key);
  if (physical !== undefined) {
    return {
      runtime: physical,
      detached: false,
      current: () => passiveQueryRuntime(state, binding, key) === physical,
      release: () => undefined,
    };
  }
  const owner = binding ?? state.graph;
  const address = String(key);
  let rows = passiveQueryRuntimes.get(owner);
  if (rows === undefined) {
    rows = new Map();
    passiveQueryRuntimes.set(owner, rows);
  }
  let pending = rows.get(address);
  if (pending === undefined) {
    const runtime = makeStore(state, key, undefined, binding);
    inheritTransferCoverage(runtime);
    admitDetachedStore(runtime);
    pending = { runtime, refs: 0 };
    rows.set(address, pending);
  }
  pending.refs += 1;
  let released = false;
  return {
    runtime: pending.runtime,
    detached: true,
    current: () =>
      passiveQueryRuntimes.get(owner) === rows &&
      rows!.get(address) === pending &&
      passiveQueryRuntime(state, binding, key) === undefined,
    release: () => {
      if (released) return;
      released = true;
      pending!.refs -= 1;
      if (pending!.refs !== 0) return;
      if (rows!.get(address) === pending) {
        rows!.delete(address);
        if (rows!.size === 0 && passiveQueryRuntimes.get(owner) === rows)
          passiveQueryRuntimes.delete(owner);
      }
      if (passiveQueryRuntime(state, binding, key) !== pending!.runtime)
        cleanupDetachedPassiveStore(pending!.runtime);
    },
  };
}

function passiveQueryBaseCurrent(record: PassiveQueryClaimRecord): boolean {
  if (record.discarded || record.published) return false;
  const { state, binding, runtime, key } = record;
  if (
    state.disposed ||
    state.resetEpoch !== record.resetEpoch ||
    binding?.context.disposed === true ||
    !record.graphCurrent() ||
    !record.bindingCurrent() ||
    !record.runtimeCurrent() ||
    (record.configuration !== undefined && !record.configuration.current()) ||
    (record.configuration === undefined && !samePassiveQueryPolicy(runtime, record.policy)) ||
    (record.progress !== undefined && !record.progress.current()) ||
    runtime.incarnation !== record.incarnationBefore ||
    runtime.dropped !== record.droppedBefore ||
    runtime.disposed ||
    runtime.source !== record.sourceBefore ||
    runtime.streamSource !== record.streamSourceBefore ||
    runtime.onReadableActivation !== record.activationBefore ||
    runtime.callers !== record.callerRowsBefore ||
    state.stores !== record.graphStoresBefore ||
    binding?.stores !== record.bindingStoresBefore ||
    !transferStoreAdmissible(runtime)
  )
    return false;
  if (
    binding !== undefined &&
    (binding.addresses.get(String(key)) !== record.addressRecord ||
      record.addressRecord?.guard !== record.addressGuard)
  )
    return false;
  const current = passiveQueryRuntime(state, binding, key);
  if (record.detached) return record.runtimeReservation?.current() ?? current === undefined;
  return current === runtime;
}

function passiveQueryCurrent(record: PassiveQueryClaimRecord): boolean {
  return passiveQueryBaseCurrent(record) && (record.observation?.current() ?? true);
}

function releasePassiveObservationDemand(record: PassiveQueryClaimRecord): void {
  const group = record.observationGroup;
  const demand = record.observationDemand;
  if (group === undefined || group.published || demand === undefined || !demand.active) return;
  demand.active = false;
  for (let index = 0; index < group.demands.length; index += 1)
    if (group.demands[index]!.active) return;
  group.discarded = true;
  if (group.lease !== undefined && passiveObservationGroups.get(group.lease as object) === group)
    passiveObservationGroups.delete(group.lease as object);
  group.observation?.discard();
}

function removeRuntimeStop<T>(stops: T[], stop: T): void {
  const index = stops.indexOf(stop);
  if (index >= 0) stops.splice(index, 1);
}

function cleanupDetachedPassiveStore(runtime: StoreRuntime<unknown>): void {
  if (runtime.disposed) return;
  noteTransferChange(runtime);
  const failures: unknown[] = [];
  for (const stop of runtime.readStops.splice(0)) {
    try {
      stop();
    } catch (error) {
      failures.push(error);
    }
  }
  for (const stop of runtime.livenessStops.splice(0)) {
    try {
      stop();
    } catch (error) {
      failures.push(error);
    }
  }
  if (runtime.queryProgressCell !== undefined) {
    try {
      retireQueryProgress(runtime);
    } catch (error) {
      failures.push(error);
    }
  }
  runtimeByStore.delete(runtime.store);
  runtime.disposed = true;
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, "Passive Store cleanup failed");
}

function createPassiveQueryView(
  runtime: StoreRuntime<unknown>,
  caller: QueryCaller<unknown>,
  progressSource: Readable<unknown>,
  isPublished: () => boolean,
): {
  readonly store: QueryStore<unknown, unknown>;
  readonly readStops: readonly Unsubscribe[];
  readonly facets: readonly PassiveNativeFacet[];
  readonly observations: readonly {
    readonly readable: Readable<unknown>;
    readonly observation: {
      readonly isValue?: boolean;
      readonly armsFreshness?: boolean;
      readonly metadata?: boolean;
    };
  }[];
} {
  const readStops: Unsubscribe[] = [];
  const facets: PassiveNativeFacet[] = [];
  const observations: {
    readonly readable: Readable<unknown>;
    readonly observation: {
      readonly isValue?: boolean;
      readonly armsFreshness?: boolean;
      readonly metadata?: boolean;
    };
  }[] = [];
  const report = (fault: Fault): void => graphFault(runtime.graph.__runtime, fault);
  const passiveReadable = <T>(
    source: Readable<T>,
    name: string,
    role: string,
    observation?: {
      readonly isValue?: boolean;
      readonly armsFreshness?: boolean;
      readonly metadata?: boolean;
    },
    caller?: QueryCaller<unknown>,
  ): Readable<T> => {
    const readable = computed(
      () => {
        if (!isPublished()) throw new TypeError("The passive query Store was not published");
        assertStoreOpen(runtime);
        return source.get();
      },
      { label: "query:" + String(runtime.key) + ":passive-" + name },
    );
    reactiveInternal.bindReadable(readable, {
      graphId: runtime.graph.id,
      reportFault: report,
    });
    const guard = reactiveInternal.onActualRead(readable, () => {
      if (!isPublished()) throw new TypeError("The passive query Store is still dormant");
      assertStoreOpen(runtime);
    });
    readStops.push(guard);
    registerTransferReadable(readable, runtime, role, caller);
    facets.push({ target: readable, source });
    if (observation !== undefined) observations.push({ readable, observation });
    return readable;
  };

  const value = passiveReadable(
    runtime.value,
    "value",
    "passive-value",
    {
      isValue: true,
      armsFreshness: true,
    },
    caller,
  );
  const available = passiveReadable(
    runtime.store.available,
    "available",
    "passive-available",
    {
      metadata: true,
    },
    caller,
  );
  const error = passiveReadable(
    runtime.error,
    "error",
    "passive-error",
    { armsFreshness: true },
    caller,
  );
  const status = passiveReadable(
    caller.status,
    "status",
    "passive-query-status",
    {
      armsFreshness: true,
    },
    caller,
  );
  const pending = passiveReadable(
    caller.pending,
    "pending",
    "passive-query-pending",
    {
      armsFreshness: true,
    },
    caller,
  );
  const progress = passiveReadable(
    progressSource,
    "progress",
    "passive-query-progress",
    undefined,
    caller,
  );

  let handle!: QueryStore<unknown, unknown>;
  const assertPublished = (): void => {
    assertStoreHandlePublished(handle);
    assertStoreOpen(runtime);
  };
  const revalidate = (options?: { readonly abortSignal?: AbortSignal }): Promise<void> => {
    assertPublished();
    if (options !== undefined && !isCancellationOptions(options))
      throw new TypeError("Invalid query revalidation options.");
    nextGeneration(runtime.generation);
    addCaller(runtime, caller);
    return startQuery(runtime, caller, true, options?.abortSignal ?? caller.abortSignal);
  };
  handle = Object.create(null) as QueryStore<unknown, unknown>;
  Object.defineProperties(handle, {
    identity: { configurable: false, enumerable: true, value: runtime.identity },
    key: { configurable: false, enumerable: true, value: runtime.key },
    graph: { configurable: false, enumerable: true, value: runtime.graph.id },
    value: { configurable: false, enumerable: true, value },
    available: { configurable: false, enumerable: true, value: available },
    set: {
      configurable: false,
      enumerable: true,
      value: (next: unknown): void => {
        assertPublished();
        runtime.store.set(next);
      },
    },
    update: {
      configurable: false,
      enumerable: true,
      value: (next: (current: unknown) => unknown): void => {
        assertPublished();
        runtime.store.update(next);
      },
    },
    ready: {
      configurable: false,
      enumerable: true,
      get: () => {
        assertPublished();
        if (queryIsStale(caller) && isCurrentRequest(runtime) === undefined)
          void startQuery(runtime, caller, false, caller.abortSignal, runtime.failing);
        return ready(runtime, caller);
      },
    },
    pending: { configurable: false, enumerable: true, value: pending },
    error: { configurable: false, enumerable: true, value: error },
    status: { configurable: false, enumerable: true, value: status },
    progress: { configurable: false, enumerable: true, value: progress },
    revalidate: { configurable: false, enumerable: true, value: revalidate },
    [storeSubscribe]: {
      configurable: false,
      enumerable: false,
      value: (observer: (value: unknown) => void): Unsubscribe => {
        assertPublished();
        const subscribeStore = (runtime.store as unknown as Record<PropertyKey, unknown>)[
          storeSubscribe
        ];
        if (typeof subscribeStore !== "function")
          throw new TypeError("The native Store has no subscription owner");
        return (subscribeStore as (observer: (value: unknown) => void) => Unsubscribe)(observer);
      },
    },
  });
  Object.freeze(handle);
  return { store: handle, readStops, observations, facets };
}

interface PassiveReadableObservation {
  readonly readable: Readable<unknown>;
  readonly observation: {
    readonly isValue?: boolean;
    readonly armsFreshness?: boolean;
    readonly metadata?: boolean;
  };
}

interface PassiveStoreShell {
  readonly store: Store<unknown>;
  readonly handle: Record<PropertyKey, unknown>;
  readonly readStops: Unsubscribe[];
  readonly observations: PassiveReadableObservation[];
  readonly facets: PassiveNativeFacet[];
  freeze(): void;
}

function createPassiveStoreShell(
  runtime: StoreRuntime<unknown>,
  isPublished: () => boolean,
  valueRole: string,
  availableRole: string,
): PassiveStoreShell {
  const readStops: Unsubscribe[] = [];
  const observations: PassiveReadableObservation[] = [];
  const facets: PassiveNativeFacet[] = [];
  const report = (fault: Fault): void => graphFault(runtime.graph.__runtime, fault);
  const passiveReadable = <T>(
    source: Readable<T>,
    name: string,
    role: string,
    observation?: PassiveReadableObservation["observation"],
  ): Readable<T> => {
    const readable = computed(
      () => {
        if (!isPublished()) throw new TypeError("The passive Store was not published");
        assertStoreOpen(runtime);
        return source.get();
      },
      {
        label: "store:" + String(runtime.key) + ":passive-" + name,
      },
    );
    reactiveInternal.bindReadable(readable, { graphId: runtime.graph.id, reportFault: report });
    const guard = reactiveInternal.onActualRead(readable, () => {
      if (!isPublished()) throw new TypeError("The passive Store is still dormant");
      assertStoreOpen(runtime);
    });
    readStops.push(guard);
    registerTransferReadable(readable, runtime, role);
    facets.push({ target: readable, source });
    if (observation !== undefined) observations.push({ readable, observation });
    return readable;
  };
  const value = passiveReadable(runtime.value, "value", valueRole, {
    isValue: true,
    armsFreshness: true,
  });
  const available = passiveReadable(runtime.store.available, "available", availableRole, {
    metadata: true,
  });
  const handle = Object.create(null) as Record<PropertyKey, unknown>;
  const assertPublished = (): void => {
    assertStoreHandlePublished(handle);
    assertStoreOpen(runtime);
  };
  Object.defineProperties(handle, {
    identity: { configurable: false, enumerable: true, value: runtime.identity },
    key: { configurable: false, enumerable: true, value: runtime.key },
    graph: { configurable: false, enumerable: true, value: runtime.graph.id },
    value: { configurable: false, enumerable: true, value },
    available: { configurable: false, enumerable: true, value: available },
    set: {
      configurable: false,
      enumerable: true,
      value: (next: unknown): void => {
        assertPublished();
        runtime.store.set(next);
      },
    },
    update: {
      configurable: false,
      enumerable: true,
      value: (next: (current: unknown) => unknown): void => {
        assertPublished();
        runtime.store.update(next);
      },
    },
    [storeSubscribe]: {
      configurable: false,
      enumerable: false,
      value: (observer: (value: unknown) => void): Unsubscribe => {
        assertPublished();
        const subscribeStore = (runtime.store as unknown as Record<PropertyKey, unknown>)[
          storeSubscribe
        ];
        if (typeof subscribeStore !== "function")
          throw new TypeError("The native Store has no subscription owner");
        return (subscribeStore as (observer: (value: unknown) => void) => Unsubscribe)(observer);
      },
    },
  });
  return {
    store: handle as unknown as Store<unknown>,
    handle,
    readStops,
    observations,
    facets,
    freeze: () => Object.freeze(handle),
  };
}
function createPassiveStreamView(
  runtime: StoreRuntime<unknown>,
  stream: NativeStreamDefinition<unknown, unknown, any, any, any>,
  isPublished: () => boolean,
): PassiveStoreShell {
  const shell = createPassiveStoreShell(
    runtime,
    isPublished,
    "passive-stream-value",
    "passive-stream-available",
  );
  const report = (fault: Fault): void => graphFault(runtime.graph.__runtime, fault);
  const passiveReadable = <T>(
    source: Readable<T>,
    name: string,
    role: string,
    observation?: PassiveReadableObservation["observation"],
  ): Readable<T> => {
    const readable = computed(
      () => {
        if (!isPublished()) throw new TypeError("The passive stream Store was not published");
        assertStoreOpen(runtime);
        return source.get();
      },
      {
        label: "stream:" + stream.name + ":" + String(runtime.key) + ":passive-" + name,
      },
    );
    reactiveInternal.bindReadable(readable, { graphId: runtime.graph.id, reportFault: report });
    const guard = reactiveInternal.onActualRead(readable, () => {
      if (!isPublished()) throw new TypeError("The passive stream Store is still dormant");
      assertStoreOpen(runtime);
    });
    shell.readStops.push(guard);
    registerTransferReadable(readable, runtime, role);
    shell.facets.push({ target: readable, source });
    if (observation !== undefined) shell.observations.push({ readable, observation });
    return readable;
  };
  const status = passiveReadable(runtime.streamStatus, "status", "passive-stream-status", {
    armsFreshness: true,
  });
  const pending = computed(
    () => {
      if (!isPublished()) throw new TypeError("The passive stream Store was not published");
      assertStoreOpen(runtime);
      return runtime.streamStatus.get() === "opening";
    },
    { label: "stream:" + stream.name + ":" + String(runtime.key) + ":passive-pending" },
  );
  reactiveInternal.bindReadable(pending, { graphId: runtime.graph.id, reportFault: report });
  const pendingGuard = reactiveInternal.onActualRead(pending, () => {
    if (!isPublished()) throw new TypeError("The passive stream Store is still dormant");
    assertStoreOpen(runtime);
  });
  shell.readStops.push(pendingGuard);
  registerTransferReadable(pending, runtime, "passive-stream-pending");
  shell.facets.push({ target: pending, source: runtime.streamStatus });
  shell.observations.push({ readable: pending, observation: { armsFreshness: true } });
  const error = passiveReadable(runtime.error, "error", "passive-stream-error");
  Object.defineProperties(shell.handle, {
    pending: { configurable: false, enumerable: true, value: pending },
    error: { configurable: false, enumerable: true, value: error },
    status: { configurable: false, enumerable: true, value: status },
  });
  shell.freeze();
  return shell;
}
interface PassiveClaimRetirement {
  readonly promise: Promise<void>;
  resolve(): void;
  reject(cause: unknown): void;
}

function passiveClaimRetirement(): PassiveClaimRetirement {
  const completion = createPhysicalCompletion<void>();
  return {
    promise: completion.promise,
    resolve: () => completion.resolve(undefined),
    reject: completion.reject,
  };
}

function createPassiveQueryRecord(args: {
  readonly state: GraphRuntimeState<any>;
  readonly binding?: QueryBindingRuntime;
  readonly key: StoreKey;
  readonly runtime: StoreRuntime<unknown>;
  readonly runtimeReservation?: PassiveRuntimeReservation;
  readonly store: Store<unknown>;
  readonly caller?: QueryCaller<unknown>;
  readonly progress?: PreparedQueryProgressCell;
  readonly incarnationBefore: object | undefined;
  readonly incarnationAfter: object;
  readonly droppedBefore: boolean;
  readonly detached: boolean;
  readonly graphStoresBefore: GraphRuntimeState<any>["stores"];
  readonly bindingStoresBefore?: QueryBindingRuntime["stores"];
  readonly callerRowsBefore: StoreRuntime<unknown>["callers"];
  readonly graphEntryBefore: StoreRuntime<unknown> | undefined;
  readonly bindingEntryBefore: StoreRuntime<unknown> | undefined;
  readonly addressRecord?: ReturnType<typeof bindingAddress>;
  readonly addressGuard?: object;
  readonly resetEpoch: number;
  readonly graphCurrent: () => boolean;
  readonly bindingCurrent: () => boolean;
  readonly runtimeCurrent: () => boolean;
  readonly configuration?: PassiveQueryConfiguration;
  readonly policy: PassiveQueryPolicy;
  readonly sourceBefore: StoreRuntime<unknown>["source"];
  readonly streamSourceBefore: StoreRuntime<unknown>["streamSource"];
  readonly activationBefore: StoreRuntime<unknown>["onReadableActivation"];
  readonly sourceAfter?: NonNullable<StoreRuntime<unknown>["source"]>;
  readonly streamSourceAfter?: NonNullable<StoreRuntime<unknown>["streamSource"]>;
  readonly activationAfter?: () => void;
  readonly readStops: readonly Unsubscribe[];
  readonly livenessStops: readonly Unsubscribe[];
  readonly facets: readonly PassiveNativeFacet[];
  readonly observations: readonly {
    readonly readable: Readable<unknown>;
    readonly observation: {
      readonly isValue?: boolean;
      readonly armsFreshness?: boolean;
      readonly metadata?: boolean;
    };
  }[];
  readonly isPublished: () => boolean;
  readonly setPublished: () => void;
  readonly retirement: PassiveClaimRetirement;
  readonly releasePrepared: () => void;
}): PreparedNativePassiveTransferClaim {
  const record: PassiveQueryClaimRecord = {
    state: args.state,
    binding: args.binding,
    key: args.key,
    runtime: args.runtime,
    runtimeReservation: args.runtimeReservation,
    store: args.store,
    caller: args.caller,
    progress: args.progress,
    incarnationBefore: args.incarnationBefore,
    incarnationAfter: args.incarnationAfter,
    droppedBefore: args.droppedBefore,
    detached: args.detached,
    graphStoresBefore: args.graphStoresBefore,
    bindingStoresBefore: args.bindingStoresBefore,
    callerRowsBefore: args.callerRowsBefore,
    graphEntryBefore: args.graphEntryBefore,
    bindingEntryBefore: args.bindingEntryBefore,
    addressRecord: args.addressRecord,
    addressGuard: args.addressGuard,
    resetEpoch: args.resetEpoch,
    graphCurrent: args.graphCurrent,
    bindingCurrent: args.bindingCurrent,
    runtimeCurrent: args.runtimeCurrent,
    configuration: args.configuration,
    policy: args.policy,
    sourceBefore: args.sourceBefore,
    streamSourceBefore: args.streamSourceBefore,
    activationBefore: args.activationBefore,
    sourceAfter: args.sourceAfter,
    streamSourceAfter: args.streamSourceAfter,
    activationAfter: args.activationAfter,
    claimReadStops: args.readStops,
    claimLivenessStops: args.livenessStops,
    claimObservations: args.observations,
    facets: args.facets,
    retired: args.retirement.promise,
    resolveRetired: args.retirement.resolve,
    rejectRetired: args.retirement.reject,
    releasePrepared: args.releasePrepared,
    published: false,
    discarded: false,
    inAggregate: false,
    observationPrepared: false,
    current: () => passiveQueryCurrent(record),
    setPublished: () => {
      record.published = true;
      args.setPublished();
    },
    discard: () => {
      if (record.published || record.discarded) return;
      record.discarded = true;
      const failures: unknown[] = [];
      for (const stop of record.claimReadStops) {
        try {
          stop();
        } catch (cause) {
          failures.push(cause);
        } finally {
          removeRuntimeStop(record.runtime.readStops, stop);
        }
      }
      for (const stop of record.claimLivenessStops) {
        try {
          stop();
        } catch (cause) {
          failures.push(cause);
        } finally {
          removeRuntimeStop(record.runtime.livenessStops, stop);
        }
      }
      if (record.observationGroup !== undefined) {
        try {
          releasePassiveObservationDemand(record);
        } catch (cause) {
          failures.push(cause);
        }
      } else if (record.observationPrepared && record.observation !== undefined) {
        try {
          record.observation.discard();
        } catch (cause) {
          failures.push(cause);
        }
      }

      try {
        record.releasePrepared();
      } catch (cause) {
        failures.push(cause);
      }
      if (record.detached && record.runtimeReservation === undefined) {
        try {
          cleanupDetachedPassiveStore(record.runtime);
        } catch (cause) {
          failures.push(cause);
        }
      }
      if (failures.length === 0) {
        record.resolveRetired();
        return;
      }
      const cause =
        failures.length === 1
          ? failures[0]
          : new AggregateError(failures, "Passive claim cleanup failed");
      record.rejectRetired(cause);
      throw cause;
    },
  };
  const prepareObservation = (
    existingLease?: ObservationLease<unknown>,
  ): PreparedObservationTransfer<unknown> => {
    if (record.caller === undefined)
      throw new TypeError("Only query claims can prepare an observation");
    if (record.observationPrepared) {
      if (record.observationLease !== existingLease && record.observation?.lease !== existingLease)
        throw new TypeError("A passive query claim already has a different observation lease");
      if (record.observation === undefined)
        throw new TypeError("The query observation participant was not prepared");
      return record.observation;
    }
    if (record.inAggregate || !passiveQueryBaseCurrent(record))
      throw new TypeError("Passive query claim is no longer current for observation");
    if (existingLease !== undefined) {
      const pendingGroup = passiveObservationGroups.get(existingLease as object);
      if (pendingGroup !== undefined) {
        if (
          pendingGroup.lease !== existingLease ||
          pendingGroup.runtime !== record.runtime ||
          pendingGroup.incarnation !== record.incarnationAfter ||
          pendingGroup.inAggregate ||
          pendingGroup.discarded ||
          pendingGroup.published ||
          pendingGroup.observation === undefined ||
          !pendingGroup.observation.current()
        )
          throw new TypeError("The pending observation lease belongs to another native cohort");
        const demand: PassiveObservationDemand = { record, active: true };
        pendingGroup.demands.push(demand);
        record.observationGroup = pendingGroup;
        record.observationDemand = demand;
        record.observation = pendingGroup.observation;
        record.observationLease = existingLease;
        record.observationPrepared = true;
        return pendingGroup.observation;
      }
      const observation = prepareObservationTransfer(
        record.runtime,
        record.incarnationAfter,
        () => passiveQueryBaseCurrent(record),
        existingLease,
      );
      record.observation = observation;
      record.observationLease = existingLease;
      record.observationPrepared = true;
      return observation;
    }
    const group: PassiveObservationGroup = {
      runtime: record.runtime,
      incarnation: record.incarnationAfter,
      demands: [],
      inAggregate: false,
      published: false,
      discarded: false,
    };
    const demand: PassiveObservationDemand = { record, active: true };
    group.demands.push(demand);
    const current = (): boolean => {
      if (group.discarded || group.published) return false;
      let active = false;
      for (let index = 0; index < group.demands.length; index += 1) {
        const candidate = group.demands[index]!;
        if (!candidate.active) continue;
        active = true;
        if (!passiveQueryBaseCurrent(candidate.record)) return false;
      }
      return active;
    };
    const observation = prepareObservationTransfer(
      record.runtime,
      record.incarnationAfter,
      current,
    );
    group.observation = observation;
    group.lease = observation.lease;
    passiveObservationGroups.set(observation.lease as object, group);
    record.observationGroup = group;
    record.observationDemand = demand;
    record.observation = observation;
    record.observationPrepared = true;
    return observation;
  };
  const claim: PreparedNativePassiveTransferClaim = Object.freeze({
    kind:
      args.caller !== undefined
        ? "query"
        : args.streamSourceAfter !== undefined
          ? "stream"
          : "store",
    runtime: args.runtime,
    policy: args.policy,
    compare: args.configuration?.compare,
    incarnation: args.incarnationAfter,
    store: args.store,
    caller: args.caller,
    progressCell: args.progress?.cell,
    facets: args.facets,
    existing: !args.detached,
    prepareObservation,
    observation() {
      return record.observation;
    },
    current: record.current,
    discard: record.discard,
    retired: args.retirement.promise,
  });
  passiveQueryClaims.set(claim, record);
  return claim;
}

type PassiveValueOptions = Pick<
  ValueOptions<unknown>,
  "abortSignal" | "initial" | "equals" | "snapshot"
>;
type PassiveQueryOptions = Pick<
  StoreOptions<unknown>,
  "abortSignal" | "initial" | "equals" | "snapshot" | "revalidateAfterMs"
>;

function copyPassiveQueryOptions(
  options: StoreOptions<unknown> | undefined,
): PassiveQueryOptions | undefined {
  if (options === undefined) return undefined;
  if (!isStoreOptions(options)) throw new TypeError("Invalid passive native query options");
  return {
    ...(Object.hasOwn(options, "abortSignal") ? { abortSignal: options.abortSignal } : {}),
    ...(Object.hasOwn(options, "initial") ? { initial: options.initial } : {}),
    ...(Object.hasOwn(options, "equals") ? { equals: options.equals } : {}),
    ...(Object.hasOwn(options, "snapshot") ? { snapshot: options.snapshot } : {}),
    ...(Object.hasOwn(options, "revalidateAfterMs")
      ? { revalidateAfterMs: options.revalidateAfterMs }
      : {}),
  };
}

function copyPassiveValueOptions(
  options: ValueOptions<unknown> | undefined,
): PassiveValueOptions | undefined {
  if (options === undefined) return undefined;
  if (!isValueOptions(options)) throw new TypeError("Invalid passive native value options");
  return {
    ...(Object.hasOwn(options, "abortSignal") ? { abortSignal: options.abortSignal } : {}),
    ...(Object.hasOwn(options, "initial") ? { initial: options.initial } : {}),
    ...(Object.hasOwn(options, "equals") ? { equals: options.equals } : {}),
    ...(Object.hasOwn(options, "snapshot") ? { snapshot: options.snapshot } : {}),
  };
}

function createPassiveNativeRecord(
  args: Parameters<typeof createPassiveQueryRecord>[0],
): PreparedNativePassiveTransferClaim {
  return createPassiveQueryRecord(args);
}

function prepareNativeStoreTransferClaim(
  graph: Graph,
  source: object,
  info: TransferSourceRegistration,
  input: unknown,
  options: ValueOptions<unknown> | undefined,
): PreparedNativePassiveTransferClaim {
  if (transferSource(source) !== info) throw new TypeError("Raw Store source registration changed");
  if (input !== undefined || options !== undefined)
    throw new TypeError("Raw Store claims do not accept input or value options");
  const state = transferGraph(graph);
  if (state === undefined || state.graph !== graph || info.graph !== graph || info.kind !== "store")
    throw new TypeError("Raw Store claims require the genuine source on this graph");
  assertGraphOpen(state);
  const definition = info.definition as StoreDefinition<unknown>;
  const resetEpoch = state.resetEpoch;
  const graphEpoch = transferEpochCurrent(state);
  const admission = beginTransferAdmission(state, definition, undefined, input);
  let runtime: StoreRuntime<unknown> | undefined;
  let detached = false;
  let shell: PassiveStoreShell | undefined;
  let configuration: PassiveQueryConfiguration | undefined;
  const preparedReleases: (() => void)[] = [];
  const releasePrepared = (): void => releasePreparedPassiveRows(preparedReleases);
  let published = false;
  const isPublished = (): boolean => published;
  const setPublished = (): void => {
    published = true;
  };
  try {
    const key = storeKey(definition.name);
    if (state.declaredStores.get(String(key)) !== definition)
      throw new TypeError("Raw Store definition is not declared on this graph");
    const storesBefore = state.stores;
    const graphEntryBefore = state.stores.get(String(key)) as StoreRuntime<unknown> | undefined;
    runtime = graphEntryBefore;
    if (runtime !== undefined) {
      if (runtime.binding !== undefined || runtime.graph !== state.graph || runtime.disposed)
        throw new TypeError("Raw Store address is owned by another native binding");
      inheritTransferCoverage(runtime);
      if (!runtime.configured) {
        configuration = preparePassiveDeclaredStoreConfiguration(runtime, definition);
        if (!configuration.current())
          throw new TypeError("Declared Store policy changed during passive preparation");
      }
    } else {
      runtime = makeStore(state, key, definition, undefined, true);
      detached = true;
      inheritTransferCoverage(runtime);
    }
    if (!transferStoreAdmissible(runtime))
      throw new TypeError("Raw Store target is not available for passive transfer");
    const incarnationBefore = runtime.incarnation;
    const droppedBefore = runtime.dropped;
    const incarnationReservation = detached
      ? { value: runtime.incarnation!, release: () => undefined }
      : reservePassiveIncarnation(runtime);
    preparedReleases.push(incarnationReservation.release);
    const incarnationAfter = incarnationReservation.value;
    const callersBefore = runtime.callers;
    const sourceBefore = runtime.source;
    const streamSourceBefore = runtime.streamSource;
    const activationBefore = runtime.onReadableActivation;
    const runtimeCurrent = transferEpochCurrent(runtime);
    shell = createPassiveStoreShell(runtime, isPublished, "passive-value", "passive-available");
    const retirement = passiveClaimRetirement();
    shell.freeze();
    if (transferSource(source) !== info)
      throw new TypeError("Stream source registration changed during passive preparation");
    registerTransferStoreHandle(shell.store, runtime, info);
    registerDormantStoreHandle(shell.store, runtime, isPublished, false);
    return createPassiveNativeRecord({
      state,
      key,
      runtime,
      store: shell.store,
      incarnationBefore,
      incarnationAfter,
      droppedBefore,
      detached,
      graphStoresBefore: storesBefore,
      callerRowsBefore: callersBefore,
      graphEntryBefore,
      bindingEntryBefore: undefined,
      resetEpoch,
      graphCurrent: () =>
        graphEpoch() && state.resetEpoch === resetEpoch && state.stores === storesBefore,
      bindingCurrent: () => true,
      runtimeCurrent,
      configuration,
      policy: configuration?.after ?? passiveQueryPolicy(runtime),
      sourceBefore,
      streamSourceBefore,
      activationBefore,
      readStops: shell.readStops,
      livenessStops: [],
      facets: shell.facets,
      observations: shell.observations,
      isPublished,
      setPublished,
      retirement,
      releasePrepared,
    });
  } catch (error) {
    if (shell !== undefined)
      for (const stop of shell.readStops) {
        try {
          stop();
        } catch (cleanupError) {
          reportContinuationError(state, cleanupError);
        }
      }
    if (shell !== undefined)
      for (const facet of shell.facets) unregisterTransferReadable(facet.target, runtime!);
    try {
      releasePrepared();
    } catch (cleanupError) {
      reportContinuationError(state, cleanupError);
    }
    if (detached && runtime !== undefined) {
      try {
        cleanupDetachedPassiveStore(runtime);
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
    }
    throw error;
  } finally {
    finishTransferAdmission(admission, runtime);
  }
}

function prepareNativeStreamTransferClaim(
  graph: Graph,
  source: object,
  info: TransferSourceRegistration,
  input: unknown,
  options: ValueOptions<unknown> | undefined,
): PreparedNativePassiveTransferClaim {
  if (info.binding !== undefined)
    throw new TypeError("Bound stream claims require a native BoundStreamSession reservation");
  const state = transferGraph(graph);
  if (
    state === undefined ||
    state.graph !== graph ||
    info.graph !== graph ||
    info.kind !== "stream"
  )
    throw new TypeError("Stream claims require the genuine source on this graph");
  assertGraphOpen(state);
  const stream = info.definition as NativeStreamDefinition<unknown, unknown, any, any, any>;
  if (stream.kind === "positioned-stream")
    throw new TypeError("Positioned stream claims require an admitted reader session");
  const resetEpoch = state.resetEpoch;
  const graphEpoch = transferEpochCurrent(state);
  const admission = beginTransferAdmission(state, stream, undefined, input);
  let runtime: StoreRuntime<unknown> | undefined;
  let detached = false;
  let shell: PassiveStoreShell | undefined;
  const preparedReleases: (() => void)[] = [];
  const releasePrepared = (): void => releasePreparedPassiveRows(preparedReleases);
  let published = false;
  const isPublished = (): boolean => published;
  const setPublished = (): void => {
    published = true;
  };
  try {
    const keyForInput = stream.key;
    const key = storeKey(Reflect.apply(keyForInput, stream, [input]));
    if (transferSource(source) !== info)
      throw new TypeError("Stream source registration changed during passive preparation");
    assertGraphOpen(state);
    if (state.resetEpoch !== resetEpoch) throw new TypeError("Stream input crossed a graph reset");
    const storesBefore = state.stores;
    const graphEntryBefore = state.stores.get(String(key)) as StoreRuntime<unknown> | undefined;
    runtime = graphEntryBefore;
    if (runtime !== undefined) {
      if (runtime.binding !== undefined || runtime.graph !== state.graph || runtime.disposed)
        throw new TypeError("Stream address is owned by another native binding");
      if (runtime.readerScope !== undefined)
        throw new TypeError("Positioned reader Store cannot join an ordinary stream claim");
      inheritTransferCoverage(runtime);
    } else {
      runtime = makeStore(state, key, undefined);
      detached = true;
      inheritTransferCoverage(runtime);
      admitDetachedStore(runtime);
    }
    if (!transferStoreAdmissible(runtime))
      throw new TypeError("Stream target is not available for passive transfer");
    const runtimeBeforeConfiguration = transferEpochCurrent(runtime);
    const selectedOptions = copyPassiveValueOptions(options);
    if (!runtimeBeforeConfiguration())
      throw new TypeError("Stream target changed during passive option preparation");
    const configuration = preparePassiveQueryConfiguration(runtime, stream, selectedOptions);
    if (!configuration.current())
      throw new TypeError("Stream target changed during passive configuration validation");

    const beforeSource = runtime.streamSource;
    const activeSession = runtime.stream !== undefined && !runtime.stream.ended;
    if (
      activeSession &&
      (beforeSource?.stream !== stream ||
        !Object.is(beforeSource.input, input) ||
        beforeSource.member !== source)
    )
      throw new TypeError("Active stream session belongs to another native source or input");
    const streamSourceAfter = activeSession
      ? beforeSource!
      : {
          stream,
          input,
          member: source,
          ...(selectedOptions?.abortSignal === undefined
            ? {}
            : { abortSignal: selectedOptions.abortSignal }),
        };
    if (detached) configuration.publish();
    const preparedRuntimeCurrent = transferEpochCurrent(runtime);
    const incarnationBefore = runtime.incarnation;
    const droppedBefore = runtime.dropped;
    const incarnationReservation = detached
      ? { value: runtime.incarnation!, release: () => undefined }
      : reservePassiveIncarnation(runtime);
    preparedReleases.push(incarnationReservation.release);
    const incarnationAfter = incarnationReservation.value;
    const callersBefore = runtime.callers;
    const sourceBefore = runtime.source;
    const activationBefore = runtime.onReadableActivation;
    const activationAfter = activeSession
      ? activationBefore
      : createStreamRuntimeActivation(
          runtime,
          stream,
          input,
          source,
          selectedOptions?.abortSignal,
          streamSourceAfter,
        );
    shell = createPassiveStreamView(runtime, stream, isPublished);
    if (!preparedRuntimeCurrent())
      throw new TypeError("Stream target changed during passive view preparation");

    const retirement = passiveClaimRetirement();
    registerTransferStoreHandle(shell.store, runtime, info);
    registerDormantStoreHandle(shell.store, runtime, isPublished, false);
    return createPassiveNativeRecord({
      state,
      key,
      runtime,
      store: shell.store,
      incarnationBefore,
      incarnationAfter,
      droppedBefore,
      detached,
      graphStoresBefore: storesBefore,
      callerRowsBefore: callersBefore,
      graphEntryBefore,
      bindingEntryBefore: undefined,
      resetEpoch,
      graphCurrent: () =>
        graphEpoch() && state.resetEpoch === resetEpoch && state.stores === storesBefore,
      bindingCurrent: () => true,
      runtimeCurrent: transferEpochCurrent(runtime),
      configuration,
      policy: configuration.after,
      sourceBefore,
      streamSourceBefore: beforeSource,
      activationBefore,
      streamSourceAfter,
      activationAfter,
      readStops: shell.readStops,
      livenessStops: [],
      facets: shell.facets,
      observations: shell.observations,
      isPublished,
      setPublished,
      retirement,
      releasePrepared,
    });
  } catch (error) {
    if (shell !== undefined)
      for (const stop of shell.readStops) {
        try {
          stop();
        } catch (cleanupError) {
          reportContinuationError(state, cleanupError);
        }
      }
    if (shell !== undefined)
      for (const facet of shell.facets) unregisterTransferReadable(facet.target, runtime!);
    try {
      releasePrepared();
    } catch (cleanupError) {
      reportContinuationError(state, cleanupError);
    }
    if (detached && runtime !== undefined) {
      try {
        cleanupDetachedPassiveStore(runtime);
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
    }
    throw error;
  } finally {
    finishTransferAdmission(admission, runtime);
  }
}
/**
 * Prepares a genuine query, declared Store, or raw unpositioned stream target without invoking its public member.
 * The returned Store facade refuses body reads and operations until its aggregate publishes.
 * Query callers, configured policy, progress ownership, and stream activation are staged with the target.
 * @param graph - Exact native graph that registered the source.
 * @param source - Genuine raw member or bound query handle.
 * @param input - Already-prepared input retained by the source.
 * @param options - Query caller policy and freshness, or Store/stream value policy retained for native work.
 * @returns A dormant native Store claim for one exact source address.
 * @throws TypeError if the source, address, binding, policy, or stream session is unsupported or no longer current.
 */
export function prepareNativePassiveTransferClaim(
  graph: Graph,
  source: object,
  input: unknown,
  options?: StoreOptions<unknown>,
  positioned?: {
    readonly policies: readonly ProjectionPolicy<any, any>[];
    readonly seed: PositionedStoreRestoreSeed<any, any, readonly unknown[]>;
    readonly reader: unknown;
  },
): PreparedNativePassiveTransferClaim {
  const info = transferSource(source);
  if (info === undefined || info.graph !== graph)
    throw new TypeError("Passive transfer claims require a genuine source on this graph");
  if (positioned !== undefined) {
    if (
      info.kind !== "stream" ||
      (info.definition as NativeStreamDefinition<any, any, any, any, any>).kind !==
        "positioned-stream"
    )
      throw new TypeError("Positioned preparation requires its genuine positioned source");
    return preparePositionedPassiveClaim(source, input, options, positioned);
  }
  if (info.kind === "store")
    return prepareNativeStoreTransferClaim(graph, source, info, input, options);
  if (info.kind === "stream")
    return prepareNativeStreamTransferClaim(graph, source, info, input, options);
  if (info.kind === "mutation")
    throw new TypeError("Mutation sources require a joined native dispatch, not a passive claim");
  if (info.kind !== "query")
    throw new TypeError(
      "Passive transfer claims require a genuine query, Store, or raw stream source",
    );
  const state = transferGraph(graph);
  if (state === undefined || state.graph !== graph)
    throw new TypeError("Passive transfer claims require a genuine graph");
  assertGraphOpen(state);
  const binding = info.binding === undefined ? undefined : (info.binding as QueryBindingRuntime);
  if (
    binding !== undefined &&
    (binding.kind !== "query" ||
      binding.context.state !== state ||
      binding.definition !== info.definition)
  )
    throw new TypeError("Passive transfer source has a crossed query binding");
  const definition = info.definition as QueryDefinition<unknown, unknown, any, any>;
  if (!state.queryDefinitions.has(definition))
    throw new TypeError("Passive transfer source is not registered on this graph");
  const query = binding?.execution ?? definition;
  const resetEpoch = state.resetEpoch;
  const graphEpoch = transferEpochCurrent(state);
  const bindingEpoch = binding === undefined ? () => true : transferEpochCurrent(binding);
  const contextEpoch = binding === undefined ? () => true : transferEpochCurrent(binding.context);
  let admission: TransferAdmission | undefined = beginTransferAdmission(
    state,
    definition,
    binding,
    input,
  );
  let runtime: StoreRuntime<unknown> | undefined;
  let detached = false;
  let passiveView: ReturnType<typeof createPassiveQueryView> | undefined;
  const preparedReleases: (() => void)[] = [];
  const releasePrepared = (): void => releasePreparedPassiveRows(preparedReleases);
  let callerForCleanup: QueryCaller<unknown> | undefined;
  const finishAdmission = (): void => {
    if (admission === undefined) return;
    const current = admission;
    admission = undefined;
    finishTransferAdmission(current, runtime);
  };
  try {
    const selectedOptions = copyPassiveQueryOptions(options);
    const window = asWindow(selectedOptions?.revalidateAfterMs, query.revalidateAfterMs);
    assertGraphOpen(state);
    if (state.resetEpoch !== resetEpoch)
      throw new AbortError("Passive query options crossed a graph reset.");
    if (binding !== undefined) assertBindingContextOpen(binding.context);
    if (!graphEpoch() || !bindingEpoch() || !contextEpoch())
      throw new TypeError("Native query source changed during option preparation");
    const key = storeKey(query.key(input));
    if (transferSource(source) !== info)
      throw new TypeError("Query source registration changed during passive preparation");
    assertGraphOpen(state);
    if (state.resetEpoch !== resetEpoch)
      throw new TypeError("Passive query input crossed a graph reset");
    if (!graphEpoch() || !bindingEpoch() || !contextEpoch())
      throw new TypeError("Native query source changed during key preparation");
    const storesBefore = state.stores;
    const bindingStoresBefore = binding?.stores;
    const graphEntryBefore =
      binding === undefined
        ? (state.stores.get(String(key)) as StoreRuntime<unknown> | undefined)
        : undefined;
    const bindingEntryBefore = binding?.stores.get(String(key));
    const addressRecord = binding?.addresses.get(String(key));
    const addressGuard = addressRecord?.guard;
    const runtimeReservation = reservePassiveQueryRuntime(state, binding, key);
    runtime = runtimeReservation.runtime;
    detached = runtimeReservation.detached;
    preparedReleases.push(runtimeReservation.release);
    if (runtime.graph !== state.graph || runtime.binding !== binding || runtime.disposed)
      throw new TypeError("Existing native Store at the passive query address is foreign");
    if (!detached) inheritTransferCoverage(runtime);
    if (
      (binding === undefined && graphEntryBefore !== (detached ? undefined : runtime)) ||
      (binding !== undefined && bindingEntryBefore !== (detached ? undefined : runtime))
    )
      throw new TypeError("A native Store appeared during passive query preparation");
    if (!transferStoreAdmissible(runtime))
      throw new TypeError("Native query target is not available for passive transfer");
    const runtimeBeforeConfiguration = transferEpochCurrent(runtime);

    if (binding !== undefined) {
      assertBindingContextOpen(binding.context);
      if (
        binding.value !== undefined &&
        Object.hasOwn(binding.value, "initial") &&
        selectedOptions !== undefined &&
        Object.hasOwn(selectedOptions, "initial") &&
        !Object.is(binding.value.initial, selectedOptions.initial)
      )
        throw new TypeError("Initial value conflicts with query binding configuration");
    }

    const incarnationBefore = runtime.incarnation;
    const droppedBefore = runtime.dropped;
    const incarnationReservation = detached
      ? { value: runtime.incarnation!, release: () => undefined }
      : reservePassiveIncarnation(runtime);
    preparedReleases.push(incarnationReservation.release);
    const incarnationAfter = incarnationReservation.value;
    const callersBefore = runtime.callers;
    const sourceBefore = runtime.source;
    const activationBefore = runtime.onReadableActivation;
    const configurationOptions =
      !runtime.configured && binding?.value !== undefined && Object.hasOwn(binding.value, "initial")
        ? { initial: binding.value.initial, ...selectedOptions }
        : selectedOptions;
    if (!runtimeBeforeConfiguration())
      throw new TypeError("Native query target changed during passive option preparation");
    const configuration = preparePassiveQueryConfiguration(runtime, query, configurationOptions);
    if (!configuration.current())
      throw new TypeError("Native query target changed during passive configuration validation");

    if (detached) configuration.publish();
    const progressReservation = reservePassiveProgressCell(runtime);
    preparedReleases.push(progressReservation.release);
    const progress = progressReservation.value;
    const preparedRuntimeCurrent = transferEpochCurrent(runtime);
    const winner = passiveQueryRuntime(state, binding, key);
    if (
      winner !== (detached ? undefined : runtime) ||
      state.stores !== storesBefore ||
      (binding !== undefined && binding.stores !== bindingStoresBefore) ||
      (binding === undefined ? state.stores.get(String(key)) : binding.stores.get(String(key))) !==
        (detached ? undefined : runtime) ||
      (binding !== undefined &&
        (binding.addresses.get(String(key)) !== addressRecord ||
          addressRecord?.guard !== addressGuard))
    )
      throw new TypeError("A physical Store won the passive query address during preparation");
    const caller = createCaller(runtime, query, input, window, selectedOptions?.abortSignal);
    callerForCleanup = caller;
    registerTransferReadable(caller.statusCell, runtime, "query-status-cell", caller);
    registerTransferReadable(caller.status, runtime, "query-status", caller);
    registerTransferReadable(caller.pending, runtime, "query-pending", caller);
    const sourceAfter = { query, input };
    if (!preparedRuntimeCurrent())
      throw new TypeError("Native query target changed during passive caller preparation");
    const activationAfter = createQueryRuntimeActivation(runtime);
    let published = false;
    const isPublished = (): boolean => published;
    const setPublished = (): void => {
      published = true;
    };
    passiveView = createPassiveQueryView(runtime, caller, progress.readable, isPublished);
    const retirement = passiveClaimRetirement();
    if (transferSource(source) !== info)
      throw new TypeError("Query source registration changed during passive preparation");
    registerTransferStoreHandle(passiveView.store, runtime, info);
    registerTransferCaller(passiveView.store, runtime, caller);
    registerDormantStoreHandle(passiveView.store, runtime, isPublished);
    finishAdmission();
    const runtimeCurrent = transferEpochCurrent(runtime);
    const policy: PassiveQueryConfiguration = {
      ...configuration,
      current: () => configuration.current(),
      publish: () => configuration.publish(),
    };
    const recordArgs = {
      state,
      binding,
      key,
      runtime,
      runtimeReservation,
      store: passiveView.store,
      caller,
      progress,
      incarnationBefore,
      incarnationAfter,
      droppedBefore,
      detached,
      graphStoresBefore: storesBefore,
      bindingStoresBefore,
      callerRowsBefore: callersBefore,
      graphEntryBefore,
      bindingEntryBefore,
      addressRecord,
      addressGuard,
      resetEpoch,
      graphCurrent: () =>
        graphEpoch() &&
        state.resetEpoch === resetEpoch &&
        state.stores === storesBefore &&
        (binding === undefined || binding.stores === bindingStoresBefore),
      bindingCurrent: () =>
        bindingEpoch() && contextEpoch() && (binding === undefined || !binding.context.disposed),
      runtimeCurrent,
      configuration: policy,
      policy: configuration.after,
      sourceBefore,
      streamSourceBefore: runtime.streamSource,
      activationBefore,
      sourceAfter,
      activationAfter,
      readStops: passiveView.readStops,
      livenessStops: [],
      facets: passiveView.facets,
      observations: [
        ...passiveView.observations,
        { readable: caller.status, observation: { armsFreshness: true } },
        { readable: caller.pending, observation: { armsFreshness: true } },
      ],
      isPublished,
      setPublished,
      retirement,
      releasePrepared,
    };
    return createPassiveQueryRecord(recordArgs);
  } catch (error) {
    if (passiveView !== undefined) {
      for (const stop of passiveView.readStops) {
        try {
          stop();
        } catch (cleanupError) {
          reportContinuationError(state, cleanupError);
        }
      }
      for (const facet of passiveView.facets) unregisterTransferReadable(facet.target, runtime!);
    }
    if (callerForCleanup !== undefined) {
      unregisterTransferReadable(callerForCleanup.statusCell, runtime!);
      unregisterTransferReadable(callerForCleanup.status, runtime!);
      unregisterTransferReadable(callerForCleanup.pending, runtime!);
    }
    try {
      releasePrepared();
    } catch (cleanupError) {
      reportContinuationError(state, cleanupError);
    }
    throw error;
  } finally {
    finishAdmission();
  }
}

interface PositionedClaimRecord {
  readonly transfer: PreparedPositionedStoreTransfer;
  inAggregate: boolean;
  published: boolean;
  discarded: boolean;
  resolve(): void;
  reject(cause: unknown): void;
}
const positionedPassiveClaims = new WeakMap<object, PositionedClaimRecord>();

function preparePositionedPassiveClaim(
  source: object,
  input: unknown,
  options: ValueOptions<unknown> | undefined,
  request: {
    readonly policies: readonly ProjectionPolicy<any, any>[];
    readonly seed: PositionedStoreRestoreSeed<any, any, readonly unknown[]>;
    readonly reader: unknown;
  },
): PreparedNativePassiveTransferClaim {
  const info = transferSource(source)!;
  const state = transferGraph(info.graph)!;
  const admission = beginTransferAdmission(state, info.definition, info.binding, input);
  let prepared: PreparedPositionedStoreTransfer | undefined;
  try {
    const policies = request.policies;
    const seed = request.seed;
    const reader = request.reader;
    const transfer = preparePositionedStoreTransfer(
      source as Parameters<typeof preparePositionedStoreTransfer>[0],
      input,
      policies,
      seed,
      reader,
      options,
    );
    prepared = transfer;
    const completion = createPhysicalCompletion<void>();
    const record: PositionedClaimRecord = {
      transfer,
      inAggregate: false,
      published: false,
      discarded: false,
      resolve: completion.resolve,
      reject: completion.reject,
    };
    const currentAt = (now: number): boolean =>
      !record.published && !record.discarded && transfer.currentAt(now);
    const discard = (): void => {
      if (record.published || record.discarded) return;
      record.discarded = true;
      try {
        transfer.discard();
        completion.resolve();
      } catch (cause) {
        completion.reject(cause);
        throw cause;
      }
    };
    const claim: PreparedNativePassiveTransferClaim = Object.freeze({
      kind: "stream",
      runtime: transfer.runtime,
      policy: passiveQueryPolicy(transfer.runtime),
      store: transfer.restoration.source,
      facets: Object.freeze([]),
      existing: false,
      incarnation: transfer.runtime.incarnation!,
      positioned: transfer,
      restoration: transfer.restoration,
      currentAt,
      current: () => currentAt(readerLifetimeNow()),
      discard,
      retired: completion.promise,
      prepareObservation: () => {
        throw new TypeError("A positioned stream has no query observation");
      },
      observation: () => undefined,
    });
    positionedPassiveClaims.set(claim, record);
    observeReaderCompletion(
      transfer.restoration.retired,
      () => {
        if (!record.published && !record.discarded) {
          record.discarded = true;
          completion.resolve();
        }
      },
      (cause) => {
        if (!record.published && !record.discarded) {
          record.discarded = true;
          completion.reject(cause);
        }
      },
    );
    return claim;
  } catch (cause) {
    if (prepared !== undefined) {
      try {
        prepared.discard();
      } catch (cleanupCause) {
        reportContinuationError(state, cleanupCause);
      }
      observeUnreturnedReaderRetirement(prepared.restoration.retired);
    }
    throw cause;
  } finally {
    finishTransferAdmission(admission, prepared?.runtime);
  }
}

/** Prepares one successor per graph, binding, caller, source, progress, and incarnation owner; publish only flips those prepared fields. */
export function prepareNativePassiveTransferClaims(
  claims: readonly PreparedNativePassiveTransferClaim[],
): PreparedNativePassiveTransferClaims {
  const records: PassiveQueryClaimRecord[] = [];
  const positionedRecords: PositionedClaimRecord[] = [];
  const seenPositioned = new Set<PositionedClaimRecord>();
  const seen = new Set<PassiveQueryClaimRecord>();
  for (const claim of claims) {
    const positioned = positionedPassiveClaims.get(claim);
    if (positioned !== undefined) {
      if (
        seenPositioned.has(positioned) ||
        positioned.inAggregate ||
        positioned.published ||
        positioned.discarded ||
        positioned.transfer.group.reader?.owned !== true ||
        !positioned.transfer.currentAt(readerLifetimeNow())
      )
        throw new TypeError("Positioned claim is not current and unique");
      seenPositioned.add(positioned);
      positionedRecords.push(positioned);
      continue;
    }
    const record = passiveQueryClaims.get(claim);
    if (record === undefined || seen.has(record) || record.inAggregate || !record.current())
      throw new TypeError("Passive transfer aggregate requires distinct current claims");
    seen.add(record);
    records.push(record);
  }

  const graphRows = new Map<
    GraphRuntimeState<any>,
    {
      readonly before: GraphRuntimeState<any>["stores"];
      readonly after: Map<string | StoreIdentity, StoreRuntime<unknown>>;
    }
  >();
  const graphBeforeEntries = new Map<
    GraphRuntimeState<any>,
    readonly (readonly [string | StoreIdentity, StoreRuntime<unknown>])[]
  >();
  const bindingBeforeEntries = new Map<
    QueryBindingRuntime,
    readonly (readonly [string, StoreRuntime<unknown>])[]
  >();
  const bindingRows = new Map<
    QueryBindingRuntime,
    {
      readonly before: QueryBindingRuntime["stores"];
      readonly after: Map<string, StoreRuntime<unknown>>;
    }
  >();
  const callerRows = new Map<
    StoreRuntime<unknown>,
    { readonly before: StoreRuntime<unknown>["callers"]; readonly after: QueryCaller<unknown>[] }
  >();
  const progressRows = new Map<StoreRuntime<unknown>, PreparedQueryProgressCell>();
  const incarnationRows = new Map<
    StoreRuntime<unknown>,
    { readonly before: object | undefined; readonly after: object; readonly dropped: boolean }
  >();
  const configurationRows = new Map<StoreRuntime<unknown>, PassiveQueryConfiguration>();
  const sourceRows = new Map<
    StoreRuntime<unknown>,
    {
      readonly source?: NonNullable<StoreRuntime<unknown>["source"]>;
      readonly streamSource?: NonNullable<StoreRuntime<unknown>["streamSource"]>;
      readonly activation?: () => void;
    }
  >();
  const addressOwners = new Map<object, Map<string, StoreRuntime<unknown>>>();

  const graphRow = (state: GraphRuntimeState<any>) => {
    let row = graphRows.get(state);
    if (row === undefined) {
      graphBeforeEntries.set(state, Array.from(state.stores));
      row = { before: state.stores, after: new Map(state.stores) };
      graphRows.set(state, row);
    }
    return row;
  };
  const bindingRow = (binding: QueryBindingRuntime) => {
    let row = bindingRows.get(binding);
    if (row === undefined) {
      bindingBeforeEntries.set(binding, Array.from(binding.stores));
      row = { before: binding.stores, after: new Map(binding.stores) };
      bindingRows.set(binding, row);
    }
    return row;
  };

  for (const record of records) {
    const owner = record.binding ?? record.state.graph;
    let addresses = addressOwners.get(owner);
    if (addresses === undefined) {
      addresses = new Map();
      addressOwners.set(owner, addresses);
    }
    const address = String(record.key);
    const existingRuntime = addresses.get(address);
    if (existingRuntime !== undefined && existingRuntime !== record.runtime)
      throw new TypeError("Two passive claims compete for one native query address");
    addresses.set(address, record.runtime);

    if (record.detached) {
      const graph = graphRow(record.state);
      const graphKey: string | StoreIdentity =
        record.binding === undefined ? String(record.key) : record.runtime.identity;
      const graphExisting = graph.after.get(graphKey);
      if (graphExisting !== undefined && graphExisting !== record.runtime)
        throw new TypeError("A physical Store already owns the passive query address");
      graph.after.set(graphKey, record.runtime);
      if (record.binding !== undefined) {
        const bound = bindingRow(record.binding);
        const boundExisting = bound.after.get(String(record.key));
        if (boundExisting !== undefined && boundExisting !== record.runtime)
          throw new TypeError("A physical bound Store already owns the passive query address");
        bound.after.set(String(record.key), record.runtime);
      }
    }

    if (record.caller !== undefined) {
      let callers = callerRows.get(record.runtime);
      if (callers === undefined) {
        callers = { before: record.runtime.callers, after: [...record.runtime.callers] };
        callerRows.set(record.runtime, callers);
      }
      if (!callers.after.includes(record.caller)) callers.after.push(record.caller);
    }

    if (record.progress !== undefined) {
      const priorProgress = progressRows.get(record.runtime);
      if (priorProgress !== undefined && priorProgress !== record.progress)
        throw new TypeError("Passive claims disagree on their query progress owner");
      progressRows.set(record.runtime, record.progress);
    }
    const priorIncarnation = incarnationRows.get(record.runtime);
    if (priorIncarnation !== undefined && priorIncarnation.after !== record.incarnationAfter)
      throw new TypeError("Passive claims disagree on their native incarnation");
    incarnationRows.set(record.runtime, {
      before: record.incarnationBefore,
      after: record.incarnationAfter,
      dropped: record.droppedBefore,
    });
    if (record.configuration !== undefined) {
      const priorConfiguration = configurationRows.get(record.runtime);
      if (
        priorConfiguration !== undefined &&
        !samePassiveQueryPolicies(priorConfiguration.after, record.configuration.after)
      )
        throw new TypeError("Passive claims disagree on native query value policy");
      if (priorConfiguration === undefined)
        configurationRows.set(record.runtime, record.configuration);
    }
    if (
      record.sourceAfter !== undefined ||
      record.streamSourceAfter !== undefined ||
      record.activationAfter !== undefined
    ) {
      const priorSource = sourceRows.get(record.runtime);
      if (
        (record.sourceAfter !== undefined && priorSource?.streamSource !== undefined) ||
        (record.streamSourceAfter !== undefined && priorSource?.source !== undefined)
      )
        throw new TypeError("Passive claims disagree on native source kind");
      sourceRows.set(record.runtime, {
        ...(priorSource ?? {}),
        ...(record.sourceAfter === undefined ? {} : { source: record.sourceAfter }),
        ...(record.streamSourceAfter === undefined
          ? {}
          : { streamSource: record.streamSourceAfter }),
        ...(record.activationAfter === undefined ? {} : { activation: record.activationAfter }),
      });
    }
  }

  const positionedBindings = new Map<
    StreamBindingRuntime,
    {
      readonly beforeReaders: Set<StoreRuntime<unknown>> | undefined;
      readonly readers: Set<StoreRuntime<unknown>>;
      readonly beforeAddresses: Map<string, BindingAddressRuntime>;
      readonly addresses: Map<string, BindingAddressRuntime>;
      readonly members: readonly StoreRuntime<unknown>[];
      readonly entries: readonly (readonly [string, BindingAddressRuntime])[];
    }
  >();
  const controllerRows = new Map<
    GraphRuntimeState<any>,
    {
      readonly before: Set<AbortController>;
      readonly after: Set<AbortController>;
      readonly members: readonly AbortController[];
    }
  >();
  const projectionRows = new Map<
    GraphRuntimeState<any>,
    {
      readonly before: Set<GraphProjection>;
      readonly after: Set<GraphProjection>;
      readonly members: readonly GraphProjection[];
    }
  >();
  const contextRows = new Map<
    BindingContextRuntime,
    {
      readonly before: Set<() => void> | undefined;
      readonly after: Set<() => void>;
      readonly members: readonly (() => void)[];
    }
  >();
  const addressChanges = new Map<
    BindingAddressRuntime,
    {
      readonly before: object;
      readonly after: object;
      readonly change: ReturnType<typeof prepareTransferChange>;
    }
  >();
  const positionedOwners = new Set<object>();
  for (const record of positionedRecords) {
    const transfer = record.transfer;
    const runtime = transfer.runtime;
    const state = runtime.graph.__runtime;
    const table = graphRow(state);
    if (table.after.has(runtime.identity))
      throw new TypeError("Positioned reader identity is already published");
    table.after.set(runtime.identity, runtime);
    positionedOwners.add(state);
    let controllers = controllerRows.get(state);
    if (controllers === undefined) {
      controllers = {
        before: state.activeControllers,
        after: new Set(state.activeControllers),
        members: Array.from(state.activeControllers),
      };
      controllerRows.set(state, controllers);
    }
    controllers.after.add(transfer.session.controller);
    let projections = projectionRows.get(state);
    if (projections === undefined) {
      projections = {
        before: state.projections,
        after: new Set(state.projections),
        members: Array.from(state.projections),
      };
      projectionRows.set(state, projections);
    }
    for (const registration of transfer.group.preparation!.registrations)
      projections.after.add(registration);
    const binding = runtime.binding;
    if (binding !== undefined) {
      if (binding.kind !== "stream")
        throw new TypeError("Positioned group crossed a non-stream binding");
      positionedOwners.add(binding);
      positionedOwners.add(binding.handle);
      positionedOwners.add(binding.context);
      let row = positionedBindings.get(binding);
      if (row === undefined) {
        row = {
          beforeReaders: binding.readerStores,
          readers: new Set(binding.readerStores),
          beforeAddresses: binding.addresses,
          addresses: new Map(binding.addresses),
          members: Array.from(binding.readerStores ?? []),
          entries: Array.from(binding.addresses),
        };
        positionedBindings.set(binding, row);
      }
      row.readers.add(runtime);
      const key = String(runtime.key);
      let address = row.addresses.get(key);
      if (address === undefined) {
        address = { binding, key, operations: new Set(), guard: {} };
        row.addresses.set(key, address);
      }
      if (!addressChanges.has(address))
        addressChanges.set(address, {
          before: address.guard,
          after: {},
          change: prepareTransferChange(address),
        });
      const stops = transfer.group.preparation!.contextStops;
      if (stops !== undefined && stops.length > 0) {
        let context = contextRows.get(binding.context);
        if (context === undefined) {
          context = {
            before: binding.context.positionedProjections,
            after: new Set(binding.context.positionedProjections),
            members: Array.from(binding.context.positionedProjections ?? []),
          };
          contextRows.set(binding.context, context);
        }
        for (const stop of stops) context.after.add(stop);
      }
    }
  }
  const positionedRootGuards: (() => boolean)[] = [];
  for (const [state, row] of graphRows) {
    const entries = graphBeforeEntries.get(state)!;
    positionedRootGuards.push(() => {
      if (state.stores !== row.before || row.before.size !== entries.length) return false;
      for (let index = 0; index < entries.length; index++) {
        const entry = entries[index]!;
        if (row.before.get(entry[0]) !== entry[1]) return false;
      }
      return true;
    });
  }
  for (const [binding, row] of bindingRows) {
    const entries = bindingBeforeEntries.get(binding)!;
    positionedRootGuards.push(() => {
      if (binding.stores !== row.before || row.before.size !== entries.length) return false;
      for (let index = 0; index < entries.length; index++) {
        const entry = entries[index]!;
        if (row.before.get(entry[0]) !== entry[1]) return false;
      }
      return true;
    });
  }
  for (const [binding, row] of positionedBindings)
    positionedRootGuards.push(() => {
      if (
        binding.readerStores !== row.beforeReaders ||
        (row.beforeReaders?.size ?? 0) !== row.members.length ||
        binding.addresses !== row.beforeAddresses ||
        row.beforeAddresses.size !== row.entries.length
      )
        return false;
      for (let index = 0; index < row.members.length; index++)
        if (!row.beforeReaders!.has(row.members[index]!)) return false;
      for (let index = 0; index < row.entries.length; index++) {
        const entry = row.entries[index]!;
        if (row.beforeAddresses.get(entry[0]) !== entry[1]) return false;
      }
      return true;
    });
  for (const [state, row] of controllerRows)
    positionedRootGuards.push(() => {
      if (state.activeControllers !== row.before || row.before.size !== row.members.length)
        return false;
      for (let index = 0; index < row.members.length; index++)
        if (!row.before.has(row.members[index]!)) return false;
      return true;
    });
  for (const [state, row] of projectionRows)
    positionedRootGuards.push(() => {
      if (state.projections !== row.before || row.before.size !== row.members.length) return false;
      for (let index = 0; index < row.members.length; index++)
        if (!row.before.has(row.members[index]!)) return false;
      return true;
    });
  for (const [context, row] of contextRows)
    positionedRootGuards.push(() => {
      if (
        context.positionedProjections !== row.before ||
        (row.before?.size ?? 0) !== row.members.length
      )
        return false;
      for (let index = 0; index < row.members.length; index++)
        if (!row.before!.has(row.members[index]!)) return false;
      return true;
    });
  const positionedOwnerGuards = [...positionedOwners].map(transferEpochCurrent);
  for (const record of positionedRecords) {
    record.transfer.validate();
    const now = readerLifetimeNow();
    for (const previous of positionedRecords)
      if (!previous.transfer.currentAt(now))
        throw new TypeError("A positioned reader changed during another reader validation");
    for (const normal of records)
      if (!normal.current())
        throw new TypeError("A native source changed during positioned validation");
    for (const guard of positionedOwnerGuards)
      if (!guard()) throw new TypeError("A positioned owner changed during validation");
    for (const guard of positionedRootGuards)
      if (!guard())
        throw new TypeError("A native membership root changed during reader validation");
  }
  const positionedBindingRows = [...positionedBindings];
  const positionedControllerRows = [...controllerRows];
  const positionedProjectionRows = [...projectionRows];
  const positionedContextRows = [...contextRows];
  const positionedAddressRows = [...addressChanges];
  const positionedOwnerChanges = [...positionedOwners].map(prepareTransferChange);

  const observationRows = new Map<
    ObservationState,
    {
      readonly runtime: StoreRuntime<unknown>;
      readonly participants: PreparedObservationTransfer<unknown>[];
      readonly groups: PassiveObservationGroup[];
    }
  >();
  const observationRuntimeStates = new Map<StoreRuntime<unknown>, ObservationState>();
  const aggregateRecords = new Set(records);
  for (let index = 0; index < records.length; index += 1) {
    const record = records[index]!;
    const participant = record.observation;
    if (participant === undefined) continue;
    const group = record.observationGroup;
    if (record.caller === undefined)
      throw new TypeError("Observation participant lacks its authenticated query claim");
    const priorState = observationRuntimeStates.get(record.runtime);
    if (priorState !== undefined && priorState !== participant.state)
      throw new TypeError("Independent observation publishers target one native Store");
    observationRuntimeStates.set(record.runtime, participant.state);
    let row = observationRows.get(participant.state);
    if (row !== undefined && row.runtime !== record.runtime)
      throw new TypeError("Observation state crossed native Store runtimes");
    if (row === undefined) {
      row = { runtime: record.runtime, participants: [], groups: [] };
      observationRows.set(participant.state, row);
    }
    if (row.participants.length > 0 && row.participants[0]!.lease !== participant.lease)
      throw new TypeError("Independent observation leases target one shared state");
    if (!row.participants.includes(participant)) row.participants.push(participant);
    if (group !== undefined && !row.groups.includes(group)) row.groups.push(group);
  }
  for (const row of observationRows.values()) {
    for (const group of row.groups) {
      if (group.inAggregate || group.published || group.discarded)
        throw new TypeError("Observation lease group is already reserved");
      for (const demand of group.demands)
        if (demand.active && !aggregateRecords.has(demand.record))
          throw new TypeError("Required adoption omitted an owning pending observation claim");
    }
  }
  const observationTableRows = [...observationRows.values()];
  const graphTableRows = [...graphRows].map(([state, row]) => ({ state, ...row }));
  const changedRuntimes = [
    ...new Set([
      ...records.map((record) => record.runtime),
      ...positionedRecords.map((record) => record.transfer.runtime),
    ]),
  ];
  const runtimeChanges = changedRuntimes.map((runtime) => prepareTransferChange(runtime));
  const changedBindings = [
    ...new Set(records.flatMap((record) => (record.binding === undefined ? [] : [record.binding]))),
  ];
  const bindingChanges = changedBindings.flatMap((binding) => [
    prepareTransferChange(binding),
    prepareTransferChange(binding.handle),
  ]);
  const bindingTableRows = [...bindingRows].map(([binding, row]) => ({ binding, ...row }));
  const callerTableRows = [...callerRows].map(([runtime, row]) => ({ runtime, ...row }));
  const configurationTableRows = [...configurationRows];
  const progressTableRows = [...progressRows];
  const incarnationTableRows = [...incarnationRows];
  const sourceTableRows = [...sourceRows];
  let published = false;
  let delivered = false;
  const current = (time?: number): boolean => {
    if (published) return false;
    const now = time ?? readerLifetimeNow();
    for (let index = 0; index < positionedRecords.length; index++)
      if (!positionedRecords[index]!.transfer.currentAt(now)) return false;
    for (let index = 0; index < positionedOwnerGuards.length; index++)
      if (!positionedOwnerGuards[index]!()) return false;
    for (let index = 0; index < positionedRootGuards.length; index++)
      if (!positionedRootGuards[index]!()) return false;
    for (let index = 0; index < positionedBindingRows.length; index++) {
      const entry = positionedBindingRows[index]!;
      if (
        entry[0].readerStores !== entry[1].beforeReaders ||
        entry[0].addresses !== entry[1].beforeAddresses
      )
        return false;
    }
    for (let index = 0; index < positionedControllerRows.length; index++) {
      const entry = positionedControllerRows[index]!;
      if (entry[0].activeControllers !== entry[1].before) return false;
    }
    for (let index = 0; index < positionedProjectionRows.length; index++) {
      const entry = positionedProjectionRows[index]!;
      if (entry[0].projections !== entry[1].before) return false;
    }
    for (let index = 0; index < positionedContextRows.length; index++) {
      const entry = positionedContextRows[index]!;
      if (entry[0].positionedProjections !== entry[1].before) return false;
    }
    for (let index = 0; index < positionedAddressRows.length; index++) {
      const entry = positionedAddressRows[index]!;
      if (entry[0].guard !== entry[1].before || !entry[1].change.current()) return false;
    }
    for (let index = 0; index < records.length; index += 1)
      if (!records[index]!.current()) return false;
    for (let index = 0; index < graphTableRows.length; index += 1) {
      const row = graphTableRows[index]!;
      if (row.state.stores !== row.before) return false;
    }
    for (let index = 0; index < bindingTableRows.length; index += 1) {
      const row = bindingTableRows[index]!;
      if (row.binding.stores !== row.before) return false;
    }
    for (let index = 0; index < callerTableRows.length; index += 1) {
      const row = callerTableRows[index]!;
      if (row.runtime.callers !== row.before) return false;
    }
    for (let index = 0; index < progressTableRows.length; index += 1) {
      const row = progressTableRows[index]!;
      if (!row[1].current()) return false;
    }
    for (let index = 0; index < observationTableRows.length; index += 1) {
      const row = observationTableRows[index]!;
      for (
        let participantIndex = 0;
        participantIndex < row.participants.length;
        participantIndex += 1
      )
        if (!row.participants[participantIndex]!.current()) return false;
    }
    for (let index = 0; index < incarnationTableRows.length; index += 1) {
      const row = incarnationTableRows[index]!;
      if (row[0].incarnation !== row[1].before || row[0].dropped !== row[1].dropped) return false;
    }
    return true;
  };
  const publish = (): void => {
    if (published) return;
    for (let index = 0; index < configurationTableRows.length; index += 1)
      configurationTableRows[index]![1].publish();
    for (let index = 0; index < runtimeChanges.length; index += 1) runtimeChanges[index]!.publish();
    for (let index = 0; index < bindingChanges.length; index += 1) bindingChanges[index]!.publish();
    for (let index = 0; index < progressTableRows.length; index += 1)
      progressTableRows[index]![1].publish();
    for (let index = 0; index < incarnationTableRows.length; index += 1) {
      const row = incarnationTableRows[index]!;
      if (row[0].incarnation !== row[1].after || row[0].dropped) {
        row[0].incarnation = row[1].after;
        row[0].dropped = false;
      }
    }
    for (let index = 0; index < sourceTableRows.length; index += 1) {
      const row = sourceTableRows[index]!;
      if (row[1].source !== undefined) row[0].source = row[1].source;
      if (row[1].streamSource !== undefined) row[0].streamSource = row[1].streamSource;
      if (row[1].activation !== undefined) row[0].onReadableActivation = row[1].activation;
    }
    for (let index = 0; index < graphTableRows.length; index += 1) {
      const row = graphTableRows[index]!;
      row.state.stores = row.after;
    }
    for (let index = 0; index < bindingTableRows.length; index += 1) {
      const row = bindingTableRows[index]!;
      row.binding.stores = row.after;
    }
    for (let index = 0; index < callerTableRows.length; index += 1) {
      const row = callerTableRows[index]!;
      row.runtime.callers = row.after;
    }
    for (let index = 0; index < observationTableRows.length; index += 1) {
      const row = observationTableRows[index]!;
      for (
        let participantIndex = 0;
        participantIndex < row.participants.length;
        participantIndex += 1
      )
        row.participants[participantIndex]!.publish();
      for (let groupIndex = 0; groupIndex < row.groups.length; groupIndex += 1) {
        const group = row.groups[groupIndex]!;
        group.published = true;
        if (
          group.lease !== undefined &&
          passiveObservationGroups.get(group.lease as object) === group
        )
          passiveObservationGroups.delete(group.lease as object);
      }
    }
    for (let index = 0; index < positionedOwnerChanges.length; index++)
      positionedOwnerChanges[index]!.publish();
    for (let index = 0; index < positionedBindingRows.length; index++) {
      const entry = positionedBindingRows[index]!;
      entry[0].readerStores = entry[1].readers;
      entry[0].addresses = entry[1].addresses;
    }
    for (let index = 0; index < positionedControllerRows.length; index++) {
      const entry = positionedControllerRows[index]!;
      entry[0].activeControllers = entry[1].after;
    }
    for (let index = 0; index < positionedProjectionRows.length; index++) {
      const entry = positionedProjectionRows[index]!;
      entry[0].projections = entry[1].after;
    }
    for (let index = 0; index < positionedContextRows.length; index++) {
      const entry = positionedContextRows[index]!;
      entry[0].positionedProjections = entry[1].after;
    }
    for (let index = 0; index < positionedAddressRows.length; index++) {
      const entry = positionedAddressRows[index]!;
      entry[0].guard = entry[1].after;
      entry[1].change.publish();
    }
    for (let index = 0; index < positionedRecords.length; index++) {
      const record = positionedRecords[index]!;
      record.transfer.publish();
      record.published = true;
    }
    for (let index = 0; index < records.length; index += 1) records[index]!.setPublished();
    published = true;
  };
  const deliver = (): void => {
    if (!published || delivered) return;
    delivered = true;
    for (let index = 0; index < observationTableRows.length; index += 1) {
      const row = observationTableRows[index]!;
      for (
        let participantIndex = 0;
        participantIndex < row.participants.length;
        participantIndex += 1
      ) {
        try {
          row.participants[participantIndex]!.deliver();
        } catch (error) {
          reportContinuationError(row.runtime.graph.__runtime, error);
        }
      }
    }
    for (let index = 0; index < positionedRecords.length; index++) {
      const record = positionedRecords[index]!;
      try {
        record.transfer.deliver();
        record.resolve();
      } catch (cause) {
        record.reject(cause);
        reportContinuationError(record.transfer.runtime.graph.__runtime, cause);
      }
    }
    for (let index = 0; index < progressTableRows.length; index += 1) {
      const row = progressTableRows[index]!;
      try {
        row[1].deliver();
      } catch (error) {
        reportContinuationError(row[0].graph.__runtime, error);
      }
    }
    for (let index = 0; index < records.length; index += 1) {
      const record = records[index]!;
      for (let stopIndex = 0; stopIndex < record.claimReadStops.length; stopIndex += 1)
        record.runtime.readStops.push(record.claimReadStops[stopIndex]!);
      for (
        let observationIndex = 0;
        observationIndex < record.claimObservations.length;
        observationIndex += 1
      ) {
        const observation = record.claimObservations[observationIndex]!;
        try {
          observeStoreReadable(record.runtime, observation.readable, observation.observation);
        } catch (error) {
          reportContinuationError(record.state, error);
        }
      }
      try {
        touch(record.runtime);
        armStoreTimer(record.runtime);
      } catch (error) {
        reportContinuationError(record.state, error);
      }
      let releaseFailed = false;
      try {
        record.releasePrepared();
      } catch (error) {
        releaseFailed = true;
        record.rejectRetired(error);
        reportContinuationError(record.state, error);
      }
      if (!releaseFailed) record.resolveRetired();
    }
  };
  for (const record of records) record.inAggregate = true;
  for (const record of positionedRecords) record.inAggregate = true;
  for (let index = 0; index < observationTableRows.length; index += 1) {
    const row = observationTableRows[index]!;
    for (let groupIndex = 0; groupIndex < row.groups.length; groupIndex += 1)
      row.groups[groupIndex]!.inAggregate = true;
  }
  return Object.freeze({ current, publish, deliver });
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
  prepareNativePassiveTransferClaim,
  prepareNativePassiveTransferClaims,
  realmPreparationCurrent,
  realmCurrent,
});
