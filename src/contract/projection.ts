import { storeKey } from "./identity.ts";
import type {
  Graph,
  OpenOptions,
  StreamDefinition,
  NativeStreamDefinition,
  PositionedStreamDefinition,
  PositionedStreamSource,
  PositionedFoldReceipt,
  PositionedProjectionCheckpoint,
  PositionedRestoreSeed,
  PositionedStoreCheckpoint,
  PositionedStoreRestoreSeed,
  PositionedStoreRestoration,
  PositionedStorePolicyValues,
  PositionedStoreProjectionValues,
  StoreKey,
  StreamStore,
  ValueOptions,
  PositionedStreamMember,
} from "./types.ts";
import { isValueOptions } from "./types.ts";
import {
  assertBindingContextOpen,
  assertStoreHandlePublished,
  boundStreamByStore,
  graphRuntimeByGraph,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
  runtimeByStore,
  rawStreamMemberByFunction,
  streamBindingByHandle,
  positionedStreamSourceByDefinition,
  positionedPositionWriterByOpenOptions,
  positionedOpenRequestByOpenOptions,
} from "./registry.ts";
import { Fault } from "../fault.ts";
import { isObject } from "../utils.ts";
import { __internal as reactiveInternal, Signal, signal } from "../reactive.ts";
import type { Readable } from "../reactive.ts";
import { assertGraphOpen, assertStoreOpen, reportContinuationError } from "./faults.ts";
import {
  discardPositionedStoreSession,
  isLive,
  scheduleSweep,
  updateCollectionCandidate,
} from "./collection.ts";
import type {
  BoundStreamSessionRuntime,
  GraphProjection,
  GraphRuntimeState,
  StreamEvent,
  StreamBindingRuntime,
  PositionedStoreEvent,
  PositionedStoreParticipant,
  PositionedStoreSessionState,
  PositionedStorePreparation,
  PositionedContinuationAnchor,
  StreamSession,
  StoreRuntime,
} from "./runtime.ts";
import type { RetirementOwner } from "./retirement.ts";
import {
  activatePositionedStoreSession,
  attachPositionedStoreAbort,
  createPositionedStoreSession,
  publishPositionedStoreSession,
  deliverPositionedStoreSession,
} from "./stream.ts";
import {
  beginPhysicalWork,
  finishPhysicalWork,
  normalizePhysicalPromise,
  observePhysicalWork,
  physicalCleanup,
  physicalReaderRelease,
  recordCleanupFailure,
  prepareReaderRetirement,
  prepareReaderLifetime,
  readerLifetimeNow,
  retireOwner,
  observeUnreturnedReaderRetirement,
} from "./retirement.ts";
import {
  beginTransferAdmission,
  finishTransferAdmission,
  noteTransferChange,
  observePositionedTransferChanges,
  registerTransferProjection,
  registerTransferWork,
  transferEpochCurrent,
} from "./query-policy.ts";

const projectionInitialByValueCell = new WeakMap<object, unknown>();
const projectionFacetByReadable = new WeakMap<
  object,
  { readonly runtime: ProjectionRuntime<unknown, unknown>; readonly role: ProjectionTransferRole }
>();
const projectionSourceRuntimeByOwner = new WeakMap<object, StoreRuntime<unknown>>();

interface TerminalProjectionCleanup {
  remainingReadables: number;
  readonly state: WeakRef<GraphRuntimeState<any>>;
  readonly projection: WeakRef<GraphProjection>;
}

const terminalProjectionReadableFinalizer = new FinalizationRegistry<TerminalProjectionCleanup>(
  (cleanup) => {
    cleanup.remainingReadables -= 1;
    if (cleanup.remainingReadables === 0) {
      const state = cleanup.state.deref();
      const projection = cleanup.projection.deref();
      if (state !== undefined && projection !== undefined) state.projections.delete(projection);
    }
  },
);

/**
 * Lifecycle state of a stream projection.
 *
 * A projection starts `open`, becomes `closed` on explicit close, normal
 * iterator completion, or graph disposal, and becomes `failed` when its
 * source reports a gap, the iterator fails, accumulation overflows with the
 * default policy, or a reducer throws.
 */
export type ProjectionStatus = "open" | "closed" | "failed";

/**
 * Accumulates stream emissions in a bounded array.
 *
 * `max` must be a finite positive integer. When the bound is reached,
 * `onOverflow` defaults to `"fail"`; `"drop-oldest"` keeps the newest `max`
 * items instead.
 */
export type AccumulatePolicy = {
  /** Selects bounded array accumulation. */
  readonly kind: "accumulate";

  /** Maximum number of emissions retained in the projected value. */
  readonly max: number;

  /** Overflow behavior; defaults to `"fail"`. */
  readonly onOverflow?: "drop-oldest" | "fail";
};

/**
 * Folds each stream emission into one projected value.
 *
 * The initial value is exposed before the first emission. `step` is called
 * once per emission with the previous projected value and the new item.
 */
export type ReducePolicy<T, V> = {
  /** Selects reducer-based projection. */
  readonly kind: "reduce";

  /** Projected value before the first stream item is received. */
  readonly initial: V;

  /** Computes the next projected value from the previous value and an item. */
  readonly step: (accumulator: V, item: T) => V;
};

/** The accumulation or reduction policy used by {@link project}. */
export type ProjectionPolicy<T, V> = AccumulatePolicy | ReducePolicy<T, V>;

/** Validates policy fields without executing the reducer or inspecting its data. */
function isProjectionPolicy(value: unknown): value is ProjectionPolicy<unknown, unknown> {
  if (!isObject(value) || !("kind" in value)) return false;
  if (value.kind === "reduce")
    return "initial" in value && "step" in value && typeof value.step === "function";
  if (value.kind !== "accumulate") return false;
  return (
    "max" in value &&
    typeof value.max === "number" &&
    (!("onOverflow" in value) ||
      value.onOverflow === undefined ||
      value.onOverflow === "fail" ||
      value.onOverflow === "drop-oldest")
  );
}

/**
 * One-shot handle for a provider-authenticated positioned restore.
 *
 * Preparation installs a borrowed seed without opening the provider source.
 * Activation opens the admitted reader once; discarding closes the projection
 * and releases its reader reservation.
 */
export interface PositionedRestoration<V> {
  /** Dormant projection seeded from the admitted checkpoint. */
  readonly projection: Projection<V>;
  /** Opens the reserved reader once, after the captured position. */
  activate(): Projection<V>;
  /** Discards an unused restoration or closes its activated projection. */
  discard(): void;
}

/** Raw graph member or exact native binding for one positioned declaration. */
export type PositionedStreamAccess<Input, Value, Position, Reader> =
  | PositionedStreamMember<Input, Value, Position, Reader>
  | import("./stream-binding.ts").StreamBinding<
      PositionedStreamDefinition<Input, Value, any, Position, Reader>
    >;

/**
 * A value maintained from stream emissions.
 *
 * The graph/definition overload of {@link project} opens an independent source.
 * The StreamStore overload consumes future emissions from its existing shared
 * session. Closing a shared projection releases only that projection's listener;
 * the source remains subject to the graph's normal listener/collection lifetime.
 */
export interface Projection<V> {
  /** The accumulated array or reduced value produced so far. */
  readonly value: Readable<V>;

  /** The projection's current lifecycle state. */
  readonly status: Readable<ProjectionStatus>;

  /** The failure reason, or `undefined` until a failure occurs. */
  readonly error: Readable<unknown>;

  /**
   * Terminates the projection and releases its source listener.
   * An independently opened source is also aborted.
   *
   * Closing is idempotent. It sets `status` to `"closed"`, does not create an
   * error, and ignores later source emissions. A projection that has already
   * failed remains `"failed"` when closed again.
   */
  close(): void;
}

interface ProjectionSource<T> {
  readonly retirement: RetirementOwner;
  readonly controller: AbortController;
  readonly generation: number;
  iterator?: AsyncIterator<T>;
  detach?: () => void;
  retired: boolean;
}

interface PositionedProjectionTuple<V, Position> {
  readonly value: V;
  readonly status: ProjectionStatus;
  readonly error: unknown;
  readonly hasPosition: boolean;
  readonly position: Position | undefined;
  readonly generation: number;
  readonly revision: number;
  readonly resumable: boolean;
}

interface PositionedProjectionRuntime {
  readonly graphState: GraphRuntimeState<any>;
  graphEpoch: number;
  readonly definition: PositionedStreamDefinition<any, any, any, any, any>;
  readonly provider: PositionedStreamSource<any, any, any, any>;
  readonly input: unknown;
  readonly policy: ProjectionPolicy<any, any>;
  key?: StoreKey;
  hasKey: boolean;
  openingKey?: StoreKey;
  hasOpeningKey: boolean;
  restoreSeed?: unknown;
  readonly member: object;
  readonly binding?: StreamBindingRuntime;
  tuple: PositionedProjectionTuple<any, any>;
  pendingPosition: unknown;
  hasPendingPosition: boolean;
  openingRequest?: { readonly after?: unknown; readonly reader?: unknown };
  reader?: unknown;
  readerAfter?: unknown;
  readerPresent: boolean;
  readerReleased: boolean;
  contextCleanup?: () => void;
  latestReceipt?: PositionedFoldReceipt<unknown>;
  restored: boolean;
  transitioning: boolean;
  storeGroup?: PositionedStoreSessionState<any>;
  storeParticipant?: PositionedStoreParticipant;
}

const positionedProjectionByHandle = new WeakMap<object, ProjectionRuntime<unknown, unknown>>();
const positionedCheckpointByHandle = new WeakMap<
  object,
  {
    readonly runtime: ProjectionRuntime<unknown, unknown>;
    readonly tuple: PositionedProjectionTuple<unknown, unknown>;
  }
>();
const positionedReceiptByHandle = new WeakMap<
  object,
  {
    readonly runtime: ProjectionRuntime<unknown, unknown>;
    readonly tuple: PositionedProjectionTuple<unknown, unknown>;
    readonly storeEvent?: PositionedStoreEvent;
  }
>();

interface PositionedStoreProjectionCapture {
  readonly handle: object;
  readonly runtime: ProjectionRuntime<unknown, unknown>;
  readonly tuple: PositionedProjectionTuple<unknown, unknown>;
  readonly participant: PositionedStoreParticipant;
  readonly event?: PositionedStoreEvent;
  readonly failed: boolean;
}

interface PositionedStoreCheckpointCapture {
  readonly source: object;
  readonly runtime: StoreRuntime<unknown>;
  readonly session: StreamSession<unknown>;
  readonly group: PositionedStoreSessionState<unknown>;
  readonly continuation?: PositionedContinuationAnchor;
  readonly completed?: PositionedStoreEvent;
  readonly dispatch?: PositionedStoreEvent;
  readonly baseRevision?: number;
  readonly diverged: boolean;
  readonly hasPosition: boolean;
  readonly position: unknown;
  readonly valueRevision: number;
  readonly graphEpoch: number;
  readonly projections: readonly PositionedStoreProjectionCapture[];
  readonly values: readonly unknown[];
}

const positionedStoreCheckpointByHandle = new WeakMap<object, PositionedStoreCheckpointCapture>();

interface ProjectionRuntime<T, V> {
  boundSession?: BoundStreamSessionRuntime<T>;
  readonly graph: Graph;
  readonly valueCell: ReturnType<typeof signal<V>>;
  readonly statusCell: ReturnType<typeof signal<ProjectionStatus>>;
  readonly errorCell: ReturnType<typeof signal<unknown>>;
  readonly policy: ProjectionPolicy<T, V>;
  source: ProjectionSource<T>;
  positioned?: PositionedProjectionRuntime;
  unregister?: () => void;
  restartSource?: () => void;
  closed: boolean;
}

export type ProjectionTransferRole = "projection-value" | "projection-status" | "projection-error";

export interface TransferProjectionFacet {
  readonly owner: object;
  readonly graph: Graph;
  readonly role: ProjectionTransferRole;
  readonly sourceRuntime?: StoreRuntime<unknown>;
  current(): boolean;
}

/** @internal Returns exact current native ownership for a projection facet. */
export function getTransferProjectionFacet(readable: object): TransferProjectionFacet | undefined {
  const row = projectionFacetByReadable.get(readable);
  if (row === undefined) return undefined;
  const runtime = row.runtime;
  const graphState = graphRuntimeByGraph.get(runtime.graph as object);
  if (graphState === undefined) return undefined;
  const graphEpoch = graphState.resetEpoch;
  const source = runtime.source;
  const sourceGeneration = source.generation;
  const sourceRetired = source.retired;
  const closed = runtime.closed;
  const positioned = runtime.positioned;
  const positionedTuple = positioned?.tuple;
  const positionedTransitioning = positioned?.transitioning;
  const boundSession = runtime.boundSession;
  const session = boundSession?.session;
  const sessionPhase = boundSession?.phase;
  const ownerCurrent = transferEpochCurrent(runtime);
  const sourceRuntime = projectionSourceRuntimeByOwner.get(runtime);
  const sourceSession = sourceRuntime?.stream;
  const sourceIncarnation = sourceRuntime?.incarnation;
  const current = (): boolean =>
    ownerCurrent() &&
    graphRuntimeByGraph.get(runtime.graph as object) === graphState &&
    !graphState.disposed &&
    graphState.resetEpoch === graphEpoch &&
    runtime.source === source &&
    source.generation === sourceGeneration &&
    source.retired === sourceRetired &&
    runtime.closed === closed &&
    runtime.positioned === positioned &&
    (positioned === undefined ||
      (positioned.tuple === positionedTuple &&
        positioned.transitioning === positionedTransitioning)) &&
    runtime.boundSession === boundSession &&
    (boundSession === undefined ||
      (boundSession.session === session && boundSession.phase === sessionPhase)) &&
    (sourceRuntime === undefined ||
      (sourceRuntime.stream === sourceSession && sourceRuntime.incarnation === sourceIncarnation));
  return {
    owner: runtime,
    graph: runtime.graph,
    role: row.role,
    ...(sourceRuntime === undefined ? {} : { sourceRuntime }),
    current,
  };
}

function registerProjectionFacet<T, V>(
  runtime: ProjectionRuntime<T, V>,
  readable: Readable<unknown>,
  role: ProjectionTransferRole,
): void {
  registerTransferProjection(readable, runtime.graph, runtime);
  projectionFacetByReadable.set(readable, {
    runtime: runtime as unknown as ProjectionRuntime<unknown, unknown>,
    role,
  });
}

function validateMax(max: number): void {
  // short-circuit to avoid the more expensive Number.isInteger check when max is not finite
  if (!(Number.isFinite(max) && max >= 1 && Number.isInteger(max))) {
    throw new Fault("contract", ["max"]);
  }
}

function reportProjectionError<T, V>(runtime: ProjectionRuntime<T, V>, error: unknown): void {
  reportGraphContinuationError(runtime.graph, error);
}

function safeSet<T, V>(runtime: ProjectionRuntime<T, V>, set: () => void): void {
  try {
    set();
  } catch (error) {
    reportProjectionError(runtime, error);
  }
}

function returnProjectionIterator<T>(
  source: ProjectionSource<T>,
  iterator: AsyncIterator<T>,
): void {
  const work = beginPhysicalWork(source.retirement, "projection-return");
  registerTransferWork(source, work);
  noteTransferChange(source);
  let hasOriginal = false;
  try {
    let close: AsyncIterator<T>["return"];
    try {
      close = iterator.return;
    } catch (cause) {
      recordCleanupFailure(work, cause);
      throw cause;
    }
    if (typeof close !== "function") return;
    let original: unknown;
    try {
      original = close.call(iterator);
    } catch (cause) {
      recordCleanupFailure(work, cause);
      return;
    }
    hasOriginal = true;
    noteTransferChange(work);
    work.original = original;
    try {
      observePhysicalWork(work, normalizePhysicalPromise(original), true, original);
    } catch {
      // The returned original remains unacknowledged if normalization or attachment fails.
    }
  } finally {
    if (!hasOriginal) finishPhysicalWork(work);
  }
}

function stopSource<T>(source: ProjectionSource<T>, reason?: unknown): (() => void) | undefined {
  if (source.retired) return undefined;
  const cleanupWork = beginPhysicalWork(source.retirement, "cleanup");
  registerTransferWork(source, cleanupWork);
  noteTransferChange(source);
  source.retired = true;
  const detach = source.detach;
  const iterator = source.iterator;
  source.detach = undefined;
  source.iterator = undefined;
  let cleaned = false;
  return () => {
    if (cleaned) return;
    cleaned = true;
    const failures: unknown[] = [];
    const attempt = (action: () => void, tracked = true): void => {
      try {
        if (tracked) physicalCleanup(source.retirement, action);
        else action();
      } catch (error) {
        failures.push(error);
      }
    };
    if (detach !== undefined) attempt(detach);
    attempt(() => source.controller.abort(reason));
    if (iterator != null) attempt(() => returnProjectionIterator(source, iterator), false);
    finishPhysicalWork(cleanupWork);
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1)
      throw new AggregateError(failures, "Several projection cleanup operations failed");
  };
}

function registerTerminalReset<V>(
  graph: Graph,
  terminalStatus: ProjectionStatus,
  valueCell: WeakRef<ReturnType<typeof signal<V>>>,
  statusCell: WeakRef<ReturnType<typeof signal<ProjectionStatus>>>,
  errorCell: WeakRef<ReturnType<typeof signal<unknown>>>,
  resetTuple?: (resetEpoch: number) => boolean,
): void {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined || state.disposed) return;

  let unregister: (() => void) | undefined;
  const finalizerToken = {};
  let active = true;
  const reset = (): void => {
    if (!active) return;
    noteTransferChange(projection);
    noteTransferChange(state);
    active = false;
    const resetEpoch = state.resetEpoch;
    const current = (): boolean => state.resetEpoch === resetEpoch;
    terminalProjectionReadableFinalizer.unregister(finalizerToken);
    unregister?.();
    unregister = undefined;

    const value = valueCell.deref();
    const status = statusCell.deref();
    const error = errorCell.deref();
    const initial =
      value !== undefined && projectionInitialByValueCell.has(value)
        ? projectionInitialByValueCell.get(value)
        : ([] as unknown as V);
    if (value !== undefined) projectionInitialByValueCell.delete(value);

    let tupleReset = false;
    try {
      tupleReset = resetTuple?.(resetEpoch) === true;
    } catch (reason) {
      reportGraphContinuationError(graph, reason);
    }
    if (tupleReset) return;

    if (status !== undefined) {
      try {
        status.set(terminalStatus);
      } catch (reason) {
        reportGraphContinuationError(graph, reason);
      }
    }
    if (current() && value !== undefined) {
      try {
        value.set(initial as V);
      } catch (reason) {
        reportGraphContinuationError(graph, reason);
      }
    }
    if (current() && error !== undefined) {
      try {
        error.set(undefined);
      } catch (reason) {
        reportGraphContinuationError(graph, reason);
      }
    }
  };
  const projection: GraphProjection = {
    captureCancellation: () => undefined,
    reset: () => {
      reset();
      return undefined;
    },
    restart: () => undefined,
    dispose: () => undefined,
  };
  unregister = registerProjection(graph, projection);
  const cleanup: TerminalProjectionCleanup = {
    remainingReadables: 0,
    state: new WeakRef(state),
    projection: new WeakRef(projection),
  };
  for (const readable of [valueCell, statusCell, errorCell]) {
    const cell = readable.deref();
    if (cell === undefined) continue;
    cleanup.remainingReadables += 1;
    terminalProjectionReadableFinalizer.register(cell, cleanup, finalizerToken);
  }
}

function resetPositionedTerminal<T, V>(
  runtime: ProjectionRuntime<T, V>,
  state: GraphRuntimeState<any>,
  resetEpoch: number,
  terminalStatus: ProjectionStatus,
): boolean {
  const positioned = runtime.positioned;
  if (
    positioned === undefined ||
    state.disposed ||
    !runtime.closed ||
    !runtime.source.retired ||
    positioned.transitioning
  )
    return false;
  const previous = positioned.tuple;
  const initial = runtime.policy.kind === "reduce" ? runtime.policy.initial : ([] as unknown as V);
  const reset: PositionedProjectionTuple<V, unknown> = {
    ...previous,
    value: initial,
    status: terminalStatus,
    error: undefined,
    hasPosition: false,
    position: undefined,
    generation: runtime.source.generation,
    revision: previous.revision + 1,
    resumable: false,
  };
  let committed = false;
  const landed = writePositionedTuple(
    runtime,
    reset,
    () =>
      state.resetEpoch === resetEpoch &&
      !state.disposed &&
      runtime.closed &&
      runtime.source.retired &&
      runtime.positioned === positioned &&
      positioned.tuple === previous &&
      !positioned.transitioning,
    undefined,
    () => {
      positioned.graphEpoch = resetEpoch;
      committed = true;
    },
  );
  return landed && committed;
}

function finish<T, V>(
  runtime: ProjectionRuntime<T, V>,
  failed: boolean,
  reason?: unknown,
  source = runtime.source,
  abortReason?: unknown,
): void {
  if (runtime.positioned !== undefined) {
    finishPositioned(runtime, failed, reason, source, abortReason);
    return;
  }
  if (source.retired) return;
  const completion = beginPhysicalWork(source.retirement, "cleanup");
  registerTransferWork(runtime, completion);
  registerTransferWork(source, completion);
  noteTransferChange(runtime);
  noteTransferChange(source);
  try {
    const cleanup = stopSource(source, abortReason);
    if (cleanup === undefined) return;
    if (runtime.source !== source || runtime.closed) {
      cleanup();
      return;
    }
    runtime.closed = true;
    const state = graphRuntimeByGraph.get(runtime.graph as object);
    const resetEpoch = state?.resetEpoch;
    const resetVersion = state?.resetVersion.peek();
    const current = (): boolean =>
      runtime.source === source &&
      state?.resetEpoch === resetEpoch &&
      state?.resetVersion.peek() === resetVersion;
    const unregister = runtime.unregister;
    runtime.unregister = undefined;
    const terminalStatus = failed ? "failed" : "closed";
    if (unregister !== undefined) physicalCleanup(source.retirement, unregister);
    if (current())
      registerTerminalReset(
        runtime.graph,
        terminalStatus,
        new WeakRef(runtime.valueCell),
        new WeakRef(runtime.statusCell),
        new WeakRef(runtime.errorCell),
      );
    let cleanupError: unknown;
    let cleanupFailed = false;
    try {
      cleanup();
    } catch (error) {
      cleanupFailed = true;
      cleanupError = error;
    }
    if (current() && failed && reason != null)
      safeSet(runtime, () => runtime.errorCell.set(reason));
    if (current()) {
      try {
        physicalCleanup(source.retirement, () => runtime.statusCell.set(terminalStatus));
      } catch (cause) {
        reportProjectionError(runtime, cause);
      }
    }
    if (cleanupFailed) throw cleanupError;
  } finally {
    finishPhysicalWork(completion);
  }
}

function prepareReset<T, V>(runtime: ProjectionRuntime<T, V>): (() => void) | undefined {
  if (runtime.closed) return undefined;
  if (runtime.positioned?.restored === true) {
    finishPositioned(runtime, false);
    const positioned = runtime.positioned;
    if (positioned !== undefined && !positioned.graphState.disposed)
      resetPositionedTerminal(
        runtime,
        positioned.graphState,
        positioned.graphState.resetEpoch,
        positioned.tuple.status,
      );
    return undefined;
  }
  if (runtime.positioned !== undefined) {
    const positioned = runtime.positioned;
    if (positioned.transitioning) return undefined;
    noteTransferChange(runtime);
    noteTransferChange(positioned);
    positioned.transitioning = true;
    const previous = runtime.source;
    noteTransferChange(runtime);
    noteTransferChange(previous);
    const cleanup = stopSource(previous);
    const source: ProjectionSource<T> = {
      retirement: previous.retirement,
      controller: new AbortController(),
      generation: previous.generation + 1,
      retired: false,
    };
    noteTransferChange(runtime);
    noteTransferChange(source);
    runtime.source = source;
    runtime.closed = false;
    positioned.graphEpoch = positioned.graphState.resetEpoch;
    positioned.pendingPosition = undefined;
    positioned.hasPendingPosition = false;
    positioned.openingRequest = undefined;
    positioned.readerPresent = false;
    positioned.readerReleased = false;
    positioned.latestReceipt = undefined;
    const initial =
      runtime.policy.kind === "reduce" ? runtime.policy.initial : ([] as unknown as V);
    const next: PositionedProjectionTuple<V, unknown> = {
      value: initial,
      status: "open",
      error: undefined,
      hasPosition: false,
      position: undefined,
      generation: source.generation,
      revision: positioned.tuple.revision + 1,
      resumable: false,
    };
    try {
      writePositionedTuple(
        runtime,
        next,
        () =>
          runtime.source === source &&
          positioned.graphState.resetEpoch === positioned.graphEpoch &&
          !positioned.graphState.disposed &&
          positioned.transitioning,
        undefined,
        () => {
          positioned.transitioning = false;
        },
      );
    } catch (error) {
      reportProjectionError(runtime, error);
    } finally {
      noteTransferChange(runtime);
      noteTransferChange(positioned);
      positioned.transitioning = false;
    }
    return cleanup;
  }
  const previous = runtime.source;
  noteTransferChange(runtime);
  noteTransferChange(previous);
  const cleanup = stopSource(previous);
  const source: ProjectionSource<T> = {
    retirement: previous.retirement,
    controller: new AbortController(),
    generation: previous.generation + 1,
    retired: false,
  };
  noteTransferChange(runtime);
  noteTransferChange(source);
  runtime.source = source;
  const state = graphRuntimeByGraph.get(runtime.graph as object);
  const resetEpoch = state?.resetEpoch;
  const current = (): boolean =>
    isCurrentGeneration(runtime, source.controller, source.generation) &&
    state?.resetEpoch === resetEpoch;
  safeSet(runtime, () => {
    const initial =
      runtime.policy.kind === "reduce" ? runtime.policy.initial : ([] as unknown as V);
    if (current()) runtime.valueCell.set(initial);
  });
  if (current()) safeSet(runtime, () => runtime.errorCell.set(undefined));
  if (current()) safeSet(runtime, () => runtime.statusCell.set("open"));
  return cleanup;
}

function fail<T, V>(runtime: ProjectionRuntime<T, V>, reason: unknown): void {
  finish(runtime, true, reason);
}

function isCurrentGeneration<T, V>(
  runtime: ProjectionRuntime<T, V>,
  controller: AbortController,
  generation: number,
): boolean {
  return (
    !runtime.closed &&
    !runtime.source.retired &&
    runtime.positioned?.binding?.context.disposed !== true &&
    runtime.source.controller === controller &&
    runtime.source.generation === generation
  );
}

function accept<T, V>(
  runtime: ProjectionRuntime<T, V>,
  item: T,
  controller: AbortController,
  generation: number,
): void {
  if (!isCurrentGeneration(runtime, controller, generation)) return;
  const policy = runtime.policy;
  noteTransferChange(runtime);
  noteTransferChange(runtime.source);
  if (policy.kind === "reduce") {
    let next: V;
    try {
      next = policy.step(runtime.valueCell.peek(), item);
    } catch (error) {
      if (isCurrentGeneration(runtime, controller, generation)) fail(runtime, error);
      return;
    }
    if (!isCurrentGeneration(runtime, controller, generation)) return;
    safeSet(runtime, () => runtime.valueCell.set(next));
    return;
  }

  const current = runtime.valueCell.peek() as unknown as readonly T[];
  if (current.length >= policy.max) {
    if ((policy.onOverflow ?? "fail") === "fail") {
      fail(runtime, new Error("Aeolia projection accumulation exceeded max"));
      return;
    }
    safeSet(runtime, () => runtime.valueCell.set([...current.slice(1), item] as unknown as V));
    return;
  }
  safeSet(runtime, () => runtime.valueCell.set([...current, item] as unknown as V));
}

interface PositionedMemberRecord {
  readonly graphState: GraphRuntimeState<any>;
  readonly definition: PositionedStreamDefinition<any, any, any, any, any>;
  readonly provider: PositionedStreamSource<any, any, any, any>;
  readonly member: object;
  readonly binding?: StreamBindingRuntime;
}

function positionedMemberRecord(member: unknown): PositionedMemberRecord {
  if (typeof member === "function") {
    const raw = rawStreamMemberByFunction.get(member);
    if (raw === undefined || raw.definition.kind !== "positioned-stream")
      throw new TypeError("A positioned projection requires a genuine positioned raw member");
    const definition = raw.definition as PositionedStreamDefinition<any, any, any, any, any>;
    const provider = positionedStreamSourceByDefinition.get(definition);
    if (provider === undefined || !raw.state.positionedDefinitions.has(definition))
      throw new TypeError("The positioned raw member is not registered with this graph");
    return { graphState: raw.state, definition, provider, member };
  }
  if (member !== null && typeof member === "object") {
    const binding = streamBindingByHandle.get(member);
    if (binding !== undefined && binding.definition.kind === "positioned-stream") {
      const definition = binding.definition as PositionedStreamDefinition<any, any, any, any, any>;
      const provider = positionedStreamSourceByDefinition.get(definition);
      if (provider === undefined || !binding.context.state.positionedDefinitions.has(definition))
        throw new TypeError("The positioned binding is not registered with this graph");
      return { graphState: binding.context.state, definition, provider, member, binding };
    }
  }
  throw new TypeError("The value is not an authentic positioned stream member or binding");
}

function capturePositionedPolicy<T, V>(policy: ProjectionPolicy<T, V>): ProjectionPolicy<T, V> {
  const kind = policy.kind;
  if (kind === "reduce") {
    const initial = policy.initial;
    const step = policy.step;
    if (typeof step !== "function") throw new TypeError("Invalid positioned reducer policy.");
    return Object.freeze({ kind, initial, step });
  }
  if (kind !== "accumulate") throw new TypeError("Invalid positioned projection policy.");
  const max = policy.max;
  const onOverflow = policy.onOverflow;
  validateMax(max);
  if (onOverflow !== undefined && onOverflow !== "fail" && onOverflow !== "drop-oldest")
    throw new TypeError("Invalid positioned accumulation policy.");
  return Object.freeze({
    kind,
    max,
    ...(onOverflow === undefined ? {} : { onOverflow }),
  });
}

function assertPositionedAccumulationSeedWithinMax<T, V>(
  policy: ProjectionPolicy<T, V>,
  seedValue: unknown,
): void {
  if (policy.kind !== "accumulate") return;
  if (!Array.isArray(seedValue))
    throw new TypeError("Accumulated positioned restores require an array seed");
  const length = seedValue.length;
  if (!Number.isSafeInteger(length) || length < 0 || length > policy.max)
    throw new TypeError("Accumulated positioned restore seed exceeds its maximum");
}

function newPositionedState(
  record: PositionedMemberRecord,
  input: unknown,
  runtime: ProjectionRuntime<any, any>,
  preparation?: PositionedStorePreparation,
): PositionedProjectionRuntime {
  const value = runtime.policy.kind === "reduce" ? runtime.policy.initial : ([] as unknown[]);
  const positioned: PositionedProjectionRuntime = {
    graphState: record.graphState,
    graphEpoch: record.graphState.resetEpoch,
    definition: record.definition,
    provider: record.provider,
    input,
    policy: runtime.policy,
    hasKey: false,
    hasOpeningKey: false,
    member: record.member,
    ...(record.binding === undefined ? {} : { binding: record.binding }),
    tuple: {
      value,
      status: "open",
      error: undefined,
      hasPosition: false,
      position: undefined,
      generation: runtime.source.generation,
      revision: 0,
      resumable: false,
    },
    pendingPosition: undefined,
    hasPendingPosition: false,
    readerPresent: false,
    readerReleased: false,
    restored: false,
    transitioning: false,
  };
  noteTransferChange(runtime);
  runtime.positioned = positioned;
  if (record.binding !== undefined) {
    const close = (): void => finishPositioned(runtime, false);
    positioned.contextCleanup = close;
    if (preparation !== undefined && !preparation.published) {
      (preparation.contextStops ??= []).push(close);
    } else {
      const projections = record.binding.context.positionedProjections ?? new Set<() => void>();
      record.binding.context.positionedProjections = projections;
      projections.add(close);
    }
  }
  return positioned;
}

function writePositionedTuple<T, V>(
  runtime: ProjectionRuntime<T, V>,
  next: PositionedProjectionTuple<V, unknown>,
  current: () => boolean,
  receipt?: PositionedFoldReceipt<unknown>,
  onCommit?: () => void,
): boolean {
  const positioned = runtime.positioned;
  if (positioned === undefined) return false;
  let committed = false;
  try {
    reactiveInternal.batch(() => {
      const landed = reactiveInternal.guardedWrite(runtime.valueCell, next.value, current, () => {
        noteTransferChange(runtime);
        noteTransferChange(positioned);
        positioned.tuple = next;
        if (receipt !== undefined) {
          positioned.latestReceipt = receipt;
          positionedReceiptByHandle.set(receipt, {
            runtime: runtime as any,
            tuple: next as any,
            ...(positioned.storeGroup?.dispatch === undefined
              ? {}
              : { storeEvent: positioned.storeGroup.dispatch }),
          });
        }
        onCommit?.();
        committed = true;
      });
      if (!landed && !committed) return;
      runtime.statusCell.set(next.status);
      runtime.errorCell.set(next.error);
    });
  } catch (error) {
    if (committed) reportProjectionError(runtime, error);
    else throw error;
  }
  return committed;
}
function releasePositionedReader<T, V>(
  runtime: ProjectionRuntime<T, V>,
  source: ProjectionSource<T>,
): void {
  const positioned = runtime.positioned;
  if (positioned === undefined || !positioned.readerPresent || positioned.readerReleased) return;
  noteTransferChange(runtime);
  noteTransferChange(positioned);
  positioned.readerReleased = true;
  try {
    physicalReaderRelease(source.retirement, () =>
      positioned.provider.releaseReader(positioned.input, positioned.reader),
    );
  } catch (error) {
    reportContinuationError(positioned.graphState, error);
  }
}

function finishPositioned<T, V>(
  runtime: ProjectionRuntime<T, V>,
  failed: boolean,
  reason?: unknown,
  source = runtime.source,
  abortReason?: unknown,
): void {
  const positioned = runtime.positioned;
  if (
    positioned === undefined ||
    positioned.transitioning ||
    source !== runtime.source ||
    source.retired
  )
    return;
  noteTransferChange(runtime);
  noteTransferChange(source);
  noteTransferChange(positioned);
  if (positioned.binding !== undefined && positioned.storeGroup?.preparation?.published !== false)
    noteTransferChange(positioned.binding.handle);
  positioned.transitioning = true;
  if (positioned.storeGroup !== undefined && positioned.storeParticipant !== undefined) {
    noteTransferChange(positioned.storeGroup);
    noteTransferChange(positioned.storeParticipant);
    if (failed) {
      positioned.storeParticipant.failed = true;
      positioned.storeParticipant.failure = { cause: reason };
      positioned.storeGroup.diverged = true;
    } else positioned.storeGroup.participants.delete(positioned.storeParticipant);
  }
  runtime.closed = true;
  if (positioned.contextCleanup !== undefined) {
    positioned.binding?.context.positionedProjections?.delete(positioned.contextCleanup);
    positioned.contextCleanup = undefined;
  }
  const state = graphRuntimeByGraph.get(runtime.graph as object);
  const resetEpoch = state?.resetEpoch;
  const current = (): boolean =>
    runtime.source === source && state?.resetEpoch === resetEpoch && positioned.transitioning;
  const work = beginPhysicalWork(source.retirement, "cleanup");
  registerTransferWork(runtime, work);
  registerTransferWork(source, work);
  const terminalStatus: ProjectionStatus = failed ? "failed" : "closed";
  let cleanup: (() => void) | undefined;
  let cleanupFailed = false;
  try {
    cleanup = stopSource(source, abortReason);
    runtime.unregister?.();
    runtime.unregister = undefined;
    if (cleanup !== undefined) {
      try {
        cleanup();
      } catch (error) {
        cleanupFailed = true;
        reportProjectionError(runtime, error);
      }
    }
    const previous = positioned.tuple;
    noteTransferChange(runtime);
    noteTransferChange(source);
    noteTransferChange(positioned);
    const terminal: PositionedProjectionTuple<V, unknown> = {
      ...previous,
      status: terminalStatus,
      error: failed ? reason : undefined,
      generation: source.generation,
      resumable: !failed && !cleanupFailed && previous.hasPosition && previous.resumable,
    };
    try {
      writePositionedTuple(runtime, terminal, current);
    } catch (error) {
      reportProjectionError(runtime, error);
    }
  } finally {
    if (positioned.storeGroup === undefined) releasePositionedReader(runtime, source);
    noteTransferChange(runtime);
    noteTransferChange(positioned);
    positioned.transitioning = false;
    finishPhysicalWork(work);
  }
  if (
    state !== undefined &&
    !state.disposed &&
    positioned.storeGroup?.preparation?.published !== false &&
    runtime.closed &&
    runtime.source === source &&
    source.retired
  ) {
    if (state.resetEpoch !== resetEpoch)
      resetPositionedTerminal(runtime, state, state.resetEpoch, terminalStatus);
    const runtimeRef = new WeakRef(runtime);
    const stateRef = new WeakRef(state);
    registerTerminalReset(
      runtime.graph,
      terminalStatus,
      new WeakRef(runtime.valueCell),
      new WeakRef(runtime.statusCell),
      new WeakRef(runtime.errorCell),
      (nextEpoch) => {
        const currentRuntime = runtimeRef.deref();
        const currentState = stateRef.deref();
        return (
          currentRuntime !== undefined &&
          currentState !== undefined &&
          resetPositionedTerminal(currentRuntime, currentState, nextEpoch, terminalStatus)
        );
      },
    );
  }
}

function failPositioned<T, V>(
  runtime: ProjectionRuntime<T, V>,
  reason: unknown,
  source = runtime.source,
): void {
  finishPositioned(runtime, true, reason, source);
}

function acceptPositioned<T, V>(
  runtime: ProjectionRuntime<T, V>,
  item: T,
  position: unknown,
  controller: AbortController,
  generation: number,
): void {
  const positioned = runtime.positioned;
  if (
    positioned === undefined ||
    positioned.transitioning ||
    !isCurrentGeneration(runtime, controller, generation)
  )
    return;
  const previous = positioned.tuple;
  noteTransferChange(runtime);
  noteTransferChange(runtime.source);
  noteTransferChange(positioned);
  let nextValue: V;
  const policy = runtime.policy;
  if (policy.kind === "reduce") {
    try {
      nextValue = policy.step(previous.value as V, item);
    } catch (error) {
      if (isCurrentGeneration(runtime, controller, generation)) failPositioned(runtime, error);
      return;
    }
  } else {
    const current = previous.value as unknown as readonly T[];
    if (current.length >= policy.max) {
      if ((policy.onOverflow ?? "fail") === "fail") {
        failPositioned(runtime, new Error("Aeolia projection accumulation exceeded max"));
        return;
      }
      nextValue = [...current.slice(1), item] as unknown as V;
    } else nextValue = [...current, item] as unknown as V;
  }
  if (!isCurrentGeneration(runtime, controller, generation) || positioned.transitioning) return;
  const next: PositionedProjectionTuple<V, unknown> = {
    value: nextValue,
    status: "open",
    error: undefined,
    hasPosition: true,
    position,
    generation,
    revision: previous.revision + 1,
    resumable: true,
  };
  const receipt = Object.freeze({
    position,
    generation,
    revision: next.revision,
  }) as PositionedFoldReceipt<unknown>;
  try {
    writePositionedTuple(
      runtime,
      next,
      () =>
        isCurrentGeneration(runtime, controller, generation) &&
        !positioned.transitioning &&
        positioned.tuple === previous,
      receipt,
      () => {
        const group = positioned.storeGroup;
        const participant = positioned.storeParticipant;
        const event = group?.dispatch;
        if (
          group !== undefined &&
          participant !== undefined &&
          event !== undefined &&
          Object.is(event.position, position) &&
          Object.is(event.latest, item)
        ) {
          noteTransferChange(group);
          noteTransferChange(participant);
          participant.event = event;
          participant.anchor = event.identity;
        }
      },
    );
  } catch (error) {
    if (positioned.tuple !== next) failPositioned(runtime, error);
    else reportProjectionError(runtime, error);
  }
}

async function consume<T, V>(
  runtime: ProjectionRuntime<T, V>,
  controller: AbortController,
  generation: number,
): Promise<void> {
  const iterator = runtime.source.iterator;
  if (iterator === undefined) return;
  try {
    while (isCurrentGeneration(runtime, controller, generation)) {
      const result = await iterator.next();
      if (!isCurrentGeneration(runtime, controller, generation)) return;
      if (result.done) {
        finish(runtime, false);
        return;
      }
      if (runtime.positioned !== undefined) {
        if (!runtime.positioned.hasPendingPosition) {
          failPositioned(runtime, new Error("Positioned stream event is missing its position"));
          return;
        }
        const position = runtime.positioned.pendingPosition;
        runtime.positioned.hasPendingPosition = false;
        acceptPositioned(runtime, result.value, position, controller, generation);
      } else accept(runtime, result.value, controller, generation);
    }
  } catch (error) {
    if (isCurrentGeneration(runtime, controller, generation)) fail(runtime, error);
  }
}

function initializeProjection<T, V>(
  graph: Graph,
  policy: ProjectionPolicy<T, V>,
  retirement?: RetirementOwner,
  preparation?: PositionedStorePreparation,
): { runtime: ProjectionRuntime<T, V>; projection: Projection<V> } {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  const valueCell = signal<V>(policy.kind === "reduce" ? policy.initial : ([] as unknown as V));
  const statusCell = signal<ProjectionStatus>("open");
  const errorCell = signal<unknown>(undefined);
  const controller = new AbortController();
  const runtime: ProjectionRuntime<T, V> = {
    graph,
    valueCell,
    statusCell,
    errorCell,
    policy,
    source: {
      controller,
      generation: 0,
      retired: false,
      retirement: retirement ?? state.retirement,
    },
    closed: false,
  };
  registerProjectionFacet(runtime, valueCell, "projection-value");
  registerProjectionFacet(runtime, statusCell, "projection-status");
  registerProjectionFacet(runtime, errorCell, "projection-error");
  if (policy.kind === "reduce") projectionInitialByValueCell.set(valueCell, policy.initial);

  const close = (): void => finish(runtime, false);
  const projection: Projection<V> = Object.freeze({
    value: valueCell,
    status: statusCell,
    error: errorCell,
    close,
  });

  const registration: GraphProjection = {
    captureCancellation: () => {
      const source = runtime.source;
      return runtime.closed || source.retired
        ? undefined
        : (reason) => finish(runtime, false, undefined, source, reason);
    },
    reset: () => {
      const cleanup = prepareReset(runtime);
      if (runtime.boundSession !== undefined) finish(runtime, false);
      return cleanup;
    },
    restart: () => {
      if (runtime.positioned?.restored === true) return;
      if (!runtime.closed) runtime.restartSource?.();
    },
    dispose: close,
  };
  if (preparation !== undefined && !preparation.published) {
    let attached = false;
    preparation.registrations.push(registration);
    (preparation.publicationMarkers ??= []).push(() => {
      attached = true;
    });
    runtime.unregister = () => {
      if (attached && state.projections.has(registration)) {
        noteTransferChange(state);
        noteTransferChange(registration);
        state.projections.delete(registration);
      }
    };
  } else runtime.unregister = registerProjection(graph, registration);
  return { runtime, projection };
}

function makeProjection<T, V>(
  graph: Graph,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  policy: ProjectionPolicy<T, V>,
): Projection<V> {
  const { runtime, projection } = initializeProjection(graph, policy);
  runtime.restartSource = () => openIndependent(runtime, graph, stream, input);
  openIndependent(runtime, graph, stream, input, true);
  return projection;
}

function openIndependent<T, V>(
  runtime: ProjectionRuntime<T, V>,
  graph: Graph,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  propagateKeyError = false,
): void {
  const source = runtime.source;
  if (runtime.closed || source.retired) return;
  const state = graphRuntimeByGraph.get(graph as object)!;
  assertGraphOpen(state);
  const physical = beginPhysicalWork(source.retirement, "projection");
  registerTransferWork(runtime, physical);
  registerTransferWork(source, physical);
  let admission: ReturnType<typeof beginTransferAdmission> | undefined;
  let observed = false;
  try {
    admission = beginTransferAdmission(state, stream, runtime.positioned?.binding?.handle, input);
    const controller = source.controller;
    const generation = source.generation;
    let key: ReturnType<typeof storeKey>;
    try {
      const positioned = runtime.positioned;
      if (positioned?.hasOpeningKey === true) {
        key = positioned.openingKey!;
        positioned.hasOpeningKey = false;
        positioned.key = key;
        positioned.hasKey = true;
      } else {
        const keyForInput = stream.key;
        if (state.disposed || !isCurrentGeneration(runtime, controller, generation)) return;
        key = storeKey(Reflect.apply(keyForInput, stream, [input]));
        if (positioned !== undefined) {
          positioned.key = key;
          positioned.hasKey = true;
        }
      }
    } catch (error) {
      if (!isCurrentGeneration(runtime, controller, generation)) return;
      if (propagateKeyError) {
        runtime.unregister?.();
        runtime.unregister = undefined;
        noteTransferChange(runtime);
        noteTransferChange(source);
        runtime.closed = true;
        stopSource(source)?.();
        throw error;
      }
      fail(runtime, error);
      return;
    }
    if (!isCurrentGeneration(runtime, controller, generation)) return;
    const options: OpenOptions = {
      abortSignal: controller.signal,
      graph: graph.id,
      key,
      reportGap: () => {
        if (isCurrentGeneration(runtime, controller, generation))
          fail(runtime, new Error("Aeolia projection stream reported a gap"));
      },
    };
    if (runtime.positioned !== undefined) {
      const positioned = runtime.positioned;
      positionedPositionWriterByOpenOptions.set(options, (position) => {
        if (
          runtime.positioned !== positioned ||
          !isCurrentGeneration(runtime, controller, generation) ||
          runtime.closed ||
          positioned.transitioning
        )
          return;
        noteTransferChange(runtime);
        noteTransferChange(source);
        noteTransferChange(positioned);
        positioned.pendingPosition = position;
        positioned.hasPendingPosition = true;
      });
      if (runtime.positioned.openingRequest !== undefined)
        positionedOpenRequestByOpenOptions.set(options, runtime.positioned.openingRequest);
    }
    let iterable: AsyncIterable<T>;
    try {
      const open = stream.open;
      if (state.disposed || !isCurrentGeneration(runtime, controller, generation)) return;
      iterable = Reflect.apply(open, stream, [input, options]);
      if (!isCurrentGeneration(runtime, controller, generation)) return;
      const iterator = iterable[Symbol.asyncIterator]();
      if (!isCurrentGeneration(runtime, controller, generation)) {
        returnProjectionIterator(source, iterator);
        return;
      }
      noteTransferChange(runtime);
      noteTransferChange(source);
      source.iterator = iterator;
    } catch (error) {
      if (!isCurrentGeneration(runtime, controller, generation)) return;
      fail(runtime, error);
      return;
    }
    observePhysicalWork(physical, consume(runtime, controller, generation));
    observed = true;
  } finally {
    try {
      if (admission !== undefined) finishTransferAdmission(admission);
    } finally {
      if (!observed) finishPhysicalWork(physical);
    }
  }
}

function makeSharedProjection<T, V>(
  source: StreamStore<T>,
  policy: ProjectionPolicy<T, V>,
  detached = false,
): Projection<V> {
  const store = (detached ? runtimeByStore.get(source) : runtimeForStore(source)) as
    | StoreRuntime<T>
    | undefined;
  const bound = boundStreamByStore.get(source);
  if (
    store === undefined ||
    (store.binding?.kind === "stream" ? bound === undefined : source.status !== store.streamStatus)
  )
    throw new TypeError("A shared projection requires a genuine Aeolia StreamStore");
  assertStoreOpen(store);
  if (bound !== undefined) {
    assertBindingContextOpen(bound.binding.context);
    if (bound.phase === "retiring")
      throw new TypeError("The stream is completing native retirement");
  }
  const sourceDefinition = bound?.binding.definition ?? store.streamSource?.stream;
  const positionedDefinition =
    sourceDefinition?.kind === "positioned-stream" &&
    store.graph.__runtime.positionedDefinitions.has(sourceDefinition)
      ? sourceDefinition
      : undefined;
  const projectionPolicy =
    positionedDefinition === undefined ? policy : capturePositionedPolicy(policy);
  const { runtime, projection } = initializeProjection<T, V>(
    store.graph,
    projectionPolicy,
    store.stream?.physical.owner ?? store.graph.__runtime.retirement,
    detached ? store.stream?.positioned?.preparation : undefined,
  );
  projectionSourceRuntimeByOwner.set(runtime, store as StoreRuntime<unknown>);
  if (bound !== undefined) runtime.boundSession = bound as unknown as BoundStreamSessionRuntime<T>;
  const sourceMember = bound?.binding.handle ?? store.streamSource?.member;
  if (
    sourceDefinition?.kind === "positioned-stream" &&
    sourceMember !== undefined &&
    store.graph.__runtime.positionedDefinitions.has(sourceDefinition)
  ) {
    const provider = positionedStreamSourceByDefinition.get(sourceDefinition);
    if (provider !== undefined) {
      newPositionedState(
        {
          graphState: store.graph.__runtime,
          definition: sourceDefinition,
          provider,
          member: sourceMember,
          ...(bound === undefined ? {} : { binding: bound.binding }),
        },
        store.streamSource?.input,
        runtime,
        detached ? store.stream?.positioned?.preparation : undefined,
      );
      if (store.streamSource !== undefined) {
        runtime.positioned!.key = store.key;
        runtime.positioned!.hasKey = true;
      }
      const group = store.stream?.positioned;
      if (group !== undefined && group.runtime === store) {
        const participant: PositionedStoreParticipant = {
          projection,
          ...(group.completed === undefined ? {} : { event: group.completed }),
          ...(group.continuation === undefined ? {} : { anchor: group.continuation.token }),
          failed: false,
        };
        noteTransferChange(group);
        group.participants.add(participant);
        runtime.positioned!.storeGroup = group;
        runtime.positioned!.storeParticipant = participant;
      }
      positionedProjectionByHandle.set(
        projection,
        runtime as unknown as ProjectionRuntime<unknown, unknown>,
      );
    }
  }
  runtime.restartSource = () => attachSharedProjection(runtime, store, source);
  attachSharedProjection(runtime, store, source);
  return projection;
}

function attachSharedProjection<T, V>(
  runtime: ProjectionRuntime<T, V>,
  store: StoreRuntime<T>,
  source: StreamStore<T>,
): void {
  if (runtime.closed || runtime.source.retired || runtime.source.detach !== undefined) return;
  const generationSource = runtime.source;
  const bound = runtime.boundSession;
  const session = bound?.session ?? store.stream;
  if (session === undefined || session.ended) {
    const failed = source.status.peek() === "failed";
    finish(runtime, failed, failed ? source.error.peek() : undefined);
    return;
  }
  const controller = generationSource.controller;
  const generation = generationSource.generation;
  const listener = (event: StreamEvent<unknown>): void => {
    if (
      !isCurrentGeneration(runtime, controller, generation) ||
      store.stream !== session ||
      session.resetEpoch !== store.graph.__runtime.resetEpoch ||
      (bound !== undefined &&
        event.kind !== "close" &&
        (bound.phase !== "active" || store.incarnation !== bound.incarnation))
    )
      return;
    if (event.kind === "value") {
      if (runtime.positioned !== undefined) {
        if (event.hasPosition !== true) {
          failPositioned(runtime, new Error("Positioned stream event is missing its position"));
          return;
        }
        acceptPositioned(runtime, event.value as T, event.position, controller, generation);
      } else accept(runtime, event.value as T, controller, generation);
    } else if (event.kind === "gap")
      fail(runtime, new Error("Aeolia projection stream reported a gap"));
    else finish(runtime, event.failed, event.reason);
  };
  noteTransferChange(runtime);
  noteTransferChange(generationSource);
  noteTransferChange(store);
  noteTransferChange(session);
  registerTransferWork(runtime, session.physical);
  registerTransferWork(generationSource, session.physical);
  const nextListeners = new Set(session.listeners);
  nextListeners.add(listener);
  session.listeners = nextListeners;
  session.unobservedSince = undefined;
  if (session.positioned?.preparation?.published !== false) {
    updateCollectionCandidate(store);
    scheduleSweep(store.graph.__runtime);
  }
  generationSource.detach = () => {
    noteTransferChange(runtime);
    noteTransferChange(generationSource);
    noteTransferChange(store);
    noteTransferChange(session);
    session.listeners.delete(listener);
    if (store.stream === session && !isLive(store)) session.unobservedSince = Date.now();
    if (session.positioned?.preparation?.published !== false) {
      updateCollectionCandidate(store);
      scheduleSweep(store.graph.__runtime);
    }
  };
}

/**
 * Opens a positioned stream through an authentic raw member or bound stream
 * binding. The projection records a cursor only after its reducer/value landing
 * succeeds. A member created by copying a declaration does not grant admission.
 *
 * @param member - Exact graph member or native binding for the declaration.
 * @param input - Input passed to the provider source.
 * @param policy - Existing bounded accumulation or reduction policy.
 * @returns Projection with non-pulling positioned capture support.
 * @throws TypeError when member provenance or policy is invalid.
 * @throws Fault when its graph is disposed or an accumulation bound is invalid.
 */
export function projectPositioned<Input, Event, Position, Reader>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policy: AccumulatePolicy,
): Projection<readonly Event[]>;
export function projectPositioned<Input, Event, Position, Reader, Value>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policy: ReducePolicy<Event, Value>,
): Projection<Value>;
export function projectPositioned<Input, Event, Position, Reader, Value>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policy: ProjectionPolicy<Event, Value>,
): Projection<Value> {
  const record = positionedMemberRecord(member);
  if (record.binding !== undefined) assertBindingContextOpen(record.binding.context);
  else assertGraphOpen(record.graphState);
  const capturedPolicy = capturePositionedPolicy(policy);
  const physical = beginPhysicalWork(
    record.binding?.context.retirement ?? record.graphState.retirement,
    "acquisition",
  );
  try {
    if (record.binding !== undefined) assertBindingContextOpen(record.binding.context);
    else assertGraphOpen(record.graphState);
    const graph = record.graphState.graph;
    const { runtime, projection } = initializeProjection<Event, Value>(
      graph,
      capturedPolicy,
      record.binding?.context.retirement,
    );
    newPositionedState(record, input, runtime);
    positionedProjectionByHandle.set(
      projection,
      runtime as unknown as ProjectionRuntime<unknown, unknown>,
    );
    runtime.restartSource = () => openIndependent(runtime, graph, record.definition, input);
    openIndependent(runtime, graph, record.definition, input, true);
    return projection;
  } finally {
    finishPhysicalWork(physical);
  }
}

/**
 * Captures a coherent native fold tuple without opening, pulling, subscribing,
 * or retaining a provider reader. Payload and position are borrowed references.
 * A failed reducer is never resumable because it may have mutated its previous
 * accumulator before throwing.
 *
 * @param projection - Exact projection returned by projectPositioned or project
 * over an active positioned session.
 * @returns Frozen tuple view with a native identity used by the currentness check.
 * @throws TypeError for a projection without native positioned state.
 */
export function capturePositionedProjection<Value, Position>(
  projection: Projection<Value>,
): PositionedProjectionCheckpoint<Value, Position> {
  const runtime = positionedProjectionByHandle.get(projection as object) as
    | ProjectionRuntime<Value, unknown>
    | undefined;
  const positioned = runtime?.positioned;
  if (runtime === undefined || positioned === undefined)
    throw new TypeError("Projection has no positioned native state");
  const tuple = positioned.tuple;
  const checkpoint = Object.freeze({
    value: tuple.value as Value,
    status: tuple.status,
    error: tuple.error,
    hasPosition: tuple.hasPosition,
    position: tuple.position as Position | undefined,
    generation: tuple.generation,
    revision: tuple.revision,
    resumable: tuple.resumable && !positioned.transitioning,
  }) as PositionedProjectionCheckpoint<Value, Position>;
  positionedCheckpointByHandle.set(checkpoint as object, {
    runtime: runtime as unknown as ProjectionRuntime<unknown, unknown>,
    tuple: tuple as PositionedProjectionTuple<unknown, unknown>,
  });
  return checkpoint;
}

/**
 * Rechecks an authentic capture without pulling or consulting the provider.
 * @param checkpoint - Exact capture returned by capturePositionedProjection.
 * @returns True while the native tuple and graph generation remain current.
 */
export function isPositionedCheckpointCurrent<Value, Position>(
  checkpoint: PositionedProjectionCheckpoint<Value, Position>,
): boolean {
  const captured = positionedCheckpointByHandle.get(checkpoint as object);
  const runtime = captured?.runtime;
  const positioned = runtime?.positioned;
  return (
    captured !== undefined &&
    runtime !== undefined &&
    positioned !== undefined &&
    positioned.tuple === captured.tuple &&
    !positioned.transitioning &&
    (!runtime.closed || positioned.tuple.status !== "open") &&
    !positioned.graphState.disposed &&
    positioned.binding?.context.disposed !== true &&
    positioned.graphState.resetEpoch === positioned.graphEpoch &&
    runtime.source.generation === positioned.tuple.generation
  );
}

function positionedStoreCaptureCurrent(
  capture: PositionedStoreCheckpointCapture,
  now = readerLifetimeNow(),
): boolean {
  const { source, runtime, session, group } = capture;
  try {
    assertStoreHandlePublished(source);
  } catch {
    return false;
  }
  if (
    runtimeByStore.get(source) !== runtime ||
    runtime.stream !== session ||
    session.ended ||
    session.positioned !== group ||
    session.hasPosition !== capture.hasPosition ||
    !Object.is(session.position, capture.position) ||
    group.runtime !== runtime ||
    group.completed !== capture.completed ||
    group.continuation !== capture.continuation ||
    group.preparation?.lifetime?.currentAt(now) === false ||
    group.dispatch !== capture.dispatch ||
    group.baseRevision !== capture.baseRevision ||
    group.diverged !== capture.diverged ||
    runtime.valueRevision !== capture.valueRevision ||
    runtime.graph.__runtime.resetEpoch !== capture.graphEpoch ||
    runtime.graph.__runtime.disposed ||
    runtime.disposed ||
    runtime.binding?.context.disposed === true
  )
    return false;
  for (let index = 0; index < capture.projections.length; index++) {
    const item = capture.projections[index]!;
    const positioned = item.runtime.positioned;
    if (
      positionedProjectionByHandle.get(item.handle) !== item.runtime ||
      positioned === undefined ||
      positioned.storeGroup !== group ||
      positioned.storeParticipant !== item.participant ||
      positioned.tuple !== item.tuple ||
      !group.participants.has(item.participant) ||
      item.participant.event !== item.event ||
      item.participant.failed !== item.failed
    )
      return false;
  }
  return true;
}

function positionedStoreCutResumable(capture: PositionedStoreCheckpointCapture): boolean {
  const { runtime, session, group, completed, continuation } = capture;
  if (
    continuation === undefined ||
    capture.dispatch !== undefined ||
    capture.diverged ||
    runtime.invalidated ||
    group.phase === "closed"
  )
    return false;
  const predictions = reactiveInternal.signalValue(runtime.predictionStack);
  for (let index = 0; index < predictions.length; index++)
    if (!predictions[index]!.dead) return false;
  if (
    completed !== undefined &&
    (group.baseRevision !== completed.landingRevision ||
      !session.hasPosition ||
      !Object.is(session.position, completed.position))
  )
    return false;
  if (completed === undefined && continuation.kind === "event" && runtime.readerScope === undefined)
    return false;
  for (let index = 0; index < capture.projections.length; index++) {
    const item = capture.projections[index]!;
    const positioned = item.runtime.positioned;
    if (
      positioned === undefined ||
      item.tuple.status !== "open" ||
      item.failed ||
      positioned.transitioning ||
      item.tuple.generation !== item.runtime.source.generation
    )
      return false;
    if (completed !== undefined) {
      if (
        !item.tuple.hasPosition ||
        !Object.is(item.tuple.position, completed.position) ||
        !item.tuple.resumable ||
        item.event !== completed
      )
        return false;
    } else if (item.participant.anchor !== continuation.token) return false;
    else if (continuation.kind === "initial") {
      if (item.tuple.hasPosition || item.tuple.revision !== 0) return false;
    } else if (
      !item.tuple.hasPosition ||
      !item.tuple.resumable ||
      !Object.is(item.tuple.position, continuation.after)
    )
      return false;
  }
  return true;
}

/**
 * Captures one actual positioned StreamStore session and its required shared folds.
 *
 * This operation does not pull the source or call the provider. The returned
 * values and latest Event are borrowed. A capture is resumable only when the
 * source event has completed, every supplied projection folded that same event,
 * and the visible Store has no live prediction overlay or divergent base write.
 * Later native changes make the exact checkpoint stale; mutations inside
 * borrowed user values are not detected.
 *
 * @param source - The exact genuine positioned StreamStore.
 * @param projections - Exact Projection handles created from this source session.
 * @returns Frozen native identity and borrowed Event/fold values; eligibility is indicated by resumable.
 * @throws TypeError when the source or a supplied projection lacks matching native session provenance.
 * @throws Fault when the Store graph or binding context is disposed.
 */
export function capturePositionedStore<Event, const Projections extends readonly Projection<any>[]>(
  source: StreamStore<Event>,
  projections: Projections,
): PositionedStoreCheckpoint<Event, PositionedStoreProjectionValues<Projections>> {
  assertStoreHandlePublished(source as object);
  const runtime = runtimeByStore.get(source as object) as StoreRuntime<Event> | undefined;
  const bound = boundStreamByStore.get(source as object);
  if (
    runtime === undefined ||
    (runtime.binding?.kind === "stream"
      ? bound === undefined
      : source.status !== runtime.streamStatus)
  )
    throw new TypeError("A positioned Store capture requires an authentic StreamStore");
  assertStoreOpen(runtime);
  if (bound !== undefined) {
    assertBindingContextOpen(bound.binding.context);
    if (bound.phase !== "active")
      throw new TypeError("A positioned Store capture requires its active native session");
  }
  const session = runtime.stream;
  const group = session?.positioned;
  if (
    session === undefined ||
    group === undefined ||
    group.runtime !== runtime ||
    group.definition.kind !== "positioned-stream"
  )
    throw new TypeError("The StreamStore has no native positioned source session");
  if (!Array.isArray(projections))
    throw new TypeError("Required positioned projections must be an array");
  const lengthDescriptor = Object.getOwnPropertyDescriptor(projections, "length");
  if (
    lengthDescriptor === undefined ||
    !("value" in lengthDescriptor) ||
    !Number.isSafeInteger(lengthDescriptor.value) ||
    lengthDescriptor.value < 0
  )
    throw new TypeError("Invalid required positioned projection tuple");
  const projectionCaptures: PositionedStoreProjectionCapture[] = [];
  const capturedValues: unknown[] = [];
  for (let index = 0; index < lengthDescriptor.value; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(projections, String(index));
    if (descriptor === undefined || !("value" in descriptor))
      throw new TypeError("Required positioned projections must be own data entries");
    const handle = descriptor.value;
    const projectionRuntime =
      handle !== null && (typeof handle === "object" || typeof handle === "function")
        ? positionedProjectionByHandle.get(handle as object)
        : undefined;
    const positioned = projectionRuntime?.positioned;
    const participant = positioned?.storeParticipant;
    if (
      projectionRuntime === undefined ||
      positioned?.storeGroup !== group ||
      participant === undefined ||
      participant.projection !== handle ||
      !group.participants.has(participant)
    )
      throw new TypeError("A required projection does not belong to this positioned Store session");
    projectionCaptures.push({
      handle: handle as object,
      runtime: projectionRuntime as ProjectionRuntime<unknown, unknown>,
      tuple: positioned.tuple as PositionedProjectionTuple<unknown, unknown>,
      participant,
      ...(participant.event === undefined ? {} : { event: participant.event }),
      failed: participant.failed,
    });
    capturedValues.push(positioned.tuple.value);
  }
  const completed = group.completed;
  const capture: PositionedStoreCheckpointCapture = {
    source: source as object,
    runtime: runtime as StoreRuntime<unknown>,
    session: session as StreamSession<unknown>,
    group: group as PositionedStoreSessionState<unknown>,
    ...(completed === undefined ? {} : { completed }),
    ...(group.continuation === undefined ? {} : { continuation: group.continuation }),
    ...(group.dispatch === undefined ? {} : { dispatch: group.dispatch }),
    ...(group.baseRevision === undefined ? {} : { baseRevision: group.baseRevision }),
    diverged: group.diverged,
    hasPosition: session.hasPosition,
    position: session.position,
    valueRevision: runtime.valueRevision,
    graphEpoch: runtime.graph.__runtime.resetEpoch,
    projections: Object.freeze(projectionCaptures),
    values: Object.freeze(capturedValues),
  };
  const hasPosition = completed !== undefined;
  const resumable = positionedStoreCaptureCurrent(capture) && positionedStoreCutResumable(capture);
  const checkpoint = Object.freeze({
    sourceIdentity: runtime.identity,
    sessionIdentity: session.token,
    present: hasPosition,
    ...(hasPosition ? { latest: completed.latest } : {}),
    hasPosition,
    position: hasPosition ? completed.position : undefined,
    ...(group.continuation === undefined
      ? {}
      : {
          continuation: Object.freeze({
            kind: group.continuation.kind,
            after: group.continuation.after,
          }),
        }),
    values: capture.values,
    resumable,
  }) as PositionedStoreCheckpoint<Event, PositionedStoreProjectionValues<Projections>>;
  positionedStoreCheckpointByHandle.set(checkpoint as object, capture);
  return checkpoint;
}

/** A non-authoritative indication that a positioned Store capture should be rechecked. */
export type PositionedStoreCaptureNotice =
  | { readonly kind: "changed"; readonly revision: object }
  | { readonly kind: "closed" }
  | { readonly kind: "invalidated" }
  | { readonly kind: "failed"; readonly cause: unknown };

/**
 * Observes changes to the native session named by an authentic Store checkpoint.
 *
 * A changed notice schedules a fresh capture; it does not establish currentness,
 * resumability, transfer, or publication. The initial notice is delivered
 * synchronously by subscribe. Later notices are delivered after native changes
 * settle. Terminal notices remain sticky and failure retains the original cause.
 * Observation does not retain, open, or stop the source. Unsubscribing removes
 * the registration and its reader deadline task.
 *
 * @param checkpoint - Exact checkpoint returned by capturePositionedStore.
 * @returns An Aeolia readable for this native session's capture-state notices.
 * @throws TypeError if the checkpoint is not authentic.
 */
export function observePositionedStoreCaptureState<Event, Values extends readonly unknown[]>(
  checkpoint: PositionedStoreCheckpoint<Event, Values>,
): Readable<PositionedStoreCaptureNotice> {
  if (checkpoint === null || typeof checkpoint !== "object")
    throw new TypeError("An authentic positioned Store checkpoint is required");
  const capture = positionedStoreCheckpointByHandle.get(checkpoint as object);
  if (capture === undefined)
    throw new TypeError("An authentic positioned Store checkpoint is required");
  return createPositionedCaptureState({
    source: new WeakRef(capture.source),
    runtime: new WeakRef(capture.runtime),
    session: new WeakRef(capture.session),
    group: new WeakRef(capture.group),
    graphEpoch: capture.graphEpoch,
    projections: capture.projections.map((item) => ({
      handle: new WeakRef(item.handle),
      runtime: new WeakRef(item.runtime),
      participant: new WeakRef(item.participant),
    })),
  });
}

interface PositionedCaptureObservation {
  readonly source: WeakRef<object>;
  readonly runtime: WeakRef<StoreRuntime<unknown>>;
  readonly session: WeakRef<StreamSession<unknown>>;
  readonly group: WeakRef<PositionedStoreSessionState<unknown>>;
  readonly graphEpoch: number;
  readonly projections: readonly {
    readonly handle: WeakRef<object>;
    readonly runtime: WeakRef<ProjectionRuntime<unknown, unknown>>;
    readonly participant: WeakRef<PositionedStoreParticipant>;
  }[];
}

function createPositionedCaptureState(
  refs: PositionedCaptureObservation,
): Readable<PositionedStoreCaptureNotice> {
  let active = false;
  let generation = 0;
  let queued = false;
  let stopChanges: (() => void) | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let terminal: PositionedStoreCaptureNotice | undefined;
  const changed = (): PositionedStoreCaptureNotice => ({ kind: "changed", revision: {} });
  const currentNotice = (): PositionedStoreCaptureNotice => {
    if (terminal !== undefined) return terminal;
    const source = refs.source.deref();
    const runtime = refs.runtime.deref();
    const session = refs.session.deref();
    const group = refs.group.deref();
    if (
      source === undefined ||
      runtime === undefined ||
      session === undefined ||
      group === undefined
    )
      return (terminal = { kind: "invalidated" });
    if (group.captureTerminal !== undefined) return (terminal = group.captureTerminal);
    try {
      assertStoreHandlePublished(source);
    } catch {
      return (terminal = { kind: "invalidated" });
    }
    const graphState = runtime.graph.__runtime;
    const lifetime = group.preparation?.lifetime;
    if (
      runtimeByStore.get(source) !== runtime ||
      group.runtime !== runtime ||
      runtime.stream !== session ||
      session.ended ||
      session.positioned !== group ||
      group.phase === "closed" ||
      graphState.disposed ||
      runtime.disposed ||
      runtime.binding?.context.disposed === true ||
      graphState.resetEpoch !== refs.graphEpoch ||
      (lifetime !== undefined && !lifetime.currentAt(readerLifetimeNow()))
    )
      return (terminal = { kind: "invalidated" });
    for (const item of refs.projections) {
      const handle = item.handle.deref();
      const projection = item.runtime.deref();
      const participant = item.participant.deref();
      if (handle === undefined || projection === undefined || participant === undefined)
        return (terminal = { kind: "invalidated" });
      const positioned = projection.positioned;
      if (participant.failed)
        return (terminal =
          participant.failure === undefined
            ? { kind: "invalidated" }
            : { kind: "failed", cause: participant.failure.cause });
      if (positioned?.tuple.status === "failed")
        return (terminal = { kind: "failed", cause: positioned.tuple.error });
      if (
        positionedProjectionByHandle.get(handle) !== projection ||
        positioned === undefined ||
        positioned.storeGroup !== group ||
        positioned.storeParticipant !== participant ||
        !group.participants.has(participant) ||
        positioned.tuple.status === "closed"
      )
        return (terminal = { kind: "invalidated" });
    }
    return changed();
  };
  const detach = (): void => {
    stopChanges?.();
    stopChanges = undefined;
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const deliver = (): void => {
    const notice = currentNotice();
    if (notice.kind !== "changed") detach();
    let deliveryFailed = false;
    let deliveryError: unknown;
    try {
      cell.set(notice);
    } catch (error) {
      deliveryFailed = true;
      deliveryError = error;
    }
    try {
      if (notice.kind === "changed" && timer === undefined) scheduleDeadline();
    } catch (error) {
      if (!deliveryFailed) throw error;
      const state = refs.runtime.deref()?.graph.__runtime;
      if (state !== undefined) {
        try {
          reportContinuationError(state, error);
        } catch {
          // Preserve the subscriber failure if reporting the deadline failure also fails.
        }
      }
    }
    if (deliveryFailed) throw deliveryError;
  };
  const schedule = (): void => {
    if (!active || queued || terminal !== undefined) return;
    queued = true;
    const scheduledGeneration = generation;
    queueMicrotask(() => {
      if (!active || generation !== scheduledGeneration) return;
      queued = false;
      try {
        deliver();
      } catch (error) {
        const state = refs.runtime.deref()?.graph.__runtime;
        if (state !== undefined) reportContinuationError(state, error);
      }
    });
  };
  const scheduleDeadline = (): void => {
    const expiresAt = refs.group.deref()?.preparation?.lifetime?.expiresAt;
    if (!active || terminal !== undefined || expiresAt === undefined || timer !== undefined) return;
    const delay = expiresAt - readerLifetimeNow();
    if (delay <= 0) {
      schedule();
      return;
    }
    timer = setTimeout(
      () => {
        timer = undefined;
        if (!active || terminal !== undefined) return;
        if (readerLifetimeNow() < expiresAt) scheduleDeadline();
        else schedule();
      },
      Math.min(Math.ceil(delay), 2_147_483_647),
    );
  };
  const cell = signal<PositionedStoreCaptureNotice>(changed(), {
    [Signal.subtle.watched]() {
      active = true;
      generation += 1;
      const runtime = refs.runtime.deref();
      const session = refs.session.deref();
      const group = refs.group.deref();
      const targets: object[] = [];
      if (runtime !== undefined) {
        targets.push(runtime, runtime.graph.__runtime);
        if (runtime.binding !== undefined) targets.push(runtime.binding, runtime.binding.context);
      }
      if (session !== undefined) targets.push(session);
      if (group !== undefined) targets.push(group);
      for (const item of refs.projections) {
        const projection = item.runtime.deref();
        const participant = item.participant.deref();
        if (projection !== undefined) {
          targets.push(projection);
          if (projection.positioned !== undefined) targets.push(projection.positioned);
        }
        if (participant !== undefined) targets.push(participant);
      }
      stopChanges = observePositionedTransferChanges(targets, schedule);
      deliver();
    },
    [Signal.subtle.unwatched]() {
      active = false;
      generation += 1;
      queued = false;
      detach();
    },
  });
  return cell;
}

/**
 * Rechecks whether an exact positioned Store checkpoint still names its captured native state.
 *
 * This check does not pull, consult the provider, or inspect borrowed values.
 * A current checkpoint may still be nonresumable; inspect its resumable field
 * before preparing a transfer.
 *
 * @param checkpoint - Exact value returned by capturePositionedStore.
 * @returns True while its source session, Store revision, group state, and projection tuples remain current.
 */
export function isPositionedStoreCheckpointCurrent(
  checkpoint: PositionedStoreCheckpoint<any, readonly unknown[]>,
): boolean {
  if (checkpoint === null || typeof checkpoint !== "object") return false;
  const capture = positionedStoreCheckpointByHandle.get(checkpoint as object);
  if (capture === undefined || !positionedStoreCaptureCurrent(capture)) return false;
  return (
    checkpoint.sourceIdentity === capture.runtime.identity &&
    checkpoint.sessionIdentity === capture.session.token &&
    checkpoint.values === capture.values &&
    checkpoint.hasPosition === (capture.completed !== undefined) &&
    Object.is(checkpoint.position, capture.completed?.position) &&
    checkpoint.resumable ===
      (positionedStoreCaptureCurrent(capture) && positionedStoreCutResumable(capture)) &&
    (capture.completed === undefined
      ? checkpoint.present === false && !Object.hasOwn(checkpoint, "latest")
      : checkpoint.present === true &&
        Object.hasOwn(checkpoint, "latest") &&
        Object.is(checkpoint.latest, capture.completed.latest))
  );
}

function readOwnDataArray(value: unknown, length: number, label: string): unknown[] {
  if (!Array.isArray(value)) throw new TypeError(label + " must be an array");
  const descriptor = Object.getOwnPropertyDescriptor(value, "length");
  if (descriptor === undefined || !("value" in descriptor) || descriptor.value !== length)
    throw new TypeError(label + " has the wrong length");
  const result: unknown[] = [];
  for (let index = 0; index < length; index += 1) {
    const item = Object.getOwnPropertyDescriptor(value, String(index));
    if (item === undefined || !("value" in item))
      throw new TypeError(label + " must contain own data entries");
    result.push(item.value);
  }
  return result;
}

function capturePositionedStoreOptions<T>(
  options: ValueOptions<T> | undefined,
  current: () => boolean,
): ValueOptions<T> | undefined {
  if (options === undefined) return undefined;
  if (!isValueOptions(options) || !current())
    throw new TypeError("Invalid positioned Store value options");
  const abortSignal = Reflect.get(options, "abortSignal");
  if (!current()) throw new TypeError("Target graph changed while reading Store options");
  const initialStated = Object.hasOwn(options, "initial");
  if (!current()) throw new TypeError("Target graph changed while reading Store options");
  const initial = initialStated ? Reflect.get(options, "initial") : undefined;
  if (!current()) throw new TypeError("Target graph changed while reading Store options");
  const equals = Reflect.get(options, "equals");
  if (!current()) throw new TypeError("Target graph changed while reading Store options");
  const snapshot = Reflect.get(options, "snapshot");
  if (!current()) throw new TypeError("Target graph changed while reading Store options");
  const captured = {
    ...(abortSignal === undefined ? {} : { abortSignal }),
    ...(initialStated ? { initial } : {}),
    ...(equals === undefined ? {} : { equals }),
    ...(snapshot === undefined ? {} : { snapshot }),
  };
  if (!isValueOptions(captured) || !current())
    throw new TypeError("Target graph changed while validating Store options");
  return captured as ValueOptions<T>;
}

/**
 * Admits a provider-authenticated continuation and fold tuple into one fresh Store/session.
 *
 * The target provider validates the own-property seed and reader before native
 * ownership transfers. Policies are target code; native does not decode data or
 * replay the source prefix. Seed values and any present base are borrowed, not
 * cloned. Initial anchors and omitted bases create no Event, landing, or fold.
 * Accumulation seeds must not exceed their target policy maximum;
 * subsequent caller mutation is not monitored.
 *
 * Optional Store options follow ordinary first-opener validation. An authored
 * initial is the Store reset baseline. The legacy latest form retains its
 * behavior; the boundary/base form preserves a genuine target base and represents
 * omitted data separately from a present undefined value.
 * Snapshot, equality, and abort behavior use the supplied first-opener options.
 * A pre-aborted or reentrantly aborted signal before ownership transfer closes
 * staging and leaves the reader with the caller. In that case preparation throws
 * AbortError. After this function returns,
 * abort closes the native session and releases its reader without opening a
 * dormant source.
 *
 * @param member - The exact target graph member or native binding.
 * @param input - Input used for target key and provider validation.
 * @param policies - Target projection policies in seed tuple order.
 * @param seed - Legacy after/latest/values or explicit boundary/base/values, authenticated by the provider.
 * @param reader - Reader already reserved by the target provider.
 * @param options - Optional ordinary Store options for this first reader Store.
 * @returns The actual reader-owned Store, seeded projections, and original resource retirement completion.
 * @throws TypeError when member, seed, tuple, policy, options, reader, or current target authority is invalid.
 * @throws AbortError when caller cancellation happens before native ownership transfer.
 */
export function preparePositionedStoreRestore<
  Input,
  Event,
  Position,
  Reader,
  const Policies extends readonly ProjectionPolicy<NoInfer<Event>, any>[],
>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policies: Policies,
  seed: PositionedStoreRestoreSeed<
    NoInfer<Event>,
    NoInfer<Position>,
    NoInfer<PositionedStorePolicyValues<Event, Policies>>
  >,
  reader: NoInfer<Reader>,
  options?: ValueOptions<NoInfer<Event>>,
): PositionedStoreRestoration<Event, PositionedStorePolicyValues<Event, Policies>> {
  return preparePositionedStoreGroup<Event>(member, input, policies, seed, reader, options, false)
    .restoration as PositionedStoreRestoration<Event, PositionedStorePolicyValues<Event, Policies>>;
}

export interface PreparedPositionedStoreTransfer<Event = unknown> {
  readonly runtime: StoreRuntime<unknown>;
  readonly session: StreamSession<unknown>;
  readonly group: PositionedStoreSessionState<unknown>;
  readonly restoration: PositionedStoreRestoration<Event, readonly unknown[]>;
  readonly seed: object;
  readonly projections: readonly Projection<unknown>[];
  issue(): void;
  currentAt(now: number): boolean;
  validate(): void;
  publish(): void;
  deliver(): void;
  discard(): void;
  facet(index: number, role: string): Readable<unknown> | undefined;
}

/** @internal Uses the same native restoration constructor with deferred membership. */
export function preparePositionedStoreTransfer(
  member: PositionedStreamAccess<any, any, any, any>,
  input: unknown,
  policies: readonly ProjectionPolicy<any, any>[],
  seed: PositionedStoreRestoreSeed<any, any, readonly unknown[]>,
  reader: unknown,
  options?: ValueOptions<unknown>,
): PreparedPositionedStoreTransfer {
  return preparePositionedStoreGroup<unknown>(member, input, policies, seed, reader, options, true);
}

function positionedOwnData(value: unknown, allowed: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("Positioned preparation requires own data records");
  const result: Record<string, unknown> = Object.create(null);
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !allowed.includes(key))
      throw new TypeError("Unknown positioned seed field");
    const field = Object.getOwnPropertyDescriptor(value, key);
    if (field === undefined || !("value" in field))
      throw new TypeError("Positioned seed fields must be own data");
    result[key] = field.value;
  }
  return result;
}

function preparePositionedStoreGroup<Event>(
  member: PositionedStreamAccess<any, Event, any, any>,
  input: unknown,
  policies: readonly ProjectionPolicy<any, any>[],
  seed: PositionedStoreRestoreSeed<Event, any, readonly unknown[]>,
  reader: unknown,
  options: ValueOptions<Event> | undefined,
  detached: boolean,
): PreparedPositionedStoreTransfer<Event> {
  const target = positionedMemberRecord(member);
  const targetEpoch = target.graphState.resetEpoch;
  const targetCurrent = (): boolean =>
    target.graphState.resetEpoch === targetEpoch &&
    !target.graphState.disposed &&
    target.binding?.context.disposed !== true;
  if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
  else assertGraphOpen(target.graphState);
  const admission = beginTransferAdmission(
    target.graphState,
    target.definition,
    target.binding?.handle,
    input,
  );
  const retirement = prepareReaderRetirement(
    target.binding?.context.retirement ?? target.graphState.retirement,
  );
  let runtime: StoreRuntime<unknown> | undefined;
  let session: StreamSession<unknown> | undefined;
  let phase: "dormant" | "active" | "discarded" = "dormant";
  let published = !detached;
  let delivered = false;
  const discard = (abortReason?: unknown): void => {
    if (phase === "discarded") return;
    phase = "discarded";
    if (runtime !== undefined && session !== undefined)
      discardPositionedStoreSession(runtime, session, abortReason);
    else retireOwner(retirement, () => {});
  };
  try {
    const capturedOptions = capturePositionedStoreOptions(options, targetCurrent);
    const data = positionedOwnData(
      seed,
      Object.hasOwn(seed, "boundary")
        ? ["boundary", "base", "values"]
        : ["after", "latest", "values"],
    );
    const legacy = !Object.hasOwn(data, "boundary");
    let boundary: { readonly kind: "event" | "initial"; readonly after: unknown };
    let base: { readonly present: false } | { readonly present: true; readonly value: unknown };
    if (legacy) {
      if (!Object.hasOwn(data, "after") || !Object.hasOwn(data, "latest"))
        throw new TypeError("Legacy positioned seed requires own after/latest properties");
      boundary = { kind: "event", after: data.after };
      base = { present: true, value: data.latest };
    } else {
      const anchor = positionedOwnData(data.boundary, ["kind", "after"]);
      const presence = positionedOwnData(data.base, ["present", "value"]);
      if (
        (anchor.kind !== "event" && anchor.kind !== "initial") ||
        !Object.hasOwn(anchor, "after") ||
        (presence.present !== true && presence.present !== false) ||
        (presence.present ? !Object.hasOwn(presence, "value") : Object.hasOwn(presence, "value"))
      )
        throw new TypeError("Invalid positioned continuation boundary or base presence");
      boundary = { kind: anchor.kind, after: anchor.after };
      base = presence.present ? { present: true, value: presence.value } : { present: false };
    }
    const length = Object.getOwnPropertyDescriptor(policies, "length");
    if (
      !Array.isArray(policies) ||
      length === undefined ||
      !("value" in length) ||
      !Number.isSafeInteger(length.value) ||
      length.value < 0
    )
      throw new TypeError("Positioned Store policies must be a tuple");
    const capturedPolicies = readOwnDataArray(
      policies,
      length.value,
      "Positioned Store policies",
    ).map((policy) => {
      if (!isProjectionPolicy(policy))
        throw new TypeError("Invalid positioned Store target policy");
      const captured = capturePositionedPolicy(policy as ProjectionPolicy<unknown, unknown>);
      if (!targetCurrent()) throw new TypeError("Target changed while capturing positioned policy");
      return captured;
    });
    const values = readOwnDataArray(
      data.values,
      capturedPolicies.length,
      "Positioned Store seed values",
    );
    for (let index = 0; index < capturedPolicies.length; index++)
      assertPositionedAccumulationSeedWithinMax(capturedPolicies[index]!, values[index]);
    if (!targetCurrent()) throw new TypeError("Target changed while checking positioned seed");
    const key = storeKey(Reflect.apply(target.definition.key, target.definition, [input]));
    if (!targetCurrent()) throw new TypeError("Target changed while resolving positioned input");
    if (
      target.provider.validateReader(input, reader, boundary.after, undefined, seed) !== true ||
      !targetCurrent()
    )
      throw new TypeError("Provider did not authenticate the exact positioned seed");
    if (
      target.provider.isReaderCurrent(input, reader, boundary.after, seed) !== true ||
      !targetCurrent()
    )
      throw new TypeError("Positioned reader is no longer current");
    if (detached && legacy)
      throw new TypeError("Required positioned transfer needs explicit boundary/base presence");
    const typedBase = base.present
      ? { present: true as const, value: base.value as Event }
      : { present: false as const };
    const setup = legacy
      ? (() => {
          if (!typedBase.present)
            throw new TypeError("Legacy positioned seed requires a latest Event");
          return {
            kind: "legacy" as const,
            latest: typedBase.value,
            construction: {
              boundary,
              base: typedBase,
              legacy: true as const,
              detached,
              retirement,
            },
          };
        })()
      : {
          kind: "boundary" as const,
          construction: {
            boundary,
            base: typedBase,
            legacy: false as const,
            detached,
            retirement,
          },
        };
    const source = createPositionedStoreSession<Event>(
      target.graphState,
      target.definition,
      input,
      key,
      target.member,
      target.binding,
      boundary.after,
      reader,
      seed,
      capturedOptions,
      setup,
    );
    runtime = runtimeByStore.get(source as object) as StoreRuntime<unknown>;
    session = runtime.stream!;
    const actualRuntime = runtime;
    const actualSession = session;
    const group = actualSession.positioned!;
    const preparation = group.preparation!;
    const anchor = group.continuation;
    if (anchor === undefined)
      throw new TypeError("Positioned restoration has no genuine continuation anchor");
    const projections: Projection<unknown>[] = [];
    const tuples: PositionedProjectionTuple<unknown, unknown>[] = [];
    const projectionRuntimes: ProjectionRuntime<unknown, unknown>[] = [];
    for (let index = 0; index < capturedPolicies.length; index++) {
      const projection = makeSharedProjection(source, capturedPolicies[index]!, detached);
      const projected = positionedProjectionByHandle.get(projection as object)!;
      const positioned = projected.positioned!;
      if (positioned.storeGroup !== group || positioned.storeParticipant === undefined)
        throw new TypeError("Restored projection did not join its actual session");
      const next: PositionedProjectionTuple<unknown, unknown> = {
        value: values[index],
        status: "open",
        error: undefined,
        hasPosition: boundary.kind === "event",
        position: boundary.kind === "event" ? boundary.after : undefined,
        generation: projected.source.generation,
        revision: 0,
        resumable: true,
      };
      const before = positioned.tuple;
      if (
        !writePositionedTuple(
          projected,
          next,
          () =>
            targetCurrent() &&
            actualRuntime.stream === actualSession &&
            !actualSession.ended &&
            group.phase === "dormant" &&
            group.continuation === anchor &&
            !group.diverged &&
            !projected.closed &&
            positioned.tuple === before,
        )
      )
        throw new TypeError("Target projection changed during seed preparation");
      positioned.restored = true;
      positioned.storeParticipant.anchor = anchor.token;
      if (legacy) positioned.storeParticipant.event = group.completed;
      projections.push(projection);
      tuples.push(next);
      projectionRuntimes.push(projected);
    }
    const lease = group.reader!;
    const incarnation = actualRuntime.incarnation;
    const currentAt = (now: number): boolean => {
      if (
        !(
          phase !== "discarded" &&
          targetCurrent() &&
          actualRuntime.incarnation === incarnation &&
          actualRuntime.stream === actualSession &&
          actualRuntime.readerScope?.session === actualSession &&
          !actualRuntime.disposed &&
          !actualSession.ended &&
          group.phase !== "closed" &&
          group.continuation === anchor &&
          !group.diverged &&
          group.dispatch === undefined &&
          !lease.released &&
          preparation.lifetime?.currentAt(now) !== false
        )
      )
        return false;
      const predictions = reactiveInternal.signalValue(actualRuntime.predictionStack);
      for (let index = 0; index < predictions.length; index++)
        if (!predictions[index]!.dead) return false;
      for (let index = 0; index < projectionRuntimes.length; index++) {
        const projected = projectionRuntimes[index]!;
        const positioned = projected.positioned;
        const participant = positioned?.storeParticipant;
        if (
          projected.closed ||
          positioned === undefined ||
          participant === undefined ||
          positioned.tuple !== tuples[index] ||
          positioned.transitioning ||
          participant.anchor !== anchor.token ||
          !group.participants.has(participant) ||
          !Object.is(reactiveInternal.signalValue(projected.valueCell), tuples[index]!.value)
        )
          return false;
      }
      return true;
    };
    attachPositionedStoreAbort(actualRuntime, actualSession, capturedOptions?.abortSignal);
    if (
      target.provider.isReaderCurrent(input, reader, boundary.after, seed) !== true ||
      !targetCurrent()
    )
      throw new TypeError("Reader changed while creating positioned counterparts");
    const lifetime = target.provider.readerLifetime;
    if (detached && lifetime === undefined)
      throw new TypeError("Required reader has no observable lifetime");
    if (lifetime !== undefined) {
      const lifetimeValue = lifetime(input, reader, boundary.after, seed);
      if (!targetCurrent()) throw new TypeError("Reader target changed during lifetime capture");
      preparation.lifetime = prepareReaderLifetime(lifetimeValue, (abortReason) => {
        try {
          discard(abortReason);
        } catch (cause) {
          reportGraphContinuationError(actualRuntime.graph, cause);
        }
      });
    }
    if (!currentAt(readerLifetimeNow()))
      throw new TypeError("Positioned group changed before ownership transfer");
    if (detached) {
      for (const projected of projectionRuntimes)
        for (const readable of [projected.valueCell, projected.statusCell, projected.errorCell])
          reactiveInternal.onActualRead(readable, () => {
            if (!preparation.published)
              throw new TypeError("The positioned projection is still unpublished");
          });
    }
    const activate = (): StreamStore<Event> => {
      if (phase !== "dormant" || !published || !preparation.published || actualSession.ended)
        throw new TypeError("Positioned Store activation requires its published dormant group");
      if (!currentAt(readerLifetimeNow())) {
        discard();
        throw new TypeError("Positioned group is no longer current");
      }
      phase = "active";
      try {
        activatePositionedStoreSession(actualRuntime, actualSession);
      } catch (cause) {
        discard();
        throw cause;
      }
      return source;
    };
    const restoration: PositionedStoreRestoration<Event, readonly unknown[]> = Object.freeze({
      source,
      projections: Object.freeze(projections),
      retired: retirement.promise,
      activate,
      discard,
    });
    const result: PreparedPositionedStoreTransfer<Event> = Object.freeze({
      runtime: actualRuntime,
      session: actualSession,
      group,
      restoration,
      seed: seed as object,
      projections: restoration.projections,
      currentAt,
      issue() {
        if (!currentAt(readerLifetimeNow()))
          throw new TypeError("Positioned reader changed before native issuance");
        lease.owned = true;
      },
      validate() {
        if (
          !currentAt(readerLifetimeNow()) ||
          target.provider.isReaderCurrent(input, reader, boundary.after, seed) !== true ||
          !currentAt(readerLifetimeNow())
        )
          throw new TypeError("Positioned reservation failed final preparation");
      },
      publish() {
        if (published) return;
        publishPositionedStoreSession(actualRuntime, actualSession);
        const markers = preparation.publicationMarkers;
        if (markers !== undefined)
          for (let index = 0; index < markers.length; index++) markers[index]!();
        published = true;
      },
      deliver() {
        if (!published || delivered) return;
        delivered = true;
        target.graphState.pendingPositionedStores?.delete(actualRuntime);
        target.binding?.context.pendingPositionedStores?.delete(actualRuntime);
        deliverPositionedStoreSession(actualRuntime, actualSession, source);
        updateCollectionCandidate(actualRuntime);
        scheduleSweep(target.graphState);
      },
      discard,
      facet(index: number, role: string) {
        const projected = projectionRuntimes[index];
        if (projected === undefined) return undefined;
        if (role === "projection-value") return projected.valueCell;
        if (role === "projection-status") return projected.statusCell;
        if (role === "projection-error") return projected.errorCell;
        return undefined;
      },
    });
    if (!detached) result.issue();
    return result;
  } catch (cause) {
    try {
      discard();
    } catch (cleanupCause) {
      reportGraphContinuationError(target.graphState.graph, cleanupCause);
    }
    observeUnreturnedReaderRetirement(retirement.promise);
    throw cause;
  } finally {
    finishTransferAdmission(admission, runtime);
  }
}

/**
 * Authenticates an already-reserved provider reader before issuing a dormant
 * projection. Validation and currentness callbacks run before the handle is
 * returned. A thrown call leaves the provider reservation with its caller.
 *
 * @param member - The exact raw member or bound stream used by the target graph.
 * @param input - Input authenticated by the provider reservation.
 * @param checkpoint - Authentic capture from the same graph, definition and
 * binding provenance.
 * @param reader - Reservation returned by the provider's real admission step.
 * @param seedOptions - Optional target value already decoded by Hesperia. The
 * provider validator authenticates its relation to this capture before native
 * dormant issuance. Accumulation seeds must be arrays no longer than the captured
 * policy maximum. Native borrows their identity and elements without cloning;
 * later caller mutation is not monitored.
 * @returns Dormant one-shot projection seeded without source open or prefix fold.
 * @throws TypeError when source, graph, binding, capture, or reader provenance
 * does not match, the provider reservation is stale, or an accumulation seed
 * exceeds its captured maximum.
 */
export function preparePositionedRestore<Input, Value, Position, Reader, V>(
  member: PositionedStreamAccess<Input, Value, Position, Reader>,
  input: Input,
  checkpoint: PositionedProjectionCheckpoint<V, Position>,
  reader: Reader,
  seedOptions?: Pick<PositionedRestoreSeed<V, Position>, "value">,
): PositionedRestoration<V> {
  const target = positionedMemberRecord(member);
  const targetEpoch = target.graphState.resetEpoch;
  const targetCurrent = (): boolean =>
    target.graphState.resetEpoch === targetEpoch &&
    !target.graphState.disposed &&
    target.binding?.context.disposed !== true;
  const captured = positionedCheckpointByHandle.get(checkpoint as object);
  const sourceRuntime = captured?.runtime as ProjectionRuntime<Value, V> | undefined;
  const sourcePositioned = sourceRuntime?.positioned;
  if (
    captured === undefined ||
    sourceRuntime === undefined ||
    sourcePositioned === undefined ||
    sourcePositioned.tuple !== captured.tuple ||
    !isPositionedCheckpointCurrent(checkpoint) ||
    !checkpoint.hasPosition ||
    !checkpoint.resumable ||
    checkpoint.status === "failed" ||
    sourcePositioned.definition !== target.definition ||
    sourcePositioned.provider !== target.provider ||
    !sourcePositioned.hasKey
  )
    throw new TypeError("Positioned checkpoint does not match the native source definition");
  if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
  else assertGraphOpen(target.graphState);
  const sourcePolicy = sourcePositioned.policy;
  const after = captured.tuple.position as Position;
  const seedValue =
    seedOptions === undefined
      ? (captured.tuple.value as V)
      : (() => {
          if (seedOptions === null || typeof seedOptions !== "object" || Array.isArray(seedOptions))
            throw new TypeError("Invalid positioned restore seed");
          const descriptor = Object.getOwnPropertyDescriptor(seedOptions, "value");
          if (descriptor === undefined || !("value" in descriptor))
            throw new TypeError("Positioned restore seed must be an own data property");
          return descriptor.value as V;
        })();
  assertPositionedAccumulationSeedWithinMax(sourcePolicy, seedValue);
  if (!targetCurrent() || !isPositionedCheckpointCurrent(checkpoint))
    throw new TypeError("Positioned checkpoint or target graph changed while reading restore seed");
  const keyForInput = target.definition.key;
  const targetKey = storeKey(Reflect.apply(keyForInput, target.definition, [input]));
  if (!targetCurrent()) throw new TypeError("Target graph changed while resolving restore input");
  if (!isPositionedCheckpointCurrent(checkpoint))
    throw new TypeError("Positioned checkpoint changed while resolving target key");
  if (targetKey !== sourcePositioned.key)
    throw new TypeError("Positioned restore input does not match the captured stream key");
  if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
  else assertGraphOpen(target.graphState);
  if (target.provider.validateReader(input, reader, after, checkpoint, seedValue) !== true)
    throw new TypeError(
      "Provider reader is not an admitted reservation for this position and seed",
    );
  if (!targetCurrent()) throw new TypeError("Target graph changed during provider validation");
  if (target.provider.isReaderCurrent(input, reader, after, seedValue) !== true)
    throw new TypeError("Provider reader reservation is no longer current");
  if (!isPositionedCheckpointCurrent(checkpoint) || !targetCurrent())
    throw new TypeError("Positioned checkpoint or target graph changed during provider validation");
  assertPositionedAccumulationSeedWithinMax(sourcePolicy, seedValue);
  if (!isPositionedCheckpointCurrent(checkpoint) || !targetCurrent())
    throw new TypeError(
      "Positioned checkpoint or target graph changed while checking restore seed maximum",
    );
  if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
  else assertGraphOpen(target.graphState);

  const sourceTuple = captured.tuple as PositionedProjectionTuple<V, Position>;
  const { runtime, projection } = initializeProjection<Value, V>(
    target.graphState.graph,
    sourcePolicy,
    target.binding?.context.retirement,
  );
  const positioned = newPositionedState(target, input, runtime);
  positioned.key = targetKey;
  positioned.hasKey = true;
  positioned.openingKey = targetKey;
  positioned.hasOpeningKey = true;
  positioned.restoreSeed = seedValue;
  positioned.reader = reader;
  positioned.readerAfter = after;
  positioned.readerPresent = false;
  positioned.restored = true;
  positioned.openingRequest = { after, reader };
  const seeded: PositionedProjectionTuple<V, Position> = {
    value: seedValue,
    status: sourceTuple.status,
    error: sourceTuple.error,
    hasPosition: true,
    position: sourceTuple.position,
    generation: runtime.source.generation,
    revision: sourceTuple.revision,
    resumable: sourceTuple.resumable,
  };
  const seedTuple = positioned.tuple;
  positionedProjectionByHandle.set(
    projection,
    runtime as unknown as ProjectionRuntime<unknown, unknown>,
  );
  const seedEpoch = targetEpoch;
  const seedGeneration = runtime.source.generation;
  let committed = false;
  const targetCurrentAfterSeed = (): boolean =>
    target.graphState.resetEpoch === seedEpoch &&
    !target.graphState.disposed &&
    target.binding?.context.disposed !== true &&
    runtime.source.generation === seedGeneration &&
    !runtime.closed &&
    !positioned.transitioning &&
    positioned.tuple === seeded;
  const current = (): boolean =>
    isPositionedCheckpointCurrent(checkpoint) &&
    captured.tuple === sourceTuple &&
    target.graphState.resetEpoch === seedEpoch &&
    !target.graphState.disposed &&
    target.binding?.context.disposed !== true &&
    runtime.source.generation === seedGeneration &&
    !runtime.closed &&
    !positioned.transitioning &&
    positioned.tuple === (committed ? seeded : seedTuple);
  try {
    reactiveInternal.batch(() => {
      const landed = reactiveInternal.guardedWrite(runtime.valueCell, seeded.value, current, () => {
        positioned.tuple = seeded;
        committed = true;
      });
      if (!landed && !committed && !current())
        throw new TypeError("Graph changed while seeding positioned restore");
      runtime.statusCell.set(seeded.status);
      runtime.errorCell.set(seeded.error);
    });
    if (!targetCurrentAfterSeed())
      throw new TypeError("Target graph changed during seed publication");
  } catch (error) {
    positioned.readerPresent = false;
    try {
      finishPositioned(runtime, true, error);
    } catch (cleanupError) {
      reportProjectionError(runtime, cleanupError);
    }
    throw error;
  }

  positioned.readerPresent = true;

  let phase: "dormant" | "active" | "discarded" = "dormant";
  let handle!: PositionedRestoration<V>;
  const activate = (): Projection<V> => {
    if (phase !== "dormant") throw new TypeError("Positioned restoration is one-shot");
    const discardAndThrow = (error: unknown): never => {
      phase = "discarded";
      finishPositioned(runtime, false);
      throw error;
    };
    try {
      if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
      else assertGraphOpen(target.graphState);
    } catch (error) {
      return discardAndThrow(error);
    }
    const activationSource = runtime.source;
    const activationTuple = positioned.tuple;
    let readerCurrent = false;
    try {
      readerCurrent =
        target.provider.isReaderCurrent(input, reader, positioned.readerAfter, seedValue) === true;
    } catch (error) {
      return discardAndThrow(error);
    }
    const activationCurrent = (): boolean =>
      readerCurrent &&
      phase === "dormant" &&
      runtime.source === activationSource &&
      positioned.tuple === activationTuple &&
      target.graphState.resetEpoch === positioned.graphEpoch &&
      !target.graphState.disposed &&
      target.binding?.context.disposed !== true &&
      runtime.source.generation === activationTuple.generation &&
      !positioned.transitioning &&
      !runtime.closed &&
      positioned.readerPresent &&
      !positioned.readerReleased;
    if (!activationCurrent())
      return discardAndThrow(new TypeError("Positioned restoration is no longer current"));
    phase = "active";
    runtime.restartSource = () =>
      openIndependent(runtime, target.graphState.graph, target.definition, input);
    positioned.openingRequest = { after, reader };
    if (positioned.tuple.status !== "open") {
      const opened: PositionedProjectionTuple<V, Position> = {
        ...positioned.tuple,
        status: "open",
        error: undefined,
      };
      writePositionedTuple(
        runtime,
        opened,
        () =>
          runtime.source === activationSource &&
          target.graphState.resetEpoch === positioned.graphEpoch &&
          !target.graphState.disposed &&
          target.binding?.context.disposed !== true &&
          !positioned.transitioning &&
          !runtime.closed,
      );
    }
    openIndependent(runtime, target.graphState.graph, target.definition, input);
    return projection;
  };
  const discard = (): void => {
    if (phase === "discarded") return;
    phase = "discarded";
    finishPositioned(runtime, false);
  };
  handle = Object.freeze({ projection, activate, discard });
  return handle;
}

/**
 * Admits a provider-authenticated decoded fold into a fresh target graph.
 *
 * Unlike the in-memory checkpoint overload, this path requires an explicit
 * target policy and does not consult a source graph or source projection. The
 * target provider validates the durable boundary and borrowed seed before the
 * native projection is seeded or exposed.
 *
 * @param member - Exact raw member or native binding in the target graph.
 * @param input - Target input used for the existing stream key and provider admission.
 * @param policy - Explicit target projection policy; native does not infer it from wire data.
 * @param seed - Decoded accumulator value and opaque provider boundary. An
 * accumulated seed must be an array no longer than the captured policy maximum;
 * native borrows its identity and elements without cloning;
 * later caller mutation is not monitored.
 * @param reader - Reader already reserved by this target provider.
 * @returns Dormant one-shot projection with no source open or prefix refold.
 * @throws TypeError when target provenance, policy, seed, key, reader admission,
 * or accumulation maximum is invalid.
 */
export function preparePositionedSeedRestore<Input, Event, Position, Reader>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policy: AccumulatePolicy,
  seed: PositionedRestoreSeed<readonly Event[], NoInfer<Position>>,
  reader: NoInfer<Reader>,
): PositionedRestoration<readonly Event[]>;
export function preparePositionedSeedRestore<Input, Event, Position, Reader, Value>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policy: ReducePolicy<Event, Value>,
  seed: PositionedRestoreSeed<Value, NoInfer<Position>>,
  reader: NoInfer<Reader>,
): PositionedRestoration<Value>;
export function preparePositionedSeedRestore<Input, Event, Position, Reader, Value>(
  member: PositionedStreamAccess<Input, Event, Position, Reader>,
  input: Input,
  policy: ProjectionPolicy<Event, Value>,
  seed: PositionedRestoreSeed<Value, Position>,
  reader: Reader,
): PositionedRestoration<Value> {
  const target = positionedMemberRecord(member);
  const targetEpoch = target.graphState.resetEpoch;
  const targetCurrent = (): boolean =>
    target.graphState.resetEpoch === targetEpoch &&
    !target.graphState.disposed &&
    target.binding?.context.disposed !== true;
  if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
  else assertGraphOpen(target.graphState);
  const capturedPolicy = capturePositionedPolicy(policy);
  if (!targetCurrent())
    throw new TypeError("Target graph changed while capturing positioned policy");
  if (seed === null || typeof seed !== "object" || Array.isArray(seed))
    throw new TypeError("Invalid positioned restore seed");
  const afterDescriptor = Object.getOwnPropertyDescriptor(seed, "after");
  const valueDescriptor = Object.getOwnPropertyDescriptor(seed, "value");
  if (
    afterDescriptor === undefined ||
    !("value" in afterDescriptor) ||
    valueDescriptor === undefined ||
    !("value" in valueDescriptor)
  )
    throw new TypeError("Positioned restore seed requires own data properties");
  const after = afterDescriptor.value as Position;
  const seedValue = valueDescriptor.value as Value;
  assertPositionedAccumulationSeedWithinMax(capturedPolicy, seedValue);
  if (!targetCurrent()) throw new TypeError("Target graph changed while reading positioned seed");
  const keyForInput = target.definition.key;
  const targetKey = storeKey(Reflect.apply(keyForInput, target.definition, [input]));
  if (!targetCurrent())
    throw new TypeError("Target graph changed while resolving positioned input");
  if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
  else assertGraphOpen(target.graphState);
  if (target.provider.validateReader(input, reader, after, undefined, seedValue) !== true)
    throw new TypeError("Provider reader did not authenticate the target seed and boundary");
  if (!targetCurrent()) throw new TypeError("Target graph changed during provider seed validation");

  const { runtime, projection } = initializeProjection<Event, Value>(
    target.graphState.graph,
    capturedPolicy,
    target.binding?.context.retirement,
  );
  const positioned = newPositionedState(target, input, runtime);
  positioned.key = targetKey;
  positioned.hasKey = true;
  positioned.openingKey = targetKey;
  positioned.hasOpeningKey = true;
  positioned.restoreSeed = seedValue;
  positioned.reader = reader;
  positioned.readerAfter = after;
  positioned.readerPresent = false;
  positioned.readerReleased = false;
  positioned.restored = true;
  positioned.openingRequest = { after, reader };
  const seeded: PositionedProjectionTuple<Value, Position> = {
    value: seedValue,
    status: "open",
    error: undefined,
    hasPosition: true,
    position: after,
    generation: runtime.source.generation,
    revision: 0,
    resumable: true,
  };
  const seedTuple = positioned.tuple;
  positionedProjectionByHandle.set(
    projection,
    runtime as unknown as ProjectionRuntime<unknown, unknown>,
  );
  const seedEpoch = targetEpoch;
  const seedGeneration = runtime.source.generation;
  let committed = false;
  const nativeCurrent = (): boolean =>
    target.graphState.resetEpoch === seedEpoch &&
    !target.graphState.disposed &&
    target.binding?.context.disposed !== true &&
    runtime.source.generation === seedGeneration &&
    !runtime.closed &&
    !positioned.transitioning &&
    positioned.tuple === (committed ? seeded : seedTuple);
  try {
    if (target.provider.isReaderCurrent(input, reader, after, seedValue) !== true)
      throw new TypeError("Target reader changed before positioned seed landing");
    assertPositionedAccumulationSeedWithinMax(capturedPolicy, seedValue);
    if (!nativeCurrent())
      throw new TypeError("Target changed while checking positioned seed maximum");
    reactiveInternal.batch(() => {
      const landed = reactiveInternal.guardedWrite(
        runtime.valueCell,
        seeded.value,
        nativeCurrent,
        () => {
          positioned.tuple = seeded;
          committed = true;
        },
      );
      if (!landed && !committed && !nativeCurrent())
        throw new TypeError("Target graph changed while seeding positioned restore");
      runtime.statusCell.set(seeded.status);
      runtime.errorCell.set(seeded.error);
    });
    if (!nativeCurrent())
      throw new TypeError("Target graph changed during positioned seed publication");
  } catch (error) {
    try {
      finishPositioned(runtime, true, error);
    } catch (cleanupError) {
      reportProjectionError(runtime, cleanupError);
    }
    throw error;
  }
  positioned.readerPresent = true;

  let phase: "dormant" | "active" | "discarded" = "dormant";
  const discardAndThrow = (error: unknown): never => {
    phase = "discarded";
    finishPositioned(runtime, false);
    throw error;
  };
  const activate = (): Projection<Value> => {
    if (phase !== "dormant") throw new TypeError("Positioned restoration is one-shot");
    try {
      if (target.binding !== undefined) assertBindingContextOpen(target.binding.context);
      else assertGraphOpen(target.graphState);
    } catch (error) {
      return discardAndThrow(error);
    }
    const activationSource = runtime.source;
    const activationTuple = positioned.tuple;
    let current = false;
    try {
      current = target.provider.isReaderCurrent(input, reader, after, seedValue) === true;
    } catch (error) {
      return discardAndThrow(error);
    }
    if (
      !current ||
      phase !== "dormant" ||
      runtime.source !== activationSource ||
      positioned.tuple !== activationTuple ||
      target.graphState.resetEpoch !== positioned.graphEpoch ||
      target.graphState.disposed ||
      target.binding?.context.disposed === true ||
      runtime.source.generation !== activationTuple.generation ||
      positioned.transitioning ||
      runtime.closed ||
      !positioned.readerPresent ||
      positioned.readerReleased
    )
      return discardAndThrow(new TypeError("Positioned restoration is no longer current"));
    phase = "active";
    runtime.restartSource = () =>
      openIndependent(runtime, target.graphState.graph, target.definition, input);
    positioned.openingRequest = { after, reader };
    openIndependent(runtime, target.graphState.graph, target.definition, input);
    return projection;
  };
  const discard = (): void => {
    if (phase === "discarded") return;
    phase = "discarded";
    finishPositioned(runtime, false);
  };
  const handle: PositionedRestoration<Value> = Object.freeze({ projection, activate, discard });
  return handle;
}

/** Returns the latest successful-fold receipt, without acknowledging it. */
export function positionedFoldReceipt<Value>(
  projection: Projection<Value>,
): PositionedFoldReceipt<unknown> | undefined {
  const runtime = positionedProjectionByHandle.get(projection as object);
  return runtime?.positioned?.latestReceipt;
}

function acknowledgePositionedStoreFold(
  receipt: PositionedFoldReceipt<unknown>,
  record: {
    readonly runtime: ProjectionRuntime<unknown, unknown>;
    readonly tuple: PositionedProjectionTuple<unknown, unknown>;
    readonly storeEvent?: PositionedStoreEvent;
  },
  runtime: ProjectionRuntime<unknown, unknown>,
  positioned: PositionedProjectionRuntime,
): void {
  const group = positioned.storeGroup;
  const participant = positioned.storeParticipant;
  const lease = group?.reader;
  const event = record.storeEvent;
  if (
    group === undefined ||
    participant === undefined ||
    record.runtime !== runtime ||
    lease === undefined ||
    !lease.owned ||
    lease.released ||
    event === undefined ||
    record.tuple.generation !== positioned.tuple.generation ||
    record.tuple.revision > positioned.tuple.revision ||
    positioned.transitioning ||
    group.phase !== "active" ||
    group.completed !== event ||
    event.landingRevision === undefined ||
    group.dispatch !== undefined ||
    group.diverged ||
    group.baseRevision !== event.landingRevision ||
    !group.participants.has(participant) ||
    participant.event !== event ||
    participant.failed ||
    [...group.participants].some((required) => required.failed || required.event !== event)
  )
    throw new TypeError("Fold receipt is not at the current completed Store group frontier");
  const bindingContextIsOpen = (): boolean => group.runtime.binding?.context.disposed !== true;
  if (
    group.runtime.stream !== group.runtime.readerScope?.session ||
    group.runtime.stream?.positioned !== group ||
    group.runtime.stream?.ended ||
    !group.runtime.stream.hasPosition ||
    !Object.is(group.runtime.stream.position, event.position) ||
    group.runtime.graph.__runtime.disposed ||
    !bindingContextIsOpen()
  )
    throw new TypeError("Positioned Store session is no longer current");
  if (group.provider.isReaderCurrent(group.input, lease.reader, lease.after, lease.seed) !== true)
    throw new TypeError("Positioned Store reader reservation is no longer current");
  const current =
    runtime.positioned === positioned &&
    positioned.storeGroup === group &&
    positioned.storeParticipant === participant &&
    group.participants.has(participant) &&
    participant.event === event &&
    !participant.failed &&
    !positioned.transitioning &&
    record.runtime === runtime &&
    record.tuple.generation === positioned.tuple.generation &&
    record.tuple.revision <= positioned.tuple.revision &&
    positionedReceiptByHandle.get(receipt as object) === record &&
    record.storeEvent === event &&
    group.reader === lease &&
    group.completed === event &&
    group.dispatch === undefined &&
    group.phase === "active" &&
    !group.diverged &&
    group.baseRevision === event.landingRevision &&
    !group.runtime.disposed &&
    group.runtime.stream === group.runtime.readerScope?.session &&
    group.runtime.stream?.positioned === group &&
    !group.runtime.stream?.ended &&
    group.runtime.graph.__runtime.resetEpoch === group.runtime.stream?.resetEpoch &&
    !group.runtime.graph.__runtime.disposed &&
    bindingContextIsOpen() &&
    lease.owned &&
    !lease.released &&
    (group.preparation?.lifetime === undefined ||
      group.preparation.lifetime.currentAt(readerLifetimeNow())) &&
    [...group.participants].every((required) => !required.failed && required.event === event);
  if (!current)
    throw new TypeError("Positioned Store reader changed during provider currentness validation");
  group.provider.acknowledge(group.input, lease.reader, receipt);
}

/**
 * Acknowledges an authentic native successful fold with its provider reader.
 *
 * For a reader-owned Store group, every still-required projection must have
 * folded the same completed source event. A receipt before that frontier, from
 * a closed participant, or from another generation is rejected. The provider
 * callback runs outside the native landing cut. This call does not prove durable
 * publication or physical iterator retirement; repeated valid receipts are
 * passed to the provider, which owns its acknowledgement semantics.
 *
 * @param projection - Exact positioned Projection that produced the receipt.
 * @param receipt - Exact native fold receipt returned for that projection.
 * @returns Nothing.
 * @throws TypeError when the receipt, projection, reader, or group frontier is stale or foreign.
 */
export function acknowledgePositionedFold<Value>(
  projection: Projection<Value>,
  receipt: PositionedFoldReceipt<unknown>,
): void {
  const record = positionedReceiptByHandle.get(receipt as object);
  const runtime = positionedProjectionByHandle.get(projection as object);
  const positioned = runtime?.positioned;
  if (record === undefined || runtime === undefined || positioned === undefined)
    throw new TypeError("Fold receipt does not belong to this active positioned reader");
  if (positioned.storeGroup !== undefined) {
    acknowledgePositionedStoreFold(receipt, record, runtime, positioned);
    return;
  }
  if (
    record.runtime !== runtime ||
    !positioned.readerPresent ||
    positioned.readerReleased ||
    record.tuple.generation !== positioned.tuple.generation ||
    record.tuple.revision > positioned.tuple.revision ||
    positioned.transitioning
  )
    throw new TypeError("Fold receipt does not belong to this active positioned reader");
  const source = runtime.source;
  const tuple = positioned.tuple;
  const graphEpoch = positioned.graphState.resetEpoch;
  const reader = positioned.reader;
  if (
    positioned.provider.isReaderCurrent(
      positioned.input,
      reader,
      positioned.readerAfter,
      positioned.restoreSeed,
    ) !== true
  )
    throw new TypeError("Provider reader reservation is no longer current");
  if (
    positionedProjectionByHandle.get(projection as object) !== runtime ||
    runtime.positioned !== positioned ||
    runtime.source !== source ||
    positioned.tuple !== tuple ||
    positioned.graphState.resetEpoch !== graphEpoch ||
    positioned.graphState.disposed ||
    positioned.binding?.context.disposed === true ||
    positioned.reader !== reader ||
    !positioned.readerPresent ||
    positioned.readerReleased ||
    positioned.transitioning
  )
    throw new TypeError("Positioned reader changed during provider currentness validation");
  positioned.provider.acknowledge(positioned.input, reader, receipt);
}

/**
 * Opens an independent stream source and projects its emissions.
 *
 * Accumulation starts with an empty array and reduction starts with the
 * policy's `initial` value. Every source gap is terminal for the projection.
 * A normal iterator completion closes it; iterator failures, source gaps,
 * reducer failures, and fatal accumulation overflow set `error` and move the
 * status to `"failed"`. The value remains at its last successful state when a
 * projection fails.
 *
 * `project` does not reuse or write the keyed stream store. Its source is
 * opened separately and its abort signal belongs to this projection alone.
 * The graph closes registered projections during graph disposal.
 *
 * @param graph - The open graph that owns the projection lifecycle.
 * @param stream - The stream definition whose source is opened independently.
 * @param input - Input passed to the stream key and open callbacks.
 * @param policy - Bounded accumulation or reducer policy.
 * @returns A projection whose reactive value and lifecycle readables are
 * updated as source items arrive.
 * @throws {@link Fault} With kind `contract` when an accumulation maximum is
 * not a finite positive integer, or kind `disposed` when the graph is closed.
 * The stream key's synchronous error is rethrown. Errors from opening or
 * consuming the source are represented by a failed projection instead.
 * @throws TypeError if the policy fields or reducer type are malformed.
 *
 * @example
 * ```ts
 * import { project, subscribe, type Graph, type StreamDefinition } from "aeolia";
 *
 * declare const graph: Graph;
 * declare const events: StreamDefinition<void, number>;
 *
 * const projection = project(graph, events, undefined, {
 *   kind: "accumulate",
 *   max: 50,
 *   onOverflow: "drop-oldest",
 * });
 * const stop = subscribe(projection.value, (value) => console.log(value));
 * // Later:
 * stop();
 * projection.close();
 *
 * const total = project(graph, events, undefined, {
 *   kind: "reduce",
 *   initial: 0,
 *   step: (sum, item) => sum + item,
 * });
 * ```
 */
export function project<I, T, V, K extends "reduce" | "accumulate">(
  graph: Graph,
  stream: StreamDefinition<I, T>,
  input: I,
  policy: ProjectionPolicy<T, V> & { readonly kind: K },
): Projection<K extends "accumulate" ? readonly T[] : V>;
/**
 * Projects future emissions from an existing keyed stream session.
 *
 * Does not open a source or replay the store's retained value. Every subsequent
 * source emission is consumed, including equal values suppressed by the store's
 * equality rule. Direct store writes are not source emissions. Each projection
 * has separate accumulation/reduction state and counts as a source listener until
 * it closes or fails. Source gaps fail this projection without closing the shared
 * source. Normal source completion closes it; source failure preserves the reason.
 * A later source reopen does not reopen an already-terminal projection.
 *
 * @param source - StreamStore whose current session supplies emissions.
 * @param policy - Bounded accumulation or reduction, with the same rules as the
 * independent overload.
 * @returns An eager projection with its own value and lifecycle readables.
 * @throws TypeError if source is not an Aeolia StreamStore or the policy is malformed.
 * @throws Fault if its graph is disposed or the accumulation maximum is invalid.
 */
export function project<T, V, K extends "reduce" | "accumulate">(
  source: StreamStore<T>,
  policy: ProjectionPolicy<T, V> & { readonly kind: K },
): Projection<K extends "accumulate" ? readonly T[] : V>;
export function project<I, T, V>(
  graphOrSource: Graph | StreamStore<T>,
  streamOrPolicy: StreamDefinition<I, T> | ProjectionPolicy<T, V>,
  input?: I,
  suppliedPolicy?: ProjectionPolicy<T, V>,
): Projection<V> {
  const store = suppliedPolicy === undefined ? runtimeForStore(graphOrSource) : undefined;
  const state =
    suppliedPolicy === undefined
      ? store?.graph.__runtime
      : graphRuntimeByGraph.get(graphOrSource as object);
  const bound =
    suppliedPolicy === undefined
      ? boundStreamByStore.get(graphOrSource as StreamStore<T>)
      : undefined;
  if (bound !== undefined) assertBindingContextOpen(bound.binding.context);
  else if (state !== undefined) assertGraphOpen(state);
  const acquisition =
    state === undefined
      ? undefined
      : beginPhysicalWork(bound?.session.physical.owner ?? state.retirement, "acquisition");
  try {
    const policy = suppliedPolicy ?? (streamOrPolicy as ProjectionPolicy<T, V>);
    if (!isProjectionPolicy(policy)) throw new TypeError("Invalid projection policy.");
    if (policy.kind === "accumulate") validateMax(policy.max);
    if (bound !== undefined) assertBindingContextOpen(bound.binding.context);
    else if (state !== undefined) assertGraphOpen(state);
    if (suppliedPolicy === undefined)
      return makeSharedProjection(graphOrSource as StreamStore<T>, policy);
    return makeProjection(
      graphOrSource as Graph,
      streamOrPolicy as StreamDefinition<I, T>,
      input,
      policy,
    );
  } finally {
    if (acquisition !== undefined) finishPhysicalWork(acquisition);
  }
}

export interface TransferPositionedCheckpoint {
  readonly runtime: StoreRuntime<unknown>;
  readonly source: StreamStore<unknown>;
  readonly seed: {
    readonly boundary: { readonly kind: "event" | "initial"; readonly after: unknown };
    readonly base:
      | { readonly present: false }
      | { readonly present: true; readonly value: unknown };
    readonly values: readonly unknown[];
  };
  readonly projections: number;
  currentAt(now: number): boolean;
  facet(
    readable: object,
  ): { readonly index: number; readonly role: ProjectionTransferRole } | undefined;
}

/** @internal Authenticates the exact borrowed Store cut for required transfer. */
export function transferPositionedCheckpoint(
  checkpoint: PositionedStoreCheckpoint<any, readonly unknown[]>,
): TransferPositionedCheckpoint {
  const capture = positionedStoreCheckpointByHandle.get(checkpoint as object);
  if (
    capture === undefined ||
    !positionedStoreCaptureCurrent(capture) ||
    !positionedStoreCutResumable(capture) ||
    capture.continuation === undefined
  )
    throw new TypeError("Required positioned capture needs a current resumable native boundary");
  const runtime = capture.runtime;
  const seen = new Set<object>();
  for (const item of capture.projections) {
    if (seen.has(item.runtime)) throw new TypeError("A required fold must be enrolled once");
    seen.add(item.runtime);
  }
  const continuation = capture.continuation;
  const present = runtime.snapshot && runtime.baseAvailable && runtime.transferEligible;
  const seed = Object.freeze({
    boundary: Object.freeze({ kind: continuation.kind, after: continuation.after }),
    base: present
      ? Object.freeze({
          present: true as const,
          value: reactiveInternal.signalValue(runtime.committed),
        })
      : Object.freeze({ present: false as const }),
    values: capture.values,
  });
  return Object.freeze({
    runtime,
    source: capture.source as StreamStore<unknown>,
    seed,
    projections: capture.projections.length,
    currentAt: (now: number): boolean =>
      positionedStoreCaptureCurrent(capture, now) && positionedStoreCutResumable(capture),
    facet(readable: object) {
      const facet = projectionFacetByReadable.get(readable);
      if (facet === undefined) return undefined;
      for (let index = 0; index < capture.projections.length; index++)
        if (capture.projections[index]!.runtime === facet.runtime)
          return { index, role: facet.role };
      return undefined;
    },
  });
}
