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
  StoreKey,
  StreamStore,
  ValueOptions,
  PositionedStreamMember,
} from "./types.ts";
import {
  assertBindingContextOpen,
  boundStreamByStore,
  graphRuntimeByGraph,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
  rawStreamMemberByFunction,
  streamBindingByHandle,
  positionedStreamSourceByDefinition,
  positionedPositionWriterByOpenOptions,
  positionedOpenRequestByOpenOptions,
} from "./registry.ts";
import { Fault } from "../fault.ts";
import { isObject } from "../utils.ts";
import { __internal as reactiveInternal, signal } from "../reactive.ts";
import type { Readable } from "../reactive.ts";
import { assertGraphOpen, assertStoreOpen } from "./faults.ts";
import { isLive, scheduleSweep, updateCollectionCandidate } from "./collection.ts";
import type {
  BoundStreamSessionRuntime,
  GraphProjection,
  GraphRuntimeState,
  StreamEvent,
  StoreRuntime,
  StreamBindingRuntime,
} from "./runtime.ts";
import type { RetirementOwner } from "./retirement.ts";
import {
  beginPhysicalWork,
  finishPhysicalWork,
  normalizePhysicalPromise,
  observePhysicalWork,
  physicalCleanup,
  recordCleanupFailure,
} from "./retirement.ts";

const projectionInitialByValueCell = new WeakMap<object, unknown>();

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
  }
>();

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
    positioned.transitioning = true;
    const previous = runtime.source;
    const cleanup = stopSource(previous);
    const source: ProjectionSource<T> = {
      retirement: previous.retirement,
      controller: new AbortController(),
      generation: previous.generation + 1,
      retired: false,
    };
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
      positioned.transitioning = false;
    }
    return cleanup;
  }
  const previous = runtime.source;
  const cleanup = stopSource(previous);
  const source: ProjectionSource<T> = {
    retirement: previous.retirement,
    controller: new AbortController(),
    generation: previous.generation + 1,
    retired: false,
  };
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
  runtime.positioned = positioned;
  if (record.binding !== undefined) {
    const close = (): void => finishPositioned(runtime, false);
    positioned.contextCleanup = close;
    const projections = record.binding.context.positionedProjections ?? new Set<() => void>();
    record.binding.context.positionedProjections = projections;
    projections.add(close);
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
        positioned.tuple = next;
        if (receipt !== undefined) {
          positioned.latestReceipt = receipt;
          positionedReceiptByHandle.set(receipt, { runtime: runtime as any, tuple: next as any });
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
function releasePositionedReader<T, V>(runtime: ProjectionRuntime<T, V>): void {
  const positioned = runtime.positioned;
  if (positioned === undefined || !positioned.readerPresent || positioned.readerReleased) return;
  positioned.readerReleased = true;
  try {
    positioned.provider.releaseReader(positioned.input, positioned.reader);
  } catch (error) {
    reportProjectionError(runtime, error);
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
  positioned.transitioning = true;
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
    releasePositionedReader(runtime);
    positioned.transitioning = false;
    finishPhysicalWork(work);
  }
  if (
    state !== undefined &&
    !state.disposed &&
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
  if (policy.kind === "reduce") projectionInitialByValueCell.set(valueCell, policy.initial);

  const close = (): void => finish(runtime, false);
  const projection: Projection<V> = Object.freeze({
    value: valueCell,
    status: statusCell,
    error: errorCell,
    close,
  });

  runtime.unregister = registerProjection(graph, {
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
  });
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
  let observed = false;
  try {
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
      source.iterator = iterator;
    } catch (error) {
      if (!isCurrentGeneration(runtime, controller, generation)) return;
      fail(runtime, error);
      return;
    }
    observePhysicalWork(physical, consume(runtime, controller, generation));
    observed = true;
  } finally {
    if (!observed) finishPhysicalWork(physical);
  }
}

function makeSharedProjection<T, V>(
  source: StreamStore<T>,
  policy: ProjectionPolicy<T, V>,
): Projection<V> {
  const store = runtimeForStore(source) as StoreRuntime<T> | undefined;
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
    bound?.session.physical.owner ?? store.graph.__runtime.retirement,
  );
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
      );
      if (store.streamSource !== undefined) {
        runtime.positioned!.key = store.key;
        runtime.positioned!.hasKey = true;
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
  const nextListeners = new Set(session.listeners);
  nextListeners.add(listener);
  session.listeners = nextListeners;
  session.unobservedSince = undefined;
  updateCollectionCandidate(store);
  scheduleSweep(store.graph.__runtime);
  generationSource.detach = () => {
    session.listeners.delete(listener);
    if (store.stream === session && !isLive(store)) session.unobservedSince = Date.now();
    updateCollectionCandidate(store);
    scheduleSweep(store.graph.__runtime);
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

/**
 * Lets the same provider reader acknowledge one native successful fold.
 * Provider acknowledgement is a callout and occurs outside the native landing
 * cut. A receipt from a failed reducer, foreign projection, or other generation
 * is rejected.
 */
export function acknowledgePositionedFold<Value>(
  projection: Projection<Value>,
  receipt: PositionedFoldReceipt<unknown>,
): void {
  const record = positionedReceiptByHandle.get(receipt as object);
  const runtime = positionedProjectionByHandle.get(projection as object);
  const positioned = runtime?.positioned;
  if (
    record === undefined ||
    runtime === undefined ||
    positioned === undefined ||
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
