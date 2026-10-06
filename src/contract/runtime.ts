import type {
  Contract,
  Graph,
  QueryDefinition,
  Readable,
  Store,
  StoreDefinition,
  StoreKey,
  StoreStatus,
  StreamDefinition,
  StreamStatus,
} from "./types.ts";
import type { Fault, Unsubscribe } from "../fault.ts";
import type { Computed, WritableSignal } from "../reactive.ts";

export type TimerHandle = ReturnType<typeof setTimeout>;

export interface QuerySource<T> {
  readonly query: QueryDefinition<unknown, T, any>;
  readonly input: unknown;
}

export interface Prediction<T> {
  readonly id: number;
  readonly apply: (current: T | undefined) => T | undefined;
  /** Whether the mutation that created this prediction completed successfully. */
  succeeded: boolean;
  dead: boolean;
  reported: boolean;
}

export interface QueryCaller<T> extends QuerySource<T> {
  readonly windowMs: number;
  readonly runtime: StoreRuntime<T>;
  readonly statusCell: WritableSignal<StoreStatus>;
  readonly status: Readable<StoreStatus>;
  readonly pending: Readable<boolean>;
  readonly abortSignal?: AbortSignal;
}

export interface QueryRequest {
  readonly controller: AbortController;
  readonly generation: number;
  /** Reset identity under which this request was issued. */
  readonly resetEpoch: number;
  /** Whether this request is the one automatic recovery allowed after a failure. */
  readonly automaticRecovery: boolean;
  /** Successful predictions present when this request was issued. */
  readonly predictionIds: readonly number[];
  promise: Promise<void>;
  outcome: Promise<QueryOutcome>;
  resolveOutcome: (outcome: QueryOutcome) => void;
  outcomeReported: boolean;
  removeCallerAbort: () => void;
  removeRequestAbort?: () => void;
  settled: boolean;
  superseded: boolean;
}

export type QueryOutcome = { readonly ok: true } | { readonly ok: false; readonly error: unknown };

export interface StreamSession<T> {
  readonly controller: AbortController;
  /** Reset identity under which this session was opened. */
  readonly resetEpoch: number;
  readonly listeners: Set<(event: StreamEvent<T>) => void>;
  iterator?: AsyncIterator<T>;
  /** Start of the current uninterrupted interval without live readables. */
  unobservedSince?: number;
  ended: boolean;
  emitted: boolean;
}

export interface StreamSource<T> {
  readonly stream: StreamDefinition<unknown, T, any>;
  readonly input: unknown;
  readonly abortSignal?: AbortSignal;
}

export interface GraphResetOperation {
  readonly epoch: number;
  readonly signal: AbortSignal;
  readonly reject: (reason: unknown) => void;
  readonly abort: (reason: unknown) => void;
}

export interface GraphProjection {
  readonly reset: () => (() => void) | undefined;
  readonly restart: () => void;
  readonly dispose: () => void;
}

export type StreamEvent<T> =
  | { readonly kind: "value"; readonly value: T }
  | { readonly kind: "gap" }
  | { readonly kind: "close"; readonly failed: boolean; readonly reason?: unknown };

export interface CollectionCandidate {
  readonly runtime: StoreRuntime<unknown>;
  deadline: number;
}

export interface StoreRuntime<T> {
  readonly graph: GraphRuntime<any>;
  readonly key: StoreKey;
  readonly store: Store<T>;
  readonly committed: WritableSignal<T | undefined>;
  readonly predictionStack: WritableSignal<readonly Prediction<T>[]>;
  readonly presence: WritableSignal<boolean>;
  readonly value: Computed<T | undefined>;
  readonly lifecycle: WritableSignal<number>;
  readonly error: Computed<unknown>;
  readonly errorCell: WritableSignal<unknown>;
  readonly streamStatusCell: WritableSignal<StreamStatus>;
  readonly streamStatus: Computed<StreamStatus>;
  readonly callers: QueryCaller<T>[];
  readonly requests: Set<QueryRequest>;
  readonly livenessStops: Unsubscribe[];
  readonly readStops: Unsubscribe[];
  source?: QuerySource<T>;
  streamSource?: StreamSource<T>;
  onReadableActivation?: () => void;
  refreshOnActivation: boolean;
  activeRequest?: QueryRequest;
  stream?: StreamSession<T>;
  timer?: TimerHandle;
  initial?: T;
  equals: (a: T, b: T) => boolean;
  snapshot: boolean;
  configured: boolean;
  declared: boolean;
  initialSpecified: boolean;
  equalsSpecified: boolean;
  snapshotSpecified: boolean;
  hasCommitted: boolean;
  failing: boolean;
  /** True after failed automatic recovery until explicit retry re-arms the query. */
  recoveryDisarmed: boolean;
  invalidated: boolean;
  lastLandingAt?: number;
  lastSettledAt?: number;
  generation: number;
  lastInteraction: number;
  liveReadableCount: number;
  readyWaiterCount: number;
  valueLive: boolean;
  collectionIndex: number;
  dropped: boolean;
  disposed: boolean;
}

export interface GraphRuntime<C extends Contract> extends Graph<C> {
  readonly __runtime: GraphRuntimeState<C>;
}

export interface GraphRuntimeState<C extends Contract> {
  readonly graph: GraphRuntime<C>;
  readonly namespace: string;
  readonly idleMs: number;
  readonly maxPredictions: number;
  readonly onUnobservedFault?: (error: unknown) => void;
  readonly stores: Map<string, StoreRuntime<unknown>>;
  readonly declaredStores: Map<string, StoreDefinition<unknown>>;
  readonly faultObservers: Set<(fault: Fault) => void>;
  readonly activeControllers: Set<AbortController>;
  readonly projections: Set<GraphProjection>;
  readonly collectionHeap: CollectionCandidate[];
  stopAbort?: Unsubscribe;
  collectionTimer?: TimerHandle;
  sweepScheduled: boolean;
  nextPredictionId: number;
  resetEpoch: number;
  readonly resetVersion: WritableSignal<number>;
  resetOperation?: GraphResetOperation;
  disposed: boolean;
}

/**
 * Graph-owned store state exposed only to Aeolia's realm-crossing implementation.
 *
 * @internal
 */
export interface RealmStoreState {
  /** Exact graph-local key for this runtime. */
  readonly key: StoreKey;

  /** Whether the committed value is eligible for snapshot encoding. */
  readonly snapshot: boolean;

  /** Whether the runtime currently contains a committed value. */
  readonly hasCommitted: boolean;

  /** Generation attached to the current committed value. */
  readonly generation: number;

  /** Host timestamp of the most recent committed landing, when one exists. */
  readonly lastLandingAt?: number;

  /** Reads the committed base without folding optimistic predictions over it. */
  readonly committedValue: () => unknown;
}
