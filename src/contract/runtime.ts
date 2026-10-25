import type {
  Contract,
  Affected,
  Graph,
  MutationDefinition,
  CallContext,
  QueryAcquisition,
  QueryDefinition,
  QueryProgressEvent,
  QueryProgressSnapshot,
  Readable,
  Store,
  StoreDefinition,
  StoreKey,
  StoreIdentity,
  StoreStatus,
  StreamDefinition,
  StreamStatus,
  StreamStore,
  ValueRevision,
  ValueOptions,
} from "./types.ts";
import type { Fault, Unsubscribe } from "../fault.ts";
import type { Computed, WritableSignal } from "../reactive.ts";
import type { BindingContext } from "./query-binding.ts";
import type { BoundStreamSession, StreamDetachment } from "./stream-binding.ts";
import type { PhysicalWork, RetirementOwner } from "./retirement.ts";

export type TimerHandle = ReturnType<typeof setTimeout>;

export interface QuerySource<T> {
  readonly query: QueryDefinition<unknown, T, any, any>;
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
  /** Store incarnation under which this request was issued. */
  readonly incarnation: object;
  /** Frozen opaque identity exposed by this request's progress snapshots. */
  readonly progressIdentity: object;
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

export interface QueryProgressListener {
  readonly observer: (event: QueryProgressEvent<unknown>) => void;
  active: boolean;
  frontier: number;
}

export interface QueryProgressDelivery {
  readonly sequence: number;
  readonly event: QueryProgressEvent<unknown>;
  readonly request?: QueryRequest;
  readonly target?: QueryProgressListener;
}

/** Plain per-query authority behind the coalescing reactive projection. */
export interface QueryProgressAuthority {
  current: QueryProgressSnapshot<unknown>;
  request?: QueryRequest;
  sequence: number;
  readonly listeners: Set<QueryProgressListener>;
  readonly queue: QueryProgressDelivery[];
  readonly continuationErrors: unknown[];
  dispatching: boolean;
}

export type QueryOutcome = { readonly ok: true } | { readonly ok: false; readonly error: unknown };

export interface StreamSession<T> {
  readonly physical: PhysicalWork;
  readonly controller: AbortController;
  readonly token: object;
  /** Reset identity under which this session was opened. */
  readonly resetEpoch: number;
  readonly listeners: Set<(event: StreamEvent<T>) => void>;
  iterator?: AsyncIterator<T>;
  removeCallerAbort?: () => void;
  removeControllerAbort?: () => void;
  /** Start of the current uninterrupted interval without live readables. */
  unobservedSince?: number;
  ended: boolean;
  emitted: boolean;
  bound?: BoundStreamSessionRuntime<T>;
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
  readonly captureCancellation: () => ((reason: unknown) => void) | undefined;
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

/** Issue and publication ownership for one native incarnation. */
export interface ObservationState {
  readonly incarnation: object;
  readonly issue: WritableSignal<Readonly<object> | undefined>;
  readonly live: WritableSignal<boolean>;
  publisherActivity: WritableSignal<boolean> | undefined;
  readonly stops: Unsubscribe[];
  publisher: object | undefined;
  retired: boolean;
  liveReadables: number;
  issueReadable?: Readable<Readonly<object> | undefined>;
}

export interface StoreRuntime<T> {
  readonly graph: GraphRuntime<any>;
  readonly identity: StoreIdentity;
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
  /** Lazily created query-only progress metadata; never part of store values. */
  queryProgressCell?: WritableSignal<QueryProgressSnapshot<unknown>>;
  /** Non-coalescing event authority; capture does not read the reactive cell. */
  queryProgressAuthority?: QueryProgressAuthority;
  /** Interrupts the current reporter before a non-request authority generation changes. */
  queryProgressAuthorityChange?: (request: QueryRequest) => void;
  /** Resets retained progress data when an idle keyed store is collected. */
  queryProgressClear?: () => void;
  readonly binding?: StoreBindingRuntime;
  /** Current entry authority, independent of request success and the legacy dropped flag. */
  incarnation: object | undefined;
  source?: QuerySource<T>;
  streamSource?: StreamSource<T>;
  onReadableActivation?: () => void;
  refreshOnActivation: boolean;
  activeRequest?: QueryRequest;
  stream?: StreamSession<T>;
  streamToken?: object;
  boundRetirement?: BoundStreamSessionRuntime<T>;
  timer?: TimerHandle;
  initial?: T;
  equals: (a: T, b: T) => boolean;
  snapshot: boolean;
  configured: boolean;
  declared: boolean;
  initialSpecified: boolean;
  equalsSpecified: boolean;
  snapshotSpecified: boolean;
  baseAvailable: boolean;
  hasAcceptedLanding: boolean;
  valueRevision: ValueRevision;
  /** One unclaimed import on this exact native base; stale stamps grant no authority. */
  importedBaseline?: {
    readonly incarnation: object;
    readonly resetEpoch: number;
    readonly generation: number;
    readonly valueRevision: ValueRevision;
  };
  transferEligible: boolean;
  /** A reset is clearing this entry; capture must not expose partial state. */
  transitioning: boolean;
  observation?: ObservationState;
  failing: boolean;
  /** True after failed automatic recovery until explicit retry re-arms the query. */
  recoveryDisarmed: boolean;
  invalidated: boolean;
  lastLandingAt?: number;
  lastSettledAt?: number;
  generation: number;
  lastInteraction: number;
  liveReadableCount: number;
  /** Live query-progress readers retain collection without touching freshness. */
  progressReadableCount: number;
  readyWaiterCount: number;
  valueLive: boolean;
  collectionIndex: number;
  dropped: boolean;
  disposed: boolean;
}

export interface GraphRuntime<C extends Contract> extends Graph<C> {
  readonly __runtime: GraphRuntimeState<C>;
}

export type GraphDisposalPhase =
  | "observation"
  | "contexts"
  | "reset-reject"
  | "reset-abort"
  | "lifetime"
  | "collection"
  | "projections"
  | "stores"
  | "controllers"
  | "fault-channel"
  | "fault-observers"
  | "complete";

export interface GraphStoreDisposalProgress {
  readonly runtime: StoreRuntime<unknown>;
  timerAttempted: boolean;
  stopsCaptured: boolean;
  livenessStops?: Unsubscribe[];
  livenessIndex: number;
  readStops?: Unsubscribe[];
  readIndex: number;
  stopsCleared: boolean;
  streamCaptured: boolean;
  stream?: StreamSession<unknown>;
  streamAttempted: boolean;
  publicationAttempted: boolean;
}

export interface GraphDisposalProgress {
  phase: GraphDisposalPhase;
  readonly revisions: Map<StoreRuntime<unknown>, ValueRevision>;
  contexts?: BindingContextRuntime[];
  contextIndex: number;
  resetOperationCaptured: boolean;
  resetOperation?: GraphResetOperation;
  resetRejectAttempted: boolean;
  resetAbortAttempted: boolean;
  stopAbortCaptured: boolean;
  stopAbort?: Unsubscribe;
  stopAbortAttempted: boolean;
  collectionTimerCaptured: boolean;
  collectionTimer?: TimerHandle;
  collectionTimerAttempted: boolean;
  projections?: GraphProjection[];
  projectionIndex: number;
  stores?: GraphStoreDisposalProgress[];
  storeIndex: number;
  controllers?: AbortController[];
  controllerIndex: number;
  faultChannelUnregistered: boolean;
  faultObserversCleared: boolean;
  active: boolean;
  complete: boolean;
  resumeWork?: PhysicalWork;
}

export interface GraphRuntimeState<C extends Contract> {
  readonly retirement: RetirementOwner;
  readonly graph: GraphRuntime<C>;
  readonly namespace: string;
  readonly idleMs: number;
  readonly maxPredictions: number;
  readonly onUnobservedFault?: (error: unknown) => void;
  readonly stores: Map<string | StoreIdentity, StoreRuntime<unknown>>;
  readonly queryDefinitions: Map<QueryDefinition<any, any, any, any>, QueryAcquisition>;
  readonly mutationDefinitions: Set<MutationDefinition<any, any>>;
  readonly bindingContexts: Set<BindingContextRuntime>;
  readonly declaredStores: Map<string, StoreDefinition<unknown>>;
  readonly faultObservers: Set<(fault: Fault) => void>;
  readonly activeControllers: Set<AbortController>;
  readonly projections: Set<GraphProjection>;
  readonly collectionHeap: CollectionCandidate[];
  disposal?: GraphDisposalProgress;
  stopAbort?: Unsubscribe;
  collectionTimer?: TimerHandle;
  sweepScheduled: boolean;
  nextPredictionId: number;
  resetEpoch: number;
  readonly resetVersion: WritableSignal<number>;
  resetOperation?: GraphResetOperation;
  disposed: boolean;
}

export interface BindingContextRuntime {
  readonly retirement: RetirementOwner;
  readonly state: GraphRuntimeState<any>;
  readonly handle: BindingContext;
  readonly activeCell: WritableSignal<boolean>;
  readonly bindings: Map<QueryDefinition<any, any, any, any>, Map<object, QueryBindingRuntime>>;
  readonly streamBindings: Map<StreamDefinition<any, any, any>, Map<object, StreamBindingRuntime>>;
  readonly mutations: Map<MutationDefinition<any, any>, Map<object, MutationBindingRuntime>>;
  readonly mutationControllers: Set<AbortController>;
  disposed: boolean;
}

export interface QueryBindingRuntime {
  readonly kind: "query";
  readonly addresses: Map<string, BindingAddressRuntime>;
  readonly context: BindingContextRuntime;
  readonly definition: QueryDefinition<any, any, any, any>;
  readonly profile: object;
  readonly execution: QueryDefinition<any, any, any, any>;
  readonly value: ValueOptions<any> | undefined;
  handle: object;
  readonly stores: Map<string, StoreRuntime<unknown>>;
}

export interface StreamBindingRuntime {
  readonly kind: "stream";
  readonly context: BindingContextRuntime;
  readonly definition: StreamDefinition<any, any, any>;
  readonly originalMember: object;
  readonly stores: Map<string, StoreRuntime<unknown>>;
  readonly addresses: Map<string, BindingAddressRuntime>;
  handle: object;
}

export type StoreBindingRuntime = QueryBindingRuntime | StreamBindingRuntime;

export interface BindingAddressRuntime {
  readonly binding: StoreBindingRuntime;
  readonly key: string;
  readonly operations: Set<object>;
  guard: object;
}

export interface BoundStreamSessionRuntime<T> {
  readonly runtime: StoreRuntime<T>;
  readonly binding: StreamBindingRuntime;
  readonly session: StreamSession<T>;
  readonly incarnation: object;
  readonly handle: BoundStreamSession<T>;
  readonly lifecycle: WritableSignal<number>;
  readonly resolveDetached: (receipt: StreamDetachment<T>) => void;
  phase: "active" | "retiring" | "detached";
  liveReadables: number;
  facets?: Pick<StreamStore<T>, "value" | "available" | "status" | "pending" | "error">;
  receipt?: StreamDetachment<T>;
  retirement?: { readonly token: object; readonly epoch: number };
}

export interface QueryTargetRuntime {
  readonly binding: QueryBindingRuntime;
  readonly runtime: StoreRuntime<unknown>;
  readonly incarnation: object;
}

export interface ProfileEffectRuntime {
  readonly affected: Affected<any>;
  readonly mutation: MutationDefinition<any, any>;
  readonly index: number;
  readonly query: QueryDefinition<any, any, any, any>;
  readonly prepare: (phase: object) => unknown;
  readonly settle: (phase: object) => unknown;
}

export interface MutationBindingRuntime {
  readonly context: BindingContextRuntime;
  readonly definition: MutationDefinition<any, any>;
  readonly profile: object;
  readonly run: (input: unknown, context: CallContext) => unknown;
  readonly effects: readonly ProfileEffectRuntime[];
}

export interface BoundProducerRuntime {
  readonly mutation: MutationDefinition<any, any>;
  readonly target: QueryTargetRuntime;
  readonly producer: (value: unknown, input: unknown) => unknown;
}

export interface EffectPhaseRuntime {
  readonly binding: MutationBindingRuntime;
  readonly effect: ProfileEffectRuntime;
  readonly kind: "prepare" | "settle";
  readonly members: Map<StoreRuntime<unknown>, Set<object>>;
  active: boolean;
}

export interface EffectInstructionRuntime {
  readonly phase: EffectPhaseRuntime;
  readonly target: QueryTargetRuntime;
  readonly kind: "prepare" | "original" | "predict" | "selected" | "current";
  readonly prediction?: BoundProducerRuntime;
  readonly input?: unknown;
}

/**
 * Graph-owned store state exposed only to Aeolia's realm-crossing implementation.
 *
 * @internal
 */
export interface RealmStoreState {
  /** Whether v1's raw-key representation cannot describe this store. */
  readonly bound: boolean;
  /** Exact native binding; undefined retains the raw-key namespace. */
  readonly binding: StoreBindingRuntime | undefined;
  /** Actual graph-local physical entry, never a transport address. */
  readonly identity: StoreIdentity;
  /** Captured local base revision, independent of request generation. */
  readonly valueRevision: ValueRevision;
  /** Exact graph-local key for this runtime. */
  readonly key: StoreKey;

  /** Whether the committed value is eligible for snapshot encoding. */
  readonly snapshot: boolean;

  /** Whether the runtime currently contains a committed value. */
  readonly baseAvailable: boolean;
  /** Whether the current base has an accepted landing representable by v1. */
  readonly hasAcceptedLanding: boolean;
  /** Eligibility attached to this exact committed base revision. */
  readonly transferEligible: boolean;
  /** Checks the captured native identity and state after authored encoding. */
  readonly current: () => boolean;

  /** Generation attached to the current committed value. */
  readonly generation: number;

  /** Host timestamp of the most recent committed landing, when one exists. */
  readonly lastLandingAt?: number;

  /** Reads the committed base without folding optimistic predictions over it. */
  readonly committedValue: () => unknown;
}
