import type { Graph, QueryStore, Readable, QueryDefinition, ValueRevision } from "./types.ts";
import type { GraphRuntimeState, StoreRuntime, QueryCaller } from "./runtime.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { assertStoreHandlePublished, streamBindingByHandle } from "./registry.ts";
import { createPhysicalCompletion } from "./retirement.ts";

interface Epoch {
  value: object;
}
const epochs = new WeakMap<object, Epoch>();
const positionedCaptureListeners = new WeakMap<object, Set<() => void>>();

/** @internal Attach only the targets of an authentic positioned Store capture. */
export function observePositionedTransferChanges(
  targets: readonly object[],
  notify: () => void,
): () => void {
  const unique = new Set(targets);
  for (const target of unique) {
    let listeners = positionedCaptureListeners.get(target);
    if (listeners === undefined) {
      listeners = new Set();
      positionedCaptureListeners.set(target, listeners);
    }
    listeners.add(notify);
  }
  return stopPositionedCaptureChanges(
    [...unique].map((target) => new WeakRef(target)),
    notify,
  );
}

function stopPositionedCaptureChanges(
  targets: readonly WeakRef<object>[],
  notify: () => void,
): () => void {
  return () => {
    for (const reference of targets) {
      const target = reference.deref();
      if (target === undefined) continue;
      const listeners = positionedCaptureListeners.get(target);
      listeners?.delete(notify);
      if (listeners?.size === 0) positionedCaptureListeners.delete(target);
    }
  };
}

function notifyPositionedCaptureChange(target: object): void {
  const listeners = positionedCaptureListeners.get(target);
  if (listeners === undefined) return;
  for (const notify of listeners) {
    try {
      notify();
    } catch {
      // Observer scheduling cannot change the producer's result.
    }
  }
}

function epoch(target: object): Epoch {
  let record = epochs.get(target);
  if (record === undefined) {
    record = { value: {} };
    epochs.set(target, record);
  }
  return record;
}
/** @internal Records an actual native transition, including equality-preserving transitions. */
export function noteTransferChange(target: object): void {
  epoch(target).value = {};
  notifyPositionedCaptureChange(target);
}
/** @internal Preallocates a native epoch replacement for a fixed publication tail. */
export function prepareTransferChange(target: object): {
  current: () => boolean;
  afterCurrent: () => boolean;
  publish: () => void;
} {
  const record = epoch(target);
  const before = record.value;
  const after = {};
  return {
    current: () => record.value === before,
    afterCurrent: () => record.value === after,
    publish: () => {
      record.value = after;
      notifyPositionedCaptureChange(target);
    },
  };
}
/** @internal A non-evaluating native transition witness. */
export function transferEpochCurrent(target: object): () => boolean {
  const record = epoch(target);
  const value = record.value;
  return () => record.value === value;
}

/** @internal Actual native source provenance; never accepted from a package consumer. */
export interface TransferSourceRegistration {
  readonly graph: Graph;
  readonly kind: "query" | "mutation" | "stream" | "store";
  readonly definition: object;
  readonly binding?: object;
  readonly invokeQuery?: (
    args: readonly unknown[],
    entries: unknown,
    assertCurrent?: () => void,
  ) => QueryStore<any, any>;
}
const graphs = new WeakMap<object, GraphRuntimeState<any>>();
const sources = new WeakMap<object, TransferSourceRegistration>();
const stores = new WeakMap<object, StoreRuntime<any>>();
const storeSources = new WeakMap<object, TransferSourceRegistration>();
const readables = new WeakMap<object, StoreRuntime<any>>();
const readableRoles = new WeakMap<object, string>();
const readableCallers = new WeakMap<object, QueryCaller<any>>();
const callerRecords = new WeakMap<object, TransferCallerRecord>();
/** @internal Exact native acquisition owner of per-caller facets. */
export interface TransferCallerRecord {
  readonly caller: QueryCaller<any>;
  readonly runtime: StoreRuntime<any>;
  readonly store: QueryStore<any, any>;
}
const sourceLists = new WeakMap<object, TransferSourceRegistration[]>();
interface WorkMembership {
  readonly set: Set<object>;
  rows: object[];
}
const workMembership = new WeakMap<object, WorkMembership>();
const workTargets = new WeakMap<object, Set<object>>();
interface StoreRecord {
  readonly works: Set<object>;
}
const storeRecords = new WeakMap<object, StoreRecord>();
interface AddressBook {
  current: Map<string, CoverageRecord>;
  version: object;
}
interface CoverageRecord {
  readonly owner: object;
  readonly key: string;
  operations: Set<OperationRecord>;
  version: object;
}
const books = new WeakMap<object, AddressBook>();
function book(owner: object): AddressBook {
  let record = books.get(owner);
  if (record === undefined) {
    record = { current: new Map(), version: {} };
    books.set(owner, record);
  }
  return record;
}
function ownerFor(runtime: StoreRuntime<any>): object {
  return runtime.binding ?? runtime.graph;
}
function coverageFor(runtime: StoreRuntime<any>): CoverageRecord | undefined {
  return books.get(ownerFor(runtime))?.current.get(runtime.key);
}
function storeRecord(runtime: StoreRuntime<any>): StoreRecord {
  let record = storeRecords.get(runtime);
  if (record === undefined) {
    record = { works: workSet(runtime) };
    storeRecords.set(runtime, record);
  }
  return record;
}
/** @internal Registers the real graph before its native API is exposed. */
export function registerTransferGraph(graph: Graph, state: GraphRuntimeState<any>): void {
  graphs.set(graph, state);
  book(graph);
  epoch(state);
}
/** @internal Registers a genuine member or binding created by a native owner. */
export function registerTransferSource(source: object, info: TransferSourceRegistration): void {
  sources.set(source, info);
  if (info.binding !== undefined) {
    sources.set(info.binding, info);
    book(info.binding);
  }
  const list = sourceLists.get(info.graph) ?? [];
  if (
    !list.some(
      (entry) =>
        entry.kind === info.kind &&
        entry.definition === info.definition &&
        entry.binding === info.binding,
    )
  ) {
    list.push(info);
    sourceLists.set(info.graph, list);
  }
}
/** @internal Registers an actual native Store and its canonical reactive facets. */
export function registerTransferStore<T>(runtime: StoreRuntime<T>): void {
  stores.set(runtime.store, runtime);
  storeRecord(runtime);
  epoch(runtime);
  book(ownerFor(runtime));
  for (const readable of [
    runtime.committed,
    runtime.predictionStack,
    runtime.presence,
    runtime.value,
    runtime.lifecycle,
    runtime.error,
    runtime.errorCell,
    runtime.streamStatusCell,
    runtime.streamStatus,
  ]) {
    readables.set(readable, runtime);
  }
  const facets = [
    [runtime.committed, "committed"],
    [runtime.predictionStack, "predictions"],
    [runtime.presence, "available"],
    [runtime.value, "value"],
    [runtime.lifecycle, "lifecycle"],
    [runtime.error, "error"],
    [runtime.errorCell, "error-cell"],
    [runtime.streamStatusCell, "stream-status-cell"],
    [runtime.streamStatus, "stream-status"],
  ] as const;
  for (const [readable, role] of facets) readableRoles.set(readable, role);
}
/** @internal Registers an authentic returned wrapper, without granting authority to lookalikes. */
export function registerTransferStoreHandle<T>(
  handle: object,
  runtime: StoreRuntime<T>,
  nativeSource?: TransferSourceRegistration,
): void {
  stores.set(handle, runtime);
  const entries = sourceLists.get(runtime.graph);
  const info =
    nativeSource ??
    entries?.find(
      (entry) =>
        entry.binding === runtime.binding &&
        (runtime.binding !== undefined ||
          entry.definition === runtime.source?.query ||
          entry.definition === runtime.streamSource?.stream),
    );
  if (info !== undefined) storeSources.set(handle, info);
}
/** @internal Registers a real caller/progress facet against its owning physical Store. */
export function registerTransferReadable<T>(
  readable: Readable<unknown>,
  runtime: StoreRuntime<T>,
  role?: string,
  caller?: QueryCaller<any>,
): void {
  readables.set(readable, runtime);
  if (role !== undefined) readableRoles.set(readable, role);
  if (caller !== undefined) readableCallers.set(readable, caller);
}
/** @internal Enrolls the exact returned native wrapper and its actual caller. */
export function registerTransferCaller(
  handle: QueryStore<any, any>,
  runtime: StoreRuntime<any>,
  caller: QueryCaller<any>,
): void {
  if (caller.runtime !== runtime)
    throw new TypeError("Native caller belongs to another physical Store");
  callerRecords.set(caller, { caller, runtime, store: handle });
  registerTransferReadable(caller.statusCell, runtime, "query-status-cell", caller);
  registerTransferReadable(caller.status, runtime, "query-status", caller);
  registerTransferReadable(caller.pending, runtime, "query-pending", caller);
}
/** @internal Exact native per-caller association, never inferred from a field name. */
export function transferReadableCaller(readable: object): TransferCallerRecord | undefined {
  const caller = readableCallers.get(readable);
  const record = caller === undefined ? undefined : callerRecords.get(caller);
  if (record !== undefined) assertStoreHandlePublished(record.store);
  return record;
}
/** @internal Actual returned acquisition wrapper for a known native caller. */
export function transferCaller(caller: object): TransferCallerRecord | undefined {
  const record = callerRecords.get(caller);
  if (record !== undefined) assertStoreHandlePublished(record.store);
  return record;
}
/** @internal Releases only the exact unpublished native facet association. */
export function unregisterTransferReadable(readable: object, runtime: StoreRuntime<any>): void {
  if (readables.get(readable) === runtime) {
    readables.delete(readable);
    readableRoles.delete(readable);
    readableCallers.delete(readable);
  }
}
/** @internal Native facet role from its real owner. */
export function transferReadableRole(readable: object): string | undefined {
  return readableRoles.get(readable);
}
/** @internal Looks up a native source already authenticated at its actual creation. */
export function transferSource(source: object): TransferSourceRegistration | undefined {
  let info = sources.get(source);
  if (info === undefined) {
    const binding = streamBindingByHandle.get(source);
    if (binding !== undefined) {
      info = {
        graph: binding.context.state.graph,
        kind: "stream",
        definition: binding.definition,
        binding,
      };
      registerTransferSource(source, info);
    }
  }
  return info;
}
/** @internal Native graph lookup with no public property protocol. */
export function transferGraph(graph: Graph): GraphRuntimeState<any> | undefined {
  return graphs.get(graph);
}
/** @internal Exact returned Store lookup. */
export function transferStore(handle: object): StoreRuntime<any> | undefined {
  assertStoreHandlePublished(handle);
  return stores.get(handle);
}
/** @internal Real reactive-facet ownership, excluding any value-based inference. */
export function transferReadableStore(readable: object): StoreRuntime<any> | undefined {
  return readables.get(readable);
}
/** @internal Sources actually constructed by the graph and binding owners. */
export function transferGraphSources(graph: Graph): readonly TransferSourceRegistration[] {
  return sourceLists.get(graph) ?? [];
}

/** @internal Pending actual native argument/key/options admission. */
export interface TransferAdmission {
  readonly state: GraphRuntimeState<any>;
  readonly definition: object;
  readonly binding: object | undefined;
  readonly input: unknown;
  active: boolean;
}
const admissions = new Set<TransferAdmission>();
let admissionRows: TransferAdmission[] = [];
/** @internal Begins before authored normalization/key callbacks, without creating a Store. */
export function beginTransferAdmission(
  state: GraphRuntimeState<any>,
  definition: object,
  binding?: object,
  input?: unknown,
): TransferAdmission {
  const admission = { state, definition, binding, input, active: true };
  admissions.add(admission);
  admissionRows = [...admissions];
  return admission;
}
/** @internal Completes the same admission even when authored preparation throws. */
export function finishTransferAdmission(
  admission: TransferAdmission,
  runtime?: StoreRuntime<any>,
): void {
  admission.active = false;
  admissions.delete(admission);
  admissionRows = [...admissions];
  if (runtime !== undefined) inheritTransferCoverage(runtime);
}
/** @internal Existing persistent coverage is resolved by the genuine native address owner. */
export function inheritTransferCoverage<T>(runtime: StoreRuntime<T>): void {
  book(ownerFor(runtime));
  storeRecord(runtime);
}
/** @internal Reports only the actual in-progress graph admission window. */
export function transferHasAdmission(graph: Graph): boolean {
  for (let index = 0; index < admissionRows.length; index += 1) {
    const admission = admissionRows[index]!;
    if (admission.active && admission.state.graph === graph) return true;
  }
  return false;
}
/** @internal Links native original work; logical Promise settlement does not finish it. */
export function registerTransferWork(target: object, work: object): void {
  const runtime =
    stores.get(target) ?? (storeRecords.has(target) ? (target as StoreRuntime<any>) : undefined);
  const targets = runtime !== undefined && target !== runtime ? [target, runtime] : [target];
  let reverse = workTargets.get(work);
  if (reverse === undefined) {
    reverse = new Set();
    workTargets.set(work, reverse);
  }
  for (const owner of targets) {
    const membership = workMembershipFor(owner);
    membership.set.add(work);
    membership.rows = [...membership.set];
    reverse.add(owner);
    noteTransferChange(owner);
  }
  if (runtime !== undefined) storeRecord(runtime);
}
/** @internal Genuine native context authority, distinct from authored State. */
export interface TransferAuthority {
  readonly graph: Graph;
  readonly owner: object;
  readonly kind: "context-active" | "context-active-cell";
}
const authorities = new WeakMap<object, TransferAuthority>();
/** @internal Records the actual context's cells at native construction. */
export function registerTransferAuthority(
  readable: Readable<unknown>,
  authority: TransferAuthority,
): void {
  authorities.set(readable, authority);
}
/** @internal Returns native authority identity without reading public metadata. */
export function transferAuthority(readable: object): TransferAuthority | undefined {
  return authorities.get(readable);
}

interface ProjectionRecord {
  readonly graph: Graph;
  readonly owner: object;
}
const projections = new WeakMap<object, ProjectionRecord>();
/** @internal Registers the actual source generation of a native projection readable. */
export function registerTransferProjection(
  readable: Readable<unknown>,
  graph: Graph,
  owner: object,
): void {
  projections.set(readable, { graph, owner });
  epoch(owner);
}
/** @internal Genuine projection source association. */
export function transferProjection(readable: object): ProjectionRecord | undefined {
  return projections.get(readable);
}

interface ScopeRecord {
  readonly handle: TransferScope;
  readonly source: TransferSourceRegistration;
  readonly coverage: CoverageRecord;
  readonly incarnation: object | undefined;
  readonly runtime?: StoreRuntime<any>;
}
interface DispatchExpectation {
  readonly role: "raw" | "bound";
  readonly source: TransferSourceRegistration;
  readonly sourceHandle: object;
  readonly input: object;
  joined: boolean;
}
interface OperationRecord {
  readonly handle: TransferOperation;
  scopes: Set<ScopeRecord>;
  dispatches: readonly DispatchExpectation[];
  works: Set<object>;
  active: boolean;
  retired: boolean;
  version: object;
}
declare const scopeBrand: unique symbol;
declare const operationBrand: unique symbol;
/** Persistent authority over one authenticated affected native address. */
export interface TransferScope {
  readonly [scopeBrand]: true;
}
/** Native operation ownership, separate from the external result and retirement channels. */
export interface TransferOperation {
  readonly [operationBrand]: true;
}
const scopeRecords = new WeakMap<object, ScopeRecord>();
const operationRecords = new WeakMap<object, OperationRecord>();
const carrierOperations = new WeakMap<object, OperationRecord>();
/** @internal Actual native dispatch joined before authored predictors or effects. */
export interface TransferDispatch {
  readonly operation: OperationRecord;
  readonly work: object;
}
/** @internal Joins the genuine role/source/carrier to its actual original PhysicalWork. */
export function beginTransferDispatch(
  graph: Graph,
  definition: object,
  binding: object | undefined,
  input: unknown,
  work: object,
  source?: object,
): TransferDispatch | undefined {
  if ((typeof input !== "object" || input === null) && typeof input !== "function")
    return undefined;
  const operation = carrierOperations.get(input);
  if (operation === undefined) return undefined;
  if (!operation.active || operation.retired)
    throw new TypeError("The native transfer operation is not dispatchable");
  const expected = operation.dispatches.find(
    (entry) =>
      entry.input === input &&
      entry.source.graph === graph &&
      entry.source.definition === definition &&
      entry.source.binding === binding &&
      (source === undefined || entry.sourceHandle === source || entry.source.binding === source),
  );
  if (expected === undefined || expected.joined)
    throw new TypeError("Native transfer dispatch has a crossed or repeated role");
  expected.joined = true;
  operation.works.add(work);
  operation.version = {};
  return { operation, work };
}
/** @internal A real mutation target must be covered before any affected write. */
export function registerTransferDispatchTarget(
  dispatch: TransferDispatch | undefined,
  runtime: StoreRuntime<any>,
): void {
  if (dispatch === undefined) return;
  const covered = [...dispatch.operation.scopes].some(
    (scope) =>
      scope.coverage.owner === ownerFor(runtime) && scope.coverage.key === String(runtime.key),
  );
  if (!covered) throw new TypeError("Native transfer dispatch reached an uncovered target");
  registerTransferWork(runtime, dispatch.work);
}
/** @internal Pure required-transfer eligibility; it does not change the visible cache. */
export function transferStoreAdmissible(runtime: StoreRuntime<any>): boolean {
  if (
    runtime.disposed ||
    runtime.transitioning ||
    runtime.graph.__runtime.disposed ||
    runtime.binding?.context.disposed ||
    runtime.boundRetirement !== undefined ||
    runtime.requests.size > 0 ||
    coverageFor(runtime)?.operations.size
  )
    return false;
  const predictions = reactiveInternal.signalValue(runtime.predictionStack);
  for (let index = 0; index < predictions.length; index += 1)
    if (!predictions[index]!.dead) return false;
  if (
    runtime.observation !== undefined &&
    reactiveInternal.signalValue(runtime.observation.issue) !== undefined
  )
    return false;
  const linked = workMembership.get(runtime)?.rows;
  if (linked !== undefined)
    for (let index = 0; index < linked.length; index += 1) {
      const work = linked[index] as { active?: boolean; kind?: string; cleanupFailed?: boolean };
      if (
        work.cleanupFailed ||
        (work.active &&
          (work.kind === "query" ||
            work.kind === "mutation" ||
            work.kind === "acquisition" ||
            work.kind === "mutation-phase"))
      )
        return false;
    }
  return true;
}

/** @internal Committed-base eligibility remains separate from empty-target admission. */
export function transferStoreEligible(runtime: StoreRuntime<any>): boolean {
  return runtime.transferEligible && transferStoreAdmissible(runtime);
}

function workMembershipFor(target: object): WorkMembership {
  let record = workMembership.get(target);
  if (record === undefined) {
    record = { set: new Set(), rows: [] };
    workMembership.set(target, record);
  }
  return record;
}
function workSet(target: object): Set<object> {
  return workMembershipFor(target).set;
}
/** @internal Removes only actually finished successful work from target membership. */
export function releaseTransferWork(work: object): void {
  const physical = work as { active?: boolean; cleanupFailed?: boolean };
  if (physical.active || physical.cleanupFailed) return;
  const targets = workTargets.get(work);
  if (targets === undefined) return;
  for (const target of targets) {
    const record = workMembership.get(target);
    if (record === undefined) continue;
    record.set.delete(work);
    record.rows = [...record.set];
    noteTransferChange(target);
  }
  workTargets.delete(work);
}

/** Deferred native delivery, after the caller has published its entire logical state. */
export interface TransferNotifications {
  /** Delivers once; callback failures preserve the published outcome. */
  deliver(): void;
}
/** Preallocated native-owned result of a successful final seal. */
export interface TransferCommit {
  /** Publishes once on the same synchronous stack as the successful seal. */
  publish(): void;
  /** Preallocated delivery port. */
  readonly notifications: TransferNotifications;
}
/** One pending, discardable native publication. */
export interface TransferPublication {
  /** Returns the exact prepared commit if all native witnesses are still current. */
  seal(): TransferCommit | undefined;
  /** Releases pending registration custody; published ownership is retained. */
  discard(): void;
  /** Completion of this plan's registration cleanup, independent of external operations. */
  readonly retired: Promise<void>;
}

/** @internal Composes prepared native root/field swaps without calling authored code in the cut. */
export function prepareTransferPublication(
  current: () => boolean,
  publishFields: () => void,
  deliverNotifications: () => void = () => {},
  discardPending: () => void = () => {},
): TransferPublication {
  let phase: "pending" | "sealed" | "published" | "discarded" = "pending";
  let delivered = false;
  const completion = createPhysicalCompletion<void>();
  const retired = completion.promise;
  const resolve = (): void => completion.resolve(undefined);
  const reject = completion.reject;
  const publish = (): void => {
    if (phase === "published") return;
    if (phase !== "sealed") throw new TypeError("Transfer publication has not been sealed");
    publishFields();
    phase = "published";
  };
  const deliver = (): void => {
    if (phase !== "published") throw new TypeError("Transfer publication has not been published");
    if (delivered) return;
    delivered = true;
    try {
      deliverNotifications();
      resolve();
    } catch (cause) {
      reject(cause);
      throw cause;
    }
  };
  const notifications: TransferNotifications = Object.freeze(
    Object.assign(Object.create(null), { deliver }),
  );
  const commit: TransferCommit = Object.freeze(
    Object.assign(Object.create(null), { publish, notifications }),
  );
  return Object.freeze({
    seal: (): TransferCommit | undefined => {
      if (phase !== "pending" || !current()) return undefined;
      phase = "sealed";
      return commit;
    },
    discard: (): void => {
      if (phase !== "pending") return;
      phase = "discarded";
      try {
        discardPending();
        resolve();
      } catch (cause) {
        reject(cause);
        throw cause;
      }
    },
    retired,
  });
}

/** Genuine query acquisition function or its native binding handle. */
export type QueryTransferSource =
  | ((...args: any[]) => QueryStore<any, any>)
  | import("./query-binding.ts").QueryBindingHandle;
/** Genuine raw mutation member or native mutation binding. */
export type MutationTransferSource =
  | ((...args: any[]) => any)
  | import("./mutation-binding.ts").MutationBinding<any, any>;
/** One exact native mutation role and its original opaque input carrier. */
export interface TransferOperationDispatch {
  /** Selects the actual raw or bound native dispatch. */
  readonly role: "raw" | "bound";
  /** Native member or binding authenticated at construction. */
  readonly source: MutationTransferSource;
  /** Exact carrier passed to that dispatch, without copying or projection. */
  readonly input: object;
}
/** A successor native address for an unresolved operation. */
export interface TransferScopeSuccessor {
  /** Still-active original native operation. */
  readonly operation: TransferOperation;
  /** Earlier authenticated affected address. */
  readonly predecessor: TransferScope;
  /** Genuine replacement graph. */
  readonly graph: Graph;
  /** Actual replacement query source. */
  readonly source: QueryTransferSource;
  /** Once-prepared native input; this entry never invokes the public member. */
  readonly input: unknown;
}

function sourceCurrent(info: TransferSourceRegistration): () => boolean {
  const state = graphs.get(info.graph);
  if (state === undefined) throw new TypeError("Unknown native graph");
  const reset = state.resetEpoch;
  const binding = info.binding as { context?: { disposed: boolean } } | undefined;
  const guards = [transferEpochCurrent(state)];
  if (binding !== undefined) {
    guards.push(transferEpochCurrent(binding));
    if (binding.context !== undefined) guards.push(transferEpochCurrent(binding.context));
  }
  return () => {
    if (state.disposed || state.resetEpoch !== reset || binding?.context?.disposed) return false;
    for (let index = 0; index < guards.length; index += 1) if (!guards[index]!()) return false;
    return true;
  };
}
function authenticSource(
  graph: Graph,
  source: object,
  kind: "query" | "mutation",
): TransferSourceRegistration {
  const info = sources.get(source);
  if (
    info === undefined ||
    info.graph !== graph ||
    info.kind !== kind ||
    graphs.get(graph) === undefined
  )
    throw new TypeError("Expected a genuine native transfer source on this graph");
  return info;
}
function scopeAt(
  info: TransferSourceRegistration,
  key: string,
  runtime?: StoreRuntime<any>,
): ScopeRecord {
  const owner = info.binding ?? info.graph;
  const addressBook = book(owner);
  let coverage = addressBook.current.get(key);
  if (coverage === undefined) {
    coverage = { owner, key, operations: new Set(), version: {} };
    addressBook.current.set(key, coverage);
    addressBook.version = {};
  }
  return scopeRecord(info, coverage, runtime);
}
function scopeRecord(
  info: TransferSourceRegistration,
  coverage: CoverageRecord,
  runtime?: StoreRuntime<any>,
): ScopeRecord {
  const handle = Object.freeze(Object.create(null)) as TransferScope;
  const record: ScopeRecord = {
    handle,
    source: info,
    coverage,
    incarnation: runtime?.incarnation,
    runtime,
  };
  scopeRecords.set(handle, record);
  return record;
}

/**
 * Authenticates persistent coverage from an actual acquired native query target.
 * @param graph - Actual graph owning the source and target.
 * @param source - Genuine raw query member or query binding.
 * @param actualTarget - Handle returned by that exact source.
 * @returns Opaque address authority; issuance does not start an operation.
 * @throws TypeError for crossed, fabricated, inactive, or non-query handles.
 */
export function createTransferScope(
  graph: Graph,
  source: QueryTransferSource,
  actualTarget: QueryStore<any, any>,
): TransferScope {
  const info = authenticSource(graph, source, "query");
  assertStoreHandlePublished(actualTarget);
  const runtime = stores.get(actualTarget);
  if (
    runtime === undefined ||
    runtime.graph !== graph ||
    runtime.binding !== info.binding ||
    runtime.source === undefined ||
    runtime.disposed ||
    runtime.incarnation === undefined ||
    storeSources.get(actualTarget)?.definition !== info.definition ||
    storeSources.get(actualTarget)?.binding !== info.binding
  )
    throw new TypeError("Transfer scope requires the actual source target");
  if (!sourceCurrent(info)()) throw new TypeError("Transfer source is no longer current");
  return scopeAt(info, String(runtime.key), runtime).handle;
}

/**
 * Prepares persistent exclusion before the exact native mutation roles dispatch.
 * @param scopes - Authentic affected addresses; overlaps retain independent operations.
 * @param dispatches - Exact raw/bound source and carrier identities.
 * @returns Pending native publication and its opaque operation.
 * @throws TypeError for crossed sources, repeated roles/carriers, or stale scope issuance.
 */
export function prepareTransferOperation(
  scopes: readonly TransferScope[],
  dispatches: readonly TransferOperationDispatch[],
): TransferPublication & { readonly operation: TransferOperation } {
  const scopeHandles = Array.from(scopes);
  const dispatchRows = Array.from(dispatches, (entry) => ({
    role: entry.role,
    source: entry.source,
    input: entry.input,
  }));
  if (scopeHandles.length === 0 || dispatchRows.length === 0)
    throw new TypeError("An operation requires affected scopes and actual dispatch roles");
  const scopeSet = new Set<ScopeRecord>();
  const guards: (() => boolean)[] = [];
  for (const handle of scopeHandles) {
    const scope = scopeRecords.get(handle);
    if (scope === undefined) throw new TypeError("Fabricated transfer scope");
    if (
      scope.runtime !== undefined &&
      (scope.runtime.disposed || scope.runtime.incarnation !== scope.incarnation)
    )
      throw new TypeError("Scope issuance is stale");
    scopeSet.add(scope);
    guards.push(sourceCurrent(scope.source));
    if (scope.runtime !== undefined) guards.push(transferEpochCurrent(scope.runtime));
  }
  const roles = new Set<string>();
  const carriers = new Set<object>();
  const expected: DispatchExpectation[] = [];
  for (const dispatch of dispatchRows) {
    const info = sources.get(dispatch.source);
    if (
      info === undefined ||
      info.kind !== "mutation" ||
      (info.binding === undefined ? "raw" : "bound") !== dispatch.role ||
      ![...scopeSet].some((scope) => scope.source.graph === info.graph) ||
      roles.has(dispatch.role) ||
      carriers.has(dispatch.input) ||
      ((typeof dispatch.input !== "object" || dispatch.input === null) &&
        typeof dispatch.input !== "function") ||
      carrierOperations.has(dispatch.input)
    )
      throw new TypeError("Invalid native mutation dispatch role or carrier");
    roles.add(dispatch.role);
    carriers.add(dispatch.input);
    guards.push(sourceCurrent(info));
    expected.push({
      role: dispatch.role,
      source: info,
      sourceHandle: dispatch.source,
      input: dispatch.input,
      joined: false,
    });
  }
  const handle = Object.freeze(Object.create(null)) as TransferOperation;
  const record: OperationRecord = {
    handle,
    scopes: scopeSet,
    dispatches: expected,
    works: new Set(),
    active: false,
    retired: false,
    version: {},
  };
  operationRecords.set(handle, record);
  const changes: {
    coverage: CoverageRecord;
    previous: Set<OperationRecord>;
    next: Set<OperationRecord>;
    version: object;
    nextVersion: object;
  }[] = [];
  for (const scope of scopeSet)
    if (!changes.some((change) => change.coverage === scope.coverage)) {
      const coverage = scope.coverage;
      changes.push({
        coverage,
        previous: coverage.operations,
        next: new Set([...coverage.operations, record]),
        version: coverage.version,
        nextVersion: {},
      });
    }
  const publication = prepareTransferPublication(
    () => {
      if (record.active || record.retired) return false;
      for (let index = 0; index < guards.length; index += 1) if (!guards[index]!()) return false;
      for (let index = 0; index < changes.length; index += 1) {
        const change = changes[index]!;
        if (
          change.coverage.operations !== change.previous ||
          change.coverage.version !== change.version
        )
          return false;
      }
      return true;
    },
    () => {
      for (let index = 0; index < changes.length; index += 1) {
        const change = changes[index]!;
        change.coverage.operations = change.next;
        change.coverage.version = change.nextVersion;
      }
      record.active = true;
    },
    undefined,
    () => {
      for (const entry of expected)
        if (carrierOperations.get(entry.input) === record) carrierOperations.delete(entry.input);
      record.retired = true;
    },
  );
  const result = Object.freeze({ ...publication, operation: handle });
  try {
    for (const entry of expected) carrierOperations.set(entry.input, record);
  } catch (cause) {
    publication.discard();
    throw cause;
  }
  return result;
}

/**
 * Prepares all replacement addresses while retaining unresolved predecessor coverage.
 * @param entries - Actual replacement sources and once-prepared native inputs.
 * @returns One publication for the complete successor set and its opaque scopes.
 * @throws TypeError for unknown operations, crossed predecessors, or incompatible addresses.
 */
export function prepareTransferScopeSuccessors(
  entries: readonly TransferScopeSuccessor[],
): TransferPublication & { readonly scopes: readonly TransferScope[] } {
  const successorEntries = Array.from(entries, (entry) => ({
    operation: entry.operation,
    predecessor: entry.predecessor,
    graph: entry.graph,
    source: entry.source,
    input: entry.input,
  }));
  const staged: { operation: OperationRecord; scope: ScopeRecord }[] = [];
  const guards: (() => boolean)[] = [];
  const operationVersions = new Map<OperationRecord, object>();
  const bookChanges = new Map<
    AddressBook,
    {
      before: Map<string, CoverageRecord>;
      next: Map<string, CoverageRecord>;
      version: object;
      nextVersion: object;
    }
  >();
  for (const entry of successorEntries) {
    const operation = operationRecords.get(entry.operation);
    const predecessor = scopeRecords.get(entry.predecessor);
    if (
      operation === undefined ||
      predecessor === undefined ||
      !operation.active ||
      operation.retired ||
      !operation.scopes.has(predecessor)
    )
      throw new TypeError("Successor requires unresolved original operation ownership");
    const info = authenticSource(entry.graph, entry.source, "query");
    if (
      info.definition !== predecessor.source.definition ||
      (info.binding === undefined) !== (predecessor.source.binding === undefined)
    )
      throw new TypeError("Incompatible native successor source");
    if (info.binding !== undefined) {
      const oldBinding = predecessor.source.binding as { profile: object };
      if ((info.binding as { profile: object }).profile !== oldBinding.profile)
        throw new TypeError("Successor profile differs");
    }
    const before = sourceCurrent(info);
    const key = (info.definition as QueryDefinition<any, any, any, any>).key(entry.input);
    if (
      typeof key !== "string" ||
      key.length === 0 ||
      key !== predecessor.coverage.key ||
      !before()
    )
      throw new TypeError("Successor native address is incompatible or changed during preparation");
    guards.push(before);
    operationVersions.set(operation, operation.version);
    const addressBook = book(info.binding ?? info.graph);
    let bookChange = bookChanges.get(addressBook);
    if (bookChange === undefined) {
      bookChange = {
        before: addressBook.current,
        next: new Map(addressBook.current),
        version: addressBook.version,
        nextVersion: {},
      };
      bookChanges.set(addressBook, bookChange);
    }
    let coverage = bookChange.next.get(key);
    if (coverage === undefined) {
      coverage = { owner: info.binding ?? info.graph, key, operations: new Set(), version: {} };
      bookChange.next.set(key, coverage);
    }
    staged.push({ operation, scope: scopeRecord(info, coverage) });
  }
  const changes = new Map<
    CoverageRecord,
    {
      previous: Set<OperationRecord>;
      version: object;
      next: Set<OperationRecord>;
      nextVersion: object;
    }
  >();
  const nextScopes = new Map<OperationRecord, Set<ScopeRecord>>();
  for (const { operation, scope } of staged) {
    let change = changes.get(scope.coverage);
    if (change === undefined) {
      change = {
        previous: scope.coverage.operations,
        version: scope.coverage.version,
        next: new Set(scope.coverage.operations),
        nextVersion: {},
      };
      changes.set(scope.coverage, change);
    }
    change.next.add(operation);
    let next = nextScopes.get(operation);
    if (next === undefined) {
      next = new Set(operation.scopes);
      nextScopes.set(operation, next);
    }
    next.add(scope);
  }
  const addressRows = [...changes];
  const scopeRows = [...nextScopes];
  const versionRows = [...operationVersions];
  const bookRows = [...bookChanges];
  const nextVersions = scopeRows.map(() => ({}));
  const publication = prepareTransferPublication(
    () => {
      for (let index = 0; index < guards.length; index += 1) if (!guards[index]!()) return false;
      for (let index = 0; index < bookRows.length; index += 1) {
        const row = bookRows[index]!;
        if (row[0].current !== row[1].before || row[0].version !== row[1].version) return false;
      }
      for (let index = 0; index < versionRows.length; index += 1) {
        const [operation, version] = versionRows[index]!;
        if (!operation.active || operation.retired || operation.version !== version) return false;
      }
      for (let index = 0; index < addressRows.length; index += 1) {
        const [coverage, change] = addressRows[index]!;
        if (coverage.operations !== change.previous || coverage.version !== change.version)
          return false;
      }
      return true;
    },
    () => {
      for (let index = 0; index < bookRows.length; index += 1) {
        const row = bookRows[index]!;
        row[0].current = row[1].next;
        row[0].version = row[1].nextVersion;
      }
      for (let index = 0; index < addressRows.length; index += 1) {
        const row = addressRows[index]!;
        const coverage = row[0];
        const change = row[1];
        coverage.operations = change.next;
        coverage.version = change.nextVersion;
      }
      for (let index = 0; index < scopeRows.length; index += 1) {
        const row = scopeRows[index]!;
        row[0].scopes = row[1];
        row[0].version = nextVersions[index]!;
      }
    },
  );
  return Object.freeze({
    ...publication,
    scopes: Object.freeze(staged.map((entry) => entry.scope.handle)),
  });
}

/**
 * Prepares release after the caller confirms external retirement and every joined native original is known finished.
 * @param operation - Published original operation, including every successor scope.
 * @returns One publication releasing all its addresses together.
 * @throws TypeError while any dispatch is unknown, active, or cleanup-failed.
 */
export function prepareTransferOperationRetirement(
  operation: TransferOperation,
): TransferPublication {
  const record = operationRecords.get(operation);
  if (record === undefined || !record.active || record.retired)
    throw new TypeError("Unknown active transfer operation");
  const version = record.version;
  const workRows = [...record.works] as { active: boolean; cleanupFailed: boolean }[];
  const finished = (): boolean => {
    for (let index = 0; index < record.dispatches.length; index += 1)
      if (!record.dispatches[index]!.joined) return false;
    for (let index = 0; index < workRows.length; index += 1)
      if (workRows[index]!.active || workRows[index]!.cleanupFailed) return false;
    return true;
  };
  if (!finished()) throw new TypeError("Native original retirement is still unknown or incomplete");
  const changes: {
    coverage: CoverageRecord;
    previous: Set<OperationRecord>;
    next: Set<OperationRecord>;
    version: object;
    nextVersion: object;
  }[] = [];
  for (const scope of record.scopes)
    if (!changes.some((change) => change.coverage === scope.coverage)) {
      const coverage = scope.coverage;
      const next = new Set(coverage.operations);
      next.delete(record);
      changes.push({
        coverage,
        previous: coverage.operations,
        next,
        version: coverage.version,
        nextVersion: {},
      });
    }
  const nextVersion = {};
  const retiredScopes = new Set<ScopeRecord>();
  const retiredWorks = new Set<object>();
  const retiredDispatches: readonly DispatchExpectation[] = Object.freeze([]);
  return prepareTransferPublication(
    () => {
      if (!record.active || record.retired || record.version !== version || !finished())
        return false;
      for (let index = 0; index < changes.length; index += 1) {
        const change = changes[index]!;
        if (
          change.coverage.operations !== change.previous ||
          change.coverage.version !== change.version
        )
          return false;
      }
      return true;
    },
    () => {
      for (let index = 0; index < changes.length; index += 1) {
        const change = changes[index]!;
        change.coverage.operations = change.next;
        change.coverage.version = change.nextVersion;
      }
      record.active = false;
      record.retired = true;
      record.version = nextVersion;
      record.scopes = retiredScopes;
      record.works = retiredWorks;
      record.dispatches = retiredDispatches;
    },
  );
}

/** @internal Captures actual per-address eligibility and transition membership, without evaluation. */
export function transferStoreCurrent(
  runtime: StoreRuntime<any>,
  admittedFailure = false,
): () => boolean {
  const targets: object[] = [runtime, runtime.graph.__runtime];
  if (runtime.binding !== undefined) {
    targets.push(runtime.binding, runtime.binding.context);
  }
  if (runtime.observation !== undefined) targets.push(runtime.observation);
  if (runtime.stream !== undefined) targets.push(runtime.stream);
  if (runtime.boundRetirement !== undefined) targets.push(runtime.boundRetirement);
  const guards = targets.map(transferEpochCurrent);
  const address = coverageFor(runtime);
  const operations = address?.operations;
  const version = address?.version;
  const reset = runtime.graph.__runtime.resetEpoch;
  const incarnation = runtime.incarnation;
  const workRows = targets.flatMap((target) => [...(workMembership.get(target)?.rows ?? [])]);
  const workGuards = workRows.map(transferEpochCurrent);
  return () => {
    if (
      runtime.graph.__runtime.resetEpoch !== reset ||
      runtime.incarnation !== incarnation ||
      coverageFor(runtime) !== address ||
      address?.operations !== operations ||
      address?.version !== version ||
      !transferStoreAdmissible(runtime) ||
      (runtime.baseAvailable && !runtime.transferEligible && !(admittedFailure && runtime.failing))
    )
      return false;
    for (let index = 0; index < guards.length; index += 1) if (!guards[index]!()) return false;
    for (let index = 0; index < workGuards.length; index += 1)
      if (!workGuards[index]!()) return false;
    return true;
  };
}

/** @internal Cache validity guard excludes policy-only changes but includes real native semantic replacement. */
export function transferStoreSemanticCurrent(
  runtime: StoreRuntime<any>,
  prospective?: {
    readonly generation: number;
    readonly revision: ValueRevision;
    readonly incarnation: object;
    readonly base: boolean;
    readonly failing: boolean;
    readonly invalidated: boolean;
    readonly committed: unknown;
    readonly error: unknown;
  },
): () => boolean {
  const reset = runtime.graph.__runtime.resetEpoch;
  const incarnation = prospective?.incarnation ?? runtime.incarnation;
  const base = prospective?.base ?? runtime.baseAvailable;
  return () =>
    !runtime.disposed &&
    !runtime.graph.__runtime.disposed &&
    !runtime.binding?.context.disposed &&
    runtime.graph.__runtime.resetEpoch === reset &&
    runtime.incarnation === incarnation &&
    runtime.baseAvailable === base;
}
