import { Fault } from "../fault.ts";
import type { Unsubscribe } from "../fault.ts";
import { isObject, isPromiseLike } from "../utils.ts";
import { readableBrand, unwatched, watched } from "./symbols.ts";
import type {
  ComputedOptions,
  Readable,
  SignalOptions,
  WritableSignal,
  Computed,
  Signal,
  LifecycleCallback,
} from "./types.ts";
import { isSignalOptions } from "./types.ts";
import type { Watcher } from "./classes.ts";
import type { Observer } from "./types.ts";

interface WatchRecord {
  readonly source: Node<unknown>;
  readonly observer: () => void;
  active: boolean;
}

interface SubscriptionRecord {
  readonly source: Node<unknown>;
  readonly observer: (value: unknown) => void;
  lastToken: number;
  active: boolean;
  initializing: boolean;
}

interface ComputationFrame {
  readonly owner: ComputedNode<unknown> | ObserverState;
  readonly dependencies: Node<unknown>[];
  readonly dependencySet: Set<Node<unknown>>;
  readonly dependencyTokens: Map<Node<unknown>, number>;
  graphId?: string;
}

type FaultReporter = (fault: Fault) => void;
type LivenessObserver = (live: boolean) => void;
type ReadObserver = (operation: "get" | "peek") => void;

interface NodeBase<T> {
  readonly name: string;
  readonly api: Readable<T>;
  readonly equals: (this: Signal<T>, a: T, b: T) => boolean;
  dependents: Map<ComputedNode<unknown>, number>;
  captureEpoch: number;
  semanticEpoch: number;
  readonly watchers: WatchRecord[];
  subscriptions?: SubscriptionRecord[];
  readonly canonicalWatchers: Set<Watcher>;
  readonly observers: Set<ObserverState>;
  readonly watchedCallback?: LifecycleCallback<T>;
  readonly unwatchedCallback?: LifecycleCallback<T>;
  readonly livenessObservers: Set<LivenessObserver>;
  readonly readObservers: Set<ReadObserver>;
  faultReporter?: FaultReporter;
  explicitGraphId?: string;
  graphId?: string;
  token: number;
  liveDependentRefs: number;
  watcherRefs: number;
  live: boolean;
}

interface SignalNode<T> extends NodeBase<T> {
  readonly kind: "signal";
  value: T;
  writeEpoch: number;
}

interface ComputedNode<T> extends NodeBase<T> {
  readonly kind: "computed";
  readonly compute: (this: Computed<T>) => T;
  dependencies: Node<unknown>[];
  dependencyTokens: Map<Node<unknown>, number>;
  hasRun: boolean;
  hasValue: boolean;
  hasError: boolean;
  value?: T;
  error?: unknown;
  computing: boolean;
  marked: boolean;
  adoptedBaseline?: AdoptedBaseline;
  linkedBaseline?: AdoptedBaseline;
}

type Node<T> = SignalNode<T> | ComputedNode<T>;

interface AdoptedBaseline {
  readonly sources: readonly Node<unknown>[];
  readonly epochs: readonly number[];
  readonly semanticCurrent?: () => boolean;
}

interface ObserverState {
  readonly observer: Observer;
  readonly notifyCallback: (this: Observer) => void;
  dependencies: Node<unknown>[];
  dependencyTokens: Map<Node<unknown>, number>;
  pending: boolean;
  active: boolean;
  tracking: boolean;
}

const nodeByReadable = new WeakMap<object, Node<unknown>>();
const constructionByNode = new WeakMap<Node<unknown>, ReactiveConstruction<unknown>>();
const watcherNodes = new WeakSet<object>();
const sourceNodesByWatcher = new WeakMap<object, Set<Node<unknown>>>();

interface WatcherState {
  readonly notifyCallback: () => void;
  readonly sources: Set<Node<unknown>>;
  readonly pending: Set<Node<unknown>>;
  armed: boolean;
}

const watcherStates = new WeakMap<object, WatcherState>();
const observerStates = new WeakMap<object, ObserverState>();

let anonymousReadableId = 0;

const trackingStack: ComputationFrame[] = [];
let trackingSuppressed = 0;
let predictionReadRefusalDepth = 0;
let stalenessWalkDepth = 0;
let activeWalk: Set<Node<unknown>> | undefined;
let helperNotificationDepth = 0;
let canonicalNotificationDepth = 0;
let subscriptionDeliveryDepth = 0;
let observerNotificationDepth = 0;
let writeRefusalDepth = 0;
let stateEqualityDepth = 0;

interface PropagationState {
  readonly pendingSeeds: Set<Node<unknown>>;
  readonly batchSeeds: Set<Node<unknown>>;
  inProgress: boolean;
  batchDepth: number;
}

const propagation: PropagationState = {
  pendingSeeds: new Set(),
  batchSeeds: new Set(),
  inProgress: false,
  batchDepth: 0,
};

const linkedEvaluationSeeds = new Set<Node<unknown>>();
const pendingObserverNotifications = new Set<ObserverState>();
const afterPropagationQueue = new Set<() => void>();

const MAX_CONSECUTIVE_WRITES = 8;

function isReadable(value: unknown): value is Readable<unknown> {
  return isObject(value) && (value as { [readableBrand]?: unknown })[readableBrand] === true;
}

export function nodeFor<T>(readable: Readable<T>): Node<T> {
  if (!isReadable(readable)) throw new TypeError("The value is not an Aeolia readable");
  const node = nodeByReadable.get(readable);
  if (node === undefined) throw new TypeError("The value is not an Aeolia readable");
  return node as Node<T>;
}

function watcherReadFault(node: Node<unknown>): Fault {
  return new Fault("watcher-read", [node.name]);
}

function watcherWriteFault(node: Node<unknown>): Fault {
  return new Fault("watcher-write", [node.name]);
}

function writeForbiddenFault<T>(node: Node<T>): Fault {
  return new Fault("write-forbidden", [node.name]);
}

function ensureReadableOperationAllowed(node: Node<unknown>): void {
  if (
    helperNotificationDepth > 0 ||
    canonicalNotificationDepth > 0 ||
    observerNotificationDepth > 0
  ) {
    throw watcherReadFault(node);
  }
  if (predictionReadRefusalDepth > 0) throw new Fault("prediction", [node.name]);
}

function recordDependency(node: Node<unknown>): ComputationFrame | undefined {
  const frame = trackingStack[trackingStack.length - 1];
  if (frame === undefined || trackingSuppressed > 0) return frame;
  if (!frame.dependencySet.has(node)) {
    frame.dependencySet.add(node);
    frame.dependencies.push(node);
    frame.dependencyTokens.set(node, node.token);
  }
  return frame;
}

function checkGraph(node: Node<unknown>, frame: ComputationFrame | undefined): void {
  if (frame === undefined || trackingSuppressed > 0 || node.graphId === undefined) return;
  if (frame.graphId === undefined) frame.graphId = node.graphId;
  else if (frame.graphId !== node.graphId) {
    throw new Fault("cross-graph", [frame.graphId, node.graphId]);
  }
}

let nativeReadDepth = 0;

function notifyRead(node: Node<unknown>, operation: "get" | "peek"): void {
  if (stalenessWalkDepth > 0 || nativeReadDepth > 0) return;
  if (node.readObservers.size === 0) return;
  for (const observer of node.readObservers) observer(operation);
}

function cycleFault(node: ComputedNode<unknown>): Fault {
  let index = -1;
  for (let i = 0; i < trackingStack.length; i++) {
    if (trackingStack[i]!.owner === node) {
      index = i;
      break;
    }
  }
  const cycleFrames = index < 0 ? trackingStack : trackingStack.slice(index);
  const involved = new Set<string>();
  for (let i = 0; i < cycleFrames.length; i++) {
    const owner = cycleFrames[i]!.owner;
    const name = "name" in owner ? owner.name : "observer";
    if (!involved.has(name)) involved.add(name);
  }
  if (involved.size === 0) involved.add(node.name);
  return new Fault("cycle", [...involved]);
}

function addLiveEdge(source: Node<unknown>, dependent: ComputedNode<unknown>): void {
  const count = source.dependents.get(dependent) ?? 0;
  source.dependents.set(dependent, count + 1);
  source.liveDependentRefs += 1;
  source.captureEpoch += 1;
  if (!source.live && source.watcherRefs + source.liveDependentRefs > 0) activate(source);
}

function attachObserver(state: ObserverState, node: Node<unknown>): void {
  if (node.observers.has(state)) return;
  node.observers.add(state);
  node.watcherRefs += 1;
  node.captureEpoch += 1;
  if (!node.live) activate(node);
}

function detachObserver(state: ObserverState, node: Node<unknown>): void {
  if (!node.observers.delete(state)) return;
  node.watcherRefs = Math.max(0, node.watcherRefs - 1);
  node.captureEpoch += 1;
  if (node.live && node.watcherRefs + node.liveDependentRefs === 0) deactivate(node);
}

function replaceObserverDependencies(
  state: ObserverState,
  next: Node<unknown>[],
  tokens: Map<Node<unknown>, number>,
): void {
  const previous = new Set(state.dependencies);
  const incoming = new Set(next);
  state.dependencies = next;
  state.dependencyTokens = new Map(tokens);
  for (const dependency of previous) {
    if (!incoming.has(dependency)) detachObserver(state, dependency);
  }
  for (const dependency of next) {
    if (!previous.has(dependency)) attachObserver(state, dependency);
  }
}

function removeLiveEdge(source: Node<unknown>, dependent: ComputedNode<unknown>): void {
  const count = source.dependents.get(dependent);
  if (count === undefined) return;
  if (count <= 1) source.dependents.delete(dependent);
  else source.dependents.set(dependent, count - 1);
  source.liveDependentRefs = Math.max(0, source.liveDependentRefs - 1);
  source.captureEpoch += 1;
  if (source.live && source.watcherRefs + source.liveDependentRefs === 0) deactivate(source);
}

let lifecycleErrors: unknown[] | undefined;

export function withLifecycleErrors<T>(run: () => T): T {
  const outer = lifecycleErrors === undefined;
  if (outer) lifecycleErrors = [];
  let result!: T;
  let primary: unknown;
  let failed = false;
  try {
    result = run();
  } catch (error) {
    failed = true;
    primary = error;
  }
  if (!outer) {
    if (failed) throw primary;
    return result;
  }
  const errors = lifecycleErrors ?? [];
  lifecycleErrors = undefined;
  if (failed) {
    if (errors.length > 0) throwOrAggregate([...errors, primary]);
    throw primary;
  }
  if (errors.length > 0) throwOrAggregate(errors);
  return result;
}

function invokeLifecycle<T>(node: Node<T>, callback?: LifecycleCallback<T>): void {
  if (callback === undefined) return;
  try {
    callback.call(node.api);
  } catch (error) {
    if (lifecycleErrors === undefined) throw error;
    lifecycleErrors.push(error);
  }
}

function notifyLiveness(node: Node<unknown>, live: boolean): void {
  for (const observer of node.livenessObservers) {
    try {
      observer(live);
    } catch {
      // Bookkeeping must not corrupt the reactive graph.
    }
  }
}

function activate(node: Node<unknown>): void {
  if (node.live) return;
  node.live = true;
  node.captureEpoch += 1;
  invokeLifecycle(node, node.watchedCallback);
  notifyLiveness(node, true);
  if (node.kind !== "computed") return;
  for (const dependency of node.dependencies) {
    if (dependency !== node) addLiveEdge(dependency, node);
  }
}

function deactivate(node: Node<unknown>): void {
  if (!node.live) return;
  node.live = false;
  node.captureEpoch += 1;
  if (node.kind === "computed") {
    for (const dependency of node.dependencies) {
      if (dependency !== node) removeLiveEdge(dependency, node);
    }
    node.marked = false;
  }
  invokeLifecycle(node, node.unwatchedCallback);
  notifyLiveness(node, false);
}

function replaceDependencies(node: ComputedNode<unknown>, next: Node<unknown>[]): void {
  const previous = new Set(node.dependencies);
  const incoming = new Set(next);
  node.dependencies = next;
  node.dependencyTokens = new Map(next.map((dependency) => [dependency, dependency.token]));
  node.captureEpoch += 1;
  node.semanticEpoch += 1;
  node.adoptedBaseline = undefined;
  node.linkedBaseline = undefined;
  if (!node.live) return;
  for (const dependency of previous) {
    if (!incoming.has(dependency) && dependency !== node) removeLiveEdge(dependency, node);
  }
  for (const dependency of next) {
    if (!previous.has(dependency) && dependency !== node) addLiveEdge(dependency, node);
  }
}

function throwOrAggregate(errors: readonly unknown[]): never | void {
  if (errors.length === 0) return;
  if (errors.length === 1) throw errors[0];
  throw new AggregateError(errors, "Aeolia propagation failed");
}

function ensureFreshNode(node: Node<unknown>): void {
  if (node.kind === "computed") ensureFreshComputed(node);
}

function withWalk<T>(run: () => T): T {
  const root = activeWalk === undefined;
  if (root) activeWalk = new Set();
  stalenessWalkDepth += 1;
  try {
    return run();
  } finally {
    stalenessWalkDepth -= 1;
    if (root) activeWalk = undefined;
  }
}

function dependenciesChanged(
  dependencies: readonly Node<unknown>[],
  dependencyTokens: ReadonlyMap<Node<unknown>, number>,
): boolean {
  return withWalk(() => {
    for (const dependency of dependencies) {
      if (activeWalk?.has(dependency)) {
        if (dependency.token !== dependencyTokens.get(dependency)) return true;
        continue;
      }
      activeWalk?.add(dependency);
      const recordedToken = dependencyTokens.get(dependency);
      const previousSuppressed = trackingSuppressed;
      trackingSuppressed += 1;
      try {
        ensureFreshNode(dependency);
      } catch {
        // A first failing child run changes its token; a cached failure does
        // not, so the owner does not recompute forever.
        if (dependency.token !== recordedToken) return true;
        continue;
      } finally {
        trackingSuppressed = previousSuppressed;
      }
      if (dependency.token !== recordedToken) return true;
    }
    return false;
  });
}

function adoptedBaselineCurrent(baseline: AdoptedBaseline): boolean {
  for (let index = 0; index < baseline.sources.length; index += 1) {
    const source = baseline.sources[index]!;
    if (source.semanticEpoch !== baseline.epochs[index]) return false;
    if (
      source.kind === "computed" &&
      source.adoptedBaseline !== undefined &&
      !adoptedBaselineCurrent(source.adoptedBaseline)
    )
      return false;
  }
  return baseline.semanticCurrent?.() ?? true;
}

function dependencyChanged(node: ComputedNode<unknown>): boolean {
  return dependenciesChanged(node.dependencies, node.dependencyTokens);
}

function ensureFreshComputed<T>(node: ComputedNode<T>): void {
  if (node.computing) throw cycleFault(node as ComputedNode<unknown>);
  if (node.hasRun) {
    if (node.adoptedBaseline !== undefined) {
      if (adoptedBaselineCurrent(node.adoptedBaseline)) {
        if (node.marked) {
          node.marked = false;
          node.captureEpoch += 1;
        }
        if (node.hasError) throw node.error;
        return;
      }
      node.adoptedBaseline = undefined;
      node.captureEpoch += 1;
      recompute(node);
      if (node.hasError) throw node.error;
      return;
    }
    if (node.live) {
      if (!node.marked && propagation.pendingSeeds.size === 0) {
        if (node.hasError) throw node.error;
        return;
      }
      if (!dependencyChanged(node as ComputedNode<unknown>)) {
        node.marked = false;
        if (node.hasError) throw node.error;
        return;
      }
    } else if (!dependencyChanged(node as ComputedNode<unknown>)) {
      if (node.hasError) throw node.error;
      return;
    }
  }
  recompute(node);
  if (node.hasError) throw node.error;
}

function queueLinkedEvaluation(node: ComputedNode<unknown>, previousToken: number): void {
  if (node.token === previousToken) return;
  if (propagation.inProgress) propagation.pendingSeeds.add(node);
  else linkedEvaluationSeeds.add(node);
}

function flushLinkedEvaluationSeeds(): void {
  if (
    linkedEvaluationSeeds.size === 0 ||
    trackingStack.length > 0 ||
    stalenessWalkDepth > 0 ||
    stateEqualityDepth > 0 ||
    propagation.batchDepth > 0 ||
    propagation.inProgress ||
    subscriptionDeliveryDepth > 0 ||
    nativeReadDepth > 0
  )
    return;
  beginPropagation([]);
}

function recompute<T>(node: ComputedNode<T>): void {
  const linkedFirstRun = !node.hasRun && node.linkedBaseline !== undefined && node.live;
  const previousToken = node.token;
  const frame: ComputationFrame = {
    owner: node as ComputedNode<unknown>,
    dependencies: [],
    dependencySet: new Set(),
    dependencyTokens: new Map(),
    ...(node.explicitGraphId === undefined ? {} : { graphId: node.explicitGraphId }),
  };
  const hadValue = node.hasValue;
  const previous = node.value as T;
  node.computing = true;
  trackingStack.push(frame);

  let result: T | undefined;
  let thrown: unknown;
  let succeeded = false;
  let changed = true;
  const previousTrackingSuppressed = trackingSuppressed;
  trackingSuppressed = 0;
  try {
    result = node.compute.call(node.api);
    if (isPromiseLike(result)) {
      thrown = new Fault("async-compute", [node.name]);
    } else {
      succeeded = true;
      // Keep this frame active through equality. Reads in an equality
      // callback belong to this computed, not to an outer computation.
      if (hadValue) changed = !node.equals.call(node.api, previous, result as T);
    }
  } catch (error) {
    thrown = error;
  } finally {
    if (trackingStack[trackingStack.length - 1] === frame) trackingStack.pop();
    else {
      const frameIndex = trackingStack.indexOf(frame);
      if (frameIndex >= 0) trackingStack.splice(frameIndex, 1);
    }
    trackingSuppressed = previousTrackingSuppressed;
    node.computing = false;
  }

  replaceDependencies(node as ComputedNode<unknown>, frame.dependencies);
  if (node.explicitGraphId === undefined) node.graphId = frame.graphId;
  node.hasRun = true;
  node.marked = false;
  node.captureEpoch += 1;
  node.semanticEpoch += 1;
  if (!succeeded || thrown != null) {
    // A failed equality comparison is different from a failed computation:
    // the previous landing remains the cached value, and the comparison is
    // retried after the next dependency change.
    node.hasValue = succeeded && hadValue;
    node.hasError = true;
    node.error = thrown;
    node.token += 1;
    if (linkedFirstRun) queueLinkedEvaluation(node as ComputedNode<unknown>, previousToken);
    return;
  }

  node.hasValue = true;
  node.hasError = false;
  node.error = undefined;
  node.value = result as T;
  if (!hadValue || changed) node.token += 1;
  if (linkedFirstRun) queueLinkedEvaluation(node as ComputedNode<unknown>, previousToken);
}

export function readNode<T>(node: Node<T>, shouldTrack: boolean): T {
  ensureReadableOperationAllowed(node as Node<unknown>);
  const frame = shouldTrack
    ? recordDependency(node as Node<unknown>)
    : trackingStack[trackingStack.length - 1];
  notifyRead(node as Node<unknown>, shouldTrack ? "get" : "peek");
  if (node.kind === "signal") {
    checkGraph(node as Node<unknown>, frame);
    return node.value;
  }
  if (node.computing) throw cycleFault(node as ComputedNode<unknown>);
  let readFailure: unknown;
  let readFailed = false;
  try {
    ensureFreshComputed(node);
  } catch (error) {
    readFailed = true;
    readFailure = error;
  } finally {
    checkGraph(node as Node<unknown>, frame);
    if (frame !== undefined && node.kind === "computed") {
      frame.dependencyTokens.set(node as Node<unknown>, node.token);
    }
  }
  const value = node.value as T;
  let notificationFailure: unknown;
  let notificationFailed = false;
  try {
    flushLinkedEvaluationSeeds();
  } catch (error) {
    notificationFailed = true;
    notificationFailure = error;
  }
  if (readFailed && notificationFailed)
    throw new AggregateError([readFailure, notificationFailure], "Aeolia reactive read failed");
  if (readFailed) throw readFailure;
  if (notificationFailed) throw notificationFailure;
  return value;
}

function markFrom(
  seeds: Iterable<Node<unknown>>,
  protectedTransfers?: ReadonlySet<Node<unknown>>,
  directAdoptions?: ReadonlySet<Node<unknown>>,
): {
  readonly helperWatchers: WatchRecord[];
  readonly canonicalWatchers: Set<Watcher>;
  readonly subscriptions?: Set<SubscriptionRecord>;
} {
  const queue = [...seeds];
  const visited = new Set<Node<unknown>>();
  const reached = new Set<WatchRecord>();
  const canonical = new Set<Watcher>();
  let subscriptions: Set<SubscriptionRecord> | undefined;
  let queueIndex = 0;
  while (queueIndex < queue.length) {
    const current = queue[queueIndex++]!;
    if (visited.has(current)) continue;
    visited.add(current);
    if (
      protectedTransfers?.has(current) &&
      !directAdoptions?.has(current) &&
      current.kind === "computed" &&
      (current.adoptedBaseline !== undefined || current.linkedBaseline !== undefined) &&
      adoptedBaselineCurrent(current.adoptedBaseline ?? current.linkedBaseline!)
    )
      continue;
    const becameMarked = current.kind === "computed" && !current.marked;
    if (becameMarked) {
      current.marked = true;
      current.captureEpoch += 1;
    }
    for (const watcher of current.watchers) {
      if (watcher.active) reached.add(watcher);
    }
    const currentSubscriptions = current.subscriptions;
    if (currentSubscriptions !== undefined) {
      for (const subscription of currentSubscriptions) {
        if (subscription.active) (subscriptions ??= new Set()).add(subscription);
      }
    }
    for (const watcher of current.canonicalWatchers) {
      // A computed stays dirty until it is pulled. Repeated writes while it
      // remains dirty must not generate another proposal notification. State
      // signals have no dirty state of their own and notify per write.
      if (current.kind === "signal" || becameMarked) {
        markCanonicalPending(watcher, current);
        canonical.add(watcher);
      }
    }
    for (const observer of current.observers) {
      if (!observer.active || observer.pending) continue;
      observer.pending = true;
      pendingObserverNotifications.add(observer);
    }
    for (const dependent of current.dependents.keys()) queue.push(dependent);
  }
  // Sibling registration order is intentionally not a public contract.
  return {
    helperWatchers: [...reached],
    canonicalWatchers: canonical,
    ...(subscriptions === undefined ? {} : { subscriptions }),
  };
}

function beginPropagation(
  seeds: Iterable<Node<unknown>>,
  protectedTransfers?: ReadonlySet<Node<unknown>>,
  directAdoptions?: ReadonlySet<Node<unknown>>,
): void {
  if (propagation.inProgress) {
    for (const seed of seeds) propagation.pendingSeeds.add(seed);
    for (const seed of linkedEvaluationSeeds) propagation.pendingSeeds.add(seed);
    linkedEvaluationSeeds.clear();
    return;
  }
  propagation.inProgress = true;
  let consecutiveWrites = new Map<Node<unknown>, number>();
  const errors: unknown[] = [];
  let pendingSubscriptions: Set<SubscriptionRecord> | undefined;
  try {
    for (const seed of seeds) propagation.pendingSeeds.add(seed);
    for (const seed of linkedEvaluationSeeds) propagation.pendingSeeds.add(seed);
    linkedEvaluationSeeds.clear();
    while (
      propagation.pendingSeeds.size > 0 ||
      (pendingSubscriptions?.size ?? 0) > 0 ||
      afterPropagationQueue.size > 0 ||
      pendingObserverNotifications.size > 0
    ) {
      if (propagation.pendingSeeds.size > 0) {
        const currentSeeds = new Set(propagation.pendingSeeds);
        propagation.pendingSeeds.clear();
        const { helperWatchers, canonicalWatchers, subscriptions } = markFrom(
          currentSeeds,
          protectedTransfers,
          directAdoptions,
        );
        protectedTransfers = undefined;
        directAdoptions = undefined;
        if (subscriptions !== undefined) {
          for (const subscription of subscriptions)
            (pendingSubscriptions ??= new Set()).add(subscription);
        }
        for (const watcher of helperWatchers) {
          if (!watcher.active) continue;
          const previousDepth = helperNotificationDepth;
          helperNotificationDepth = previousDepth + 1;
          try {
            watcher.observer();
          } catch (error) {
            errors.push(error);
          } finally {
            helperNotificationDepth = previousDepth;
          }
        }
        for (const watcher of canonicalWatchers) {
          try {
            notifyCanonicalWatcher(watcher);
          } catch (error) {
            errors.push(error);
          }
        }
      }

      if (pendingObserverNotifications.size > 0) {
        const observers = [...pendingObserverNotifications];
        for (const observer of observers) {
          if (!observer.active) {
            pendingObserverNotifications.delete(observer);
            continue;
          }
          const previousDepth = observerNotificationDepth;
          observerNotificationDepth = previousDepth + 1;
          try {
            observer.notifyCallback.call(observer.observer);
          } catch (error) {
            errors.push(error);
          } finally {
            observerNotificationDepth = previousDepth;
            pendingObserverNotifications.delete(observer);
          }
        }
      }

      if (propagation.pendingSeeds.size === 0 && pendingSubscriptions !== undefined) {
        const settledSubscriptions = [...pendingSubscriptions];
        pendingSubscriptions = undefined;
        for (let index = 0; index < settledSubscriptions.length; index += 1) {
          const subscription = settledSubscriptions[index]!;
          if (!subscription.active || subscription.initializing) continue;
          try {
            const value = readNode(subscription.source, false);
            if (subscription.lastToken === subscription.source.token) continue;
            // Update before user code runs so a callback that writes cannot
            // recursively receive the value it is currently handling.
            subscription.lastToken = subscription.source.token;
            subscriptionDeliveryDepth += 1;
            try {
              untrack(() => subscription.observer(value));
            } finally {
              subscriptionDeliveryDepth -= 1;
            }
          } catch (error) {
            errors.push(error);
          }
          if (propagation.pendingSeeds.size > 0) {
            // The current pass is no longer settled. Defer the remaining
            // subscribers until the new seeds have gone through marking and
            // watcher notification, so they cannot read stale computeds.
            for (let remaining = index + 1; remaining < settledSubscriptions.length; remaining += 1)
              (pendingSubscriptions ??= new Set()).add(settledSubscriptions[remaining]!);
            break;
          }
        }
      }

      if (
        propagation.pendingSeeds.size === 0 &&
        (pendingSubscriptions?.size ?? 0) === 0 &&
        afterPropagationQueue.size > 0
      ) {
        const callbacks = [...afterPropagationQueue];
        afterPropagationQueue.clear();
        for (const callback of callbacks) {
          try {
            callback();
          } catch (error) {
            errors.push(error);
          }
        }
      }

      const writesThisPass = new Set(propagation.pendingSeeds);
      const nextConsecutive = new Map<Node<unknown>, number>();
      let exceeded = false;
      for (const written of writesThisPass) {
        const count = (consecutiveWrites.get(written) ?? 0) + 1;
        nextConsecutive.set(written, count);
        if (count >= MAX_CONSECUTIVE_WRITES) exceeded = true;
      }
      consecutiveWrites = nextConsecutive;
      if (exceeded) {
        errors.push(
          new Fault(
            "propagation",
            [...writesThisPass].map((node) => node.name),
          ),
        );
        propagation.pendingSeeds.clear();
        break;
      }
    }
  } finally {
    propagation.inProgress = false;
    propagation.pendingSeeds.clear();
  }
  throwOrAggregate(errors);
}

function invokeStateEquals<T>(
  node: SignalNode<T>,
  a: T,
  b: T,
  override?: (previous: T, proposed: T) => boolean,
): boolean {
  const previousSuppressed = trackingSuppressed;
  trackingSuppressed += 1;
  stateEqualityDepth += 1;
  try {
    return override === undefined
      ? node.equals.call(node.api, a, b)
      : override.call(node.api, a, b);
  } finally {
    stateEqualityDepth -= 1;
    trackingSuppressed = previousSuppressed;
  }
}

export function writeNode<T>(node: SignalNode<T>, value: T): void {
  writeState(node, value);
}

function writeState<T>(
  node: SignalNode<T>,
  value: T,
  current: () => boolean = () => true,
  commit: () => void = () => undefined,
): boolean {
  ensureWritesAllowed(node as Node<unknown>);
  if (canonicalNotificationDepth > 0 || observerNotificationDepth > 0)
    throw watcherWriteFault(node as Node<unknown>);
  const equal = invokeStateEquals(node, node.value, value);
  if (!current()) {
    flushLinkedEvaluationSeeds();
    return false;
  }
  commit();
  node.writeEpoch += 1;
  node.captureEpoch += 1;
  if (equal) {
    flushLinkedEvaluationSeeds();
    return true;
  }
  node.semanticEpoch += 1;
  if (helperNotificationDepth > 0) {
    node.value = value;
    node.token += 1;
    propagation.pendingSeeds.add(node as Node<unknown>);
    return true;
  }
  if (subscriptionDeliveryDepth > 0) {
    node.value = value;
    node.token += 1;
    if (propagation.batchDepth > 0) propagation.batchSeeds.add(node as Node<unknown>);
    else propagation.pendingSeeds.add(node as Node<unknown>);
    return true;
  }
  node.value = value;
  node.token += 1;
  if (propagation.batchDepth > 0) {
    propagation.batchSeeds.add(node as Node<unknown>);
    return true;
  }
  beginPropagation([node as Node<unknown>]);
  return true;
}

function removeWatchRecord(node: Node<unknown>, record: WatchRecord): void {
  record.active = false;
  const index = node.watchers.indexOf(record);
  if (index >= 0) node.watchers.splice(index, 1);
  node.watcherRefs = Math.max(0, node.watcherRefs - 1);
  node.captureEpoch += 1;
  if (node.live && node.watcherRefs + node.liveDependentRefs === 0) deactivate(node);
}

/** Register the eager no-argument watcher used by {@link watch}. */
export function watchReadable(source: Readable<unknown>, observer: () => void): Unsubscribe {
  return withLifecycleErrors(() => {
    const node = nodeFor(source);
    // The convenience watcher eagerly establishes a computed and permits
    // writes delivered on the next propagation pass.
    if (node.kind === "computed") readNode(node, false);

    const record: WatchRecord = {
      source: node,
      observer,
      active: true,
    };
    node.watchers.push(record);
    node.watcherRefs += 1;
    node.captureEpoch += 1;
    if (!node.live) activate(node);

    let unsubscribed = false;
    return () => {
      withLifecycleErrors(() => {
        if (unsubscribed) return;
        unsubscribed = true;
        removeWatchRecord(node, record);
      });
    };
  });
}

function removeSubscriptionRecord(node: Node<unknown>, record: SubscriptionRecord): void {
  record.active = false;
  const subscriptions = node.subscriptions;
  if (subscriptions !== undefined) {
    const index = subscriptions.indexOf(record);
    if (index >= 0) subscriptions.splice(index, 1);
    if (subscriptions.length === 0) node.subscriptions = undefined;
  }
  node.watcherRefs = Math.max(0, node.watcherRefs - 1);
  node.captureEpoch += 1;
  if (node.live && node.watcherRefs + node.liveDependentRefs === 0) deactivate(node);
}

/** Register a value subscription with immediate initial delivery. */
export function subscribeReadable<T>(
  source: Readable<T>,
  observer: (value: T) => void,
): Unsubscribe {
  let completed = false;
  let node: Node<unknown> | undefined;
  let record: SubscriptionRecord | undefined;
  try {
    const result = withLifecycleErrors(() => {
      const sourceNode = nodeFor(source) as Node<unknown>;
      node = sourceNode;
      if (typeof observer !== "function")
        throw new TypeError("Subscription observer must be a function");

      // Initial evaluation and delivery are isolated from an enclosing
      // computed. Register before delivery so writes made by that callback
      // are observed, but defer those writes through subscriptionDeliveryDepth
      // to avoid recursive callback entry.
      untrack(() => readNode(sourceNode, false));

      const subscription: SubscriptionRecord = {
        source: sourceNode,
        observer: observer as (value: unknown) => void,
        lastToken: sourceNode.token,
        active: true,
        initializing: true,
      };
      record = subscription;
      (sourceNode.subscriptions ??= []).push(subscription);
      sourceNode.watcherRefs += 1;
      sourceNode.captureEpoch += 1;
      if (!sourceNode.live) activate(sourceNode);

      // A watched lifecycle callback can synchronously write this source. The
      // value delivered to the subscriber is the value after that activation
      // has settled, not the pre-activation seed.
      const initialValue = untrack(() => readNode(sourceNode, false));
      subscription.lastToken = sourceNode.token;
      subscriptionDeliveryDepth += 1;
      let initialFailure: unknown;
      let failed = false;
      try {
        untrack(() => observer(initialValue as T));
      } catch (error) {
        failed = true;
        initialFailure = error;
      } finally {
        subscriptionDeliveryDepth -= 1;
      }
      subscription.initializing = false;

      // An initial callback may have queued writes before failing. Remove the
      // failed subscriber first, then drain those writes so existing live
      // observers still see the committed state before the failure escapes.
      if (failed) removeSubscriptionRecord(sourceNode, subscription);
      let propagationFailure: unknown;
      let propagationFailed = false;
      if (subscriptionDeliveryDepth === 0 && propagation.batchDepth === 0) {
        try {
          beginPropagation([]);
        } catch (error) {
          propagationFailed = true;
          propagationFailure = error;
        }
      }
      if (failed || propagationFailed) {
        const failures = [
          ...(failed ? [initialFailure] : []),
          ...(propagationFailed ? [propagationFailure] : []),
        ];
        throwOrAggregate(failures);
      }

      let unsubscribed = false;
      return () => {
        withLifecycleErrors(() => {
          if (unsubscribed) return;
          unsubscribed = true;
          removeSubscriptionRecord(sourceNode, subscription);
        });
      };
    });
    completed = true;
    return result;
  } finally {
    if (!completed && node !== undefined && record !== undefined && record.active) {
      // The wrapper reports lifecycle errors after its callback returns. If
      // that makes setup fail, remove the record before the error escapes.
      removeSubscriptionRecord(node, record);
    }
  }
}

function initializeNodeBase<T, K extends Node<T>["kind"]>(
  api: Readable<T>,
  kind: K,
  options: SignalOptions<T>,
): NodeBase<T> & { readonly kind: K } {
  if (!isSignalOptions(options)) throw new TypeError("Signal options are invalid");

  return {
    kind,
    name: options.label ?? `#${anonymousReadableId++}`,
    api,
    equals: options.equals ?? Object.is,
    dependents: new Map(),
    captureEpoch: 0,
    semanticEpoch: 0,
    watchers: [],
    canonicalWatchers: new Set(),
    observers: new Set(),
    watchedCallback: options[watched],
    unwatchedCallback: options[unwatched],
    livenessObservers: new Set(),
    readObservers: new Set(),
    token: 0,
    liveDependentRefs: 0,
    watcherRefs: 0,
    live: false,
  };
}

export function initializeSignalNode<T>(
  api: WritableSignal<T>,
  initial: T,
  options: SignalOptions<T>,
): SignalNode<T> {
  const node: SignalNode<T> = {
    ...initializeNodeBase(api, "signal", options),
    value: initial,
    writeEpoch: 0,
  };
  nodeByReadable.set(api, node as Node<unknown>);
  return node;
}

export function initializeComputedNode<T>(
  api: Computed<T>,
  compute: (this: Computed<T>) => T,
  options: ComputedOptions<T>,
): ComputedNode<T> {
  if (typeof compute !== "function") throw new TypeError("Computed callback must be a function");

  const node: ComputedNode<T> = {
    ...initializeNodeBase(api, "computed", options),
    compute,
    dependencies: [],
    dependencyTokens: new Map(),
    hasRun: false,
    hasValue: false,
    hasError: false,
    value: undefined,
    error: undefined,
    computing: false,
    marked: false,
  };
  nodeByReadable.set(api, node as Node<unknown>);
  return node;
}

function watcherState(watcher: Watcher): WatcherState {
  const state = watcherStates.get(watcher);
  if (state === undefined) throw new TypeError("Invalid Signal.subtle.Watcher receiver");
  return state;
}

function observerState(observer: Observer): ObserverState {
  const state = observerStates.get(observer);
  if (state === undefined) throw new TypeError("Invalid Aeolia observer receiver");
  return state;
}

function markCanonicalPending(watcher: Watcher, source: Node<unknown>): void {
  watcherState(watcher).pending.add(source);
}

function notifyCanonicalWatcher(watcher: Watcher): void {
  const state = watcherState(watcher);
  if (!state.armed) return;
  state.armed = false;
  const previousDepth = canonicalNotificationDepth;
  canonicalNotificationDepth = previousDepth + 1;
  try {
    state.notifyCallback.call(watcher);
  } finally {
    canonicalNotificationDepth = previousDepth;
  }
}

function attachCanonicalSource(watcher: Watcher, node: Node<unknown>): void {
  if (node.canonicalWatchers.has(watcher)) return;
  node.canonicalWatchers.add(watcher);
  node.watcherRefs += 1;
  node.captureEpoch += 1;
  if (!node.live) activate(node);
}

function detachCanonicalSource(watcher: Watcher, node: Node<unknown>): void {
  if (!node.canonicalWatchers.delete(watcher)) return;
  node.watcherRefs = Math.max(0, node.watcherRefs - 1);
  node.captureEpoch += 1;
  if (node.live && node.watcherRefs + node.liveDependentRefs === 0) deactivate(node);
}

/** Register a canonical watcher with the engine's singleton watcher tables. */
export function initializeWatcher(watcher: Watcher, notify: (this: Watcher) => void): void {
  if (typeof notify !== "function") throw new TypeError("Watcher notify must be a function");
  const state: WatcherState = {
    notifyCallback: notify,
    sources: new Set(),
    pending: new Set(),
    armed: true,
  };
  watcherNodes.add(watcher);
  watcherStates.set(watcher, state);
  sourceNodesByWatcher.set(watcher, state.sources);
}

/** Attach canonical watcher sources and arm the watcher. */
export function watcherWatch(watcher: Watcher, signals: Signal<unknown>[]): void {
  withLifecycleErrors(() => {
    const state = watcherState(watcher);
    if (canonicalNotificationDepth > 0 && signals.length > 0) {
      throw new Fault("watcher-write");
    }
    if (signals.length === 0) {
      state.armed = true;
      for (const node of state.sources) attachCanonicalSource(watcher, node);
      return;
    }
    const nodes = signals.map((readable) => nodeFor(readable));
    for (const node of nodes) {
      state.sources.add(node);
      attachCanonicalSource(watcher, node);
    }
    state.armed = true;
  });
}

/** Detach canonical watcher sources. */
export function watcherUnwatch(watcher: Watcher, signals: Signal<unknown>[]): void {
  withLifecycleErrors(() => {
    const state = watcherState(watcher);
    if (canonicalNotificationDepth > 0) {
      throw new Fault("watcher-write");
    }
    const nodes = signals.map((readable) => nodeFor(readable));
    for (const node of nodes) {
      if (!state.sources.delete(node)) continue;
      state.pending.delete(node);
      detachCanonicalSource(watcher, node);
    }
  });
}

/** Return a snapshot of canonical watcher computed sources that are dirty. */
export function watcherGetPending(watcher: Watcher): Signal<unknown>[] {
  const state = watcherState(watcher);
  return [...state.pending]
    .filter((node) => state.sources.has(node) && node.kind === "computed" && node.marked)
    .map((node) => node.api as Signal<unknown>);
}

/** Register the state for a framework-controlled observer. */
export function initializeObserver(observer: Observer, notify: (this: Observer) => void): void {
  if (typeof notify !== "function") throw new TypeError("Observer notify must be a function");
  observerStates.set(observer, {
    observer,
    notifyCallback: notify,
    dependencies: [],
    dependencyTokens: new Map(),
    pending: false,
    active: true,
    tracking: false,
  });
}

/** Evaluate observer work and replace its dynamic dependencies. */
export function observerTrack<T>(observer: Observer, run: () => T): T {
  const state = observerState(observer);
  if (!state.active) throw new Fault("disposed");
  if (
    observerNotificationDepth > 0 ||
    helperNotificationDepth > 0 ||
    canonicalNotificationDepth > 0
  ) {
    throw new Fault("watcher-read");
  }
  if (typeof run !== "function") throw new TypeError("Observer track callback must be a function");
  if (state.tracking) throw new Fault("cycle", ["observer"]);

  const frame: ComputationFrame = {
    owner: state,
    dependencies: [],
    dependencySet: new Set(),
    dependencyTokens: new Map(),
  };
  const previousTrackingSuppressed = trackingSuppressed;
  state.tracking = true;
  trackingStack.push(frame);
  trackingSuppressed = 0;
  let result!: T;
  let thrown: unknown;
  let succeeded = false;
  try {
    result = run();
    if (isPromiseLike(result)) thrown = new Fault("async-compute", ["observer"]);
    else succeeded = true;
  } catch (error) {
    thrown = error;
  } finally {
    if (trackingStack[trackingStack.length - 1] === frame) trackingStack.pop();
    else {
      const frameIndex = trackingStack.indexOf(frame);
      if (frameIndex >= 0) trackingStack.splice(frameIndex, 1);
    }
    trackingSuppressed = previousTrackingSuppressed;
    state.tracking = false;
  }

  if (state.active) {
    state.pending = false;
    pendingObserverNotifications.delete(state);
    replaceObserverDependencies(state, frame.dependencies, frame.dependencyTokens);
    for (const dependency of frame.dependencies) {
      if (dependency.token !== frame.dependencyTokens.get(dependency)) {
        state.pending = true;
        pendingObserverNotifications.add(state);
        break;
      }
    }
  }
  if (pendingObserverNotifications.has(state) && !propagation.inProgress) beginPropagation([]);
  else flushLinkedEvaluationSeeds();
  if (!succeeded) throw thrown;
  return result;
}

/** Check observer dependencies without evaluating the tracked callback. */
export function observerCheck(observer: Observer): boolean {
  const state = observerState(observer);
  if (!state.active) return false;
  if (
    observerNotificationDepth > 0 ||
    helperNotificationDepth > 0 ||
    canonicalNotificationDepth > 0
  ) {
    throw new Fault("watcher-read");
  }
  let changed = false;
  try {
    changed = dependenciesChanged(state.dependencies, state.dependencyTokens);
  } finally {
    state.pending = false;
    pendingObserverNotifications.delete(state);
  }
  flushLinkedEvaluationSeeds();
  return changed;
}

/** Dispose a framework-controlled observer and detach all its sources. */
export function observerDispose(observer: Observer): void {
  const state = observerState(observer);
  if (!state.active) return;
  state.active = false;
  state.pending = false;
  pendingObserverNotifications.delete(state);
  const dependencies = state.dependencies;
  state.dependencies = [];
  state.dependencyTokens = new Map();
  for (const dependency of dependencies) detachObserver(state, dependency);
}

/** Queue a callback for the post-propagation handoff. */
export function afterPropagation(callback: () => void): Unsubscribe {
  if (typeof callback !== "function")
    throw new TypeError("After-propagation callback must be a function");
  afterPropagationQueue.add(callback);
  if (!propagation.inProgress) {
    beginPropagation([]);
  }
  let active = true;
  return () => {
    if (!active) return;
    active = false;
    afterPropagationQueue.delete(callback);
  };
}

/** Test canonical watcher identity without importing the watcher constructor. */
export function isWatcher(value: unknown): value is Watcher {
  return isObject(value) && watcherNodes.has(value);
}

/** Test whether a value is backed by a state node in the canonical registry. */
export function isState(value: unknown): boolean {
  return isObject(value) && nodeByReadable.get(value)?.kind === "signal";
}

/** Test whether a value is backed by a computed node in the canonical registry. */
export function isComputed(value: unknown): boolean {
  return isObject(value) && nodeByReadable.get(value)?.kind === "computed";
}

/** Return the current computed owner, if dependency collection is active. */
export function currentComputed(): Computed<unknown> | null {
  if (trackingSuppressed > 0) return null;
  const owner = trackingStack[trackingStack.length - 1]?.owner;
  return owner !== undefined && "kind" in owner ? (owner.api as Computed<unknown>) : null;
}

/** Run a callback while suppressing dependency collection. */
export function untrack<T>(run: () => T): T {
  const previous = trackingSuppressed;
  trackingSuppressed += 1;
  try {
    return run();
  } finally {
    trackingSuppressed = previous;
  }
}

/** Run synchronous teardown while rejecting every reactive state write. */
export function withoutWrites<T>(run: () => T): T {
  if (typeof run !== "function") throw new TypeError("Read-only callback must be a function");
  writeRefusalDepth += 1;
  try {
    return run();
  } finally {
    writeRefusalDepth -= 1;
  }
}

/** Reject a state write when a read-only scope is active. */
export function ensureWritesAllowed<T>(node: Node<T>): void {
  if (writeRefusalDepth > 0) throw writeForbiddenFault(node);
}

/** Return a canonical readable's immediate sources without evaluating it. */
export function introspectSources(source: Computed<unknown> | Watcher): Signal<unknown>[] {
  if (isWatcher(source)) {
    return introspectWatcherSources(source);
  }
  return introspectComputedSources(source);
}

/** Return a canonical computed's immediate sources without evaluating it. */
export function introspectComputedSources(source: Computed<unknown>): Signal<unknown>[] {
  const node = nodeFor(source);
  if (node.kind !== "computed") throw new TypeError("Expected a Signal.Computed or Watcher");
  return [...node.dependencies].map((item) => item.api as Signal<unknown>);
}

/** Return a canonical watcher's attached sources without evaluating them. */
export function introspectWatcherSources(source: Watcher): Signal<unknown>[] {
  return [...(sourceNodesByWatcher.get(source) ?? [])].map((item) => item.api as Signal<unknown>);
}

/** Return a canonical readable's immediate sinks. */
export function introspectSinks(
  source: WritableSignal<unknown> | Computed<unknown>,
): (Signal<unknown> | Watcher)[] {
  const node = nodeFor(source);
  return [
    ...[...node.dependents.keys()].map((item) => item.api as Signal<unknown>),
    ...([...node.canonicalWatchers] as Watcher[]),
  ];
}

/** Test whether a computed or canonical watcher has at least one source. */
export function hasSources(source: Computed<unknown> | Watcher): boolean {
  return introspectSources(source).length !== 0;
}

/** Test whether a state or computed signal has at least one sink. */
export function hasSinks(source: WritableSignal<unknown> | Computed<unknown>): boolean {
  return introspectSinks(source).length !== 0;
}

/** Batch writes and propagate their combined seeds when the outer batch ends. */
export function batch<T>(run: () => T): T {
  const parentDepth = propagation.batchDepth;
  propagation.batchDepth = parentDepth + 1;
  let result!: T;
  let failure: unknown;
  let failed = false;
  try {
    result = run();
  } catch (error) {
    failure = error;
    failed = true;
  } finally {
    propagation.batchDepth = parentDepth;
    if (parentDepth === 0 && (propagation.batchSeeds.size > 0 || linkedEvaluationSeeds.size > 0)) {
      const seeds = [...propagation.batchSeeds];
      propagation.batchSeeds.clear();
      try {
        beginPropagation(seeds);
      } catch (error) {
        failure = failed ? new AggregateError([failure, error], "Aeolia batch failed") : error;
        failed = true;
      }
    }
  }
  if (failed) throw failure;
  return result;
}

/** Bind graph identity and optional fault reporting to a readable. */
export function bindReadable(readable: Readable<unknown>, binding: ReactiveGraphBinding): void {
  const node = nodeFor(readable);
  if (node.graphId != null && node.graphId !== binding.graphId) {
    throw new Fault("cross-graph", [node.graphId, binding.graphId]);
  }
  node.captureEpoch += 1;
  node.explicitGraphId = binding.graphId;
  node.graphId = binding.graphId;
  node.faultReporter = binding.reportFault;
}

/** Observe readable liveness transitions. */
export function onLivenessChange(
  readable: Readable<unknown>,
  observer: LivenessObserver,
): Unsubscribe {
  const node = nodeFor(readable);
  node.livenessObservers.add(observer);
  node.captureEpoch += 1;
  let active = true;
  return () => {
    if (!active) return;
    active = false;
    node.livenessObservers.delete(observer);
    node.captureEpoch += 1;
  };
}

/** Observe actual `get()` and `peek()` calls on a readable. */
export function onActualRead(readable: Readable<unknown>, observer: ReadObserver): Unsubscribe {
  const node = nodeFor(readable);
  node.readObservers.add(observer);
  let active = true;
  return () => {
    if (!active) return;
    active = false;
    node.readObservers.delete(observer);
  };
}

/** Read a writable state's committed value without invoking its public read path. */
export function signalValue<T>(source: WritableSignal<T>): T {
  const node = transferNode(source as Readable<unknown>);
  if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
  return node.value as T;
}

/** Run code while rejecting reactive reads made by an optimistic predictor. */
export function withPredictionReadRefusal<T>(run: () => T): T {
  const previousPredictionDepth = predictionReadRefusalDepth;
  const previousTrackingSuppressed = trackingSuppressed;
  predictionReadRefusalDepth += 1;
  trackingSuppressed += 1;
  try {
    return run();
  } finally {
    trackingSuppressed = previousTrackingSuppressed;
    predictionReadRefusalDepth = previousPredictionDepth;
  }
}

/**
 * Graph identity and fault reporting assigned to a readable by Aeolia's graph
 * integration.
 *
 * This interface is an internal module boundary. It is exported from this
 * module for the graph runtime, but is not part of the package root API.
 */
export interface ReactiveGraphBinding {
  /** Identifier used to reject reactive reads that cross graph boundaries. */
  readonly graphId: string;

  /** Optional graph callback used to route faults associated with this readable. */
  readonly reportFault?: FaultReporter;
}

const reactiveConstructionBrand = Symbol("Aeolia reactive construction");

/** Native construction identity for a State or Computed, bound to its installed node. */
export interface ReactiveConstruction<T> {
  /** The exact readable returned by the native constructor. */
  readonly source: Readable<T>;
  /** The installed native node kind. */
  readonly kind: "state" | "computed";
  /** Private nominal identity; copied fields cannot authenticate a construction. */
  readonly [reactiveConstructionBrand]: true;
}

/** A non-evaluating view of one node in a cached reactive closure. */
export interface ReactiveTransferNode {
  /** The exact native readable. */
  readonly source: Readable<unknown>;
  /** Native State or Computed kind. */
  readonly kind: "state" | "computed";
  /** Opaque construction identity for this node. */
  readonly construction: ReactiveConstruction<unknown>;
  /** Whether a Computed has completed an evaluation; true for State. */
  readonly hasRun: boolean;
  /** Whether a cached value is present, including present undefined. */
  readonly hasValue: boolean;
  /** Whether a cached failure is present. */
  readonly hasError: boolean;
  /** The cached value when hasValue is true. */
  readonly value: unknown;
  /** The cached failure when hasError is true. */
  readonly error: unknown;
  /** Exact immediate cached dependencies; an uninitialized node has none. */
  readonly sources: readonly Readable<unknown>[];
}

/** Cached closure and its pure, non-evaluating currentness witness. */
export interface ReactiveTransferCapture {
  /** Root-first transitive native cached nodes, each listed once. */
  readonly nodes: readonly ReactiveTransferNode[];
  /** Return whether every captured epoch, edge and cache remains current. */
  current(): boolean;
}

function transferQuiescent(): boolean {
  return (
    trackingStack.length === 0 &&
    stalenessWalkDepth === 0 &&
    stateEqualityDepth === 0 &&
    activeWalk === undefined &&
    propagation.batchDepth === 0 &&
    !propagation.inProgress &&
    propagation.pendingSeeds.size === 0 &&
    propagation.batchSeeds.size === 0 &&
    linkedEvaluationSeeds.size === 0 &&
    helperNotificationDepth === 0 &&
    canonicalNotificationDepth === 0 &&
    observerNotificationDepth === 0 &&
    subscriptionDeliveryDepth === 0
  );
}

function transferNode(source: Readable<unknown>): Node<unknown> {
  const node =
    (typeof source === "object" && source !== null) || typeof source === "function"
      ? nodeByReadable.get(source)
      : undefined;
  if (node === undefined || node.api !== source)
    throw new TypeError("Expected an authentic Aeolia readable");
  return node;
}

/** Return the stable opaque construction of an authentic native readable. */
export function transferConstruction<T>(source: Readable<T>): ReactiveConstruction<T> {
  const node = transferNode(source as Readable<unknown>);
  let construction = constructionByNode.get(node);
  if (construction === undefined) {
    construction = Object.freeze({
      source: node.api,
      kind: node.kind === "signal" ? ("state" as const) : ("computed" as const),
      [reactiveConstructionBrand]: true as const,
    });
    constructionByNode.set(node, construction);
  }
  return construction as ReactiveConstruction<T>;
}

/** Report whether an authentic native construction still has its original cache or State. */
function transferConstructionInitial(source: Readable<unknown>): boolean {
  const node = transferNode(source);
  return node.kind === "signal" ? node.writeEpoch === 0 : !node.hasRun;
}

/** Walk the actual cached dependency closure without reading or evaluating it. */
export function transferCapture(roots: readonly Readable<unknown>[]): ReactiveTransferCapture {
  if (!transferQuiescent()) throw new TypeError("Reactive transfer requires a quiescent graph");
  const pending = roots.map(transferNode);
  const seen = new Set<Node<unknown>>();
  const nodes: ReactiveTransferNode[] = [];
  const witnesses: { readonly node: Node<unknown>; readonly epoch: number }[] = [];
  for (let index = 0; index < pending.length; index += 1) {
    const node = pending[index]!;
    if (seen.has(node)) continue;
    seen.add(node);
    const dependencies = node.kind === "computed" ? node.dependencies : [];
    const sources = Object.freeze(dependencies.map((dependency) => dependency.api));
    nodes.push(
      Object.freeze({
        source: node.api,
        kind: node.kind === "signal" ? ("state" as const) : ("computed" as const),
        construction: transferConstruction(node.api),
        hasRun: node.kind === "signal" || node.hasRun,
        hasValue: node.kind === "signal" || node.hasValue,
        hasError: node.kind === "computed" && node.hasError,
        value: node.value,
        error: node.kind === "computed" ? node.error : undefined,
        sources,
      }),
    );
    witnesses.push({ node, epoch: node.captureEpoch });
    for (const dependency of dependencies) pending.push(dependency);
  }
  const captured = Object.freeze(nodes);
  return Object.freeze({
    nodes: captured,
    current: (): boolean => {
      if (!transferQuiescent()) return false;
      for (let index = 0; index < witnesses.length; index += 1) {
        const { node, epoch } = witnesses[index]!;
        if (node.captureEpoch !== epoch) return false;
        if (node.kind === "computed") {
          if (node.computing || node.marked) return false;
          if (node.adoptedBaseline !== undefined) {
            // The adopted baseline covers passive, uninitialized native facets.
            if (!adoptedBaselineCurrent(node.adoptedBaseline)) return false;
          } else if (node.hasRun) {
            for (let sourceIndex = 0; sourceIndex < node.dependencies.length; sourceIndex += 1) {
              const dependency = node.dependencies[sourceIndex]!;
              if (node.dependencyTokens.get(dependency) !== dependency.token) return false;
              if (dependency.kind === "computed" && !dependency.hasRun) return false;
            }
          }
        }
      }
      return true;
    },
  });
}

/** A mapped State write staged for the same publication as cache adoption. */
export interface ReactiveTransferWrite {
  /** Exact native State to update. */
  readonly target: WritableSignal<unknown>;
  /** Prospective value; equality is decided during preparation. */
  readonly value: unknown;
  /** Trusted native policy comparator, called once with the exact State as receiver. */
  readonly equals?: (previous: unknown, proposed: unknown) => boolean;
}

/** A cached Computed landing staged without invoking its evaluator. */
export interface ReactiveTransferAdoption {
  /** Exact native Computed to populate. */
  readonly target: Computed<unknown>;
  /** Actual mapped immediate dependencies in cached order. */
  readonly sources: readonly Readable<unknown>[];
  /** Cached value, including present undefined when hasValue is true. */
  readonly value: unknown;
  /** Cached failure when hasError is true. */
  readonly error: unknown;
  /** Whether a cached value is present. */
  readonly hasValue: boolean;
  /** Whether a cached failure is present. */
  readonly hasError: boolean;
  /** Trusted native semantic guard for Store facets omitted from the cache image. */
  readonly semanticCurrent?: () => boolean;
}

/** Native-known dependency edges installed on an unrun Computed without evaluation. */
export interface ReactiveTransferLink {
  /** Authentic unrun native Computed whose cached fields remain untouched. */
  readonly target: Computed<unknown>;
  /** Exact immediate native dependencies to install before exposure. */
  readonly sources: readonly Readable<unknown>[];
}

/** Prepared native reactive publication with a pure final guard and deferred delivery. */
export interface ReactiveTransferPlan {
  /** Check all preparation epochs without evaluating reactive or authored code. */
  current(): boolean;
  /** Read the value a planned State retains after prepared equality, before publication. */
  valueFor(target: WritableSignal<unknown>): unknown;
  /** Install the complete prebuilt State, cache and edge state without callbacks. */
  publish(): void;
  /** Deliver propagation and lifecycle callbacks after caller-wide publication. */
  notify(): void;
}

interface PreparedTransferWrite {
  readonly node: SignalNode<unknown>;
  readonly value: unknown;
  readonly changed: boolean;
  readonly token: number;
  readonly semanticEpoch: number;
}

interface PreparedTransferAdoption {
  readonly node: ComputedNode<unknown>;
  readonly sources: Node<unknown>[];
  readonly tokens: Map<Node<unknown>, number>;
  readonly baseline: AdoptedBaseline;
  readonly value: unknown;
  readonly error: unknown;
  readonly hasValue: boolean;
  readonly hasError: boolean;
  readonly token: number;
  graphId: string | undefined;
}

interface PreparedTransferLink {
  readonly node: ComputedNode<unknown>;
  readonly sources: Node<unknown>[];
  readonly tokens: Map<Node<unknown>, number>;
  readonly baseline: AdoptedBaseline;
  graphId: string | undefined;
}

interface PreparedTransferLiveness {
  readonly node: Node<unknown>;
  readonly dependents: Map<ComputedNode<unknown>, number>;
  readonly refs: number;
  readonly live: boolean;
  readonly changedLive: boolean;
}

function prepareTransferLiveness(
  nextSources: ReadonlyMap<ComputedNode<unknown>, readonly Node<unknown>[]>,
): PreparedTransferLiveness[] {
  const edges = new Map<Node<unknown>, Map<ComputedNode<unknown>, number>>();
  const refs = new Map<Node<unknown>, number>();
  const live = new Map<Node<unknown>, boolean>();
  const queue: Node<unknown>[] = [];

  const effectiveSources = (node: ComputedNode<unknown>): readonly Node<unknown>[] =>
    nextSources.get(node) ?? node.dependencies;
  const edgeDelta = (
    source: Node<unknown>,
    dependent: ComputedNode<unknown>,
    delta: number,
  ): void => {
    let next = edges.get(source);
    if (next === undefined) {
      next = new Map(source.dependents);
      edges.set(source, next);
    }
    const count = (next.get(dependent) ?? 0) + delta;
    if (count < 0) throw new TypeError("Reactive transfer has an inconsistent live edge");
    if (count === 0) next.delete(dependent);
    else next.set(dependent, count);
    refs.set(source, (refs.get(source) ?? source.liveDependentRefs) + delta);
    queue.push(source);
  };

  for (const [node, sources] of nextSources) {
    if (!node.live) continue;
    const previous = new Set(node.dependencies);
    const incoming = new Set(sources);
    for (const source of previous) {
      if (!incoming.has(source) && source !== node) edgeDelta(source, node, -1);
    }
    for (const source of incoming) {
      if (!previous.has(source) && source !== node) edgeDelta(source, node, 1);
    }
  }

  for (let index = 0; index < queue.length; index += 1) {
    const node = queue[index]!;
    const shouldLive = node.watcherRefs + (refs.get(node) ?? node.liveDependentRefs) > 0;
    const wasLive = live.get(node) ?? node.live;
    if (shouldLive === wasLive) continue;
    live.set(node, shouldLive);
    if (node.kind === "computed") {
      for (const source of effectiveSources(node)) {
        if (source !== node) edgeDelta(source, node, shouldLive ? 1 : -1);
      }
    }
    if (queue.length > 100000)
      throw new TypeError("Reactive transfer has an unstable liveness graph");
  }

  const changed = new Set<Node<unknown>>([...edges.keys(), ...live.keys()]);
  return [...changed].map((node) => ({
    node,
    dependents: edges.get(node) ?? node.dependents,
    refs: refs.get(node) ?? node.liveDependentRefs,
    live: live.get(node) ?? node.live,
    changedLive: (live.get(node) ?? node.live) !== node.live,
  }));
}

/**
 * Prepare mapped State writes, cached Computed adoption and native links without publishing.
 * The caller must run current and publish back-to-back after its other fallible
 * checks, then notify only after its whole logical transaction is visible.
 */
export function transferPrepare(
  writes: readonly ReactiveTransferWrite[],
  adoptions: readonly ReactiveTransferAdoption[],
  links: readonly ReactiveTransferLink[] = [],
): ReactiveTransferPlan {
  if (!transferQuiescent()) throw new TypeError("Reactive transfer requires a quiescent graph");

  const preparedWrites: PreparedTransferWrite[] = [];
  const preparedAdoptions: PreparedTransferAdoption[] = [];
  const preparedLinks: PreparedTransferLink[] = [];
  const writeByNode = new Map<SignalNode<unknown>, PreparedTransferWrite>();
  const adoptionByNode = new Map<ComputedNode<unknown>, PreparedTransferAdoption>();
  const linkByNode = new Map<ComputedNode<unknown>, PreparedTransferLink>();
  const originalEpochs = new Map<Node<unknown>, number>();
  const rootNodes: Node<unknown>[] = [];
  const rememberedClosure = new Set<Node<unknown>>();
  const remember = (node: Node<unknown>): void => {
    if (!originalEpochs.has(node)) originalEpochs.set(node, node.captureEpoch);
  };
  const rememberClosure = (root: Node<unknown>): void => {
    const pending = [root];
    while (pending.length > 0) {
      const node = pending.pop()!;
      if (rememberedClosure.has(node)) continue;
      rememberedClosure.add(node);
      remember(node);
      if (node.kind === "computed") pending.push(...node.dependencies);
    }
  };

  const writeInputs = writes.map((write) => {
    const node = transferNode(write.target);
    remember(node);
    const equals = write.equals;
    return { node, value: write.value, equals };
  });
  const adoptionInputs = adoptions.map((adoption) => {
    const node = transferNode(adoption.target);
    remember(node);
    const sources = adoption.sources.map((source) => {
      const dependency = transferNode(source);
      rememberClosure(dependency);
      return dependency;
    });
    return {
      node,
      sources,
      value: adoption.value,
      error: adoption.error,
      hasValue: adoption.hasValue,
      hasError: adoption.hasError,
      semanticCurrent: adoption.semanticCurrent,
    };
  });
  const linkInputs = links.map((link) => {
    const node = transferNode(link.target);
    remember(node);
    const sources = link.sources.map((source) => {
      const dependency = transferNode(source);
      rememberClosure(dependency);
      return dependency;
    });
    return { node, sources };
  });
  for (const [node, epoch] of originalEpochs) {
    if (node.captureEpoch !== epoch)
      throw new TypeError("Reactive transfer changed during input preparation");
  }
  if (!transferQuiescent())
    throw new TypeError("Reactive transfer changed during input preparation");

  for (const write of writeInputs) {
    const node = write.node;
    if (node.kind !== "signal" || writeByNode.has(node))
      throw new TypeError("Reactive transfer requires distinct native State targets");
    ensureWritesAllowed(node);
    if (write.equals !== undefined && typeof write.equals !== "function")
      throw new TypeError("Reactive transfer State comparator must be a function");
    const changed = !invokeStateEquals(node, node.value, write.value, write.equals);
    const prepared: PreparedTransferWrite = {
      node,
      value: write.value,
      changed,
      token: node.token + (changed ? 1 : 0),
      semanticEpoch: node.semanticEpoch + (changed ? 1 : 0),
    };
    preparedWrites.push(prepared);
    writeByNode.set(node, prepared);
    rootNodes.push(node);
  }
  const preparedValueFor = (target: WritableSignal<unknown>): unknown => {
    const node = transferNode(target);
    if (node.kind !== "signal")
      throw new TypeError("Reactive transfer value requires a planned native State");
    const write = writeByNode.get(node);
    if (write === undefined)
      throw new TypeError("Reactive transfer value requires a planned native State");
    return write.changed ? write.value : write.node.value;
  };

  for (const adoption of adoptionInputs) {
    const node = adoption.node;
    if (node.kind !== "computed" || adoptionByNode.has(node))
      throw new TypeError("Reactive transfer requires distinct native Computed targets");
    if (!adoption.hasValue && !adoption.hasError)
      throw new TypeError("Reactive transfer requires a cached value or failure");
    if (adoption.semanticCurrent !== undefined && typeof adoption.semanticCurrent !== "function")
      throw new TypeError("Reactive transfer semantic guard must be a function");
    const sources = adoption.sources;
    if (new Set(sources).size !== sources.length)
      throw new TypeError("Reactive transfer requires distinct cached sources");
    if (sources.includes(node)) throw new TypeError("Reactive transfer cannot adopt a self edge");
    let graphId = node.explicitGraphId;
    for (const source of sources) {
      if (source.graphId !== undefined) {
        if (graphId === undefined) graphId = source.graphId;
        else if (graphId !== source.graphId)
          throw new Fault("cross-graph", [graphId, source.graphId]);
      }
      rootNodes.push(source);
    }
    let changed = true;
    if (!adoption.hasError && node.hasValue) {
      const previousSuppressed = trackingSuppressed;
      trackingSuppressed += 1;
      try {
        changed = !node.equals.call(node.api, node.value, adoption.value);
      } finally {
        trackingSuppressed = previousSuppressed;
      }
    }
    const prepared: PreparedTransferAdoption = {
      node,
      sources,
      tokens: new Map(),
      baseline: {
        sources: [],
        epochs: [],
        ...(adoption.semanticCurrent === undefined
          ? {}
          : { semanticCurrent: adoption.semanticCurrent }),
      },
      value: adoption.value,
      error: adoption.error,
      hasValue: adoption.hasValue,
      hasError: adoption.hasError,
      token: node.token + (changed ? 1 : 0),
      graphId,
    };
    preparedAdoptions.push(prepared);
    adoptionByNode.set(node, prepared);
    rootNodes.push(node);
  }

  for (const link of linkInputs) {
    const node = link.node;
    if (node.kind !== "computed" || node.hasRun || adoptionByNode.has(node) || linkByNode.has(node))
      throw new TypeError("Reactive transfer links require distinct unrun native Computeds");
    const sources = link.sources;
    if (new Set(sources).size !== sources.length || sources.includes(node))
      throw new TypeError("Reactive transfer link has duplicate or self sources");
    for (const source of sources) rootNodes.push(source);
    const prepared: PreparedTransferLink = {
      node,
      sources,
      tokens: new Map(),
      baseline: { sources: [], epochs: [] },
      graphId: undefined,
    };
    preparedLinks.push(prepared);
    linkByNode.set(node, prepared);
    rootNodes.push(node);
  }

  for (const [node, epoch] of originalEpochs) {
    if (node.captureEpoch !== epoch)
      throw new TypeError("Reactive transfer changed during equality preparation");
  }
  if (!transferQuiescent()) throw new TypeError("Reactive transfer changed during preparation");

  const nextSources = new Map<ComputedNode<unknown>, readonly Node<unknown>[]>();
  for (const adoption of preparedAdoptions) nextSources.set(adoption.node, adoption.sources);
  for (const link of preparedLinks) nextSources.set(link.node, link.sources);

  for (const [target, sources] of nextSources) {
    const visited = new Set<Node<unknown>>();
    const pending = [...sources];
    while (pending.length > 0) {
      const source = pending.pop()!;
      if (source === target)
        throw new TypeError("Reactive transfer would create a dependency cycle");
      if (visited.has(source)) continue;
      visited.add(source);
      if (source.kind === "computed")
        pending.push(...(nextSources.get(source) ?? source.dependencies));
    }
  }

  const graphIds = new Map<ComputedNode<unknown>, string | undefined>();
  function graphIdFor(node: Node<unknown>): string | undefined {
    if (node.kind === "signal" || !nextSources.has(node)) return node.graphId;
    if (graphIds.has(node)) return graphIds.get(node);
    let graphId = node.explicitGraphId;
    for (const source of nextSources.get(node)!) {
      const sourceGraphId = graphIdFor(source);
      if (sourceGraphId === undefined) continue;
      if (graphId === undefined) graphId = sourceGraphId;
      else if (graphId !== sourceGraphId) throw new Fault("cross-graph", [graphId, sourceGraphId]);
    }
    graphIds.set(node, graphId);
    return graphId;
  }
  for (const adoption of preparedAdoptions) adoption.graphId = graphIdFor(adoption.node);
  for (const link of preparedLinks) link.graphId = graphIdFor(link.node);

  for (const prepared of [...preparedAdoptions, ...preparedLinks]) {
    for (const source of prepared.sources) {
      prepared.tokens.set(
        source,
        source.kind === "signal"
          ? (writeByNode.get(source)?.token ?? source.token)
          : (adoptionByNode.get(source)?.token ?? source.token),
      );
    }
    const baselineSources = prepared.baseline.sources as Node<unknown>[];
    const baselineSeen = new Set<Node<unknown>>();
    const baselinePending = [...prepared.sources];
    for (let index = 0; index < baselinePending.length; index += 1) {
      const source = baselinePending[index]!;
      if (baselineSeen.has(source)) continue;
      baselineSeen.add(source);
      baselineSources.push(source);
      if (source.kind === "computed")
        baselinePending.push(...(nextSources.get(source) ?? source.dependencies));
    }
    const epochs = prepared.baseline.epochs as number[];
    for (const source of baselineSources)
      epochs.push(
        source.kind === "signal"
          ? (writeByNode.get(source)?.semanticEpoch ?? source.semanticEpoch)
          : nextSources.has(source)
            ? source.semanticEpoch + 1
            : source.semanticEpoch,
      );
    if (prepared.baseline.semanticCurrent !== undefined && !prepared.baseline.semanticCurrent())
      throw new TypeError("Reactive transfer semantic source is stale");
  }

  const liveness = prepareTransferLiveness(nextSources);
  for (const entry of liveness) rootNodes.push(entry.node);
  const witness = transferCapture(rootNodes.map((node) => node.api));
  if (!witness.current()) throw new TypeError("Reactive transfer became stale during preparation");

  const changedStates = preparedWrites
    .filter((write) => write.changed)
    .map((write) => write.node as Node<unknown>);
  const changedAdoptions = preparedAdoptions
    .filter((adoption) => adoption.token !== adoption.node.token)
    .map((adoption) => adoption.node as Node<unknown>);
  const directAdoptions = new Set<Node<unknown>>(changedAdoptions);
  const protectedTransfers = new Set<Node<unknown>>([
    ...preparedAdoptions.map((adoption) => adoption.node),
    ...preparedLinks.map((link) => link.node),
  ]);
  const seeds = [...changedStates, ...changedAdoptions];
  let published = false;
  let notified = false;

  return Object.freeze({
    valueFor: (target: WritableSignal<unknown>): unknown => {
      if (published) throw new TypeError("Reactive transfer was already published");
      return preparedValueFor(target);
    },
    current: (): boolean => {
      if (published || !witness.current()) return false;
      for (let index = 0; index < preparedAdoptions.length; index += 1) {
        const semanticCurrent = preparedAdoptions[index]!.baseline.semanticCurrent;
        if (semanticCurrent !== undefined && !semanticCurrent()) return false;
      }
      return true;
    },
    publish: (): void => {
      if (published) throw new TypeError("Reactive transfer was already published");
      published = true;
      for (let index = 0; index < preparedWrites.length; index += 1) {
        const write = preparedWrites[index]!;
        if (write.changed) write.node.value = write.value;
        write.node.token = write.token;
        write.node.semanticEpoch = write.semanticEpoch;
        write.node.writeEpoch += 1;
        write.node.captureEpoch += 1;
      }
      for (let index = 0; index < preparedAdoptions.length; index += 1) {
        const adoption = preparedAdoptions[index]!;
        const node = adoption.node;
        node.dependencies = adoption.sources;
        node.dependencyTokens = adoption.tokens;
        node.hasRun = true;
        node.hasValue = adoption.hasValue;
        node.hasError = adoption.hasError;
        node.value = adoption.value;
        node.error = adoption.error;
        node.token = adoption.token;
        node.graphId = adoption.graphId;
        node.marked = false;
        node.semanticEpoch += 1;
        node.captureEpoch += 1;
        node.adoptedBaseline = adoption.baseline;
      }
      for (let index = 0; index < preparedLinks.length; index += 1) {
        const link = preparedLinks[index]!;
        const node = link.node;
        node.dependencies = link.sources;
        node.dependencyTokens = link.tokens;
        node.graphId = link.graphId;
        node.semanticEpoch += 1;
        node.captureEpoch += 1;
        node.linkedBaseline = link.baseline;
      }
      for (let index = 0; index < liveness.length; index += 1) {
        const entry = liveness[index]!;
        const node = entry.node;
        node.dependents = entry.dependents;
        node.liveDependentRefs = entry.refs;
        if (entry.changedLive) {
          node.live = entry.live;
          if (!entry.live && node.kind === "computed") node.marked = false;
        }
        node.captureEpoch += 1;
      }
    },
    notify: (): void => {
      if (!published || notified)
        throw new TypeError("Reactive transfer is not awaiting notification");
      notified = true;
      const errors: unknown[] = [];
      const deliverLiveness = (entry: PreparedTransferLiveness): void => {
        try {
          invokeLifecycle(
            entry.node,
            entry.live ? entry.node.watchedCallback : entry.node.unwatchedCallback,
          );
        } catch (error) {
          errors.push(error);
        }
        notifyLiveness(entry.node, entry.live);
      };
      propagation.batchDepth += 1;
      try {
        for (let index = liveness.length - 1; index >= 0; index -= 1) {
          const entry = liveness[index]!;
          if (entry.changedLive && !entry.live) deliverLiveness(entry);
        }
        for (let index = 0; index < liveness.length; index += 1) {
          const entry = liveness[index]!;
          if (entry.changedLive && entry.live) deliverLiveness(entry);
        }
      } finally {
        propagation.batchDepth -= 1;
      }
      const actualSeeds = [...propagation.batchSeeds];
      propagation.batchSeeds.clear();
      try {
        beginPropagation([...seeds, ...actualSeeds], protectedTransfers, directAdoptions);
      } catch (error) {
        errors.push(error);
      }
      throwOrAggregate(errors);
    },
  });
}

/**
 * Internal hooks used by the graph runtime.
 *
 * These hooks are exported from the module for implementation composition and
 * are intentionally omitted from the package root. They are not a stable
 * application API.
 */
export const __internal = Object.freeze({
  /** Return the installed construction without a public brand or evaluator call. */
  transferConstruction,

  /** Test original native construction state independently of late capture. */
  transferConstructionInitial,

  /** Snapshot the actual cached dependency closure without evaluation. */
  transferCapture,

  /** Prepare fixed reactive publication and deferred notification. */
  transferPrepare,
  /** Read the existing computed without outer tracking or retention callbacks. */
  nativePeek: <T>(readable: Readable<T>, current: () => boolean = () => true): T => {
    const node = nodeFor(readable);
    nativeReadDepth += 1;
    try {
      return readNode(node, false);
    } finally {
      nativeReadDepth -= 1;
      // Reentrant equality can leave an obsolete result beside successor tokens.
      // Cold reads ignore marked when those tokens match, so invalidate the cached run.
      if (node.kind === "computed" && !current()) {
        node.hasRun = false;
        node.marked = true;
        node.adoptedBaseline = undefined;
        node.linkedBaseline = undefined;
        node.captureEpoch += 1;
        node.semanticEpoch += 1;
      }
    }
  },

  /** Commit native bookkeeping after equality and before the guarded storage. */
  guardedWrite: <T>(
    source: WritableSignal<T>,
    value: T,
    current: () => boolean,
    commit: () => void,
  ): boolean => {
    const node = nodeFor(source);
    if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
    return writeState(node, value, current, commit);
  },
  /** Reject forbidden managed writes before their non-signal bookkeeping changes. */
  assertWritesAllowed: (readable: Readable<unknown>): void => {
    const node = nodeFor(readable);
    ensureWritesAllowed(node);
    if (canonicalNotificationDepth > 0 || observerNotificationDepth > 0)
      throw watcherWriteFault(node);
  },

  /** Batch writes and propagate their combined seeds when the outer batch ends. */
  batch,

  /** Bind a readable to a graph identity and optional asynchronous fault sink. */
  bindReadable,

  /** Report whether the active computation stack contains a live computation. */
  isCurrentComputationLive: (): boolean | undefined =>
    trackingStack.length === 0
      ? undefined
      : trackingStack.some((frame) => "kind" in frame.owner && frame.owner.live),

  /** Report whether a readable currently has a live descendant. */
  isLive: (readable: Readable<unknown>): boolean => nodeFor(readable).live,

  /** Report whether a watcher callback is currently being notified. */
  isNotifying: (): boolean =>
    helperNotificationDepth > 0 || canonicalNotificationDepth > 0 || observerNotificationDepth > 0,

  /** Report whether the graph is checking computed staleness rather than reading user state. */
  isStalenessWalk: (): boolean => stalenessWalkDepth > 0,

  /** Observe actual `get()` and `peek()` calls on a readable. */
  onActualRead,

  /** Observe transitions into and out of the live state. */
  onLivenessChange,

  /** Read a writable signal's committed value without invoking its public read path. */
  signalValue,

  /** Run code while rejecting reactive reads made by an optimistic predictor. */
  withPredictionReadRefusal,
});
