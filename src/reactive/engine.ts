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
import type { Watcher } from "./classes.ts";

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
  readonly owner: ComputedNode<unknown>;
  readonly dependencies: Node<unknown>[];
  readonly dependencySet: Set<Node<unknown>>;
  graphId?: string;
}

type FaultReporter = (fault: Fault) => void;
type LivenessObserver = (live: boolean) => void;
type ReadObserver = (operation: "get" | "peek") => void;

interface NodeBase<T> {
  readonly name: string;
  readonly api: Readable<T>;
  readonly equals: (this: Signal<T>, a: T, b: T) => boolean;
  readonly dependents: Map<ComputedNode<unknown>, number>;
  readonly watchers: WatchRecord[];
  subscriptions?: SubscriptionRecord[];
  readonly canonicalWatchers: Set<Watcher>;
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
}

type Node<T> = SignalNode<T> | ComputedNode<T>;

const nodeByReadable = new WeakMap<object, Node<unknown>>();
const watcherNodes = new WeakSet<object>();
const sourceNodesByWatcher = new WeakMap<object, Set<Node<unknown>>>();

interface WatcherState {
  readonly notifyCallback: () => void;
  readonly sources: Set<Node<unknown>>;
  readonly pending: Set<Node<unknown>>;
  armed: boolean;
}

const watcherStates = new WeakMap<object, WatcherState>();

let anonymousReadableId = 0;

const trackingStack: ComputationFrame[] = [];
let trackingSuppressed = 0;
let predictionReadRefusalDepth = 0;
let stalenessWalkDepth = 0;
let activeWalk: Set<Node<unknown>> | undefined;
let helperNotificationDepth = 0;
let canonicalNotificationDepth = 0;
let subscriptionDeliveryDepth = 0;

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

function ensureReadableOperationAllowed(node: Node<unknown>): void {
  if (helperNotificationDepth > 0 || canonicalNotificationDepth > 0) {
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

function notifyRead(node: Node<unknown>, operation: "get" | "peek"): void {
  if (stalenessWalkDepth > 0) return;
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
    const name = cycleFrames[i]!.owner.name;
    if (!involved.has(name)) involved.add(name);
  }
  if (involved.size === 0) involved.add(node.name);
  return new Fault("cycle", [...involved]);
}

function addLiveEdge(source: Node<unknown>, dependent: ComputedNode<unknown>): void {
  const count = source.dependents.get(dependent) ?? 0;
  source.dependents.set(dependent, count + 1);
  source.liveDependentRefs += 1;
  if (!source.live && source.watcherRefs + source.liveDependentRefs > 0) activate(source);
}

function removeLiveEdge(source: Node<unknown>, dependent: ComputedNode<unknown>): void {
  const count = source.dependents.get(dependent);
  if (count === undefined) return;
  if (count <= 1) source.dependents.delete(dependent);
  else source.dependents.set(dependent, count - 1);
  source.liveDependentRefs = Math.max(0, source.liveDependentRefs - 1);
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

function dependencyChanged(node: ComputedNode<unknown>): boolean {
  return withWalk(() => {
    for (const dependency of node.dependencies) {
      if (activeWalk?.has(dependency)) {
        if (dependency.token !== node.dependencyTokens.get(dependency)) return true;
        continue;
      }
      activeWalk?.add(dependency);
      const recordedToken = node.dependencyTokens.get(dependency);
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

function ensureFreshComputed<T>(node: ComputedNode<T>): void {
  if (node.computing) throw cycleFault(node as ComputedNode<unknown>);
  if (node.hasRun) {
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

function recompute<T>(node: ComputedNode<T>): void {
  const frame: ComputationFrame = {
    owner: node as ComputedNode<unknown>,
    dependencies: [],
    dependencySet: new Set(),
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
  if (!succeeded || thrown != null) {
    // A failed equality comparison is different from a failed computation:
    // the previous landing remains the cached value, and the comparison is
    // retried after the next dependency change.
    node.hasValue = succeeded && hadValue;
    node.hasError = true;
    node.error = thrown;
    node.token += 1;
    return;
  }

  node.hasValue = true;
  node.hasError = false;
  node.error = undefined;
  node.value = result as T;
  if (!hadValue || changed) node.token += 1;
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
  try {
    ensureFreshComputed(node);
  } finally {
    checkGraph(node as Node<unknown>, frame);
  }
  if (node.hasError) throw node.error;
  return node.value as T;
}

function markFrom(seeds: Iterable<Node<unknown>>): {
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
    const becameMarked = current.kind === "computed" && !current.marked;
    if (becameMarked) current.marked = true;
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
    for (const dependent of current.dependents.keys()) queue.push(dependent);
  }
  // Sibling registration order is intentionally not a public contract.
  return {
    helperWatchers: [...reached],
    canonicalWatchers: canonical,
    ...(subscriptions === undefined ? {} : { subscriptions }),
  };
}

function beginPropagation(seeds: Iterable<Node<unknown>>): void {
  if (propagation.inProgress) {
    for (const seed of seeds) propagation.pendingSeeds.add(seed);
    return;
  }
  propagation.inProgress = true;
  let consecutiveWrites = new Map<Node<unknown>, number>();
  const errors: unknown[] = [];
  let pendingSubscriptions: Set<SubscriptionRecord> | undefined;
  try {
    for (const seed of seeds) propagation.pendingSeeds.add(seed);
    while (propagation.pendingSeeds.size > 0 || (pendingSubscriptions?.size ?? 0) > 0) {
      if (propagation.pendingSeeds.size > 0) {
        const currentSeeds = new Set(propagation.pendingSeeds);
        propagation.pendingSeeds.clear();
        const { helperWatchers, canonicalWatchers, subscriptions } = markFrom(currentSeeds);
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

function invokeStateEquals<T>(node: SignalNode<T>, a: T, b: T): boolean {
  const previousSuppressed = trackingSuppressed;
  trackingSuppressed += 1;
  try {
    return node.equals.call(node.api, a, b);
  } finally {
    trackingSuppressed = previousSuppressed;
  }
}

export function writeNode<T>(node: SignalNode<T>, value: T): void {
  if (canonicalNotificationDepth > 0) throw watcherWriteFault(node as Node<unknown>);
  if (helperNotificationDepth > 0) {
    if (!invokeStateEquals(node, node.value, value)) {
      node.value = value;
      node.token += 1;
      propagation.pendingSeeds.add(node as Node<unknown>);
    }
    return;
  }
  if (subscriptionDeliveryDepth > 0) {
    if (!invokeStateEquals(node, node.value, value)) {
      node.value = value;
      node.token += 1;
      if (propagation.batchDepth > 0) propagation.batchSeeds.add(node as Node<unknown>);
      else propagation.pendingSeeds.add(node as Node<unknown>);
    }
    return;
  }
  if (invokeStateEquals(node, node.value, value)) return;
  node.value = value;
  node.token += 1;
  if (propagation.batchDepth > 0) {
    propagation.batchSeeds.add(node as Node<unknown>);
    return;
  }
  beginPropagation([node as Node<unknown>]);
}

function removeWatchRecord(node: Node<unknown>, record: WatchRecord): void {
  record.active = false;
  const index = node.watchers.indexOf(record);
  if (index >= 0) node.watchers.splice(index, 1);
  node.watcherRefs = Math.max(0, node.watcherRefs - 1);
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
  return {
    kind,
    name: options.label ?? `#${anonymousReadableId++}`,
    api,
    equals: options.equals ?? Object.is,
    dependents: new Map(),
    watchers: [],
    canonicalWatchers: new Set(),
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
  };
  nodeByReadable.set(api, node as Node<unknown>);
  return node;
}

export function initializeComputedNode<T>(
  api: Computed<T>,
  compute: (this: Computed<T>) => T,
  options: ComputedOptions<T>,
): ComputedNode<T> {
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
  if (!node.live) activate(node);
}

function detachCanonicalSource(watcher: Watcher, node: Node<unknown>): void {
  if (!node.canonicalWatchers.delete(watcher)) return;
  node.watcherRefs = Math.max(0, node.watcherRefs - 1);
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
  return (
    (trackingStack[trackingStack.length - 1]?.owner.api as Computed<unknown> | undefined) ?? null
  );
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
    if (parentDepth === 0 && propagation.batchSeeds.size > 0) {
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
  let active = true;
  return () => {
    if (!active) return;
    active = false;
    node.livenessObservers.delete(observer);
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
  const node = nodeFor(source);
  if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
  return node.value;
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

/**
 * Internal hooks used by the graph runtime.
 *
 * These hooks are exported from the module for implementation composition and
 * are intentionally omitted from the package root. They are not a stable
 * application API.
 */
export const __internal = Object.freeze({
  /** Batch writes and propagate their combined seeds when the outer batch ends. */
  batch,

  /** Bind a readable to a graph identity and optional asynchronous fault sink. */
  bindReadable,

  /** Report whether the active computation stack contains a live computation. */
  isCurrentComputationLive: (): boolean | undefined =>
    trackingStack.length === 0 ? undefined : trackingStack.some((frame) => frame.owner.live),

  /** Report whether a readable currently has a live descendant. */
  isLive: (readable: Readable<unknown>): boolean => nodeFor(readable).live,

  /** Report whether a watcher callback is currently being notified. */
  isNotifying: (): boolean => helperNotificationDepth > 0 || canonicalNotificationDepth > 0,

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
