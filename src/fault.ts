/**
 * Categories used by {@link Fault} for defects reported by Aeolia.
 *
 * A fault's `kind` is stable structured data; its message is a human-readable
 * rendering that may also include the names in `involved`.
 */
export type FaultKind =
  | "cycle"
  | "cross-graph"
  | "async-compute"
  | "watcher-read"
  | "watcher-write"
  | "propagation"
  | "disposed"
  | "contract"
  | "snapshot"
  | "encoding"
  | "equals"
  | "prediction";

/**
 * Idempotent handle returned by a reactive or fault subscription.
 *
 * Calling the function removes the subscription. Calling it more than once is
 * safe and has no further effect.
 */
export type Unsubscribe = () => void;

/**
 * Structured defect raised by Aeolia's reactive graph and public operations.
 *
 * `Fault` extends `Error`, so synchronous callers can catch it directly. The
 * `kind` identifies the failure category and `involved` carries graph-local
 * names or identifiers relevant to that failure. `involved` is frozen and is
 * safe to retain after the fault is thrown.
 */
export class Fault extends Error {
  /** Machine-readable category for this failure. */
  readonly kind: FaultKind;

  /** Graph-local names or identifiers associated with this failure. */
  readonly involved: readonly string[];

  /**
   * Create a fault with an optional list of involved names or identifiers.
   *
   * The message is formatted as `<kind>` or `<kind>: <involved names>`, and a
   * defensive frozen copy of `involved` is stored on the instance.
   *
   * @param kind - Stable category describing the defect.
   * @param involved - Optional graph-local names or identifiers to include in
   * the message and retain on the fault.
   */
  constructor(kind: FaultKind, involved: readonly string[] = []) {
    const names = [...involved];
    const suffix = names.length === 0 ? "" : `: ${names.join(", ")}`;
    super(`${kind}${suffix}`);
    this.name = "Fault";
    this.kind = kind;
    this.involved = Object.freeze(names);

    // Keep `instanceof Fault` reliable when transpilation targets older hosts.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

const faultChannels = new WeakMap<object, (observer: (fault: Fault) => void) => Unsubscribe>();

/**
 * Register an internal fault channel for a graph-like owner.
 *
 * This module-level hook is used by the graph runtime and is not re-exported
 * from the package root. Registering a new channel replaces the previous
 * channel for the same owner.
 *
 * @param graph - Owner to associate with the channel.
 * @param register - Function that subscribes an observer and returns its
 * unsubscribe handle.
 */
export function __registerFaultChannel(
  graph: object,
  register: (observer: (fault: Fault) => void) => Unsubscribe,
): void {
  faultChannels.set(graph, register);
}

/**
 * Remove the internal fault channel associated with a graph-like owner.
 *
 * @param graph - Owner whose channel should be removed.
 */
export function __unregisterFaultChannel(graph: object): void {
  faultChannels.delete(graph);
}

/**
 * Subscribe to structured faults reported by a graph.
 *
 * The graph must expose a channel registered by Aeolia's graph runtime. Faults
 * are delivered synchronously when the graph reports them; the returned handle
 * is idempotent. If an observer throws, the graph reports that error through
 * its unobserved-fault callback and continues delivering the fault to the
 * remaining observers.
 *
 * @throws {TypeError} If `graph` has no registered Aeolia fault channel.
 *
 * @param graph - Graph-like owner whose channel should receive faults.
 * @param observer - Callback invoked with each reported fault.
 * @returns A handle that removes this observer when called.
 */
export function onFault(graph: object, observer: (fault: Fault) => void): Unsubscribe {
  const register = faultChannels.get(graph);
  if (register === undefined) throw new TypeError("The graph does not expose a fault channel");
  return register(observer);
}
