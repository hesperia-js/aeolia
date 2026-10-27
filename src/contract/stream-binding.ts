import type {
  Graph,
  GraphId,
  MemberOf,
  StoreCapture,
  StoreIdentity,
  NativeStreamDefinition,
  StreamStore,
  ValueOptions,
} from "./types.ts";
import type { BindingContext } from "./query-binding.ts";
import type { BoundAddressEmpty } from "./binding-address.ts";
import type { StreamBindingRuntime } from "./runtime.ts";
import {
  assertBindingContextOpen,
  bindingContextByHandle,
  boundStreamByStore,
  graphRuntimeByGraph,
  rawStreamMemberByFunction,
  streamBindingByHandle,
} from "./registry.ts";
import { assertGraphOpen } from "./faults.ts";
import { streamMember } from "./stream.ts";

type AnyStream = NativeStreamDefinition<any, any, any, any, any>;
type StreamValue<S extends AnyStream> = S extends NativeStreamDefinition<any, infer V, any, any, any> ? V : never;
type StreamInput<S extends AnyStream> = S extends NativeStreamDefinition<infer I, any, any, any, any> ? I : never;
declare const streamSessionIdentityBrand: unique symbol;
declare const streamBindingHandleBrand: unique symbol;

/** Opaque identity of one genuine native opening, distinct from its physical store. */
export interface StreamSessionIdentity {
  /** @internal Nominal identity; no public constructor grants session authority. */
  readonly [streamSessionIdentityBrand]: true;
}
/** One coherent terminal read or its original native thrown cause. */
export type StreamTerminalCapture<Value> =
  | {
      /** The native visible-fold attempt returned a capture result. */
      readonly kind: "captured";
      /** Coherent presence/value facts or changed; the authored payload is borrowed. */
      readonly capture: StoreCapture<Value>;
    }
  | {
      /** The native read threw through its actual fault channel. */
      readonly kind: "fault";
      /** Original thrown cause, including undefined. */
      readonly cause: unknown;
    };
/** Logical source retirement; physical iterator or transport cleanup may continue. */
export interface StreamDetachment<Value> {
  /** Exact opening whose authority has been detached. */
  readonly session: StreamSessionIdentity;
  /** Native terminal source status, independent of an RPC outcome. */
  readonly status: "empty" | "closed" | "failed";
  /** Original native end reason, including undefined. */
  readonly reason: unknown;
  /** Sealed visible capture taken before destructive cleanup, or tagged read fault. */
  readonly terminalCapture: StreamTerminalCapture<Value>;
}
/** Nonretaining exact-session metadata; waiting creates no receive activity. */
export interface BoundStreamSession<Value> {
  /** Unique identity assigned before authored open callouts. */
  readonly identity: StreamSessionIdentity;
  /** Stable physical entry shared across successive openings. */
  readonly storeIdentity: StoreIdentity;
  /** Exactly the OpenOptions signal supplied to this native source. */
  readonly abortSignal: AbortSignal;
  /** Resolves once logical retirement and terminal capture are sealed, without awaiting physical cleanup. */
  readonly detached: Promise<StreamDetachment<Value>>;
}
/** Value options for the new bound member only. */
export interface BoundStreamValueOptions<Value> extends ValueOptions<Value> {
  /** Genuine one-use empty-address condition; stale or occupied state fails before clear/create/open. */
  readonly expectEmpty?: BoundAddressEmpty;
}
/**
 * Retains the original stream's actual input, value and logical-key association.
 * @param input - Original stream input; its key callback runs once for this invocation.
 * @param options - Native value policy/cancellation and optional genuine empty-address condition.
 * @returns A wrapper fixed to the exact session this invocation opened or joined.
 * @throws TypeError for invalid/stale conditional admission or a retiring address; native configuration/lifetime faults retain their channels.
 * Source open/iterator failures become failed sessions and genuine detachment metadata.
 */
export type BoundStreamMember<S extends AnyStream> = (
  input: StreamInput<S>,
  options?: BoundStreamValueOptions<StreamValue<S>>,
) => StreamStore<StreamValue<S>>;
/** Opaque dormant binding handle; public fields grant no runtime authority. */
export interface StreamBindingHandle {
  /** Owning native graph identity. */
  readonly graph: GraphId;
  /** Fixed native context owning the binding and its sessions. */
  readonly context: BindingContext;
  /** @internal Nominal binding provenance, checked in the native registry. */
  readonly [streamBindingHandleBrand]: true;
}
/** Same-engine stream binding with fixed per-invocation session facets. */
export interface StreamBinding<S extends AnyStream> extends StreamBindingHandle {
  /** Exact original stream registered by the raw member's native graph. */
  readonly definition: S;
  /** Acquires or joins a real session; retired wrappers never follow its successor. */
  readonly member: BoundStreamMember<S>;
  /**
   * Reads the exact session joined/opened by a genuine returned wrapper.
   * @param store - Wrapper returned by this binding, including synchronously retired results.
   * @returns Existing metadata without acquisition, liveness, idle touch or source activity.
   * @throws TypeError for a fabricated, foreign or wrong-binding wrapper.
   */
  session(store: StreamStore<StreamValue<S>>): BoundStreamSession<StreamValue<S>>;
}

/**
 * Memoizes a dormant native binding for the actual original raw stream member.
 * @param graph - Genuine graph containing the original declaration/member.
 * @param definition - Exact original stream; callbacks and configuration are not cloned.
 * @param originalMember - Native member created by this graph for that exact definition.
 * @param configuration - Active fixed context on the same graph.
 * @returns The shared binding for this exact graph/context/definition/member tuple.
 * @throws TypeError for crossed/fabricated provenance or malformed configuration; disposed lifetimes throw Fault.
 */
export function bindStream<S extends AnyStream>(
  graph: Graph,
  definition: S,
  originalMember: NoInfer<MemberOf<S>>,
  configuration: { readonly context: BindingContext },
): StreamBinding<S> {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  const original = rawStreamMemberByFunction.get(originalMember);
  if (original === undefined || original.state !== state || original.definition !== definition)
    throw new TypeError("Stream binding requires the exact original native member and definition");
  if (configuration === null || typeof configuration !== "object" || Array.isArray(configuration))
    throw new TypeError("Invalid stream binding configuration");
  const context = bindingContextByHandle.get(configuration.context);
  if (context === undefined || context.state !== state)
    throw new TypeError("Stream binding requires its actual graph context");
  assertBindingContextOpen(context);
  let members = context.streamBindings.get(definition);
  const existing = members?.get(originalMember);
  if (existing !== undefined) return existing.handle as StreamBinding<S>;
  let binding!: StreamBindingRuntime;
  const handle = Object.freeze({
    graph: state.graph.id,
    context: context.handle,
    definition,
    member: (input: StreamInput<S>, options?: BoundStreamValueOptions<StreamValue<S>>) =>
      streamMember(state, definition, input, options, binding) as StreamStore<StreamValue<S>>,
    session: (store: StreamStore<StreamValue<S>>): BoundStreamSession<StreamValue<S>> => {
      const record = boundStreamByStore.get(store);
      if (record === undefined || record.binding !== binding)
        throw new TypeError("Stream wrapper does not belong to this native binding");
      return record.handle as BoundStreamSession<StreamValue<S>>;
    },
  }) as StreamBinding<S>;
  binding = {
    kind: "stream",
    context,
    definition,
    originalMember,
    handle,
    stores: new Map(),
    addresses: new Map(),
  };
  streamBindingByHandle.set(handle, binding);
  if (members === undefined) {
    members = new Map();
    context.streamBindings.set(definition, members);
  }
  members.set(originalMember, binding);
  return handle;
}
