# Managed state

After a query failure, Aeolia allows one automatic recovery attempt. If that
recovery also fails, automatic recovery is disarmed. A successful recovery or
explicit `revalidate()` re-arms it.

Query member calls share that automatic recovery allowance. After it is exhausted,
opening another view of the same failed query does not dispatch another request.

Aeolia maps operation definitions to managed reactive state:

- keyed writable stores with stable identity;
- query stores with per-caller freshness, status, and explicit revalidation;
- mutation effects and optimistic predictions;
- shared keyed streams and independent stream projections;
- timed collection of idle, unobserved store contents;
- deterministic snapshots and adoption into a new graph created with the same
  contract;
- a controlled backend fixture under `aeolia/testing`.

Aeolia performs no I/O. Query, mutation, and stream definitions supply the
callbacks; a graph invokes them and manages their state.

## Runtime input checks

Aeolia checks declaration callbacks, mutation effect settings, query and stream
options, mutation options, graph cancellation options, and projection policies
at their entry points. Malformed fields throw `TypeError` synchronously, before
the call creates managed state or starts backend work. For example, `snapshot`
must be a boolean, equality and effect callbacks must be functions, and each
optimistic map entry must pair a non-empty store key with a producer function.
Store keys must be non-empty strings; Aeolia preserves their exact spelling.

Use `queryOptions({...})` for options passed as the first argument. It brands a
shallow-frozen copy; the operation validates the final options after applying
any second-argument overrides. An empty options object and explicitly
`undefined` optional fields are valid. Additional properties are ignored.
Cancellation signals and read-only maps are checked by their interfaces, so
they do not need to share the current runtime's constructors.

Invalid numeric bounds still throw a `contract` `Fault`: freshness windows
must be finite and non-negative, and projection limits must be positive
integers. A rejected freshness window does not configure the store. A
missing graph bound uses its default; `null` is not a missing bound.

These checks do not infer application data types or run callbacks to discover
their return types. Validate external payloads in your backend callbacks before
returning them to Aeolia. Callback failures continue through the operation's
existing error handling.

## Declaring operations

```ts
import { affects, createGraph, defineContract, createMutation, createQuery } from "aeolia";

const items = createQuery({
  name: "items.get",
  key: (id: string) => `items/${id}`,
  fetch: (id, { abortSignal }) => loadItem(id, abortSignal),
});

const rename = createMutation({
  name: "items.rename",
  run: ({ id, name }, { abortSignal }) => renameItem(id, name, abortSignal),
  affects: [
    affects(items, {
      select: ({ id }) => id,
      on: "revalidate",
    }),
  ],
});

const contract = defineContract({
  namespace: "example",
  operations: { items, rename },
});
const graph = createGraph({ contract });
const item = graph.api.items("one");
```

### Query acquisition

The optional `acquisition` field controls whether a fresh landing can satisfy a
query member call. It accepts `"cache"` or `"fetch"`. Omission and `undefined`
mean `"cache"`. When present, `acquisition` must be an own data property.
Accessors, inherited properties, and other values cause a synchronous
`TypeError`.

With `"cache"`, a member call reuses a fresh landing. With `"fetch"`, Aeolia
enters the ordinary request path even when a landing is fresh. The
call joins an existing current request and remains subject to cancellation,
reset checks, and the shared automatic-recovery allowance. An exhausted
recovery allowance still blocks another automatic request; call `revalidate()`
to retry explicitly. The callback may satisfy a request from a native cache,
so `"fetch"` does not guarantee network traffic. While the request runs, the
prior committed value remains available through `value`. Status is
`"revalidating"` and `pending` is `true`. `ready` waits for the request to
settle. Raw calls with the same key share one request. Later callers join a
current request, and the initiating call's callback remains responsible for
its result.

`createQuery` captures and normalizes the policy on its frozen result. A graph
captures a structural query definition's policy when it registers that
definition. Later changes to the source object do not change the graph's policy.

A query member call starts a fetch when needed. To wait for committed data
without starting another request, read the query's `ready` getter:

```ts
const value = await item.ready;
```

`ready` returns a promise for a committed value snapshot, not a reactive
accessor. It follows this caller's `pending` and `status`: it resolves when
the caller is no longer pending and its status is `"ready"`, and rejects
with the query's error when status becomes `"failed"`. A refresh failure
rejects the wait even when an older value remains available in `value`;
the same failure remains readable through `error`.

An active request sets `pending` to `true`. With no committed value its
status is `"fetching"`; with a committed value it is `"revalidating"`, even
if that older value has not exceeded its freshness window. A stale value
alone does not satisfy readiness. Reading `ready` does not start a request;
use a query member call or explicit `revalidate()` to initiate one.

With `revalidateAfterMs: 0`, a successful fetch still publishes `"ready"`
and resolves existing readiness waits. The freshness timer then marks the
value stale and starts another fetch only if the value is live. Waiting on
`.ready` alone does not keep a refresh loop running.

A committed `undefined` is a valid result when it belongs to the query's
value type. Optimistic predictions do not satisfy readiness. Cancellation
or graph disposal also ends a pending wait.

Use `subscribe(item.value, observer)` for ongoing value delivery. Readiness
and subscription are independent: one waits for the ready state, while the
other observes the store's visible value, including optimistic predictions.

To receive values only once a query or stream store has data, pass the store itself:

```ts
const stop = subscribe(stream, (value) => console.log(value));
```

This skips an empty store's placeholder. An explicit initial value or a
retained value is delivered immediately, and a real `undefined` value is
delivered when the store's value type allows it. Later equal values are
suppressed by the store's equality rule. Closing or failing a stream before
its first value does not fabricate a value callback; observe `status` and
`error` for those transitions.

`subscribe(stream.value, callback)` still observes the raw readable and
immediately delivers its current value, including the empty placeholder.
Store-aware subscription is available from `aeolia`; `aeolia/reactive`
continues to provide the readable-only subscription API.

Projections keep their initial state: accumulation starts at `[]`, and
reduction starts at the supplied `initial`. Their input consumes every source
emission, including repeated equal values; it does not pass through store
value subscriptions. `subscribe(projection, callback)` delivers that initial
state immediately and observes later projected values, just like
`subscribe(projection.value, callback)`.

An optimistic prediction remains visible after its mutation succeeds, until
a subsequent successful query refresh reconciles it. Mutation success alone
does not validate the predicted value. The refresh commits its authoritative
result and removes the predictions it covers in one batch. If the resulting
visible value compares equal, value subscribers receive no additional callback;
status, freshness, and readiness still settle.

A query captures the successful predictions eligible for reconciliation when
its request starts. It does not retire predictions from mutations still pending
at that point or from mutations started later. A superseded response cannot
reconcile anything. Mutation failure removes only that mutation's predictions.
Refresh failure retains successful, unconfirmed predictions, exposes the error,
and rejects `.ready`; a later successful retry can reconcile them. An unobserved
store's `invalidate` effect leaves its successful prediction visible until a
later refresh reconciles it.

An effect's `on` setting defaults to `"invalidate"`. After mutation success,
it marks an existing store stale and immediately starts a fresh query if the
store's value has a live observer. Observing only status, pending, or error does
not request fresh data. Without a value observer, invalidation leaves the store
stale without fetching. `"revalidate"` starts a fresh query regardless of
observers. Neither effect creates a missing store. These refreshes do not
require a freshness timer; they supersede any request started before mutation
success so that an earlier response cannot overwrite the mutation's result.

If an invalidated value becomes observed later, Aeolia schedules a refresh even
without `revalidateAfterMs`. This lets a cached page resume observation and
receive changes made while it was inactive. The last committed value remains
available until the refresh settles. Multiple returning observers share one
request, and if they all leave before dispatch, no request starts. Returning
observers do not refresh an already fresh value or bypass query recovery limits.

Effects targeting a query with `void` or `undefined` input may omit `select`;
the runtime targets that query's key directly. Queries with any other input,
including `unknown` or a union that includes `undefined`, require `select` so
the affected key is explicit.

## Cancelling pending work

`graph.cancelPending(reason?)` cancels the work already admitted when the call
starts. It aborts active query and mutation signals, closes current keyed stream
sessions and projection sources, and rejects an in-progress reset. The graph,
API, store values, optimistic predictions, and readable identities remain
available. Cancellation does not start a reset or advance `graph.resetVersion`.

```ts
graph.cancelPending(new Error("Request ended"));
```

The supplied reason becomes the captured work's abort reason and the captured
reset promise's rejection reason. Omitting the reason, or passing `undefined`,
uses an `AbortError`. A signal that was already aborted keeps its earlier reason.
Query failures and stream or projection closure retain their ordinary lifecycle
behavior; cancellation does not roll back values already committed.

Cancellation captures its work before invoking abort listeners or source
cleanup. Work explicitly started by those callbacks belongs to a later cohort
and remains admitted unless another cancellation call captures it. Freshness,
recovery, and collection timers are also outside the captured cohort. They may
still trigger ordinary work while the graph remains alive. A cancelled reset
keeps any state changes it already performed, but does not continue its remaining
refresh and restart sequence.

An abort signal cannot guarantee that authored work or an external transport
stops. A mutation callback that ignores cancellation can still settle its promise
and apply its ordinary effects in the same state epoch. A reset or graph disposal
has its own rules for ignoring results from an earlier lifetime.

Cancellation attempts every captured source's owned cleanup even if another
cleanup throws. Escaping synchronous cleanup errors are reported together in an
`AggregateError` after those attempts. Existing best-effort iterator `return()`
method failures remain swallowed, and asynchronous host listener errors keep
their host reporting behavior. Cancellation does not wait for iterator cleanup or
physical transport termination.

Calling `cancelPending()` after graph disposal does nothing. Dispose the graph
when its readable state and resources should finally be released.

## Retiring a graph

Use `graph.retire()` when the graph's owner is finished with the graph and
needs to wait for the work it admitted to complete:

```ts
try {
  await graph.retire();
} catch (error) {
  console.error("Graph cleanup failed", error);
}
```

Retirement permanently closes the graph. It disposes the graph immediately,
aborts active work, and refuses new work and store reads with a `disposed`
fault. Its promise waits for the original admitted query and
mutation work, stream and projection reads, iterator returns, and owned cleanup.
It also joins that work when an earlier reset, cancellation, or `dispose()`
removed it from active state. Calling `dispose()` alone does not wait.

After retirement is admitted, repeated and reentrant calls return the same
promise. Make the first call outside a write-restricted reactive scope. If a
first call on a live graph is write-restricted, it throws a `write-forbidden`
fault synchronously and leaves the graph open for a later attempt.

An original operation's failure remains on its existing result or fault channel;
it does not by itself reject retirement. An owned cleanup failure rejects the
retirement promise with its original cause. If several cleanup attempts fail,
the promise rejects with an `AggregateError` whose `errors` follow attempt
order, including repeated references when separate attempts throw the same
object. If constructing that report fails, the promise rejects with the report
construction failure.

Abort signals request cancellation; they cannot force a provider or transport
to stop. Retirement waits for the original work and cleanup Aeolia owns, not
for detached work that a provider has stopped representing through its returned
promise or iterator. If an owned operation or cleanup never settles, retirement
remains pending.

## Retiring a binding context

Use `await context.retire()` to close one binding context and join only its
bound queries, mutations, streams and cleanup. Graph retirement also joins raw
operations, every binding context and independent projection generation.
Context retirement leaves raw operations and other contexts running.

Retirement follows the owner's disposal path. After admission, repeated and
reentrant calls return the same `Promise<void>`. A binding-context retirement
precondition failure is retained in that context's shared promise. A graph
retirement that requires reactive writes first checks whether those writes are
allowed; a refusal throws synchronously without admitting retirement. A later
legal call can start it. You can call `retire()` after synchronous `dispose()`.
`dispose()`, `cancelPending()` and stream-session `detached` keep their existing
completion contracts; none acknowledges that provider work or physical stream
cleanup has ended.

The retirement promise fulfills after logical disposal and all original work,
native continuations and acknowledged asynchronous cleanup that Aeolia can track
through its existing call semantics finish. If any such operation never
acknowledges completion, the promise remains pending. Query, mutation,
preparation and stream failures keep their existing caller or diagnostic
behavior and do not fail retirement by themselves. Cleanup failures do reject
it: one failure preserves its exact cause, including `undefined`, and several
failures are reported in an `AggregateError` with the original causes in
deterministic owner and cleanup-attempt order. An original `AggregateError`
remains one cause.

A preparation resolver can reenter `retire()` before returning an already-running
promise. Aeolia cannot join that promise after revocation without inspecting or
calling its `then` behavior, which may run authored code. That original remains
unacknowledged, so retirement stays pending. Aeolia does not invoke a generic
thenable solely to manufacture a completion acknowledgement. Work detached from
the promise or source contract returned to Aeolia needs its provider's own
completion acknowledgement.

Custom Promise observers or constructor/species behavior can also prevent Aeolia
from proving completion through an existing call. The operation keeps its legacy
result and error behavior, while retirement remains pending. Later settlement of
an uncertified carrier does not add a new acknowledgement.

## Context-bound query profiles

Use `createBindingContext(graph)` and `bindOperation(graph, query, configuration)`
when an existing graph needs a separate query representation or fixed application
environment. Both helpers are exported by `aeolia` and `aeolia/contract`.
Registration checks the exact query declaration in the graph's contract. It does
not compute a key, acquire a store or call the backend.
Here, `userQuery` is an existing query on `graph` that accepts `{ id: string }`
and returns a user with a string `name`.

```ts
import { bindOperation, createBindingContext } from "aeolia";

const context = createBindingContext(graph);
const labels = bindOperation(graph, userQuery, {
  context,
  profile: {},
  fetch: async (input, options) => {
    const user = await userQuery.fetch(input, options);
    return { label: user.name };
  },
});
const user = labels.member({ id: "cedar" });
const label = await user.ready;
```

The context represents one fixed environment generation, such as one security
context. Keep a shared context alive independently of individual components.
Use a new context when that environment changes. Aeolia does not infer security
identity from headers or callback closures.

The native address includes the context, original query, exact opaque profile
object and original logical key. Calls through the same binding/key share value,
request and errors. Different profiles, original queries or contexts remain
separate even when their logical keys are equal. A second registration of the
same context/query/profile throws `TypeError`, including an equivalent-looking
configuration. Retain and reuse the original binding.

`Store.key` remains the original logical key. `Store.identity` identifies the
actual physical entry across caller wrappers, reset and idle collection. It is
opaque and cannot be used as a snapshot address or current-lifetime permission.
Raw `graph.api` members and `graph.at(key)` retain their raw namespace; no string
prefix is reserved, and a raw handle does not identify a bound profile.

The binding callback receives the original native input and request options.
Its fulfilled value lands through the normal native request owner, including
refresh and reset refresh. A rejected completion preserves the prior value and
uses native query failure/readiness behavior. Value defaults describe the
completed value; raw query equality is not inherited. The original snapshot
default and freshness window remain in effect unless configured otherwise.
Conflicting explicit initial defaults are rejected rather than silently replaced.
The fixed binding initial is installed when each entry is first configured.
Ordinary callers joining during or after reset do not reapply it. An explicit
caller initial remains a reseed subject to the native reset restrictions.
Inputless and branded options follow the same rules as raw query members.

`binding.target(handle)` captures a typed ticket without reading a value,
retaining the entry or starting work. It requires a genuine query handle from
that exact binding. It returns `undefined` for an inactive or collected entry;
foreign or fabricated handles throw `TypeError`. `ticket.forBinding(binding)`
checks the original association and incarnation without retention. After reset,
an old ticket stays stale and a fresh explicit target call can use the retained
handle's new cleared incarnation before a reset request succeeds. Collection
does not revive an entry through either operation. Normal member acquisition,
explicit revalidation or a local write can establish a fresh incarnation with the
same physical identity.
A current acquired incarnation has a ticket while its request is pending or
failed; it does not need a successful landing. Readiness inspection of an inactive
collected handle does not establish that authority, including across reset.

Bound call options are captured before key computation. If reading those options
revokes the binding context, the call fails before invoking the authored key or
backend callback.

`context.dispose()` first revokes admission and all its entry tickets, then
aborts only its pending requests and clears its entries. Old value/readiness
handles report the native disposed fault. Raw entries and other contexts remain
usable. Disposal is idempotent. `context.active` is nonretaining lifecycle
metadata; observing it does not keep query values alive or activate requests.
Graph disposal also retires its contexts.

Version 1 `snapshot(graph)` rejects a selected committed bound entry with an
encoding fault because its raw-key format cannot represent the address.
Profiles configured with `snapshot: false` are excluded normally; they do not
prevent eligible raw entries from being exported. Version 1 adoption continues to
address only raw entries.

## Presence, coherent capture and observed values

Every store exposes `available` as a read-only native readable. It distinguishes
absence from a present `undefined` and includes effective optimistic predictions.
It does not imply that a query is ready or a stream has emitted. Reading
`available` touches normal store retention. A live subscription or computed
dependency retains metadata and joins reset's live-query population, without
activating a fetch, freshness timer or failure recovery by itself.

Use `peekStore(store)` when one synchronous operation needs coherent presence and
value. The frozen result is either `changed`, with no value/presence, or `stable`
with `identity`, `valueRevision` and discriminated `present/value` fields. A `stable`
present `undefined` is a value. The authored value is borrowed; its object graph
is not cloned or frozen. Capture uses the existing native prediction fold and
neither tracks an outer dependency nor touches retention. It does not fetch,
subscribe, revive a collected entry or alter request outcomes. Reentrant reset,
context retirement or value changes can invalidate the capture; handle `changed`
explicitly without treating it as absence or retrying against another lifetime.
Fabricated stores throw `TypeError`, disposed native lifetimes throw their disposed
`Fault`, and ordinary native read restrictions and error channels still apply.

`ValueRevision` orders visible transitions within one physical native entry. It is
independent of request `Generation` and cannot be compared across stores/realms.
Request starts and issue-only changes do not advance it. A neutral publication
does advance it even when authored equality suppresses a value notification.
Reset and collection preserve physical `identity` while revoking old incarnation
authority. Revisions never wrap or silently lose integer precision.

`observeStore(graph, acquiredQuery)` acquires the one publication lease for an
already-existing genuine native query incarnation. Two wrappers for that entry
compete for the same publisher slot; a concurrent acquisition throws `TypeError`
without replacing the incumbent. Acquisition does not create state, compute a
key, fetch or renew retention. A collected handle cannot be revived this way;
after reset a fresh lease can use the retained genuine current-incarnation query
handle. An `active` binding context remains part of a bound query's authority.

`lease.publish(value)` updates the actual committed base beneath existing
predictions and clears the shared observation issue. It does not complete a
request, resolve a cold ready wait, advance request generation, rewrite request
error/status, change accepted landing time or renew idle activity. A first
observed base can therefore be `available` while a cold query is still fetching.
The ordinary request can subsequently land its value. Publishing after an
accepted landing preserves its freshness age, even though the visible base
changes. Equality remains the entry's authored equality; publication and issue
clearing commit atomically under native authority checks.

`lease.reject(issue, diagnostic?)` retains the caller's opaque read-only issue
object without changing value, predictions, freshness or request outcome.
An optional diagnostic uses `{cause}`, including an explicit `undefined` cause.
`lease.report(cause)` reports through the existing continuation diagnostic route
without changing value or issue. Schema validation and provider-specific issue
provenance belong to the caller. Diagnostics preserve their existing nonrecursive
failure containment. Options/diagnostics require allowed own data fields;
accessors and unknown options are rejected before publication.

The native incarnation owns the issue. Disposing a lease revokes only that
publisher and preserves its issue for the next lease. Ordinary query success,
watch stop and consumer departure do not clear it. The next valid publication
through a current lease clears it, including one with `transfer: "exclude"`.
Reset, collection and actual context/graph retirement
discard it. A retired issue readable returns `undefined` and never follows a
successor. While current, issue reads/subscriptions retain metadata without
fetch/freshness activation. `active` is separate lifecycle metadata: its reads and
subscriptions never retain a query and remain false after lease retirement.
Stale publish/reject/report returns false without side effects or diagnostics.
Normal write restrictions and synchronous propagation failures still apply.
Once publication commits, later observer revocation cannot undo that commit.

The optional publication transfer policy defaults to `eligible`. `exclude` marks
that exact observed base revision as ineligible; it is reserved for an explicitly
qualified native-owned optimistic fallback. It is not a general stream exclusion
or a runtime heuristic for recognizing optimism. Ordinary authoritative writes
and landings replace that origin metadata with their own `eligible` base.
Version 1 snapshots cannot represent selected bound entries or a selected base
with no accepted landing, so those cases throw an encoding `Fault`. An actually
excluded optional entry is omitted. A representable raw observed replacement
after an accepted landing carries its current base and actual retained age.
Encoding reentry that changes captured native authority fails export. Ordinary
v1 adoption remains raw accepted state and does not clear an observation issue.
Required framework state cannot silently disappear; a caller requiring an
unsupported or excluded entry must fail its transfer contract. Version 1 keeps
its raw-only format. Version 2 adds explicit bound-address transfer as described
below; neither version restores remote operations.

## Version 2 bound snapshots

Version 1 keeps the existing calls and return shapes: `snapshot(graph)` returns
`Snapshot`, and `adopt(graph, snapshot)` returns `AdoptionReport`. Select version
2 explicitly and pass a mapping for every selected bound query or stream:

```ts
const state = snapshot(source, {
  version: 2,
  bindings: [
    { binding: queryBinding, reference: queryReference },
    { binding: streamBinding, reference: streamReference },
  ],
});

const report = adopt(target, state, {
  bindings: [
    { binding: restoredQueryBinding, reference: queryReference },
    { binding: restoredStreamBinding, reference: streamReference },
  ],
});
```

Version 2 keeps `snapshot` and `adopt` in the root `aeolia` entry point. The
existing version 1 overloads stay unchanged. Callers that hold a union of
versions must narrow `version` before choosing an adoption overload.
For raw-only version 2, omit `bindings` when taking the snapshot and pass `{}`
as the adoption options.

Each version 2 entry carries its full address. Raw entries use `{ kind: "raw",
key }`. Query-bound entries use `{ kind: "binding", reference, key }`. Stream
entries use `{ kind: "stream-binding", reference, key }`. These entries form an
array, so equal logical keys in raw, query, and stream namespaces remain distinct.
The adoption report returns the full address and the target's native identity
for every adopted row. A stream row also returns its target `valueRevision`.
Identities and revisions describe the target graph; they are not portable clocks.

A mapping pairs a genuine active `QueryBindingHandle` or `StreamBindingHandle`
with a supported reference description. The reference identifies the original
operation and its framework-owned profile or member description. It does not add
a manually assigned profile ID. Aeolia encodes it with the tagged codec.
Plain-object keys are sorted before alias assignment,
while array order and `Map` or `Set` iteration order remain significant. The
encoder does not call `toJSON`. Use data the tagged codec supports. Do not place
callbacks, graphs, binding contexts, security objects, or other live JavaScript
machinery in a reference.

Recreate the target contract, required schemas and configuration, authored
captures, authorized context, and genuine binding handles before adoption. Map each serialized reference to the already
reconstructed handle. Adoption does not call a key function, query fetch, stream
opener, watch, or component initializer to discover a binding. It does not carry
callbacks, sessions, source iterators, projections, transports, or authorization
across the boundary. The receiving framework remains responsible for proving
that its current program and security scope match each reference.

Every selected bound address needs exactly one mapping. Aeolia validates the
complete mapping list before processing entries. Missing or duplicate handles,
duplicate canonical references, and ambiguous physical addresses fail the
envelope before any entry is written. A malformed or unsupported envelope
also fails as a whole. Aeolia skips an invalid individual address or an occupied
stream target and reports that row's address. Other independent valid entries
remain eligible. Required application state that is missing from the report
remains a transfer failure for the framework to handle.

Version 2 distinguishes `landing: { kind: "none" }` from an accepted landing.
Raw and query rows can carry either state. A no-landing row still carries its
actual committed value; Aeolia does not invent an age of zero. Accepted rows
preserve their actual age. Stream rows require an accepted landing because the
stream producer cannot create a no-landing row. An observed replacement keeps
the current committed base and the prior accepted age. Excluded optional values
remain omitted, and predictions remain outside the snapshot. Adoption preserves
the landing distinction and does not clear a separate observation issue.

An adopted stream value starts as a dormant seed. Adoption creates no stream
session and opens no source. The first genuine bound member call for that address
can claim the seed once while opening its normal source. Its session is pending
and its value is available during that opening, even when the imported value is
`undefined`. The first real source value replaces the seed. Adoption itself does
not emit a stream event, replay a source callback, or start a projection callback.

Validation failure before the native claim leaves the seed available. After a
session claims it, a failed or aborted opening consumes the seed and does not
restore it. A later session follows the ordinary fresh-session clearing policy.
An explicit `initial` option cannot overwrite an imported base. An occupied
stream target is skipped instead of being cleared to make room for an import.
An address is occupied when it has a committed base, including present
`undefined`, a visible prediction, an active or draining operation, a retiring
stream session, a finite query attempt, or an unclaimed imported seed. An opening
session with no value is also occupied.

The native claim marker belongs to the exact binding, context, key, incarnation,
graph reset epoch, and local value revision created by adoption. It stays out of
the snapshot. Reset, context or graph retirement, and a later authoritative write
invalidate it. A stale marker cannot claim a successor incarnation.

Before a framework releases application-owned I/O, it can compare the adopted
row's local identity and stream revision with `peekStore` and the genuine
`binding.session` for the opening. It must then check the current binding,
session, context, program, and security scope at the point where it releases that
I/O. Key equality and serialized references do not grant authorization. Aeolia's
package consumer does not establish Hesperia's bridge gate; the Hesperia caller
must prove that boundary. Imported values remain subject to normal collection.
Adoption and address inspection do not pin them. If collection happens before
the first claim, the session cannot reclaim the value.

Native generations use safe integers. Requests and authoritative writes check
that they can issue their generation before changing readable state or
dispatching work. Snapshot adoption checks every validated row's generation
before its commit batch; a safe-limit refusal at that preflight leaves readable
entries unchanged. An authored equality callback can reenter during commit, and
completed writes from that callback keep their own authority. Reset preflights
every store's clearing generation and any selected query refresh before changing
the graph epoch. If reset refuses at that preflight, it does not clear values,
advance `graph.resetVersion`, or start refreshes. Aeolia does not wrap, clamp, or
renumber exhausted counters, and it supplies no automatic recovery from
exhaustion. Incoming value revisions remain provenance, not a cross-graph clock.

## Required cached-state transfer

The `snapshot` and `adopt` APIs copy selected store values into another graph.
Required transfer carries a selected cached reactive closure and its native
source addresses through a caller-owned publication boundary. It does not change
the snapshot formats.

Choose already-constructed `State` and `Computed` roots with
`captureReactiveConstruction`, then prepare a capture from their cached values:

```ts
const construction = captureReactiveConstruction(derived);
const capture = prepareRequiredTransferCapture({
  graphs: [sourceGraph],
  roots: [{ source: derived, construction }],
  codec,
});
```

Capture walks the existing dependency graph and records cached results. It does
not run a `Computed` to fill a missing value. Every encoded value or admitted
failure passes through the selected codec. `seal()` checks that the captured
native sources and reactive nodes are still current; it returns `undefined` if
the source cut changed. After a successful seal, call `publish()` and then
`notifications.deliver()` at the application publication boundary.

For each native source, prepare a `preparePassiveTransferClaim` with the actual
target graph, source member or binding, input, and target value options. It uses
that input to establish the native address, but does not invoke the acquisition
member, fetch, or open a stream. Its key callback can run during preparation.
The returned handle exposes safe address metadata while body reads and
operations remain gated until publication. Discard a claim that is not included
in a published adoption.

A required query image has one `stores` row per physical address and one
`callers` row for each caller in the captured reactive closure. Each caller row
links its own slot to the shared Store slot and records that caller's normalized
freshness window and settled status. For example:

```js
{
  slot: "c0",
  store: "s0",
  window: { kind: "finite", milliseconds: 60000.5 },
  status: "ready",
}
```

Native caller facets such as `query-status` and `query-pending` name the caller
slot in `RequiredTransferNode.caller`. Their `store` field still names the
physical Store slot. A caller source witness carries `physical`, its exact status
readable, and the exact query wrapper returned to that caller. The physical base
witness also carries a genuine published acquisition wrapper.

Map every physical Store slot and caller slot in `prepareRequiredTransferAdoption`.
One passive claim can cover a physical Store slot and its own caller slot. Prepare
a distinct claim for each other caller. Claims for the same address share the
receiver runtime while retaining distinct wrappers and caller state.

Pass the matching `StoreOptions.revalidateAfterMs` on each query claim. Its
effective window must match the source caller. If both the query definition and
caller omit the option, the window is infinite. An explicit `Infinity` is invalid.
Finite non-negative values, including fractional milliseconds, are accepted. The
image records a caller's settled status; a source caller with an unresolved
request cannot be captured.

For a required closure that includes native observation facets, call `prepareObservation`
on its query claim before preparing adoption:

```ts
const prepared = claim.prepareObservation(existingLease);
const lease = prepared.lease;
```

The method returns a genuine native `ObservationLease`. Call `prepared.discard()`
to discard the owning claim. It preserves an exact existing publisher that the
participant reused. A new lease remains inactive until the required adoption
publishes. Before that cut, `active` is false. The `publish`, `reject`, and
`report` methods return false. Calling `dispose()` before
publication discards the pending lease so it cannot activate later.

To share pending observation metadata across caller claims for one physical
query, prepare the first claim without an argument. Pass its exact pending lease
to each additional claim before adoption:

```ts
const firstObservation = firstClaim.prepareObservation();
const nextObservation = nextClaim.prepareObservation(firstObservation.lease);
```

The claims then join one publisher cohort. Their prepared observations expose the
same lease, which remains inactive until adoption publishes. Separate no-argument
calls create independent candidates. They cannot satisfy one shared observation
closure in a single adoption. Discarding one independent candidate leaves the
other candidate intact.

Pass a previously published `existingLease` only when you hold that exact current
lease for the same native query incarnation. Aeolia reuses that publisher
without replacing or disposing it. A different active publisher on the receiver
causes preparation to fail. The prepared participant's `retired` is the same
Promise as the claim's `retired`. It reports claim-registration cleanup, not
provider stop or graph retirement. Keep the consumer Resource's existing close
promise for provider cleanup.

The required image does not encode private observation issue values. Keep an
existing receiver issue intact. If a required closure depends on a non-undefined
source issue and the receiver cannot provide a compatible genuine native issue
facet, the complete adoption fails before publication.

`prepareRequiredTransferAdoption` maps source slots to those genuine claims and
maps each selected reactive slot to a genuine target `State` or `Computed` plus
its own construction receipt. Computed callbacks are never copied or invoked
during preparation. A target `State` written since its construction is
browser-owned and keeps its value. If a required cold target `Computed` would
depend on a browser-owned value that differs from the image, the whole adoption
fails before publication; no target State writes or cached values from that plan
are installed. The target computation remains cold, and its first ordinary read
evaluates against receiver values. A warm target `Computed` keeps its actual
receiver cache when the rest of the required mapping is compatible; a conflict
that makes the required closure incompatible fails the whole plan.

Use `mappedStateWrites` for other genuine target `State` objects that must
change in the same publication. These writes are staged with the adopted caches
and applied before deferred notifications. A notification callback can throw
after publication; its failure is reported, while the published values remain in
place. The plan’s `retired` promise reports its own registration cleanup and
notification outcome.

Required transfer is an in-memory cache protocol. Its closed image contains
values and native address descriptors, not callbacks, sessions, transport
ownership, or authorization. It does not replace the host or provider checks
that govern release of external work.

## Required operation coverage and retirement

When an external operation can still affect a query value, create a persistent
scope from the genuine acquired target with `createTransferScope`. Pass every
affected scope and each exact raw or bound mutation carrier to
`prepareTransferOperation`. The carrier in each dispatch record must be the same
object passed to the native mutation call. The operation publication records
coverage; it does not dispatch the mutation or replace its existing result
promise.

Keep the original operation covered through graph reset, context changes,
retries, and address successors. `prepareTransferScopeSuccessors` adds
replacement query addresses for the same still-active operation in one
publication while retaining predecessor coverage. It checks the replacement
source and native key against the predecessor address.

Release coverage only after the application confirms its external operation has
retired and every joined native mutation has completed its original work and
cleanup. `prepareTransferOperationRetirement` refuses while a dispatch is
unjoined, active, or has failed cleanup. Reset, disposal, a successful query
landing, and settlement of a public result promise do not by themselves release
this coverage.

These APIs track native lifecycle facts. They do not authorize an external
write, commit a transaction, or prove that a server action has finished.

## Resetting graph state

`graph.reset()` starts a new state epoch while preserving the graph, API, store
shells, and readable identities. It restores declared stores to their initial
values. It clears keyed query and stream values, errors, optimistic predictions,
freshness timestamps, and timers. Keyed query initial configuration is cleared
with the value.

An explicit `initial` option can seed a keyed query again after reset while its
state is empty and idle. That seed may differ from the pre-reset seed. Aeolia
throws a contract `Fault` if a reset-owned query request is pending or fresh
data has already committed; this error leaves the request and its reset in
progress. The next successful reset clears the new seed configuration too.

Reset snapshots queries with any live readable, including status-only and
pending readers, then starts a fresh request for each. Its promise waits for
every required query outcome. Successful refreshes remain committed if another
refresh fails. One failure rejects with its original reason; multiple failures
reject with an `AggregateError`. Queries that become live after the snapshot do
not join that reset's wait. Reset aborts work from the previous epoch, so late
query results and mutation effects from that work cannot change the new state.

An in-flight mutation receives an abort signal when reset starts. Reset does
not wait for mutation callbacks. A callback that settles later still settles
its returned promise with its own result or error, while its old predictions
and settled effects remain ignored. A mutation that had already settled keeps
its result.

Live stream stores restart after reset with the signal supplied by the opener.
If that owner signal has already been aborted, the stream remains closed and
does not reopen. Reset does not wait for stream emissions or completion. Open
projections clear their value and error, then restart. A completed, failed, or
closed projection keeps its terminal status; reset clears its value and error
without reopening it.

Starting another reset rejects the earlier reset promise with an `AbortError`.
`graph.resetVersion` changes synchronously when each reset starts. Calling
`reset()` after graph disposal, or from a write-restricted reactive scope,
throws a `Fault` synchronously.

## Query observation and progress

Use `observeStore(graph, acquiredQuery)` to acquire the sole publisher for an
already-existing query incarnation. The query must belong to the graph and be
active. Acquisition does not create a store, fetch data, or renew idle
retention. A collected query cannot be revived by acquiring a lease. After
reset, a retained query handle can acquire a lease for its new incarnation.

`lease.publish(value)` replaces the committed base beneath predictions and
clears the incarnation's shared observation issue. It does not settle or
supersede a request, resolve `ready`, change query status or error, or count as
query progress. A request that was already running can still commit its result
after a publication. A publication after a successful landing preserves that
landing's freshness age. Equality and synchronous propagation follow the
native store rules.

`lease.reject(issue, diagnostic?)` stores the caller's opaque issue without
changing the value or request outcome. An optional diagnostic reports its
`cause` through the graph's continuation error route. `lease.report(cause)` uses
that route without changing the issue. Disposing a lease releases its publisher
slot and preserves the issue for a replacement lease on the same incarnation.
The next valid publication clears the issue. Reset, collection, and graph or
context retirement discard it. Stale lease methods return `false` without
effects.

Reading or subscribing to `lease.issue` retains query metadata while the
incarnation lives. It does not activate an ordinary fetch or freshness timer,
but a live issue reader joins reset's query refresh population. `lease.active`
is lifecycle metadata that remains false after lease retirement. Its reads and subscriptions do
not retain the query.

Each `QueryStore` exposes `progress`, a readable snapshot of the latest query
execution. `observeQueryProgress(store)` provides a non-coalescing observation
with `capture()` and `subscribe()`. A subscription receives one current seed,
then every accepted progress report and lifecycle transition in FIFO order.
The stream does not replay older events. Capturing does not retain or activate
the query. A subscription retains the query until you unsubscribe.

A snapshot identifies its execution and reports whether it has a progress
value, the latest value, its `reportRevision`, status, and original error or
interruption reason. `hasValue` distinguishes no report from a report whose
value is `undefined`. Status is `idle`, `active`, `complete`, `interrupted`, or
`failed`. Progress reports describe work inside the query callback. They do
not commit the query value or settle its request. `observeStore().publish()`
also does not create a progress report.

`ObservationPublishOptions.transfer` labels the exact observed base revision.
The default `eligible` value keeps it eligible for supported snapshots. Use
`exclude` only for a native fallback whose transfer policy has been explicitly
qualified. This label does not describe later writes or stream values.

`snapshot(graph)` serializes selected committed values, not executable graph
machinery. Reconstruction therefore means creating another graph with the same
contract and passing the snapshot to `adopt(target, state)`. Requests, timers,
predictions, open streams, projections, callbacks, and signal identities do not
cross that boundary.

This state layer is still being stabilized. Its current integration suite is a
behavioral baseline, not a compatibility promise for a published 1.0 API.

## Context-bound mutation effects

Bind a mutation when its callback completes a different result or its effects
must reach context-bound query profiles. Use the original mutation definition
and one `profileEffect` descriptor for every original effect position, in order.
Registration captures callbacks without computing keys, acquiring stores or
running selectors. Re-registering the same context, definition and profile token
throws `TypeError`, even with equivalent callbacks.

```ts
import { bindOperation, predictionFor, profileEffect } from "aeolia";

// userQuery and rename are original declarations in graph's contract.
// names is an existing QueryBinding for the same context and original userQuery.
const effect = profileEffect(rename, 0, userQuery, {
  prepare: (phase) =>
    phase.existing(phase.selectedInput).map((target) => {
      const name = target.forBinding(names);
      return name === undefined
        ? phase.prepare(target)
        : phase.predict(name, (_current, input) => ({ label: input.name }));
    }),
  settle: (phase) =>
    phase
      .existing(phase.selectedInput)
      .map((target) => phase.selected(target, phase.selectedInput)),
});
const saving = bindOperation(graph, rename, {
  context,
  profile: {},
  effects: [effect],
  run: async (input, call) => ({ receipt: await rename.run(input, call) }),
});
const current = names.target(existingNameHandle);
const options =
  current === undefined
    ? {}
    : {
        optimistic: [predictionFor(rename, current, (_value, input) => ({ label: input.name }))],
      };
await saving.member({ id: "one", name: "Cedar" }, options);
```

The resolver owns any input validation/transformation before calling
`phase.existing`. That operation finds existing active profiles for this context,
original query and logical key. It starts no query and does not retain or revive
an entry. Raw entries, other contexts and other original queries are excluded.
Each phase invokes the original selector once per effect. Settlement discovers
its population afresh, so an entry acquired during the mutation receives its
settled effect without receiving earlier optimistic work.

Preparation completes every selector/resolver and checks every returned
instruction before publishing predictions or dispatching the run callback.
`prepare` records membership without choosing a producer. `original` requests
the exact original effect's native-value producer through a compatible typed
ticket. A transformed completion uses a separately typed `predict` producer.
Neither constructor evaluates a producer. Native prediction evaluation remains
synchronous: a throw, forbidden reactive read or capacity fault is diagnosed and
that prediction is omitted. Other valid targets and the mutation can continue.
These faults do not set an observation issue.

`predictionFor` supplies a per-invocation override by physical store identity.
It takes precedence over the matching original or declared producer and does
not fall back after a predictor diagnostic. Equal logical keys can have distinct
overrides. Duplicate identities, fabricated handles, wrong mutation associations
and foreign contexts throw `TypeError` before publication or dispatch. An override
does not add an unrelated target to the original effect's membership.

Tickets capture one incarnation without a liveness pin. An expired target is
omitted without acquisition. Reset never retargets an old instruction. A fresh
explicit `binding.target(handle)` call on a retained active handle can construct
a current instruction, including before a reset request lands. An inactive or
collected handle returns `undefined`; normal query acquisition or explicit
retained-handle revalidation remains separate.

At settlement, `selected(target, input)` refreshes with the freshly validated
selector input and `current(target)` uses that target's current validated source.
Both retain its bound completion callback and the original effect's `on` policy.
An invalidate effect refreshes only a value-live store; a revalidate effect
starts its ordinary native refresh. Refresh completion is not awaited by the
mutation. Individual resolution/application failures preserve their original
causes and use the existing continuation diagnostic channels; independent
settled effects continue before the successful mutation result is returned.

The context and graph own cancellation before preparation callbacks or waits.
Caller cancellation, `graph.cancelPending()`, reset and context/graph disposal
can prevent pre-dispatch work. Once run has dispatched, an aborted transport
signal alone does not suppress a callback's successful completion or effects.
Reset, graph disposal and context retirement still prevent obsolete effects
from reaching another lifetime. `EffectContext.signal` is a cancellation hint,
not the sole post-success authority check. Rejected run/output completion
removes that mutation's predictions, applies no successful effects and does not
replay a backend write. Raw `graph.api` mutation behavior is unchanged.

## Bound stream sessions and native addresses

A stream binding fixes a genuine original stream member to one binding context.
Registration is dormant: it computes no key, creates no store and opens no source.
The binding is memoized for the exact graph, context, original declaration and raw
member. The authored key remains unchanged. Equal keys in the raw namespace or
another binding context identify separate physical entries.

```ts
import { bindStream, createBindingContext, inspectBoundAddress, storeKey } from "aeolia";

// events is the original declaration registered at graph.api.events.
const context = createBindingContext(graph);
const binding = bindStream(graph, events, graph.api.events, { context });
const address = inspectBoundAddress(binding, storeKey("events/one"));
if (address.kind === "stable" && address.state === "empty") {
  const source = binding.member({ id: "one" }, { expectEmpty: address.condition });
  const session = binding.session(source);
  const receipt = await session.detached;
  // receipt belongs to this exact native opening.
}
```

An ordinary member call joins the current session or opens a fresh one. Each
returned wrapper fixes its value, availability, status, pending and error facets,
subscriptions, writes and shared projections to the session it actually joined.
The session identity is assigned before authored source or signal callouts. Two
wrappers joining A return the same metadata object. A later B has a different
session identity and can reuse the same physical store identity.

`binding.session(source)` uses the exact returned wrapper. It remains available
after synchronous source failure, pre-abort, reset or disposal during opening,
and after detachment. It reads metadata without touching idle time, retaining the
source or performing I/O. Inheriting from a wrapper or supplying a wrapper from
another binding throws `TypeError`. Value access still checks context and graph
eligibility; disposal uses the native `Fault` channel.

Retirement first revokes A's publication and acquisition authority, then takes one
native visible capture before destructive clearing. The detachment promise
resolves after this capture and A's logical clear/status work have been sealed or
revoked. It does not wait for `iterator.return()` or transport cleanup. The receipt
contains the original native end status and reason, and a tagged
`terminalCapture`: either `{ kind: "captured", capture }` or
`{ kind: "fault", cause }`. A captured result can itself be `changed`. Faults keep
the original thrown cause, including `undefined`; presence does not use the value
as a sentinel, so a stable present `undefined` differs from absence. A receipt
makes no RPC outcome or transport acknowledgement claim.

After detachment, A's value facets read its sealed terminal record. They never
read B's current cells. A cannot write into B, and an old updater is rejected
before invocation. Old subscriptions and metadata waiters cannot retain B.
Ordinary `peekStore(A)` rejects an ended wrapper; it cannot serve as a substitute
for the terminal capture. A shared `project(A, policy)` also stays on A. Reset
applies the projection's native reset policy and closes the bound projection;
it does not attach it to another opening. An explicit fresh member call opens B
without replaying A's value, initial seed or source carrier. The first session
after version 2 adoption can claim that exact imported base once; this is the
seed's first session, not a later reopening of A. Initial, equality and snapshot
options still obey the native physical-entry conflict rules. Supplying an
accepted initial explicitly can seed a fresh session. Raw stream reset and
independent projection behavior retain their existing policies.

`inspectBoundAddress(binding, key, { expected? })` accepts a genuine query or
stream binding and an already-authored nonempty `StoreKey`. It invokes no member,
key callback, acquisition, read subscription, transport or idle renewal. Inspecting
a missing key can allocate scalar address metadata, but creates no store or store
identity. Its stable result reports separate visible `present` and committed
`basePresent` facts. An address is occupied while it has visible/base data, a
native active or draining operation, a finite query attempt, or a retiring stream.
An opening session can be occupied with both presence flags false.

A stable empty result additionally supplies an opaque, non-reserving condition.
The condition belongs to that exact inspected binding and key, whether the
binding is a query or a stream. Only the same stream binding's member can consume
its stream-origin condition through `expectEmpty`. A query-origin condition has
no query consumer and cannot authorize stream admission. The stream member
validates its condition after authored pre-acquisition callbacks and immediately
before the first native mutation.
Stale, already-used, crossed or occupied conditions throw `TypeError` before
clear, create, join or open. A condition does not reserve the address, force
replacement or join an existing session. An ordinary call without a condition can
open B after A retires, clearing the previous session state first.

Every inspection includes an opaque address-local stamp. Passing a stale genuine
stamp returns `{ kind: "changed" }`; a fabricated or crossed stamp throws
`TypeError`. Acquisition, native state changes, collection and reset permanently
invalidate old facts even when the address later appears empty again. Disposed
contexts throw `Fault`. Raw stream and query option types do not expose
`expectEmpty`; their existing runtime treatment of unknown extra fields remains
unchanged. They provide no conditional acquisition guarantee.

## Positioned stream projections

A positioned stream adds an opaque provider position to each source event. Declare one with `createPositionedStream`. The declaration keeps its existing key, store options, graph member and bound-stream behavior. A position does not create another key or store namespace.

```ts
import { createPositionedStream } from "aeolia";
import type { PositionedStreamSource } from "aeolia";

type OrderInput = { readonly orderId: string };
declare const orderEventSource: PositionedStreamSource<
  OrderInput,
  { readonly id: string },
  number,
  object
>;

const events = createPositionedStream({
  name: "orders.events",
  key: (input: OrderInput) => "orders/" + input.orderId + "/events",
  source: orderEventSource,
});
```

The provider owns the position format and ordering rules. Its `open` callback must emit events strictly after the supplied `after` boundary when a reader is present. A live open omits the `after` and `reader` properties; a restored open supplies them as own properties. The native layer does not compare opaque positions or choose a starting cursor.

Use `projectPositioned(member, input, policy)` to open an independent projection from a genuine raw member or stream binding. Use `project(streamStore, policy)` to attach a projection to a positioned stream's current shared session. Each projection tracks its own successful-fold position. A projection that joins a shared session during event delivery starts without a position and advances on the next event it actually folds.

`capturePositionedProjection(projection)` reads the native fold tuple without opening or pulling the source. It returns the borrowed value, lifecycle status, error, `hasPosition`, position, generation, revision and `resumable` state. Before the first event, `hasPosition` is false. The native layer does not invent a cursor at the provider's current head. A capture is an in-memory native handle, not a serializable authority; use the provider and application protocol to encode and authorize data for another process. `isPositionedCheckpointCurrent(capture)` detects native tuple, epoch, binding and generation changes, but it cannot certify mutations inside borrowed accumulator values. Before encoding or transferring a value, the application must apply its bounded ownership/encoding rules and recheck currentness at the required publication boundary.

A position advances only when the projection successfully installs the value produced by an event. The position and revision advance even when the projected value compares equal to the previous value. Native publication commits this tuple before notifying subscribers. An observer failure after that commit does not undo the fold. `positionedFoldReceipt(projection)` returns the latest successful-fold receipt, or `undefined` before the first successful fold; `acknowledgePositionedFold` rejects a foreign or stale-generation receipt.

A reducer failure marks the projection failed and makes its capture nonresumable. A reducer can mutate its borrowed accumulator before throwing, so the last visible value may no longer match the last successful position. Do not resume a failed projection from that value. Aeolia does not clone or freeze events, positions, reducer inputs or accumulator values.

Use `preparePositionedRestore` when the target uses the same native declaration and provider as an authentic current capture. The provider validates the target input, reader, boundary, capture and seed before Aeolia issues a dormant restoration.

Use `preparePositionedSeedRestore` when the target is a fresh graph or process and the application has decoded an accumulator value. Pass the target member, explicit target policy, `{ after, value }`, and a reader reserved by the target provider. The target provider authenticates that boundary and seed; the function does not require the source graph or its private capture identity. Accumulation seeds are arrays of emitted values, not positioned event envelopes. Reduction seeds use the reducer's accumulator type, which can differ from the event type.

Preparation validates the target reservation and installs the seed without opening the source or refolding a prefix. The returned restoration is dormant. `activate()` rechecks that the target and reader are current, then opens after the admitted boundary once. `discard()` closes an active restoration or releases a dormant reader. If a reader becomes stale after preparation, activation fails, closes the projection and releases the reservation. A preparation error leaves the reservation with its caller. Graph reset closes dormant and active restorations instead of reusing a one-shot reader, then preserves terminal status while resetting the value and error according to the projection policy. Graph disposal closes all projections; bound-context disposal also closes projections created through that context.

The provider's `validateReader` and `isReaderCurrent` callbacks must be synchronous local checks and return exactly `true` for an admitted reader. They must not perform SQL or publish external state. `acknowledgePositionedFold` passes the provider an opaque native receipt only after a successful fold. That receipt does not prove that an external write, durable transaction or required-transfer publication committed. Applications own those protocols and must acknowledge or release their provider reader at the correct external boundary.

Positions and reader reservations are provider-specific. Aeolia sets no retention window, cursor encoding, authorization rule or fallback for missing history. The provider must reject an expired or invalid reservation before dormant issuance, and it must report an ordered gap through its own source behavior rather than silently skipping events.

A provider may define `initialBoundary(input, options)` on the same positioned
source. Aeolia captures that callback with the source declaration and calls it
with the same input and per-open options passed to `open`, after `open` returns
a valid iterable and before pulling an event. The result must name the real
exclusive start or history position for that opening. If an empty source has
no available initial boundary, required continuation refuses. Aeolia does not
invent a default cursor or represent the anchor as an event.

A provider may also define
`readerLifetime(input, reader, after, seed)` on that source. Required positioned
transfer needs this capability. It returns the genuine abort signal for that
exact reservation and may return its already-issued immutable expiry as an
absolute epoch time in the native clock domain. Every allowed reservation
invalidation must be visible through the signal before the reservation becomes
stale. A deadline does not renew or extend a reservation, and one reader's
validation must not revoke another reader.

### Acquire and release a reader

A provider can supply an optional `acquireReader({ input, checkpoint, signal })`
callback on the existing positioned source. The declaration captures it without
invoking it. A framework performing automatic resumption calls it with the
retained stream input, the same decoded `PositionedStoreRestoreSeed` object it
will pass to restoration, and one effective acquisition signal. The hook adds
no checkpoint representation, Store or replacement position.

Return the original genuine Promise that completes the acquisition. Its
fulfilled value must be a nonthenable envelope with an own data property
`reader`. The Reader is opaque: it may be undefined, a Promise or a thenable.
Transport it inside `{ reader }` and pass that exact Reader to restoration;
returning or awaiting the Reader itself can assimilate it.

Reject acquisition with the original cause when it fails, is unauthorized or
cannot supply the requested continuation. There is no unavailable success
branch or implicit live-open fallback. Ordinary live sources and callers that
supply readers manually do not need this optional hook. A framework requiring
automatic resumption must reject a missing hook before opening the source.

Aborting acquisition revokes publication authority; it does not prove that the
provider operation ended. The caller retains the original Promise and releases
a late-delivered Reader once. The provider remains responsible for resources it
has not returned when acquisition rejects. The framework cannot prove or join
detached provider work that it never receives.

`releaseReader(input, reader)` runs once after its native owner closes or is
discarded. Primitive results, including null, are ignored. An object or
function result is observed with the captured native Promise mechanism; a
genuine returned cleanup Promise is retained and joined through the existing
resource and graph/context retirement. Fulfillment values are ignored.
Publication ends immediately while physical cleanup can remain pending.

A synchronous throw or cleanup rejection retains its original cause, including
undefined. Failed Promise observation also retains the original cause and
leaves retirement incomplete: it cannot be treated as successful synchronous
cleanup. Arbitrary thenables are not assimilated. Returning a Map, WeakMap or
another incidental object therefore changes retirement behavior. Use a block
for synchronous side effects:

```ts
declare const releases: WeakMap<object, number>;
const releaseReader = (_input: unknown, reader: object): void => {
  releases.set(reader, (releases.get(reader) ?? 0) + 1);
};
```

This contract applies to reader release. Other callbacks that return void keep
their own existing behavior. Claim registration completion remains distinct
from the restoration resource's original `retired` Promise.

### Restore a shared Store session

A graph member call for a positioned declaration returns a genuine `StreamStore<Event>`. Use
`project(source, policy)` to attach each projection to that Store's current native
session. The projection does not open another reader or fold the Store's retained
value. Use `capturePositionedStore(source, requiredProjections)` to capture the
Store and the exact finite tuple of projections that the target needs. Aeolia
checks that every projection belongs to the source's session and that they share
one completed event cut. Your application remains responsible for including every
projection its continuation requires.

The checkpoint records the source Store identity, session identity, latest-event
presence, provider position, required projection values in tuple order, and
whether the cut is resumable. `hasPosition` and `present` still describe an
actual completed event and its source base. A present event may have the value
`undefined`.

When present, `continuation` identifies a genuine event boundary or initial
provider anchor. An initial anchor can make an empty source resumable without
making `present` true or adding a `latest` event. `continuation.after` and
`checkpoint.position` have type `unknown`; the provider codec must validate
and narrow these positions.

```ts
import {
  capturePositionedStore,
  createGraph,
  createPositionedStream,
  defineContract,
  project,
} from "aeolia";
import type { PositionedStreamSource } from "aeolia";

type Input = { readonly id: string };
declare const sourceCallbacks: PositionedStreamSource<Input, number | undefined, number, object>;

const events = createPositionedStream({
  name: "orders.events",
  key: (input: Input) => input.id,
  source: sourceCallbacks,
});
const graph = createGraph({
  contract: defineContract({
    namespace: "positioned-store-example",
    operations: { events },
  }),
});
const source = graph.api.events({ id: "one" });
const total = project(source, {
  kind: "reduce",
  initial: 0,
  step: (sum, event) => sum + (event ?? 0),
});
const count = project(source, {
  kind: "reduce",
  initial: 0,
  step: (value) => value + 1,
});
const checkpoint = capturePositionedStore(source, [total, count] as const);
if (!checkpoint.present || !checkpoint.hasPosition || !checkpoint.resumable)
  throw new TypeError("No resumable Store cut is available");

const latestEvent: number | undefined = checkpoint.latest;
const [totalValue, countValue] = checkpoint.values;
```

The checkpoint and its values are borrowed native state. The handle is not
transfer authority that can be reconstructed from copied fields. Copy the event,
position, and fold values with your codec's bounds. Then call
`isPositionedStoreCheckpointCurrent(checkpoint)` at the publication boundary.
The check recognizes only the exact native capture. It does not pull the source
or call provider code. The API does not admit a capture during event delivery.
An incomplete source landing, a failed required reducer, or projections at
different event boundaries also cannot form one resumable group. Do not split
the projections into separate readers or refold the prefix to make the group fit.

When a checkpoint is not yet resumable, `observePositionedStoreCaptureState(checkpoint)`
returns a native `Readable` that can signal when the session's capture state
changes. Subscribe with `subscribe`: the initial notice is delivered
synchronously. A `changed` notice has an opaque revision and means the caller
should capture the actual source and required projections again. It is not a
currentness, resumability, transfer, or publication receipt. Recheck the new
checkpoint before using it.

The terminal notices are `closed`, `invalidated`, and `failed`; a `failed`
notice retains the original `cause`, including `undefined`. A terminal notice
remains sticky for that observation. Unsubscribing releases the observation and
its deadline task without retaining, reopening, or stopping the shared source.

The native group eligibility check also reads the source Store's actual prediction
state. Any live optimistic prediction on that Store makes the group cut ineligible,
even when the predicted value equals the current value. The checkpoint contains
no prediction state or callables. If a prediction changes the Store's visible
revision, an earlier eligible checkpoint becomes stale. After the prediction
layer actually retires or reconciles and the positioned source base remains
unchanged, a new group capture may be eligible. The earlier checkpoint never
becomes current again. This restriction applies to positioned group capture;
`peekStore` retains its existing effective-value capture, including optimistic
predictions.

The absence of a visible prediction does not prove that an affected operation
has retired or that complete affected-operation coverage exists. The host or
provider owns those gates.

`preparePositionedStoreRestore(member, input, policies, seed, reader, options?)` creates the
target group from a genuine target member and the target provider's admitted
reader. The policy tuple determines the projection tuple and its value types.
Target policies are code in the target runtime, not decoded wire callbacks.
Ordinary restore keeps the legacy seed `{ after, latest, values }`; `latest` is
an own property even when its value is `undefined`.

Required positioned transfer uses `{ boundary, base, values }`. `boundary` is
`{ kind: "event" | "initial", after }`; `base` is either `{ present: false }`
or `{ present: true, value }`. The boundary and source base are independent.
A present base requires its own `value` property, including when that value is
`undefined`. An omitted base has no `latest` or `value` payload. The target
member fixes the `Event`, `Position`, and `Reader` types. The options argument
does not widen `Event`. The target provider authenticates the full seed and
reservation before Aeolia transfers ownership of the reader.

Pass an optional sixth `ValueOptions<Event>` argument for the target source Store.
Options belong to the target runtime and are not part of the decoded seed.
`options.initial` supplies the target Store's authored reset baseline.
For the legacy seed, `latest` remains the restored source value. For a
boundary/base seed, `base.value` is the source value only when `base.present`
is true. Store equality, snapshot settings, defaults, and conflicting-option
faults follow the ordinary native stream configuration rules.

`options.abortSignal` remains the caller's cancellation signal for the target
session. If it aborts while the restoration is dormant, the session closes and
releases its reader without opening the source. The same signal governs the
active reader after activation. For required transfer, the separate signal from
`readerLifetime` reports invalidation of the provider's exact reservation.

Preparation does not open the source or run reducers over a prefix. The returned
`source` is the actual target `StreamStore`. A present base installs its exact
value, including `undefined`; an omitted base installs no imported source value
or committed landing. A genuine target-authored or browser base keeps its normal
precedence. Without one, the source remains unavailable until a real event
lands. While the restoration is dormant, its source status is `"empty"` and
`pending` is false. Reads and subscriptions do not activate the reader. The
returned projections attach to that same source and begin with their decoded
fold values. An initial boundary creates no source event, fold, or acknowledgment.

After the target accepts the transfer at its publication boundary, activate the
restoration:

```ts
import { preparePositionedStoreRestore, subscribe } from "aeolia";
import type {
  MemberOf,
  PositionedStreamDefinition,
  PositionedStoreRestoreSeed,
  ReducePolicy,
  ValueOptions,
} from "aeolia";

type Input = { readonly id: string };
declare const targetDefinition: PositionedStreamDefinition<
  Input,
  number | undefined,
  string,
  number,
  object
>;
declare const targetMember: MemberOf<typeof targetDefinition>;
declare const input: Input;
declare const policies: readonly [
  ReducePolicy<number | undefined, number>,
  ReducePolicy<number | undefined, number>,
];
declare const seed: PositionedStoreRestoreSeed<
  number | undefined,
  number,
  readonly [number, number]
>;
declare const reader: object;
const options: ValueOptions<number | undefined> = {
  initial: 100,
  equals: (left, right) =>
    left === undefined || right === undefined
      ? left === right
      : Math.floor(left) === Math.floor(right),
};
declare const handleEvent: (event: number | undefined) => void;

const restoration = preparePositionedStoreRestore(
  targetMember,
  input,
  policies,
  seed,
  reader,
  options,
);
const stop = subscribe(restoration.source, handleEvent);
restoration.activate();
stop();
restoration.discard();
```

`activate()` enters `opening`, opens the admitted reader once after the
selected boundary, and returns the same Store. With an initial boundary, the
first real suffix event establishes the source landing and folds once; the
initial anchor itself is not acknowledged as an event. The source is pending
while it waits for that event. Source failure or completion also ends the wait
under the existing stream contract. An admitted restored reader enters
`failed` and releases its reader on any reported gap after its boundary, even
before the first suffix event, while an ordinary unreserved `StreamStore`
keeps its existing gap behavior. A later `project(restoration.source, policy)`
starts from that policy's initial state and folds future events. It does not
receive the restored event or refold the transferred prefix.

Each restoration owns a separate Store identity, session, and reader. Two
admitted restorations may use the same declaration and logical key without
sharing their reader. An ordinary member call keeps its existing keyed-cache
behavior and does not find or join a reader-scoped restoration.

Calling `set` or `update` on the source Store changes its visible value but
does not emit a positioned event. Such a write cannot advance the source
position or any projection receipt, and it invalidates the group's continuation
relation. A later genuine source event can establish a new completed cut.
Equal-valued source events still advance the native event and fold facts even
when Store signal equality suppresses a value notification.

Use the positioned checkpoint and your application codec for reader-scoped
transfer. A reader-scoped Store triggers the existing encoding `Fault` only when
it would otherwise be included by ordinary snapshot rules: it is not disposed, its
`snapshot` option is enabled, it has a committed base value, and native transfer
eligibility allows it. The key-only snapshot format cannot represent its reader
authority, so an eligible entry fails encoding instead of being flattened into
a different identity.

A disposed, `snapshot: false`, base-absent, or otherwise ineligible reader Store
stays excluded under ordinary key-only snapshot rules. It does not block
unrelated snapshot entries. Required positioned transfer may still carry an
otherwise eligible continuation with `base.present: false`; it does not import
a source value or bypass external-operation and source eligibility guards.
The `snapshot` option does not add reader authority to the wire format. The
existing `positionedFoldReceipt` and `acknowledgePositionedFold` functions
continue to report native successful folds. A capture
or fold acknowledgement does not report an operation result, complete
affected-operation coverage, an external commit, or physical retirement. The
host or provider owns those gates.

`discard()` revokes the group synchronously and releases its reader once.
`restoration.retired` is the original completion for that session's physical
cleanup. It is separate from a passive claim's registration-only `retired`
promise and from graph or context retirement. Normal preparation does not wait
for this long-lived resource completion. If an active iterator has asynchronous
cleanup, the original `graph.retire()` or `context.retire()` promise also joins
that work and rejects on cleanup faults. The `void` return from `discard()` is
not cleanup completion. Cleanup faults remain separate from source and
projection outcomes. Reset, cancellation, or disposal invalidates the one-shot
reader. A later continuation requires a fresh provider admission.

These APIs support in-memory transfer when the provider admits a target reader.
They do not enable durable restart or failover recovery. Durable persistence
remains off unless the application explicitly enables a provider feature for it.
Reader validation, currentness, acknowledgement, and release callbacks remain
synchronous local operations. They do not run SQL or publish external state.
