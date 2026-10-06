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

`snapshot(graph)` serializes selected committed values, not executable graph
machinery. Reconstruction therefore means creating another graph with the same
contract and passing the snapshot to `adopt(target, state)`. Requests, timers,
predictions, open streams, projections, callbacks, and signal identities do not
cross that boundary.

This state layer is still being stabilized. Its current integration suite is a
behavioral baseline, not a compatibility promise for a published 1.0 API.
