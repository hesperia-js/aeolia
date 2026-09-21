# Managed state

The [query recovery invariant](query-recovery-invariant.md) limits automatic
recovery after failures. One failed recovery disarms automatic revalidation;
successful recovery or explicit retry re-arms it.

Query member calls share that automatic recovery allowance. After it is exhausted,
opening another view of the same failed query does not dispatch another request.
An explicit `revalidate()` starts a fresh attempt and re-arms recovery.

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

`snapshot(graph)` serializes selected committed values, not executable graph
machinery. Reconstruction therefore means creating another graph with the same
contract and passing the snapshot to `adopt(target, state)`. Requests, timers,
predictions, open streams, projections, callbacks, and signal identities do not
cross that boundary.

This state layer is still being stabilized. Its current integration suite is a
behavioral baseline, not a compatibility promise for a published 1.0 API.
