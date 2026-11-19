# Reactivity

The reactive kernel implements the surface of the TC39 Signals proposal over
Aeolia's own engine:

- `Signal.State`
- `Signal.Computed`
- `Signal.subtle.Watcher`
- `Signal.subtle.untrack`
- `Signal.subtle.currentComputed`
- `Signal.subtle.introspectSources` and `introspectSinks`
- `Signal.subtle.hasSources` and `hasSinks`
- `Signal.subtle.watched` and `unwatched`

Aeolia also provides these convenience APIs over the same graph:

- `signal`
- `computed`
- `subscribe`
- `watch`
- `createObserver`
- `afterPropagation`
- `withoutWrites`
- `Fault`
- `readableBrand`

The Signals namespace, reactive factories, and their types are also available
from `aeolia/reactive`. Both entry points use the same constructors, symbols,
and runtime registries, so signals created through either import work together.
The reactive entry point does not expose contract graph internals.

```ts
import { computed, signal, subscribe } from "aeolia";

const count = signal(0);
const doubled = computed(() => count.get() * 2);
const stop = subscribe(doubled, (value) => {
  console.log(value); // 0 immediately, then 2 after the write below
});

count.set(1);
console.log(doubled.get()); // 2
stop();
```

State writes and notification delivery are synchronous. A Computed evaluates
when read. `subscribe` reads its source to deliver the initial value, then
reads again after propagation to deliver changed values. Without a value
subscriber, a live Computed can remain invalid until the next read. Listener
order is not a public contract.

State and Computed construction checks option values at runtime. `equals` and
the `watched` and `unwatched` lifecycle callbacks must be functions, and
`label` must be a string. Computed construction also requires a function
callback. Invalid options or a non-function callback throw `TypeError` before
the signal is added to the reactive graph.

`State.update(next)` calls `next` once with the current value and stores its
result. The result may be any value allowed by the State's type, including a
function. Published packages include supplementary diagnostics under the
`development` export condition. Enable that condition in your module resolver
to get a descriptive `TypeError` for a non-function updater. The ordinary write
policy, including `withoutWrites`, still takes precedence.

Use `subscribe(readable, observer)` to receive values. It delivers the current
value immediately, including `undefined`, and respects the source's equality
policy (`Object.is` by default). A computed whose result remains equal does
not produce another delivery. Later callbacks run
after the notification phase, before the originating write returns. Reads in
the callback are allowed and do not become dependencies of an enclosing
computation. Delivery observes the latest value for a propagation pass, not a
history of every intermediate write. The returned function unsubscribes.

`Signal.subtle.Watcher` follows the proposal's pending and rearm model and
forbids signal reads and writes inside its notification callback. Aeolia's
`watch` helper reports invalidation without delivering a value. It evaluates
an uninitialized Computed when attached and permits writes from its callback,
but not reads; notifications caused by those writes run in the next
synchronous propagation pass. It does not call the observer on registration.

Frameworks that need to control when work runs can use `createObserver({
notify })`. Call `observer.track(run)` for initial work and whenever the
framework applies an invalidation. Tracking collects every `get()` dependency
reached by `run` and replaces the previous dependency set, so conditional
reads are dynamic. A write made while `run` is tracking remains pending for a
later check, including a write made during the initial track.

The `notify` callback is an invalidation signal only: Aeolia calls it once for
each pending propagation and freezes reactive reads and writes while it runs.
Queue framework work with `afterPropagation(callback)`; that callback runs
after propagation has settled, outside the notification freeze, and before the
outermost synchronous write returns. The usual handoff is:

```ts
const value = signal(0);
const render = () => value.get();
const observer = createObserver({
  notify() {
    afterPropagation(() => {
      if (observer.check()) observer.track(render);
    });
  },
});

observer.track(render);
```

`check()` refreshes computed dependencies and returns whether any tracked
dependency's committed change token changed. It never evaluates `render`, and
equality configured on a State or Computed can therefore suppress a rerun.
`dispose()` detaches all dependencies and makes queued invalidations harmless;
it is idempotent. `afterPropagation` callbacks are deduplicated by callback
identity within a handoff and their returned unsubscribe is idempotent.

## Transferring cached reactive values

`captureReactiveConstruction` associates a real `State` or `Computed` instance
with its native construction. `prepareRequiredTransferCapture` records its
existing cached dependency closure without running callbacks. The resulting
slots describe dependency edges and cached outcomes; they do not contain the
source callbacks.

Query `status` and `pending` facets retain a caller slot linked to the shared
physical Store slot, so the receiver maps each captured caller to its own
genuine claim. See [Managed state](managed-state.md#required-cached-state-transfer)
for the caller row and claim mapping.

`ObservationLease.active` and `issue` are also native facets. Their receiver
counterparts come from `claim.prepareObservation()`. When multiple caller slots
share one physical query and one required observation, pass the first claim's
exact pending lease to each additional claim. Independent pending leases do not
represent the same observation. The image carries no private issue payload, so
incompatible issue state fails the complete adoption.

Adoption requires actual receiver `State` and `Computed` instances with their
own construction receipts. State writes since a target State was constructed
remain visible. If a required cold receiver Computed depends on a browser-owned
State value that differs from the image, the complete adoption is refused before
any of its writes are published. The Computed stays cold; its next normal
`get()` evaluates against the receiver graph. A warm Computed can retain its
browser cache only when the complete required mapping remains compatible.

Required reactive publication separates writes from notification delivery.
`TransferCommit.publish()` installs the staged State values and compatible
caches synchronously. The caller then invokes
`TransferCommit.notifications.deliver()` at its chosen boundary. If a watcher
throws during delivery, delivery reports the failure and the installed reactive
values remain published; the transfer plan reports that same outcome through
`retired`.

This path preserves the kernel’s normal dependency tracking after publication. A
later receiver write invalidates the transferred Computed through its actual
target dependencies, and its next read follows the ordinary propagation rules.

Synchronous teardown can use `withoutWrites(cleanup)` to keep reads available
while rejecting every State write, including a same-value write that would
otherwise be suppressed by equality. Nested scopes are supported and the
normal write policy returns in a `finally` block after cleanup throws. The
scope ends when the synchronous callback returns; it does not cover work
started asynchronously.

Subscriptions added during a helper notification do not observe the write that
started that pass. They become eligible for later writes. Unsubscription is
dynamic: a listener removed before its turn may be skipped. Because sibling
delivery order is unspecified, application behavior must not depend on whether
that listener had already run.

Computed callbacks may write State, matching the proposal, but this is not a
recommended application pattern because cyclic writes can exhaust Aeolia's
bounded propagation passes. There are no timers or microtask scheduling in the
reactive kernel. Time enters Aeolia only at the store layer for freshness,
revalidation, retention, and collection.
