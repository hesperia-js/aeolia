# Signals behavior matrix

This matrix reconciles Aeolia's independently authored tests with the Signals
polyfill behavior suite at commit
[`1c33f914806f0872229cba05a1c882a38c0def4f`](https://github.com/proposal-signals/signal-polyfill/commit/1c33f914806f0872229cba05a1c882a38c0def4f).
The upstream source is an audit checklist, not copied test code.

Run the local evidence with:

```sh
bun run check
```

## Upstream behavior coverage

| Upstream file                  | Behavior families                                                                                                                                                                                                                                                           | Local evidence                                                                                                                                                            | Status                                                     |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `custom-equality.test.ts`      | State and Computed equality, equal-value pruning, equality reads tracked by the inner Computed without leaking to its caller                                                                                                                                                | `signal-conformance.test.ts`: State custom equality, Computed receiver/equality, equality dependency isolation, downstream cutoff                                         | Covered                                                    |
| `cycles.test.ts`               | Self-cycle and multi-Computed cycle detection                                                                                                                                                                                                                               | `signal-conformance.test.ts`: cycle detection and post-failure graph usability; `helpers.integration.test.ts`: named Aeolia cycle diagnostics                             | Covered                                                    |
| `dynamic-dependencies.test.ts` | Active and inactive branches in live and non-live graphs                                                                                                                                                                                                                    | `signal-conformance.test.ts`: dynamic dependency replacement; `dependency-propagation.test.ts`: live branch replacement and newly selected dirty Computed                 | Covered                                                    |
| `errors.test.ts`               | Cached computation failures, cached live failures, dependent recovery, equality failures                                                                                                                                                                                    | `signal-conformance.test.ts`: ordinary, live, and equality failure caching and recovery; `helpers.integration.test.ts`: dependent failure caching                         | Covered                                                    |
| `graph.test.ts`                | Direct-plus-derived paths, diamonds and uneven reconvergence, equal branch pruning, static siblings, topological pulls, linear and high-fanout convergence, changed dependee ordering, pending siblings, dynamic liveness, stale-source refresh, and downstream suppression | `dependency-propagation.test.ts`; helper diamond and watched-pruning cases in `helpers.integration.test.ts`                                                               | Covered by independently shaped graph fixtures             |
| `guards.test.ts`               | State, Computed, and Watcher recognition without accepting ordinary values                                                                                                                                                                                                  | `signal-conformance.test.ts`: canonical instance guards and lookalike rejection                                                                                           | Covered as a polyfill-compatible extension                 |
| `liveness.test.ts`             | First-live and last-live transitions for States and Computeds                                                                                                                                                                                                               | `signal-conformance.test.ts`: recursive liveness, hook receivers, failure-safe transitions, and aggregated cascading failures                                             | Covered, with stronger Aeolia error coverage               |
| `prohibited-contexts.test.ts`  | Writes allowed during Computed evaluation; reads and writes forbidden during canonical Watcher notification                                                                                                                                                                 | `signal-conformance.test.ts`: synchronous Computed writes and frozen Watcher notification operations                                                                      | Covered                                                    |
| `pruning.test.ts`              | Pull and live graphs stop downstream recomputation when intermediate values settle equal                                                                                                                                                                                    | `signal-conformance.test.ts`: equality cutoff; `dependency-propagation.test.ts`: equal branches and downstream suppression; `helpers.integration.test.ts`: watched cutoff | Covered                                                    |
| `receivers.test.ts`            | `this` for Computed callbacks, equality callbacks, lifecycle hooks, and Watcher notification                                                                                                                                                                                | `signal-conformance.test.ts`: receiver fixtures for every callback family                                                                                                 | Covered, including Watcher receiver not exercised upstream |
| `type-checking.test.ts`        | Prototype placement, incompatible method receivers, and invalid subtle-operation domains                                                                                                                                                                                    | `signal-conformance.test.ts`: prototypes, subclassing, receiver rejection, introspection domains, and all-or-nothing Watcher argument validation                          | Covered, including rollback safety not exercised upstream  |

## Deliberate Aeolia behavior

These choices must not be silently changed to match an incidental polyfill
implementation detail:

- `signal`, `computed`, and `watch` are Aeolia conveniences over the same engine
  used by the canonical classes.
- `Signal.subtle.Watcher` uses the proposal's frozen notification and rearm
  model. Aeolia's `watch` helper is a separate subscription API that permits
  writes and delivers their notifications in the next synchronous propagation
  pass.
- Sibling listener order is unspecified. Every listener that remains active
  when reached is invoked, and callback failures are aggregated.
- Helper subscriptions added during notification become eligible for later
  writes, not the write that began the current pass. Mid-pass unsubscription is
  dynamic, so a listener removed before its turn may be skipped.
- A valid signal passed to `Watcher.unwatch` when it is not watched is an
  idempotent no-op.
- A State equality exception aborts the write. A Computed equality exception is
  cached and rethrown until a dependency invalidates the Computed.
- `Signal.subtle.currentComputed()` returns `null` outside a tracked
  computation.
- Computed callbacks may write State, but the pattern is discouraged because
  cyclic writes can exhaust the bounded propagation passes.
- `peek`, `update`, thenable rejection, named `Fault` diagnostics, lifecycle
  failure aggregation, and the `Signal.is*` guards are Aeolia or polyfill
  extensions rather than core proposal requirements.

## Time and scheduling

The signal engine has no clock, timer, microtask scheduler, or asynchronous
notification queue. State propagation completes synchronously. Fake time
belongs to store freshness, revalidation, retention, and collection tests when
those layers are implemented.

## Scope limit

This matrix covers the repository's public behavior suite. It does not claim a
host-independent garbage-collection guarantee: forced collection and finalizer
timing are not portable observable semantics. It also does not turn the Stage 1
proposal or its preview polyfill into a stable ECMAScript contract. Future
upstream revisions require a new pinned reconciliation.
