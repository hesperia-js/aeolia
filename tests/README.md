# Test ownership

Start with `application/storefront.integration.test.ts` to follow Aeolia through
an application workflow. It uses controlled backend callbacks, not a browser or
HTTP server. The subsystem tests isolate lifecycle and failure cases that would
make that workflow difficult to diagnose if they were all folded into it.

| Directory      | Behavior under test                                                                                         |
| -------------- | ----------------------------------------------------------------------------------------------------------- |
| `reactive/`    | Signals conformance, dependency propagation, helper APIs, and readable subscriptions                        |
| `contract/`    | Definitions, stores, query freshness and readiness, mutations, streams, projections, and instance lifecycle |
| `realm/`       | Snapshot encoding and adoption                                                                              |
| `testing/`     | The controlled backend fixture used by other tests                                                          |
| `package/`     | Built exports and consumer-facing declaration types                                                         |
| `application/` | A complete workflow using the public API                                                                    |

`*.types.ts` files are compile-time fixtures. They must remain included in
TypeScript checks; a runtime test run does not verify their assertions.
`profiling-test.ts` is a separate, manually run experiment and is not part of the
deterministic suite.

Run all tests with `bun run test`. Run one subsystem with, for example,
`bun test tests/reactive`. Package tests need a build first, as do the current
consumer-type checks:

```sh
bun run build
bun run typecheck
bun test tests/package
```

For source coverage, exclude built-package tests rather than counting both
`src` and `dist` copies of the library:

```sh
bun test tests/reactive tests/contract tests/realm tests/testing tests/application --coverage
```

Test names describe observable behavior, not private helper names. A test move
must preserve its setup, subscriptions, timing, and assertions. Executing the
same source lines is not evidence that another test can replace it.
