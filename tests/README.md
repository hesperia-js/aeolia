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
| `package/`     | Built exports, installed-package workflows, source maps, and consumer-facing declaration types              |
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

## Distributed package

Install Node on `PATH` and Playwright's Chromium once, then run the package check:

```sh
bun install
bunx playwright install chromium
bun run test:package
```

On Linux, `bunx playwright install --with-deps chromium` also installs the browser's
system dependencies. The command can run inside a container, but does not create one.

`test:package` builds Aeolia, runs the existing export and runtime-name checks,
packs an archive, and installs it in a fresh directory under the operating system's
temporary directory. The consumer has no workspace links and its only dependency
is the archive. Installation disables lifecycle scripts and uses offline mode.

The check copies the existing consumer type fixtures into that directory and
checks them with declaration checking enabled and no ambient package types.
One JavaScript workflow imports all four public entry points and runs in Bun,
Node, and Chromium. It verifies cross-entry signal propagation, a query refreshed
by a mutation, reset through a preserved handle, and disposal of pending work.
Chromium loads the installed JavaScript through an import map without rebundling it.

A deliberate invalid query declaration also verifies that Node's source-map support
reports the original TypeScript line that threw. This checks the packed maps, not
just whether map files exist. Runtime failures and missing prerequisites fail the
command; temporary files and the browser are cleaned up on success or failure.

Run this command before a release. `prepublishOnly` also runs it when publishing
the checkout through npm or Bun. Publishing an existing tarball or disabling
lifecycle scripts bypasses that hook. This checks the npm archive; it does not
validate a JSR publication.

## Source coverage

For source coverage, exclude built-package tests rather than counting both
`src` and `dist` copies of the library:

```sh
bun test tests/reactive tests/contract tests/realm tests/testing tests/application --coverage
```

Test names describe observable behavior, not private helper names. A test move
must preserve its setup, subscriptions, timing, and assertions. Executing the
same source lines is not evidence that another test can replace it.
