# CI and npm releases

This guide is for maintainers configuring checks and publishing Aeolia releases.

## Required checks

The `CI` workflow runs on pull requests targeting `main` or `release/*`, on pushes
to `main`, and on manual dispatch. It checks formatting, lint, the build, and
types. Builds precede type checks because consumer fixtures use the generated
declarations. Behavioral tests run on Windows and Linux.

For a PR whose source branch belongs to this repository, verification first
checks the [branch naming convention](../CONTRIBUTING.md#issues-and-pull-requests).
Only `feature/<name>`, `fix/<name>`, and `release/<VERSION>` are accepted. A release
branch must name a supported version that matches `package.json`. Fork branch
names are exempt, and a fork's `release/*` branch does not trigger release-version
checks or publication.

After the first successful workflow run, add the **Required** check from the `CI`
workflow to the repository's Required Checks ruleset. Keep that ruleset's bypass
list empty. This check fails if any verification job fails, is cancelled, or is
unexpectedly skipped. Keep the separate review ruleset for maintainer approval
exceptions. Target both `main` and `release/*` in each ruleset.

All applicable PRs produce a check result. The workflow does not use path filters
that could leave a required result pending. Test selection happens inside the run.

This CI check blocks merging after a PR is opened; it does not prevent branch
creation. To reject other prefixes when branches are created on GitHub, create
a separate branch ruleset targeting all branches, exclude `main`, `feature/**/*`,
`fix/**/*`, and `release/**/*`, and enable **Restrict creations** with no bypass
actors. The CI check still validates the release version. See
[GitHub's ruleset guide](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository).

## Test selection

`dorny/paths-filter` compares the entire PR with its base, including added,
deleted, and renamed files. The rules live in `.github/filters.yml`; the workflow
shows each selected test group as a separate step.

- Reactive-engine or shared-runtime changes run the full suite.
- Contract changes run contract, snapshot, testing-helper, and application tests.
- Snapshot changes also run contract tests that use snapshots and adoption.
- Changed test files select their owning group. Application and CI checks also
  run for code changes.
- Changes to dependencies, build configuration, workflows, the selector, or an
  unrecognized path run the full suite.
- Known documentation-only changes can omit runtime tests. Changes to files
  included in the npm archive still run installed-package checks.
- Missing history, failed comparison, or an unexplained empty selection runs
  the full suite.

Installed-package checks exercise the candidate archive in Bun, Node, and Chromium
on both operating systems. Full behavioral runs enforce the contributor guide's
90% line and function coverage minimum and retain reports as workflow artifacts
for 14 days. Selected runs do not produce whole-project coverage claims.

To reproduce the full source-coverage check locally, run:

```sh
bun test tests/reactive tests/contract tests/realm tests/testing tests/application tests/ci --coverage --coverage-reporter=lcov --coverage-reporter=text
bun scripts/ci/coverage.ts
```

These commands do not build or run the installed-package consumer. Run
`bun run test:package` for that check. See [the test guide](../tests/README.md)
for prerequisites and individual subsystem commands.

## Configure npm publishing

1. Create a GitHub environment named `npm`. Allow deployments from `main`. The
   release merge is the publication approval; no additional environment reviewer
   is needed unless you want a second manual gate.
2. In the npm package's trusted-publisher settings, authorize the GitHub repository
   `hesperia-js/aeolia`, workflow `release.yml`, and environment `npm`. Enable direct
   publishing with `npm publish`.
3. Allow GitHub Actions to create tags. If a tag ruleset restricts `v*` creation,
   configure it to permit the release workflow's identity. Do not grant a bypass
   of the branch CI ruleset for this purpose.

The workflow uses `actions/setup-node` and the npm CLI with short-lived
trusted-publishing credentials. Do not add an `NPM_TOKEN`. Builds and tests use
the Bun version in `packageManager`, the frozen lockfile, pinned Node and npm
versions, and commit-pinned GitHub actions.

See [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/) for registry
requirements. New trusted-publisher configurations must currently complete their
first publication within two days; configure the publisher when a release is ready.

## Make a release

1. Create `release/<VERSION>` from `main` and open its release PR back to `main`.
2. Set `package.json` to the same version as the branch name. Do not add a `v` prefix.
   The version must be newer than the version on the PR base and unpublished on npm.
3. During stabilization, pause unrelated merges into `main`. Submit release fixes
   as PRs into the release branch. Those fix PRs use affected-test selection.
4. Review the final release PR. It requires the full Windows/Linux suite and
   installed-package checks on every revision.
5. Merge the release PR. The `Release` workflow checks out the exact resulting
   commit and repeats full verification. It builds one archive and tests that
   same archive on Windows and Linux before publishing it.

After verification, the workflow creates `v<version>` at that commit and publishes
the tested archive. It does not edit versions or create source commits. Stable
versions use npm's `latest` channel; `alpha`, `beta`, and `rc` prereleases use their
respective channels. Other prerelease channels and build metadata are rejected.
Ordinary main-branch merges never publish. JSR publication is not configured.

## Recover a failed release

Open the failed `Release` run and rerun its failed jobs. The workflow retains the
candidate archive for 14 days. Reusing that archive avoids changes from rebuilding.

A tag created before a failed npm upload remains at the verified commit. A retry
accepts an existing tag only if it points to that commit. If the npm version
already exists, its archive integrity must match the verified candidate exactly.
The workflow never overwrites a tag, accepts different bytes for an existing
version, or moves a channel back to an older version.

If the archive has expired, rerun the full release workflow. An already-published
version whose rebuilt bytes differ requires maintainer investigation. Do not move
the tag or reuse the version for different contents.

Publishing requires a merged PR from this repository's `release/*` branch into
`main`. Manual CI runs provide full verification but do not publish. A version
tag by itself does not trigger publication.
