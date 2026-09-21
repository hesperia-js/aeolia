# Query recovery invariant

Reader: engineers implementing or reviewing Aeolia's shared query lifecycle.

Status: approved on 2026-09-10, with mutation-triggered recovery settled on
2026-09-11. Timer and query-member recovery are implemented, and Hesperia's
query-await adapter is integrated.

## INV-A-QUERY-RECOVERY-001

Aeolia owns recovery for each shared query within its graph. A failed request
permits one recovery attempt. If recovery also fails, automatic revalidation
must be disarmed and a fault raised. Other consumers of the same query must
not each grant another automatic attempt.

The rule applies whether or not the query has earlier committed data.
Successful recovery restores the allowance and re-arms the revalidation timer.
After failed recovery, the query stays disarmed until an explicit author retry
or a successful mutation requests revalidation of that query. A mutation's
refresh must succeed before the timer resumes; failure leaves it disarmed.

Starting a request still clears the previous error. Clearing that error must
not reset the recovery allowance: only successful recovery or explicit retry
re-arms it. Retained data and existing request ownership remain unchanged.
Superseded settlements cannot alter the current recovery decision.

Aeolia's public `.ready` behavior remains unchanged. Hesperia checks status
when awaiting: it rejects an observed failure before requesting recovery.
If a new request has already cleared that failure, the await observes the
current request. It does not reconstruct an earlier failure.

No passive query constructor, committed-presence readable, or public
pause/resume operations are added by this invariant.

## Verification required

Lead verification: build and typecheck pass. The full Aeolia suite passes
231 tests with 1,215 assertions, including 24 query-readiness tests covering the
new recovery behavior. Hesperia's data suite passes 21 tests with 97 assertions,
including the real contract adapter and request-state transition coverage.

- Initial failure and refresh failure each permit one recovery.
- Failed recovery stops automatic requests despite more elapsed time or consumers.
- Successful recovery restores timed revalidation and a later recovery allowance.
- Explicit author retry re-arms a disarmed query.
- Error clearing does not reset an ongoing recovery allowance.
- Superseded results cannot change the current recovery state.
- Committing local data does not restart a disarmed timer or query member.
- Readiness, retained data, predictions, and shared request ownership remain correct.

## Prior rule

This replaces RFC-005 section 9.7's unlimited timed requests after failures.
The previous implementation allowed one request per configured window
indefinitely while watched. That is no longer the approved policy.

Implementation must distinguish explicit retry from framework-requested
recovery without bypassing the shared limit. Any required public interface
change remains a separate author decision.

## Successful mutation recovery

A successful mutation with a `revalidate` effect, or an `invalidate` effect on
a query with a live value observer, may refresh an affected query whose automatic
recovery was exhausted. Request startup still clears the error.
A successful landing restores the timer and recovery allowance. A failed
refresh leaves the query disarmed and reports recovery exhaustion. A newer
mutation may supersede that request, but its replacement retains the same
failure rule; stale settlements cannot re-arm the query.

An `invalidate` effect marks data stale and refreshes only when the query's value
has a live observer. An unobserved query stays stale without fetching; observing
only its status, pending, or error does not trigger a refresh. `revalidate`
refreshes regardless of observers. Neither effect creates a missing store or
allows ordinary query consumers to restart a disarmed query.

Focused public integration coverage in `mutation-recovery.integration.test.ts`
proves successful recovery, failed recovery with multiple consumers, and a
newer mutation superseding recovery while the older result arrives late.
The initial probe reproduced an unwanted timed request after failed mutation
recovery. All three tests pass after the repair. Lead verification on 2026-09-11:
`bun run test` builds the package and passes all 234 tests with 1,246 assertions;
typecheck and scoped mutation source/test/documentation formatting and lint pass.
Full lint still reports the unrelated profiling test's unused import, and full
formatting fails only on that profiling test. The profiling file is unchanged.
