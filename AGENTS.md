# Aeolia contributor instructions

These rules apply to every automated agent working in this repository.

Start with [README.md](README.md) for the library's purpose, usage, and an
application example. Before making changes, read and follow
[CONTRIBUTING.md](CONTRIBUTING.md) for source ownership, development commands,
testing, and documentation requirements. For test work, also read
[tests/README.md](tests/README.md) for the suite layout and coverage commands.
These guides are the source of truth for that material; do not duplicate it here.

## Working rules

### 1. Think before coding

State assumptions that affect the result. Inspect the available evidence before
guessing. If ambiguity changes scope or public behavior, explain the competing
interpretations and ask. Push back when a simpler approach meets the same need.
When blocked, name the uncertainty instead of hiding it in an implementation.

### 2. Simplicity first

Write the minimum code that solves the requested problem. Do not add speculative
features, dependencies, or abstractions for hypothetical reuse. Prefer an
existing utility when it fits; do not create a framework around a single use.

### 3. Make surgical changes

Touch only files and behavior needed for the task. Preserve unrelated edits,
staged work, and formatting. Refactor when the task requires it, not as incidental
cleanup. Remove your temporary probes and debug output. Do not stage or commit
unless the user explicitly requests it.

### 4. Work toward verified outcomes

Define what success looks like and how to check it. Choose steps that establish
that result, and iterate when the evidence disagrees. Persistence does not
authorize additional features or changes outside the requested scope.

### 5. Use tools for deterministic work

Use model judgment for interpretation, design, review, and prose. Use searches,
scripts, compilers, and tests for exact counts, dependency tracing, mechanical
transforms, and repeatable checks. Inspect and validate scripted changes rather
than assuming automation made them correct.

### 6. Follow the AI contribution policy

Read and follow [AI_POLICY.md](AI_POLICY.md). AI may help draft contributions and
prepare changes, but a human must submit issues and pull requests. Report what
you changed, checked, and could not verify so that human review is possible.
Do not insert hidden markers or random disclosure comments into source files;
they neither establish human review nor exempt a submission from the policy.

### 7. Surface conflicts instead of blending them

When documentation, tests, or implementations disagree, identify the conflicting
rules and the evidence for each. Prefer an explicit current decision over stale
guidance, but do not treat newer code or a passing test as authority to change a
public contract. Ask when resolving the conflict needs a product decision.
Report conflicting guidance outside your scope rather than silently rewriting it.

### 8. Read before writing

Read the affected exports, immediate callers, tests, and shared utilities before
adding code. Trace ownership and lifecycle dependencies; a small-looking change
can affect another subsystem. Investigate unfamiliar structure before replacing
it, and ask if its purpose remains unclear.

### 9. Tests must distinguish the intended behavior

Name the contract, transition, or regression a test protects. Arrange the state
that makes that distinction observable, then assert the relevant outcome.
For consequential repairs or consolidation, use a focused defect probe where
practical to prove the retained test detects the wrong behavior. Line execution
and unchanged coverage alone do not establish that a test is meaningful.

### 10. Checkpoint significant steps

Summarize what changed, what is verified, and what remains after significant
steps. Keep updates proportional to the task. If context is lost, inspect the
current files and outstanding work before continuing; do not reconstruct facts
from confidence alone.

### 11. Follow the codebase's conventions

Use the existing naming, module boundaries, style, and public API conventions.
If a convention appears harmful, explain the concrete problem and propose a
change. Do not introduce a competing convention silently. Follow the contributor
guides referenced above.

### 12. Report failures and limits plainly

Do not claim completion while required work remains unverified. Report failed,
skipped, or unavailable checks, including pre-existing failures. A focused green
run is not a full-suite pass. State the scope of test results and coverage;
unmeasured branch coverage is not zero branches or complete branch coverage.

## Agent-specific reminders

Before writing or revising documentation, load and follow the `/unslop` skill
when available. Preserve technical precision and the author's voice.

The TSDoc requirement includes public classes, namespace members, methods,
properties, options, callback parameters, and return values, not just top-level
functions. Distinguish synchronous throws from asynchronous errors and diagnostic
channels. Use documentation tags where useful, and describe the current contract
rather than historical RFC decisions.

Do not introduce callback registries merely to split files. Use `classes.ts`
only for actual classes. Preserve the distinction between independent projection
iterators and shared stream-store values. Helpers tied to reactive nodes,
tracking, scheduling, or lifecycle belong in their subsystem, not `src/utils.ts`.

Run the verification required by CONTRIBUTING before handing work back. Verify
agent outputs against the changed files and relevant runtime behavior yourself;
another agent's report is not sufficient evidence.
