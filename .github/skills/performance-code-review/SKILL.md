---
name: performance-code-review
description: "Perf Police performance code review for microsoft/mssql-python. Use when reviewing pull requests or diffs involving native bindings, parameter detection, setinputsizes, execute/executemany, fetch, DAE streaming, allocation, caches, or performance claims. Check ownership and protocol correctness, hot-path patterns, benchmark validity, and code organization; distinguish reproduced blockers from unverified concerns."
---

# Perf Police review

This is the shared performance-review methodology for the Perf Police custom
agent and other Copilot reviewers. Apply it to the relevant parts of a review;
it is not a replacement for unrelated repository checks.

## Scope and boundaries

- Review the actual requested revision and base. Record their SHAs and inspect
  the effective PR diff, especially after stacked branches, squash merges, or
  conflict resolution. Do not mistake historical commits for current changes.
- Read the requirement, existing review threads, and related decisions before
  proposing an architectural change. Source code establishes what runs; the
  documented API and accepted requirements establish what should happen.
- Keep production files and Git refs unchanged. Do not switch a shared checkout,
  overwrite another task's work, or rewrite published history. Use an isolated
  review checkout; put permitted scratch work under its `review/` directory.
- Use existing tools and authenticated read interfaces. Do not install tools,
  start infrastructure, edit production code, or publish extra comments merely
  because a review was requested. Return findings through the host's authorized
  review workflow; an interactive review normally returns them in chat.
- A skill does not provision a compiler, SQL Server, credentials, or a separate
  execution host. If runtime evidence cannot be obtained, state what is missing
  and keep affected conclusions unverified.

## Review procedure

Use this workflow to assess a change or plan a permitted scratch experiment;
it does not authorize production edits or additional runtime activity.

1. **Define the problem and the boundaries.** State the observable failure or
   slow operation, affected inputs, expected behavior, and what would count as
   improvement. Identify contracts and neighboring paths that must not change.
2. **Establish a trustworthy baseline.** Reproduce through the supported public
   API using the smallest relevant existing harness and the same input on the
   candidate and its actual base. Reduce the workload without removing the
   conditions that trigger the problem. Separate pre-existing behavior and
   environment/setup failures from a change-specific effect.
3. **Trace what actually runs.** Read changed units, callers, deleted checks,
   and cleanup paths. Follow dispatch rather than assuming similarly named
   operations share an implementation. For parameter work, distinguish
   `execute`, `executemany`, array binding, overrides, and DAE. Establish loop
   frequency, data flow, ownership, and timer boundaries before naming a cost.
4. **Separate observations from explanations.** Record the raw symptom, likely
   causes, and the smallest check that would distinguish them. For each proposed
   cause, predict what should change and what should stay unchanged. A costly
   pattern or a correlated timing change is a hypothesis, not a root cause.
5. **Test one explanation at a time.** Keep a pinned control and vary one
   relevant factor in an isolated experiment when execution is permitted.
   Hold unrelated code, workload, settings, and timing boundaries fixed.
   Retain adverse and inconclusive results. If a check cannot distinguish the
   causes, refine the hypothesis rather than enlarge the patch or discard data.
6. **Assess the smallest causal fix.** Connect the proposed change to the
   demonstrated mechanism, not just a better aggregate score. Prefer a local
   removal of redundant work, a valid invariant moved out of a loop, or a
   narrowly eligible fast path over a broad rewrite or new cache. Treat an
   untested alternative as a suggestion, not an equivalent fix.
7. **Trace the full impact before removing work.** Identify every caller of
   shared helpers and every correctness responsibility of the old path.
   Preserve diagnostics, error propagation, ownership, invalidation, fallback,
   and cleanup. Move a responsibility to a verified replacement before
   removing its old implementation; do not trade correctness for fewer calls.
8. **Check both the mechanism and unaffected behavior.** Inspect existing tests
   and diff coverage. Verify the intended behavior change and unchanged
   contracts against the baseline, with eligible, excluded, boundary, failure,
   and recovery cases. Assert promised operation reductions as well as values.
   Retain representative neighboring-path controls; a passing target workload
   or coverage percentage does not establish that shared behavior is safe.
9. **Decide from the evidence.** Check whether attribution supports the proposed
   cause and whether matched release measurements justify the added complexity.
   Drop disproved findings and explain corrections. Separate established
   mechanisms, measured effects, unresolved alternatives, and untested paths;
   do not force an optimization when the evidence remains inconclusive.

Before concluding, answer these questions from the evidence: does the change
address the ask; is its scope justified; does it regress other supported paths;
does it cover its promises; does it introduce fragile coupling; what must change
before merge; and is there a smaller, equally safe implementation?

Reuse prior reads and cite line ranges instead of repeatedly dumping a large
translation unit. Refresh affected sections if the target revision changes.

## Cost model: frequency before labels

Describe the workload dimensions and cite the loop or caller that establishes
them. A function entered once per `execute` can still do thousands of per-value
operations internally. Different hunks in one function can occupy different zones.

| Zone | Unit of work | Examples |
| --- | --- | --- |
| Z1: surface | Once per API call | Binding entry point, argument handling, exception translation |
| Z2: shape setup | Once per prepared/result shape, often O(columns) | Result metadata, fetch dispatch construction |
| Z3: batch orchestration | Once per batch, outside element loops | One arena allocation, batch-level dispatch or boundary crossing |
| Z4: element work | Once per value: O(parameters) per execute or O(rows x columns) for fetch | Parameter detection/conversion, per-element binding checks, row/cell construction |

Pybind11 is appropriate for Z1/Z2 and batch orchestration. Prefer raw CPython
construction and inspection in Z4. A per-parameter override loop is Z4 even when
its containing function runs once per call. If frequency is unclear, mark it
unverified rather than asserting a zone violation.

A pattern violation identifies work to investigate, not a measured regression.
Do not attach historical timings from a different PR or workload to today's hunk.
Do not expand a PR to rewrite unchanged hot loops simply because they are nearby.

## Patterns and anti-patterns

| Pattern | Review rule |
| --- | --- |
| 0: RAII ownership | Prefer `py::object` with `steal()` for new references and `borrow()` for borrowed references from `py_ref.hpp`; avoid hand-balanced refcounts in multi-exit code. |
| 1: Marshal in C | In element loops, prefer `PyList_New`, `PyFloat_FromDouble`, and checked `PyUnicode_*` operations over repeated pybind11 construction, casting, or attribute dispatch. |
| 2: Precompute invariants | Derive shape-only work once per valid shape, not once per value. Do not cache value-dependent conversions as if they were shape-only. |
| 3: Release the GIL for blocking calls | Preserve release scopes around blocking ODBC calls. Trace Python access and buffer ownership across those scopes; ODBC function names alone do not imply network I/O. |
| 4: Batch boundary crossings | Avoid Python/C++ round trips per row or parameter when one native call can process the batch with equivalent validation and error behavior. |
| 5: Bounded arenas | Consider shared allocation for genuinely bounded sizes. Preserve overflow checks, alignment, byte/unit accounting, and lifetimes; an arena still needs evidence of benefit. |
| 6: Reuse while valid | Reuse cached state only while its full contract remains compatible; explicit invalidation is part of the optimization, not optional cleanup. |

| ID | Candidate anti-pattern | What to establish before reporting |
| --- | --- | --- |
| A1 | `py::cast`, `.attr()`, `.append()`, or dispatch in an element loop | Show the frequency and distinguish an ownership wrapper from value construction. |
| A2 | Manual refcount management across branches | Trace success, early returns, exceptions, and stolen/borrowed references. |
| A3 | Python API/object access while the GIL is released | Identify the actual access and the enclosing GIL scope. |
| A4 | Borrowed data used across a GIL release without sufficient ownership | Check immutable-owner lifetime or a native snapshot; a borrowed pointer alone proves neither safety nor a bug. |
| A5 | Storage expires or moves while ODBC still retains its address | Include partial binds, error returns, reset failures, and parent teardown. |
| A6 | Per-element allocation despite a usable bounded aggregate size | Show a safe alternative and measure it; do not delete lifetime ownership to reduce allocations. |
| A7 | Repeated dispatch that depends only on stable shape | Verify the dispatch is truly invariant for the supported inputs. |
| A8 | Python-owning static state survives interpreter shutdown | Check explicit cleanup and finalization behavior; lazy initialization alone does not solve destruction ordering. |
| A9 | Optimization removes the only usable behavioral/performance control | Keep a reproducible reference, such as the pinned base build; do not resurrect an intentionally removed legacy path solely to add a toggle. |

**Bare RAII `py::object` is not A1.** A value adopted by `steal()`/`borrow()` and
used through `.ptr()` is an ownership wrapper, not the expensive conversion or
dispatch path. Never flag it solely because its type name appears in a loop.

## Current routing and Python contracts

- Inspect the reviewed revision before assuming where work runs. The native
  `execute` path now handles `setinputsizes`; the former `SQLExecuteLegacy_wrap`
  is not a standing prerequisite or universally available reference.
- `BindParameters` and `BindParameterArray` have different callers and contracts.
  Inspect exceptional branches too. Do not couple live code to obsolete paths
  merely to remove duplication; share only genuinely invariant work.
- Compare against the appropriate current/base behavior and accepted contract.
  Do not restore a superseded bug in the name of legacy parity. Testing explicit
  `setinputsizes` overrides alone does not prove automatic detection parity.
- Validate dispatch separately from capability: a selected native route requires
  its binding, but an available binding need not be selected.
- Preserve supported subclass/callback behavior, settings changes, and reentrancy
  across Python/native boundaries. An immutable snapshot still needs correct
  invalidation; callbacks may change state before a computed value is cached.
- Check pending Python errors after fallible C-API conversions. Returning a
  value while an exception remains set is not successful conversion.
- Calling a Python special method directly can bypass a builtin's validation.
  For example, a `Decimal.__format__` override can return a non-string. Validate
  results before list mutation, casting, or unchecked Unicode macros, and
  preserve the expected exception behavior.
- Keep Python code-point counts, UTF-16 code units, and encoded bytes distinct.
  Astral characters require two UTF-16 units. Size the final normalized/formatted
  value, including the chosen encoding and any required terminator. Use the same
  discipline in SQL fixtures: verify that boundary values fit the declared type
  and reach the intended driver path before interpreting a failed assertion.
- Inspect actual consumers of new metadata. Populating an unused length field
  may be a correctness prerequisite, but it is not evidence of a current
  customer-visible crash fix or a delivered speedup.

## Binding ownership and invalidation

**Not reused does not mean unnecessary.** A structure named "cache" may also
own memory that must survive an error. Before proposing an eligibility gate or
deletion, identify every lifetime responsibility and its replacement.

- Prefer ownership tied to the statement's lifetime over an unbounded map keyed
  by a recyclable raw handle or the current thread. Check handle reuse, multiple
  cursors, sequential thread handoff, and connection close.
- Establish ownership before ODBC can retain the first parameter address.
  Partial-bind and execution failures can outlive local vectors. Retain required
  storage until a successful unbind/free, or explicitly justify terminal
  abandonment without claiming native deallocation.
- Do not erase original diagnostics by resetting the statement before the caller
  has read them. Test error reporting and subsequent recovery, not just success.
- Native storage used in GIL-less cleanup must not hide Python-owned references
  or destructors that need the interpreter. Check this separately from throughput.
- Same SQL or C type is insufficient for reuse. Review parameter count, C/SQL
  types, direction, column size, precision/scale, effective encoding, data and
  indicator addresses, and actual ODBC buffer lengths.
- Capacity alone is not the binding contract. A changed encoded length may
  require rebinding even if an allocation can be retained. Positional `void*`
  reuse also requires the allocation sequence and concrete types to remain valid.
- Inspect invalidation on preparation/query change, metadata/encoding/size
  change, NULL/DAE fallback, direct/catalog/array execution, statement-attribute
  changes, explicit reset, close/free, conversion errors, and ODBC failures.
  Check both native state and Python prepared-state flags.
- Prove reuse using operation counts and changed values. Also exercise a forced
  miss, an excluded shape, and recovery to eligible inputs. A correct result from
  a path that always rebinds does not validate the optimization.

### ODBC cleanup semantics

Use the API contract, not an inferred meaning of a wrapper name:

| Event | Consequence |
| --- | --- |
| Successful `SQLDisconnect` | Associated statements are already freed; do not assume they wait for the later DBC free. |
| Failed `SQLDisconnect` | Depending on the failure, the connection and statements can remain live. Inspect the return and diagnostic state. |
| `SQLFreeHandle` returns `SQL_ERROR` | The handle remains valid; a C++ owner disappearing does not turn failure into successful deallocation. |
| Wrapper marked retired or pointer nulled | Later wrapper use may be prevented; this does not prove ODBC freed the resource. |

Trace GIL-held error propagation separately from GIL-less destructor/finalization
paths. A throwing check may bypass the apparent "unconditional" cleanup below
it. Terminal abandonment is a distinct tradeoff, not evidence that all
failure paths are safe. Fault-inject rare paths before claiming runtime proof.

Check supported callers before reporting a race. The DB-API `threadsafety`
contract matters, as do explicitly supported cancellation and sequential
handoff. A second sweep of a tracking list is not a substitute for a complete
concurrent-lifecycle design.

## Machinery must justify its complexity

- Apply the smallest safe solution first: existing helpers, standard idioms, then
  new machinery. Avoid speculative wrappers, duplicate dispatch, or knobs.
- Keep the fix separate from unrelated cleanup or architectural changes. Do not
  require a migration or dependency PR without a demonstrated call-path need;
  one exceptional path does not justify rewriting the surrounding subsystem.
- Scope a follow-up experiment around one unresolved question and the result
  that would change the decision. Reuse the existing workload and harness before
  adding another framework; more validation machinery is not performance evidence.
- Require evidence for a performance optimization's payoff. Safety ownership,
  protocol validation, and error handling do **not** need a throughput gain to
  justify their existence. Classify added code by responsibility before proposing
  cuts; neither fewer lines nor fewer calls proves lower latency. Reject a
  measured slowdown rather than preserving machinery because it looks cheaper.
- Before calling a guard redundant, prove the state unreachable through all
  supported callers. Distinguish a genuinely fixed bound from user-supplied data.
- A proposed cut must preserve diagnostics, lifetime, fallback, and public API
  behavior. Build and exercise a smaller alternative before presenting it as
  equivalent; otherwise label it an unverified suggestion.
- Separate a scoped draft experiment from merge readiness. Pending measurements
  are not a proven performance regression, and "no reproduced blockers" is not
  proof of safety or sufficient reason to approve a performance claim.

## Evaluate measurements, not just tables

This skill evaluates performance evidence. It does not introduce a new profiler
implementation or require a separate profiler agent. When new measurements are
requested and the runtime is available, use the
[mssql-profiler skill](../mssql-profiler/SKILL.md) to operate the existing
profiler. Otherwise review the available artifacts and state the evidence gaps;
do not start an unrelated benchmark merely because a review is running.

### Workload validity

- Read the requirement and confirm that the benchmark reaches the changed path.
  An override optimization needs declared overrides; a cache benefit needs hits.
  Existing benchmarks can be excellent fallback controls while never using the
  proposed fast path.
- Map changed branches to the public operations that reach them: scalar,
  rowwise, batched, Arrow, or catalog fetching as applicable. A SELECT-only
  benchmark does not cover a SQLColumns-specific branch. If a shared loop
  changes, retain ordinary-query controls even when catalog fetching is the goal.
- For batch sizing, cover empty/small results, boundaries around capacity,
  multiple growth steps, and a full final batch followed by actual EOF. Validate
  values, cell and Row types, NULLs, order, description, and messages before
  coercion or serialization can hide a difference.
- Include compatible repeated inputs, shape changes, and excluded inputs. An
  all-or-nothing cache can miss every batch containing a NULL, date, decimal, or
  DAE value even if most of the batch is otherwise eligible. Inspect the actual
  eligibility rule rather than treating an exclusion as an inherent limitation.
- Keep setup from invalidating the state under measurement. For example, issuing
  `TRUNCATE` through the same cursor between inserts can defeat prepared-query
  reuse. Distinguish cold setup, warmup, and steady-state measurements.
- Verify current values, affected rows, and relevant operation counts outside
  timed regions where possible. Use existing counters/logging for correctness;
  use normal production logging settings for timing.
- Do not change the workload registry, acceptance criteria, or agreed report
  format to rescue a result. Review necessary harness corrections explicitly,
  preserving prior evidence and identifying which comparisons need rerunning.

### Provenance and controls

- Use separate, pinned base/PR builds with matching interpreter, optimization,
  dependencies, SQL Server, and workload. Record dirty source changes, build
  flags, native binary identity, OS/architecture, warmups, repetitions, and units.
- Verify both the imported package and the actual native extension path. A `.py`
  loader can legitimately load the `.so`/`.pyd`; its own path is not sufficient
  provenance. Discard mislabeled or logging-contaminated runs.
- Use Release/`-DNDEBUG` for comparisons. Debug-only assertion costs are not
  release regressions. Verify effective configuration and compiler flags as
  described in the [profiler skill](../mssql-profiler/SKILL.md#build-and-verify-instrumentation),
  not a build's label. Do not copy another PR's numbers onto a later change.
- Counterbalance base/PR order, retain raw per-round observations, and avoid
  competing benchmarks or DB-heavy tests on the same machine/server. Alternation
  reduces order bias; it does not remove all contention or server-state effects.
- Define the timing window: input generation, setup, execute, fetching, commit,
  rollback, and cleanup are not interchangeable. Label generated data and
  local-only results; do not present them as customer production measurements.

### Profiler and uncertainty

- Use matching profiling-enabled Release builds for attribution, and separately
  compare uninstrumented Release builds for shipped latency. Verify whether the
  native profiling API is compiled in; runtime-disabled instrumentation is not
  the same configuration as instrumentation compiled out.
- Keep a single owner of process-wide profiling state. Reset measurement windows,
  exclude warmups as stated, and wait for worker activity to finish before
  collecting. Missing timer data is not proof that a path cost zero.
- Parent timers include nested instrumentation overhead. Skipping thousands of
  bind calls can also skip thousands of timer records. Do not present that
  instrumented percentage as the production speedup or sum overlapping phases.
- Trace a timer's exact source scope before naming the cause. A binding wrapper
  may include metadata access, buffer sizing/value initialization, ODBC calls,
  and diagnostics. Its total is not pure ODBC time or proof of allocator cost.
  An increased fetch-call count alone does not attribute a slowdown; compare its
  aggregate timed cost and the surrounding batch work.
- Distinguish live buffer capacity from allocation traffic, transient peak, and
  process RSS. A capacity probe establishes only the storage it observes; keep
  diagnostic overlays separate from the uninstrumented latency comparison.
- `SQLBindParameter` is not inherently a network round trip. An incomplete probe,
  cumulative counters from several workloads, or omitted phases cannot establish
  a universal upper bound on the optimization's benefit.
- Show dispersion and paired observations alongside medians. Inconclusive data
  proves neither zero benefit nor absence of regression. Overlapping ranges or
  a "delta smaller than spread" rule are not statistical significance tests.
- Name the statistic and apply the predeclared acceptance rule. A median of
  paired ratios is not a ratio of medians; a practical threshold is not a
  significance test. Below-threshold results do not establish equivalence.
- Same-code A/A variation can reveal unstable conditions, but does not explain
  away adverse A/B observations or justify subtracting "noise" from a result.
- For normalized scores, show the numerator and denominator and confirm
  comparability. A moving pyodbc denominator is a warning, not an automatic
  verdict at a fixed percentage. Uncontrolled cross-run raw times do not repair
  it; obtain a controlled comparison or state the attribution uncertainty.
- Keep per-workload regressions visible beside aggregate results. A favorable
  sum of disjoint timing windows neither cancels an adverse case nor measures
  full sequence elapsed time. A separately measured baseline is not a paired
  comparison just because both summaries contain medians.

## Organization and runtime evidence

- Cohesive new units belong in purpose-named headers such as `param_detect.hpp`,
  `py_type_cache.hpp`, or `py_ref.hpp`, not as another unrelated block in
  `ddbc_bindings.cpp`. Keep structural suggestions separate from perf defects.
- Make a pure relocation its own `REFACTOR` commit when implementing it. Do not
  mix a new policy or optimization into a claimed behavior-neutral extraction.
- Inspect the include graph. Forward declarations can break cycles when
  owning-type operations are defined out of line. Keep required template
  definitions or explicit instantiations available; forward declarations are
  not inherently invalid.
- A successful compile/link is not enough for a native extension. Import the
  rebuilt module and exercise relevant instantiations. A missing symbol can
  surface only at load time. Trace transitive includes before calling a missing
  direct include a current build failure; explicit dependency hygiene is separate.
- Use the existing [build](../../prompts/build-ddbc.prompt.md) and
  [test](../../prompts/run-tests.prompt.md) guidance and supported local tooling.
  Run targeted behavioral coverage; ownership/refcount changes also need broader
  regression coverage and lifetime checks such as `gc.collect()` and `weakref`.
  Neither replaces sanitizer/fault-injection evidence for claims requiring it.
- Exercise public operation sequences. Catalog methods may replace the handle
  and clear Python flags before the native wrapper runs. An internal test that
  sets those flags manually does not by itself prove a hidden public-API defect.
- Use short explicit pytest IDs for huge strings/bytes: pytest's current-test
  environment value can exceed Windows' 32,767-character limit before the
  product code runs.
- Keep tests focused on distinct contracts. Before consolidation or removal,
  map old cases to retained coverage and compare actual collected/executed
  identities, multiplicities, outcomes, and covered production lines. Equal
  totals or percentages can hide lost paths; renaming duplicate definitions can
  expose previously uncollected tests. Explain intentional changes and skips.
- Prefer connection-local temporary tables or unique run-owned objects. Clean
  only resources created by the repro; do not drop someone else's object to
  make a test green. Separate stale database state from a driver regression.
- Preserve failing command exit codes when piping logs. Report failures and
  isolated retries accurately instead of rewriting them as a clean full-suite
  run. Build success, import success, and runtime success are different evidence.
- A passing comparison PR does not establish the cause of a crash. A universal2
  build does not prove both architectures ran. State platform and fault-injection
  limits explicitly; do not turn an untested teardown theory into a standing rule.

## Report

Lead with the outcome and keep the report proportional to the change.

Label the evidence supporting each claim; these categories are complementary,
not interchangeable:

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| Source/mock checks | Inspected paths or simulated contracts | Native runtime safety, live SQL behavior, or latency |
| Native behavioral checks | Built/imported revision's exercised contracts | Untested failure paths or platforms |
| Profiling-enabled measurements | Observed calls and inclusive phase attribution | Shipped profiling-OFF speedup |
| Matched Release-OFF measurements | Scoped latency for the recorded workload and context | Universal improvement or absence of regressions |

Record a failure at the stage where it occurred. Establish whether setup,
fixtures, the harness, or the intended driver path failed, using the baseline
where relevant. A failed command alone does not identify a driver regression;
an unfinished required cleanup or preservation check is not a qualified run.
Retain partial measurements as explicitly limited evidence, leave unexecuted
phases UNRUN, and do not rewrite an earlier failure using a later successful
attempt. Merged status alone adds no missing runtime or performance qualification.

Bind CI evidence to the actual head/base or tested merge, build, job, attempt,
and configuration. Separate reruns at the same head; a later green run does not
diagnose an earlier failure. Distinguish generated artifacts from verified
publication, and reconcile stale check summaries with the underlying run.

- **Confirmed correctness/parity defects:** caller-visible impact, minimal
  reproducer and actual result, current file/line anchor, PR-versus-base status,
  and a concrete fix direction.
- **Performance observations:** named workload and eligibility, revisions,
  measurement mode, operation counts, timings, variability, and limitations.
- **Unverified concerns:** missing evidence and the smallest test that would
  settle the question. Never silently upgrade source inspection to runtime proof.
- **Structural/simplification suggestions:** what moves or disappears, what
  replaces it, why it remains safe, and whether equivalence was demonstrated.

Do not pile on an existing review thread or present naming/formatting preferences
as blockers. Distinguish fixed, disproved, pre-existing, and still-open findings.
Use plain, impact-first wording. Keep a top-level review summary short; put
technical evidence in the associated finding. If no blocker was established,
say that precisely rather than asserting that every platform/path is safe.

API references:
[SQLDisconnect](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldisconnect-function)
and [SQLFreeHandle](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfreehandle-function).
