---
name: code-review
description: "General repository code review for microsoft/mssql-python, based solely on .github/copilot-instructions.md. Use when reviewing pull requests or diffs across the Python API, native bindings, packaging, tests, CI, or documentation. Apply the repository's existing architecture, correctness, compatibility, testing, and credential-handling expectations without replacing normal Copilot review."
---

# Repository code review

The sole source of repository-specific rules for this skill is
[copilot-instructions.md](../../copilot-instructions.md).
Read that file before reviewing. The checklist below organizes its guidance
for review; it does not introduce personal Scout policies or a performance-only
review methodology. If this summary drifts, use the source instructions.

## Apply the instructions to the changed area

1. **Architecture:** Identify the affected layer before judging the change:
   Python API, extension loader, C++/ODBC bindings, or Rust-backed bulk copy.
   Bulk copy uses `mssql_py_core` and TDS rather than the ODBC path.
2. **Python API:** Preserve DB API 2.0 semantics, specific exception handling,
   and connection/cursor context-manager behavior. For public API changes,
   check that `__all__` and the `mssql_python.pyi` stubs stay consistent.
3. **Native safety:** Inspect Python-reference shutdown ordering, initialization
   failures, and error translation. A failed initialization must not expose a
   half-built object. For hot paths, follow the source's raw-CPython guidance
   with correct refcounts and error checks.
4. **Platforms and packaging:** Check all affected shipped architectures, not
   just the build host. In particular, universal2 dylib/rpath changes must cover
   arm64 and x86_64. Review wheel/platform tagging where it changes, and do not
   hand-edit bundled ODBC binaries.
5. **Tests:** Check that fixes have regression coverage. Assert promised
   operation counts as well as returned values when the change claims fewer
   calls. Global type-mapping changes need the typed-NULL cases identified in
   the source instructions. Keep crash-prone and global-state cases in isolated
   subprocesses.
6. **Credentials and examples:** Reject committed real credentials. Connection
   examples with `UID`/`PWD` use localhost and dummy values. Do not add `Driver=`;
   the bundled driver is selected automatically. Treat
   `TrustServerCertificate=yes` as local-development only.
7. **Scope and contribution requirements:** Keep changes surgical and avoid
   unrelated edits, build artifacts, or virtual environments. Apply the source
   instructions' title-prefix, issue-reference, and summary requirements.
8. **Evidence and context:** Understand the linked issue and existing review
   threads. Reproduce before asserting a driver bug or fix. Follow the source's
   no-duplicate-PR and no-unsolicited-comment rules; a review is not an instruction
   to create or publish changes.

## Validation guidance

Use the setup, native-build, test, and PR guides identified in
[Development workflow](../../copilot-instructions.md#development-workflow).
Build the native extension before running Python tests. Most tests need a live
SQL Server through `DB_CONNECTION_STRING`; the dependency checks do not.

Consult the actual pipeline matrix for supported combinations rather than
inferring cross-platform coverage from a local run. Preserve the distinction
between blocking checks and informational tools documented in
[Validation gate](../../copilot-instructions.md#validation-gate-run-before-you-finish--this-mirrors-ci).
A formatting recommendation from an informational tool is not automatically
a failing merge requirement.

Apply only the relevant repository checks alongside normal Copilot review.
Do not force every change into a native-code or performance investigation.
