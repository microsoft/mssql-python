# mssql-python — Copilot instructions

mssql-python is Microsoft's pip-installable Python driver for SQL Server, Azure SQL, and Azure Synapse. It is DB API 2.0 (PEP 249) compliant. The core is a C++ pybind11 native extension (`ddbc_bindings`) built with CMake and wrapped by a pure-Python package; bulk copy is backed by a separate Rust/TDS core (`mssql_py_core`). Platform-specific wheels ship for Windows (x64, ARM64), macOS (x64, ARM64), and Linux (x64, ARM64). Python 3.10+. The ODBC driver is bundled in the wheel — no external driver manager is required.

## Architecture

Call stack, top to bottom:

1. **Pure-Python API** — `mssql_python/` (`connection.py`, `cursor.py`, `row.py`, `pooling.py`, `auth.py`, `exceptions.py`, type coercion). This is the DB API 2.0 surface.
2. **Extension loader** — `mssql_python/ddbc_bindings.py` detects platform/architecture and loads the correct native binary.
3. **pybind11 binding layer** — `mssql_python/pybind/` (`ddbc_bindings.cpp`/`.h`, `connection/`, `logger_bridge.*`, `unix_utils.*`), compiled to `ddbc_bindings.cp{ver}-{arch}.{so|pyd}` in `mssql_python/`.
4. **ODBC driver stack → SQL Server.** Bulk copy takes a different path: it uses `mssql_py_core` (TDS) directly, not ODBC.

Paths you will touch:

- `mssql_python/pybind/CMakeLists.txt` — all platform/architecture build conditionals live here.
- `mssql_python/pybind/build.sh` / `build.bat` — build entry points; `configure_dylibs.sh` fixes macOS dylib paths.
- `mssql_python/libs/{windows,macos,linux}/...` — prebuilt ODBC binaries. **NEVER hand-edit these.**
- `setup.py` / `pyproject.toml` — packaging and wheel/platform tagging.
- `mssql_python/mssql_python.pyi` + `mssql_python/py.typed` — PEP 561 type stubs. Update stubs when the public API changes.
- `tests/` — mostly-numbered `test_NNN_*.py` files (a live-SQL-Server integration suite plus no-DB dependency checks).

## Development workflow

Validated, step-by-step guides live in `.github/prompts/` — use them instead of rediscovering commands:

- `setup-dev-env.prompt.md` — venv, dependencies, ODBC headers, `DB_CONNECTION_STRING`, SQL Server.
- `build-ddbc.prompt.md` — build the C++ extension.
- `run-tests.prompt.md` — pytest categories and markers.
- `create-pr.prompt.md` — the PR flow (title / issue-link / summary confirmations).

Core facts:

- **ALWAYS build the native extension before running Python tests.** Importing `mssql_python` needs the compiled `.so`/`.pyd`. Build with `cd mssql_python/pybind && ./build.sh` (or `build.bat` on Windows; pass `x64|arm64` as needed).
- **Tests need a live SQL Server** reachable through the `DB_CONNECTION_STRING` env var. `tests/test_000_dependencies.py` runs with no DB; most others require one.
- macOS wheels are universal2 (arm64 and x86_64); when touching dylib/rpath setup (`configure_dylibs.sh`), make sure both slices are handled, not just the build host.

## Validation gate (run before you finish — this mirrors CI)

```bash
black --check --line-length=100 mssql_python/ tests/     # BLOCKING in CI
python -m pytest -v                                       # 'stress' marker excluded by default
```

- **`pr-format-check` (BLOCKING):** PR title must start with one of `FEAT: FIX: DOC: CHORE: STYLE: REFACTOR: RELEASE:`; the body must link a work item/issue and have a `### Summary` of at least 10 characters.
- `flake8`, `pylint`, `mypy`, `clang-format`, and `cpplint` run but are **informational**, not blocking.
- The authoritative cross-platform validation runs on **Azure DevOps** (broader OS / Python / arch coverage than the GitHub checks); consult the specific pipeline in `eng/pipelines/` for the exact matrix rather than assuming full coverage. A coverage bot posts a report comment on the PR.

## Code standards

**Python**

- Black, line length 100. DB API 2.0 semantics take priority.
- Catch specific exceptions (`DatabaseError`, `IntegrityError`, `OperationalError`, …), **never a bare `except:`** — flake8 ignores E722, but the team enforces this in review.
- Use context managers for connections and cursors. Update `__all__` **and** `mssql_python/mssql_python.pyi` when changing the public API. Add a `tests/test_*.py` case for every fix.

**C++ / pybind11** (`mssql_python/pybind/`) — hazards to respect:

- **Process-shutdown ordering for Python handles.** Destructors of static `py::object` caches (e.g. the datetime/decimal class cache in `ddbc_bindings.cpp`) run after Python finalizes; an ODBC or threading call from a destructor after the GIL is gone can deadlock or crash the whole test suite at exit. Guard shutdown cleanup with `Py_IsInitialized()`, register cleanup via `atexit`, and prefer not to add new static Python handles.
- **Don't let a failed initialization escape as a half-built object.** `Connection::setAttribute` returns `SQLRETURN`; `applyAttrsBefore` checks it and raises via `ThrowStdException` before the connection is exposed. Follow that pattern — translate a failing return into an exception rather than handing back a partially-initialized object.
- **Handle every shipped architecture, not just `$(uname -m)`.** Universal2 bundles arm64 and x86_64; dylib/rpath work that only touches the host arch ships a broken slice for the other. Verify both.
- Hot paths favor the raw CPython API (with correct refcounting and `PyErr_Occurred()` checks) over pybind11 per-cell calls.

## Testing conventions

- Test files are mostly numbered `test_NNN_*.py`; `tests/test_000_dependencies.py` runs without a DB, most others need a live SQL Server. `-m "not stress"` is the default.
- Run segfault-prone or ODBC/pool global-state tests in an **isolated subprocess** so a crash or shared state cannot poison the rest of the suite.
- **Assert the contract, not just the output.** If a change's value is "we now call X once," assert the call/round-trip count — a correctness-only test won't catch a perf regression.
- For global type-mapping changes, add typed-NULL integration cases (VARBINARY, UNIQUEIDENTIFIER, XML, DECIMAL, stored-proc params) before applying the optimization broadly.

## Security and credentials

- **Committed connection strings that contain `UID`/`PWD` must use `SERVER=localhost` (or `127.0.0.1`) with dummy values.** Real remote or Azure credentials come only from secrets or the `DB_CONNECTION_STRING` env var, and are never committed. Automated credential scanning (see `.config/CredScanSuppressions.json`, `.gdn/`) can block unsafe patterns.
- Do **not** put `Driver=` in a connection string — the bundled driver is selected automatically.
- `TrustServerCertificate=yes` is local-development only; never suggest it in remote or production examples.

## Contributing

- Branch naming: `<name>/<short-kebab-description>` (e.g. `bewithgaurav/fix-656-macos-dylib`).
- Link exactly one reference in the PR body: maintainers use `AB#<id>` (ADO auto-close); external contributors use plain `#<N>` — **never `Closes #N`**.
- Stage specific files; **never `git add .`**. Never commit build artifacts (`*.so`, `*.pyd`, `*.dll`, `*.dylib`) or a virtual environment.
- Keep changes surgical — fix the requested thing, leave unrelated code alone, and do not add stray files.

## Working effectively in this repo

- **Reproduce before you claim.** Build and run a live repro against a real SQL Server before asserting a bug exists or that a fix works. Do not ship code-only assessments.
- Understand the linked issue and existing review threads before changing code.
- Search existing issues and PRs before opening a new one — do not create duplicates.
- Do not post PR or issue comments as part of a change unless explicitly asked.
- Verify that mutating git/gh operations actually landed (re-read the branch or PR) before reporting done.

## Trust these instructions

Trust the commands, paths, and conventions above — they were validated against the current repository. Only fall back to searching the codebase when something here is missing or proves incorrect, and when you find a gap, update these instructions.
