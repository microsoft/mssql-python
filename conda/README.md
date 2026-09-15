# Combined Conda candidate

This recipe combines the matching code and ODBC wheels into **one `mssql-python`
Conda package**, including the required bulk-copy core. Pip instead installs
`mssql-python-odbc` as a separate companion distribution. Neither requires a
separately installed ODBC driver or driver manager.

Direct recipe builds must set `MSSQL_PYTHON_VERSION` to the exact selected code-wheel
version before rendering/building. The shared orchestrator derives and supplies it
automatically; omitted input fails recipe rendering instead of choosing a release.
Both native installation and cross extraction require the bulk-copy initializer and
a compatible extension filename. The separate native audits still validate binary headers.

This is a temporary candidate, not an announcement of public channel availability.
Obtain the exact candidate archive/channel from its owner and install into a new
Conda environment. Activate it and select the same interpreter/kernel in your IDE
or notebook. Application imports and the public API remain unchanged. Use Conda
for upgrades; do not overwrite Conda-owned driver files with pip.

Linux requires **glibc >=2.34** for the complete native payload, including the
bulk-copy core, even when the binding wheel has a lower platform tag. Do not force
installation on older glibc. This does not change the separate PyPI support claim.
macOS retains its external Homebrew/MacPorts OpenSSL prerequisite for encryption;
Conda OpenSSL alone does not satisfy the driver's system-path lookup.

The release goal is the matching PyPI release's public API and supported feature
behavior on all 28 ordinary CPython variants: 3.10-3.14 on win-64, linux-64,
linux-aarch64, osx-64 and osx-arm64; 3.12-3.14 on win-arm64. No silent removal of
required native functionality is acceptable. Optional features require their
corresponding dependencies; Windows ARM64 PyArrow availability remains a blocker
to qualifying those features, not permission to drop them.

These guards are **not full-matrix parity certification**. Native target execution,
SQL, certificate-verified TLS, authentication, bulk-copy and optional-feature tests
remain required before release. Cross-build/static checks and a DB-less driver
load do not establish those results. Applicable OS, certificate and authentication
configuration remain external prerequisites.

Use only organizationally approved channels and handle applicable terms separately;
the Windows ARM64 dependency profile includes Anaconda `defaults`. Before unattended
Windows ARM64 builds, agent owners must use approved provisioning to put a disposable
Conda installation on `PATH`, with applicable channel terms handled for the job's
execution identity. The orchestrator's automatic Miniforge installation does not
establish that approval. If Conda enforces terms that have not been handled, the build
or verification solve stops with Conda's diagnostic; provision the prerequisite before
running again. Automatic acceptance, including an inherited
`CONDA_PLUGINS_AUTO_ACCEPT_TOS` opt-in, remains disabled.

Run this existing build workflow only in a disposable isolated installation: shared-environment
ownership hardening is outside this change. Publication/provenance tooling is proposed
in a separate release-additions PR, without a required merge order; see the
[release status and qualification caveats](../README.md#installation).
Neither this native-packaging change nor validate-only success authorizes production
publication.

## Maintaining the build tools

The internal `eng/conda_tools` package separates archive I/O (`archive.py`), native
compatibility policy (`contracts.py`), binary facts (`formats/elf.py`, `pe.py`, and
`macho.py`), and the shared audit lifecycle (`audit.py`). Command arguments and
human-readable reporting live in `__main__.py`. Conda provisioning and process
execution live in `environment.py`; `build.py` sequences wheel selection, building,
auditing and staging; `verify.py` keeps installed-package probes isolated.

Run the module commands from the **repository root**, followed by the existing
build or audit arguments:

```text
python -m eng.conda_tools build --help
python -m eng.conda_tools elf --root <package-directory>
python -m eng.conda_tools pe --root <package-directory> --subdir win-arm64
python -m eng.conda_tools macho --root <package-directory> --subdir osx-arm64
```

These replace the old standalone audit and build scripts. They require the source
checkout; the internal tools are not installed in the driver wheel. If Python
reports `No module named 'eng'`, run from the checkout root rather than an installed
driver environment. Build callers set the tooling subprocess's working directory
explicitly, without changing `PYTHONPATH` or the parent working directory.
Static audits do not import the driver. Runtime verification remains in separate
processes from a neutral working directory, with the required core loaded
independently before API probes.
