# Combined Conda candidate

This recipe combines matching binding, ODBC and RS wheels into **one `mssql-python`
Conda package**. The RS distribution owns the required `mssql_py_core` package and
its private native libraries; the binding wheel must not own them. Pip installs
`mssql-python-odbc` and `mssql-python-rs` as separate companion distributions.
Neither installation requires a separately installed ODBC driver or driver manager.

Direct recipe builds must set `MSSQL_PYTHON_VERSION` to the exact selected code-wheel
version before rendering/building. The shared orchestrator derives and supplies it
automatically; omitted input fails recipe rendering instead of choosing a release.
Both native installation and cross extraction require the bulk-copy initializer and
a compatible extension filename. The separate native audits still validate binary headers.
For RS-dependent bindings, the orchestrator also supplies the derived `MSSQL_RS_VERSION`
and a validated `rs-wheel-cp<minor>.txt` selection in `WHEELS_DIR`. Recipes install or
extract that entire wheel, including private libraries and distribution metadata, before
checking the required core. The selection also supports compatible stable-ABI wheels
without retagging them or executing a cross-target interpreter.
The same selection bytes are retained inside the installed RS `.dist-info` directory
as `conda-wheel-source.txt`: one wheel basename followed by a newline. This identifies
the raw wheel entry in the producer transport receipt; it does not equate raw-wheel
hashes with the final Conda archive hash after Linux relocation.

Use `build --rs-wheel-dir <producer-artifact>/rs-wheels
--rs-version-file <producer-artifact>/dist/mssql-python-rs.version` for RS inputs.
The version file asserts the selected producer's source contract, not a new release-version
setting. The distinct NuGet transport pin remains in `mssql-python-rs-nuget.version`;
the downloader records its content hash and individual wheel hashes in `transport.json`.
Conda consumers use staged producer bytes, not a fresh download using their checkout's pin.

Published historical bindings that declare no RS dependency are accepted only when their
payload and RECORD own the core initializer and native extension. They produce an explicit
**historical embedded-core / NOT current-source RS qualification** diagnostic. A supplied
RS source assertion, malformed RS declaration, or missing required RS wheel cannot fall back
to that profile. Equal binding version strings do not establish equal payload contracts.

This is a temporary candidate, not an announcement of public channel availability.
Obtain the exact candidate archive/channel from its owner and install into a new
Conda environment. Activate it and select the same interpreter/kernel in your IDE
or notebook. Application imports and the public API remain unchanged. Use Conda
for upgrades; do not overwrite Conda-owned driver files with pip.

Linux requires **glibc >=2.34** for the complete native payload, including the
bulk-copy core, even when the binding wheel has a lower platform tag. Do not force
installation on older glibc. This does not change the separate PyPI support claim.
The current RS transport's `manylinux_2_34` inputs link OpenSSL 3 and match the existing
`openssl >=3,<4` dependency; its `manylinux_2_28` inputs link incompatible OpenSSL 1.1.
The RS core and private Linux driver receive their own exact relative RPATH climbs to
the environment's `lib` directory. They are not ODBC Driver 18 distro trees and are not
deduplicated with the separate ODBC wheel's payload.
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
in a separate release-additions PR that follows this native/tooling foundation; see the
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
Release matrix and Python-admissibility policy live in `release.py`, recorded
Azure DevOps source checks in `provenance.py`, source-bound component inputs and
public wheel fetching in `inputs.py`, and staged publication/recovery in `publication.py`. Both release
and native auditing use `archive.py`; release validation retains its stricter
container and index rules rather than weakening them to the generic audit policy.

Run the module commands from the **repository root**, followed by the existing
build or audit arguments:

```text
python -m eng.conda_tools build --help
python -m eng.conda_tools elf --root <package-directory>
python -m eng.conda_tools pe --root <package-directory> --subdir win-arm64
python -m eng.conda_tools macho --root <package-directory> --subdir osx-arm64
python -m eng.conda_tools validate --root <package-directory>
python -m eng.conda_tools provenance
python -m eng.conda_tools promote --help
python -m eng.conda_tools fetch-wheels --help
python -m eng.conda_tools probe-driver
```

These replace the old standalone audit, build, and release scripts. They require the source
checkout; the internal tools are not installed in the driver wheel. If Python
reports `No module named 'eng'`, run from the checkout root rather than an installed
driver environment. Build callers set the tooling subprocess's working directory
explicitly, without changing `PYTHONPATH` or the parent working directory.
Static audits do not import the driver. Runtime verification remains in separate
processes from a neutral working directory, with the required core loaded
independently before API probes.

`provenance` uses the existing release pipeline environment and read-only job-token
access. `promote --check-local-only` needs neither a publishing token nor an
Anaconda client; actual publication and recovery retain their existing restricted
credential and single-operator prerequisites. Module placement is not a new
authorization boundary. The PowerShell upload process deadlines, attempted-file
tracking, process-tree termination, and dry-run plan remain in their pipeline tasks.

The release pipeline resolves AUTO from the **recorded upstream wheel commit**, not
the release checkout or a latest-version lookup. It cross-checks binding setup/runtime
versions and reads ODBC and, when explicitly required by that source, RS distribution
versions. RS's NuGet transport pin is separate. Missing or ambiguous source declarations
fail rather than select a historical profile. A supplied binding version is only an
assertion against this resolved version.

Both readiness and the credentialed job use `validate --release-versions` to read the
verified `RELEASE_VERSIONS` JSON from the environment; `RS_TRANSPORT_VERSION` supplies
the separate transport assertion. This avoids passing JSON quoting or empty arguments
through Windows PowerShell. Explicit JSON may also follow `--release-versions` in
source-only controls. With this flag, absent versions fail closed. Without it, `validate`
remains the metadata/matrix-only inspection command and makes no source-input claim.
The source-bound gate checks each installed component's METADATA and RECORD ownership,
the binding's exact dependency pins, and the selected RS filename/WHEEL tags against
`rs-transport.json`. Raw wheel/transport hashes remain input evidence; relocated
installed native files are checked by the native audits, not falsely compared with raw
wheel hashes. No numeric RS build ID is inferred from a transport-version suffix.

`fetch-wheels` uses exact current maintained versions, hash-required binary-only PyPI
downloads, and actual wheel metadata/ownership. An authentic published embedded-core
binding is explicitly reported as a **historical published packaging control, not
current-source RS qualification**. An RS-dependent published binding requires its exact
provider and source pin; unavailable releases, malformed declarations, or mismatches
fail with no older-version or TLS fallback. Public availability does not gate the
recorded ADO artifact path. Neither a public input check nor static archive checks
establish installed SQL/runtime qualification.

The driver-load probe's sole implementation is
`eng/conda_tools/driver_load_probe.py`. It separates native connection execution,
pure outcome classification and command reporting with typed interfaces.
Verification runs that file by absolute path with the target environment's Python.
Its standard-library-only bootstrap does not require `eng` to be installed there,
so the tooling namespace does not weaken installed-package isolation.
The `probe-driver` module command is a convenience diagnostic in the calling
interpreter, not a substitute for this neutral-directory installed-package check.

The files remaining under `conda/` are the conventional recipe entrypoints,
metadata, documentation and line-ending configuration. Shared NuGet transport,
feed-resolution and output-safety utilities remain under `eng/scripts` because
non-Conda native build/development workflows also use them; Conda reuses those
implementations rather than keeping private copies.
