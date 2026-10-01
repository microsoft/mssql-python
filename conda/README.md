# Combined Conda candidate

This recipe combines matching binding, ODBC and RS wheels into **one `mssql-python`
Conda package**. The RS distribution owns the required `mssql_py_core` package and
its private native libraries; the binding wheel must not own them. Pip installs
`mssql-python-odbc` and `mssql-python-rs` as separate companion distributions.
Neither installation requires a separately installed ODBC driver or driver manager.

For consolidated wheel artifacts, the builder first selects bindings matching the
effective target platform and `--python-versions` (or auto-detects versions from
target-compatible bindings). Other Python/platform inputs are ignored before metadata
and RS dependency checks. Selected inputs still require matching versions, ownership,
tags and native compatibility; missing required RS wheels remain fatal.
The builder validates raw wheel core ownership before staging or recipe extraction,
including unrecorded files and wheel spread-path/case aliases. ODBC never supplies
`mssql_py_core`; the selected RS provider (or historical binding) must own every core
file in RECORD. Direct recipe callers must use these validated wheel inputs.
Public and staged wheels also require one canonical root METADATA member, with no
extra nested or aliased metadata. Package audits require binding metadata and exactly
one RECORD for each installed distribution; missing or duplicate ownership records fail.
Low-level binary-format parsers remain independent of package metadata.
These checks inspect all actual METADATA/RECORD/WHEEL candidates before accepting
installed ownership, including case, backslash and normalized-path aliases that a
canonical-only metadata collector would omit. Aliased or orphan entries are rejected,
not normalized into an accepted package.

Archive readers stream ZIP components through the selected zstandard backend (or
stream legacy `.tar.bz2` files) instead of materializing complete TAR components.
Individual files still return bounded byte buffers for the native parsers. Fixed
limits fail explicitly; there is no command-line or environment override:

| Processing budget | Limit |
| --- | --- |
| Archive file / ZIP component bytes | 256 MiB |
| ZIP central-directory read / ZIP entries | 1 MiB / 10,000 |
| Expanded TAR stream, including padding | 1 GiB |
| Cumulative TAR member bytes | 512 MiB |
| Individual member / metadata member bytes | 128 MiB / 8 MiB |
| Cumulative TAR metadata / TAR headers | 64 MiB / 10,000 |
| Zstandard decoder window / frames per component | 64 MiB / 10,000 |

Checks apply before member allocation and while consuming 64 KiB chunks, including
frames without a declared expanded size. The third-party zstandard reader also
receives a bounded frame-structure pass because its streaming API alone accepts
truncated frame endings. ZIP envelopes must use stored or deflate compression;
sparse TAR extensions are rejected before sparse-map processing. Wheel metadata
reads share the ZIP and metadata budgets. These are package-reader limits, not a
claim that NuGet transport downloads or total process memory have the same bounds.

The GitHub audit job installs its complete Linux x64 / CPython 3.11 test-tool closure
from `requirements-audit.txt` with `--require-hashes --only-binary=:all:`.
`requirements-audit.in` records the reviewed direct pins and regeneration commands.
This audit-only lock is separate from the Windows publisher lock; it does not pin
the hosted runner image or every input to the later Conda build.

The `conda-audit` PR gate triggers only for `conda/`, `eng/conda_tools/`, Conda-specific
OneBranch pipelines/steps/jobs, their native/probe/archive/provenance and release tests,
and the workflow itself. General package metadata/version changes and shared wheel-build
infrastructure do not trigger it on their own; mixed PRs with a Conda change still do.
This scope does not change the unit-test runner, source-version wheel selection, or
failure handling when a required wheel is unpublished.

Run the source-only trigger regressions without a native build, SQL Server, or PyYAML:

```text
python -m pytest --noconftest tests/test_027_conda_release_metadata.py -k conda_audit -q
```

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
ownership hardening is outside this change. Publication/provenance tooling is included
alongside this native/tooling foundation; see the
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

Each `.conda` component reader requires exactly one matching `info-*.tar.zst` or
`pkg-*.tar.zst` entry. Missing components and additional matches, including duplicate
ZIP entries with the same name, are rejected before decompression.

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
Anaconda client; actual publication and recovery require restricted credentials and
the shared protected release stage described below. Module placement is not a new
authorization boundary. The PowerShell upload process deadlines, attempted-file
tracking, process-tree termination, and dry-run plan remain in their pipeline tasks.
Network publication and recovery require an explicit nonblank `ANACONDA_API_TOKEN`,
not an ambient or cached login. The publishing task removes `BINSTAR_API_TOKEN`
only from its process and children so the CLI uploader uses the same reviewed credential.
User/machine environment settings and saved credentials are not modified.

Only trusted, reviewed release revisions may be queued, including validate-only feature
branches: their YAML and Python run with `System.AccessToken`. YAML provenance checks
cannot sandbox an untrusted queued revision or protect that token from it. Administrators
must restrict pipeline editing/queueing and source access, minimize the job identity's
permissions, and separately protect publishing credentials. A dry run omits the publishing
credential group; it is not token-free execution.

Before authorizing production use of `Anaconda Publishing`, its resource owner must
configure an ADO [Branch control check](https://learn.microsoft.com/en-us/azure/devops/pipelines/process/approvals?view=azure-devops#branch-control)
on that variable group in **Pipelines > Library > Approvals and checks**:
allow only `refs/heads/main`, require branch protection, and fail when protection
cannot be verified. This resource-side check applies to every consuming stage and
pipeline, including the branches of linked producer runs; feature-branch publication
must fail before the stage receives the publishing credential. Preserve existing
permissions and approvals, and restrict permission to administer or bypass the check.
Do not grant broader pipeline access as part of configuring it.

The existing `publishToConda` condition omits this group for validate-only runs, so
trusted feature-branch validation remains separate from credentialed publication.
Neither a checkout-local branch check nor a secondary pinned checkout can replace
the resource-side control: editable YAML could bypass either. These external checks
are not installed by this repository; production must remain unapproved until the
resource owner verifies their configuration and enforcement. Branch control does
not provide the separate exclusive publication lock.

### Publication serialization

The resource owner must also enable the native **Exclusive lock** check on the same
`Anaconda Publishing` variable group. Production selects `lockBehavior: sequential`
on the enclosing `CondaRelease` stage; this setting orders waiting runs but does not
create the external check. The lock spans validation, staged uploads, the initial
label snapshot, promotion, rollback and staging cleanup until the stage ends.
All publishers and recovery paths for `microsoft/mssql-python` must consume that
same resource, across versions and target labels; separate groups would not serialize
them. Keep the group and lock behavior absent from validate-only runs.

Recover by rerunning the original protected release stage with its original artifact
inputs, preserving the build-specific staging label and reacquiring the shared lock.
Do not run mutating `promote` or `--cleanup-staging` commands outside that protected
stage: the Python helpers do not acquire an ADO resource lock themselves.
After uncertain writer termination, first confirm the writer has stopped; a native
lock does not terminate orphaned processes or repair interrupted remote operations.
Inspect check configuration and compiled stage wiring separately from run-time lock
acquisition; configuration readback alone is not evidence that a run holds the lock.

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
Release and native auditing also check all installed core paths, not only RECORD-filtered
members. This detects unowned or cross-owned additions; it cannot recover file origin
after an overwrite of an already owned path, which is why the raw pre-extraction gate
is required.

`fetch-wheels` uses exact current maintained versions, hash-required binary-only PyPI
downloads, and actual wheel metadata/ownership. Pip selects for its executing interpreter;
the requested Python/platform must match the binding, ODBC and selected RS wheel tags
before the command accepts the inputs. These arguments validate the download, rather
than enabling cross-target pip resolution. An authentic published embedded-core
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
