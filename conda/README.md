# Combined Conda candidate

This recipe combines the matching code and ODBC wheels into **one `mssql-python`
Conda package**, including the required bulk-copy core. Pip instead installs
`mssql-python-odbc` as a separate companion distribution. Neither requires a
separately installed ODBC driver or driver manager.

Direct recipe builds must set `MSSQL_PYTHON_VERSION` to the exact selected code-wheel
version before rendering/building. The shared orchestrator derives and supplies it
automatically; omitted input fails recipe rendering instead of choosing a release.

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
the Windows ARM64 dependency profile includes Anaconda `defaults`. Run this existing
build workflow only in a disposable isolated installation: shared-environment
ownership hardening is outside this change. Publication/provenance tooling is proposed
in a separate release-additions PR, without a required merge order; see the
[release status and qualification caveats](../README.md#installation).
Neither this native-packaging change nor validate-only success authorizes production
publication.
