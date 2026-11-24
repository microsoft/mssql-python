# Testing native JSON with an authorized preview

Native JSON is an isolated development feature, not a replacement for the shipped
provider. Obtain a preview through your authorized distribution channel and verify
its supplied checksums. Do not commit, publish, or install preview binaries
system-wide. Do not change the repository's dependency pins or bundled binaries.

## Qualified artifacts and remaining release gates

The locally qualified development preview is **18.7.1.1**, not the shipped
18.6 provider. The actual Windows DLL reports `18.07.0001`. The tested artifacts
have these SHA-256 identities:

| Platform | Driver artifact | SHA-256 |
| --- | --- | --- |
| Windows x64 | `msodbcsql18.dll` | `e373fd476d97d4870c1adbac65d0590dd5d7624ddef43c0b7ae5d4711654f635` |
| Linux x64 | `libmsodbcsql-18.7.so.1.1` | `85a667eb8e237015d0de2691055ea4c0b0c9235262f68a27f8b7f501f20fe1d0` |

These identify tested binaries, **not a minimum supported release version**.
Earlier previews may lack the driver fix for chunked UTF-8-to-WCHAR reads at
non-BMP character boundaries. Obtain a build containing that fix and run the
Unicode/chunk-boundary tests below. macOS has not been qualified for this milestone;
do not infer macOS support from Linux results or the shared version number.
The provider owner must confirm the minimum supported build before release.

The repository's normal CI currently uses shipped providers and does not set
`MSSQL_TEST_NATIVE_JSON`; native JSON integration cases therefore skip there.
Local preview results are not CI coverage. A preview CI leg requires an
authorized, checksum-pinned artifact source, access controls that do not expose
development binaries to untrusted PRs, and SQL Server 2025. Once that runtime is
provisioned, set the flag and run `python -m pytest tests/test_json.py -q` using
the isolated runtime below. A configured native leg must fail on unsupported
metadata or execution errors, not silently skip them.

Release follow-ups remain: authorize and provision that CI leg, qualify the
platform/server matrix, and replace the production Linux loader's hard-coded
`.so.2.1` suffix with the actual shipping artifact contract before adopting 18.7.
The development symlink below is not that production fix.

Compatibility qualification also found a separate legacy-provider limitation:
with the shipped Windows driver `18.06.0002` and SQL Server `17.0.5005.3`,
`SELECT CAST(N'{}' AS JSON)` reports SQL type `12`, then fetching raises
`SQLGetData returned an invalid UTF-16 LOB source length`. The same failure
reproduces with a rebuilt pre-JSON main revision (`389a318a`); an ordinary
`VARCHAR(MAX)` control succeeds. This PR does not fix that legacy fetch path
or promise JSON text retrieval through it. The qualified preview reports
native `-157` metadata and passes the JSON retrieval tests.

Use an owned SQL Server 2025 instance (or a qualified Azure SQL database), a
development virtual environment, and the repository's
[build guide](../.github/prompts/build-ddbc.prompt.md). Set `DB_CONNECTION_STRING`
through your environment/secret mechanism; never put credentials in scripts or
command arguments. Do not add `Driver=`. `TrustServerCertificate=yes` is only
appropriate for an owned localhost development server.

## Prepare an isolated runtime

Use a disposable source copy containing the implementation you want to test;
do not use an older installed wheel. Keep that copy, the virtual environment,
and extracted preview outside your normal checkout. The paths below are local
variables, not committed configuration:

- `JSON_SOURCE`: disposable source copy, including `mssql_python`,
  `mssql_python_odbc/__init__.py`, tests, build files, and project configuration.
- `JSON_RUNTIME`: a separate directory containing a development-only
  `mssql_python_odbc` package.
- `JSON_RUN`: an empty working directory from which to invoke Python.

Start the runtime package by copying the existing provider package's
`__init__.py` and **matching platform/architecture** support-library/resource
layout into `JSON_RUNTIME/mssql_python_odbc`. Overlay only the authorized
preview's matching driver and resources there. Preserve companion dependencies:
the Windows JSON preview does not supply the required `mssql-auth.dll`.

### Windows x64

The development runtime layout is:

```text
JSON_RUNTIME/
  mssql_python_odbc/
    __init__.py
    libs/windows/x64/
      msodbcsql18.dll
      mssql-auth.dll
      1033/msodbcsqlr18.rll
      ...existing support files...
```

Overlay the preview `msodbcsql18.dll` and its matching resources; do not install
its MSI. Build `JSON_SOURCE\mssql_python\pybind\build.bat x64` using the intended
venv's Python on `PATH`. The provider carrier's package version can remain the
shipped version on Windows: **it does not establish which DLL is loaded**.

### Linux

Extract the authorized package without installing it, for example
`dpkg-deb -x <authorized.deb> <temporary-extraction-directory>`. Copy the
extracted driver and resources into the matching runtime layout:

```text
JSON_RUNTIME/
  mssql_python_odbc/
    __init__.py
    libs/linux/debian_ubuntu/x86_64/
      lib/
        libmsodbcsql-18.7.so.1.1
        libodbcinst.so.2
      share/resources/en_US/msodbcsqlr18.rll
```

Use `arm64` rather than `x86_64` for ARM64, and the correct distro directory
(`rhel`, `suse`, or `alpine`) and matching package for those distributions.
Check the extracted package's actual filenames rather than assuming this example.

Unlike Windows, the native Linux loader embeds the driver's major/minor filename
at **build time**. For the 18.7 preview, change only the disposable source copy's
`mssql_python_odbc/__init__.py` version literal to `18.7.1.1` before building.
Do not make this change in the implementation branch. Build the disposable copy
using `bash build.sh` from `mssql_python/pybind`.

The tested 18.7 preview ELF ends in `.so.1.1`, while this source loader expects
`.so.2.1`. In the **external development runtime only**, create an explicit
relative symlink:

```sh
cd "$JSON_RUNTIME/mssql_python_odbc/libs/linux/debian_ubuntu/x86_64/lib"
ln -s libmsodbcsql-18.7.so.1.1 libmsodbcsql-18.7.so.2.1
ldd libmsodbcsql-18.7.so.1.1
```

This is a test-runtime alias, not a production compatibility claim or loader
fallback. Record both the resolved path and checksum. Do not disguise an 18.7
library under an 18.6 filename. Resolve missing shared-library dependencies in
the disposable environment before testing; never ignore an `ldd` failure.

## Select, verify, then test

Set `MSSQL_PYTHON_NATIVE_PROVIDER=msodbcsql18` and
`MSSQL_TEST_NATIVE_JSON=1`. Set `PYTHONPATH` to `JSON_RUNTIME` **first**, followed
by `JSON_SOURCE` (semicolon-separated on Windows, colon-separated on Linux).
Run from `JSON_RUN`, not from a checkout containing another provider package.
This keeps subprocess tests on the same runtime.
Install the project's pinned `mssql-python-rs` dependency in the disposable
environment as well: the bulk-copy rejection regressions exercise the actual
core, not a substitute or an older preinstalled package.

Using the selected venv's Python, verify provenance and native metadata before
running tests:

```python
import hashlib
import os
from pathlib import Path

import mssql_python
from mssql_python import Json, SQL_JSON, connect, ddbc_bindings

print("Python package:", mssql_python.__file__)
print("Native extension:", ddbc_bindings.module.__file__)
with connect(os.environ["DB_CONNECTION_STRING"]) as connection:
    info = mssql_python.get_native_provider_info()
    library = Path(info["driver_path"]).resolve(strict=True)
    print("Provider:", info["id"])
    print("Carrier package version:", info["version"])
    print("Actual driver version:", connection.getinfo(7))
    print("Actual driver path:", library)
    print("Actual driver SHA256:", hashlib.sha256(library.read_bytes()).hexdigest())
    with connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute("SELECT ? AS document", Json({"items": [1, None]}))
        assert cursor._column_sql_types == [SQL_JSON]  # Qualification-only metadata check
        assert cursor.fetchval() == {"items": [1, None]}
        cursor.execute("SELECT ? AS document", Json(' {"items":[1,null]} '))
        assert cursor._column_sql_types == [SQL_JSON]
        assert cursor.fetchval() == {"items": [1, None]}
```

Compare the paths, actual driver version, and SHA256 with your authorized inputs.
Do not rely on the carrier package version or `getTypeInfo()` alone.
`VectorTypeSupport` is not required for native JSON.

The serialized-text case above must still report native JSON metadata, not
NVARCHAR. `Json(str)` preserves already-serialized text without Python JSON
parsing/serialization; syntax and object/array-root errors surface at execution.
Do not treat constructor success as server validation or a guarantee of
byte-identical storage/results. See the [API contract](../README.md#native-json-preview)
for duplicate-key, numeric, transaction, and re-insertion caveats.

Then run (substitute your actual `JSON_SOURCE` path):

```text
python -m pytest <JSON_SOURCE>/tests/test_json.py -q -ra --tb=short
```

The `json_connection` fixture skips native success cases when
`MSSQL_TEST_NATIVE_JSON` is not enabled.
Once enabled, missing credentials, connection errors, unsupported metadata, and
provider failures must fail the run, not be reclassified as skips.
The separate unsupported-bind regressions use the ordinary database fixture and
run against the shipped classic driver with this flag unset, including in normal
CI. They cover scalar and array-bound input, original `HY004` diagnostics, no
writes, and same-statement cursor reuse. They intentionally skip on preview legs
and alternate providers, whose Python-level rejection is tested separately.

Record Python/OS/architecture, source revision plus dirty changes, native build
flags and binary hash, actual driver provenance, server version, exact command,
and results. Release timings require profiling compiled out; sanitizer and fault
builds are separate evidence. ARM64 under emulation is not physical ARM64 testing.
No Windows result qualifies Linux/macOS, and no build-only result qualifies a
runtime.

When finished, remove only the temporary files/resources you created. Never
publish the preview runtime, test credentials, or private download locations.
Arrow JSON, Rust/async, bulk-copy adapters, and TVPs remain outside this milestone.
