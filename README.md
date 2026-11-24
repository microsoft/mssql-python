# Microsoft Python Driver for SQL Server
 
**mssql-python** is a Python driver for Microsoft SQL Server and the Azure SQL family of databases. It leverages Direct Database Connectivity (DDBC) that enables direct connections to SQL Server without requiring an external driver manager. Designed to comply with the [DB API 2.0](https://peps.python.org/pep-0249/) specification, this driver also introduces Pythonic enhancements for improved usability and functionality. It supports a full range of database operations, including connection management, query execution, and transaction handling.
 
The driver is compatible with all the Python versions >= 3.10
 
[Documentation](https://github.com/microsoft/mssql-python/wiki) | [Release Notes](https://github.com/microsoft/mssql-python/releases) | [Roadmap](https://github.com/microsoft/mssql-python/blob/main/ROADMAP.md)
 
> **Note:**
> This project is now Generally Available (GA) and ready for production use. We’ve completed core functionality and incorporated feedback from the preview phase.
> 
> **Important Note:**
>
> ### ODBC Driver Distribution
> For **pip/PyPI installations**, the ODBC driver binaries used by `mssql-python` are distributed through a dedicated companion distribution:
>
> - Package: `mssql-python-odbc`
> - Import name: `mssql_python_odbc`
> - Source dependency version (release preparation): **18.6.2.2**
>
> Builds from this source pin `mssql-python-odbc==18.6.2.2`, a packaging-only revision that separates glibc (manylinux) and Alpine/musl (musllinux) wheel contents without changing the native driver binaries. This release preparation does not publish the package. Already-published `mssql-python` releases that pin `mssql-python-odbc==18.6.2.1` retain that dependency; they do not automatically adopt the new revision. The ODBC driver is loaded lazily when the first connection is created. `pip install mssql-python` transparently pulls the companion version required by the selected release — no separate install step is required.
>
> Starting with v1.13.0, the bundled `libs/` fallback that shipped in v1.12.0 has been removed. For pip installations, creating a connection will fail if `mssql-python-odbc` is not installed. If you install `mssql-python` from a private index or with `--no-deps`, make sure the exact `mssql-python-odbc` version specified in that release's dependency metadata is installed alongside it.
>
> The **temporary Conda candidate** instead combines the code and ODBC payload in one `mssql-python` Conda package. It does not require a separately installed Conda ODBC package. This is not an announcement of public channel availability or release qualification; see the [Conda installation, migration, and readiness guide](conda/README.md).
>
> ### Rust Runtime Distribution
> Bulk copy and the alternate Rust ODBC provider are supplied by `mssql-python-rs`,
> which imports as `mssql_py_core`. `mssql-python` depends on the exact compatible
> runtime version and does not duplicate its native files. Private indexes and
> `--no-deps` installations must provide that companion distribution separately.
>
> ### ODBC Provider Selection (opt-in)
> `mssql-python` also supports selecting an alternate native ODBC provider before the first connection, via the `mssql_python.native_provider` module property or the `MSSQL_PYTHON_NATIVE_PROVIDER` environment variable (which takes precedence). A conflicting property assignment emits a `RuntimeWarning`. The default, `"msodbcsql18"`, is unchanged; opting into `"mssql-odbc"` uses the Rust driver installed by `mssql-python-rs`. Call `mssql_python.get_native_provider_info()` to check the selected provider, source, package version, and resolved driver path.

## Installation

The pip commands below describe the public release. The temporary combined Conda
candidate has a separate platform matrix and Linux compatibility floor; it is not
covered by the public release's production-readiness statement above.
 
**Windows:** mssql-python can be installed with [pip](http://pypi.python.org/pypi/pip)
```bash
pip install mssql-python
```
**MacOS:** mssql-python can be installed with [pip](http://pypi.python.org/pypi/pip)
```bash
# For Mac, OpenSSL is a pre-requisite - skip if already present
brew install openssl
pip install mssql-python
```
**Linux:** mssql-python can be installed with [pip](http://pypi.python.org/pypi/pip)
```bash
# For Alpine
apk add libtool krb5-libs krb5-dev

# For Debian/Ubuntu  
apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2

# For RHEL
dnf install -y libtool-ltdl krb5-libs

# For SUSE
zypper install -y libltdl7 libkrb5-3 libgssapi-krb5-2

# For SUSE/openSUSE
zypper install -y libltdl7

# For Azure Linux
tdnf distro-sync && tdnf install -y libtool-ltdl krb5-libs glibc-iconv

pip install mssql-python
```

**Conda candidate:** Obtain the exact candidate channel and version from its owner;
these changes do not publish packages to the public `microsoft` channel. The candidate
combines the binding, ODBC Driver 18 and required RS bulk-copy core into one package,
while preserving each native provider's private libraries. Historical embedded-core
inputs are distinguished by their metadata and ownership, not version strings. Linux requires
**glibc >=2.34** for that complete payload; `krb5`, OpenSSL, and `libltdl` resolve from
`conda-forge`, so the system package steps above are not required. Windows uses SChannel.
On macOS, encrypted connections still require system OpenSSL from Homebrew
(`brew install openssl`) or MacPorts, not Conda OpenSSL. Windows ARM64 dependencies
resolve from `defaults`; handle channel terms separately, without automatic acceptance.
Use a fresh environment and replace the placeholders with the owner-provided values:
```bash
# Windows x64, macOS, and Linux
conda install -c "<candidate-channel>" -c microsoft -c conda-forge --strict-channel-priority --override-channels "mssql-python=<candidate-version>"

# Windows ARM64
conda install -c "<candidate-channel>" -c microsoft -c defaults --override-channels "mssql-python=<candidate-version>"
```

**Conda release status:** Validate-only release and staged publication tooling are included
in this repository; neither publishes packages automatically. Production publication
requires separately configured credentials, permissions and shared publishing-resource
checks, explicit authorization, and release qualification.
Static audits and import checks do not certify SQL, certificate-verified TLS, authentication,
bulk copy, or optional features across the full matrix. Validate-only success does not
authorize production publication.

## Key Features
### Supported Platforms
 
Windows, MacOS and Linux (manylinux - Debian, Ubuntu, RHEL, SUSE (x64 only) & musllinux - Alpine)

> **Note:**
> SUSE Linux ARM64 is not supported. Please use x64 architecture for SUSE deployments.

### Support for Microsoft Entra ID Authentication
 
The Microsoft mssql-python driver enables Python applications to connect to Microsoft SQL Server, Azure SQL Database, or Azure SQL Managed Instance using Microsoft Entra ID identities. It supports a variety of authentication methods, including username and password, Microsoft Entra managed identity (system-assigned and user-assigned), Integrated Windows Authentication in a federated, domain-joined environment, interactive authentication via browser, device code flow for environments without browser access, and the default authentication method based on environment and configuration. This flexibility allows developers to choose the most suitable authentication approach for their deployment scenario.

EntraID authentication is now fully supported on MacOS and Linux but with certain limitations as mentioned in the table:

| Authentication Method | Windows Support | macOS/Linux Support | Notes |
|----------------------|----------------|---------------------|-------|
| ActiveDirectoryPassword | ✅ Yes | ✅ Yes | Username/password-based authentication |
| ActiveDirectoryInteractive | ✅ Yes | ✅ Yes | Interactive login via browser; requires user interaction |
| ActiveDirectoryMSI (Managed Identity) | ✅ Yes | ✅ Yes | For Azure VMs/containers with managed identity |
| ActiveDirectoryServicePrincipal | ✅ Yes | ✅ Yes | Use client ID and secret or certificate |
| ActiveDirectoryIntegrated | ✅ Yes | ✅ Yes | Now supported on Windows, macOS, and Linux (requires Kerberos/SSPI or equivalent configuration) |
| ActiveDirectoryDeviceCode | ✅ Yes | ✅ Yes | Device code flow for authentication; suitable for environments without browser access |
| ActiveDirectoryDefault | ✅ Yes | ✅ Yes | Uses default authentication method based on environment and configuration |

> For more information on Entra ID please refer this [document](https://github.com/microsoft/mssql-python/wiki/Microsoft-Entra-ID-support)

### Connection Pooling
 
The Microsoft mssql_python driver provides built-in support for connection pooling, which helps improve performance and scalability by reusing active database connections instead of creating a new connection for every request. This feature is enabled by default. For more information, refer [Connection Pooling Wiki](https://github.com/microsoft/mssql-python/wiki/Connection#connection-pooling).

Pooled checkout validates and resets a reused connection with a temporary native connection timeout. It uses the positive login timeout from `connect(timeout=N)` or, with higher precedence, `attrs_before[SQL_ATTR_LOGIN_TIMEOUT]`; if neither specifies a positive value, validation uses 15 seconds. If the pooled connection already has a positive `SQL_ATTR_CONNECTION_TIMEOUT` (set through `attrs_before` or `conn.set_attr()`), validation uses the smaller of that value and the login-derived timeout. A zero connection timeout does not disable the finite validation timeout. A failed validation discards that candidate and attempts a new physical connection instead of probing every idle socket sequentially. The previous connection timeout is restored after successful checkout, and query timeouts remain controlled separately by `conn.timeout`. These are driver operation timeouts, not a strict end-to-end `connect()` deadline: driver retries can make stalled-socket validation take several times the configured timeout, and cancellation/cleanup and a replacement login can add time. Pool return still requires network I/O and is not bounded by the checkout validation timeout.

This favors bounded validation latency over trying every idle connection during a single checkout. If the replacement login fails, that checkout raises a connection exception even if a healthy idle connection remains; the healthy connection stays pooled and can be reused by a subsequent checkout.

> **Interactive / Device-code authentication in multi-user processes:** For `ActiveDirectoryInteractive` and `ActiveDirectoryDeviceCode`, the driver caches one credential instance per authentication type for the lifetime of the process so that pooled reconnects refresh silently instead of re-prompting. As a result, every interactive connection opened in the same process shares that one signed-in account — the first user to authenticate. If your application serves multiple end users from a single process, do **not** rely on interactive/device-code auth to isolate them; instead supply your own per-user token via a token provider so each user's connection is keyed to their own identity.

> **Bring-your-own token (`token_provider` / raw access token):** When you supply your own access token (via a token provider or a raw token in `attrs_before`), managing its lifetime is **your application's responsibility**. The driver stores a pooled connection under the identity that opened it, but it cannot refresh a token it did not mint. If a pooled connection is reused after its token has expired, the reconnect (ODBC's implicit connection resiliency) will fail. Ensure your token provider returns a currently-valid token on every call, and account for token expiry in your own logic. The driver's built-in expiry-aware checkout (refreshing a pooled connection whose token is near expiry) only applies to Managed Identity and interactive/device-code pools, whose tokens the driver can silently re-mint — not to bring-your-own tokens.

> **`DefaultAzureCredential` is not recommended for multi-user pooling:** `ActiveDirectoryDefault` resolves to whatever ambient identity `DefaultAzureCredential` discovers (environment, managed identity, developer sign-in, etc.), which is a single process-wide identity. It is well suited to single-identity services but does **not** distinguish between end users, so pooled connections opened with it are not isolated per user. The driver emits a one-time warning in this case. For genuinely multi-user workloads, use a per-user token provider instead. (You will also see this noted in the emitted log warning.) Because a `DefaultAzureCredential` pool is keyed on the token hash rather than a stable identity, it is also **not** expiry-aware: the driver-acquired token is reused until the connection dies, so it is best suited to short-lived or single-identity use.

### DBAPI v2.0 Compliance
 
The Microsoft **mssql-python** module is designed to be fully compliant with the DB API 2.0 specification. This ensures that the driver adheres to a standardized interface for database access in Python, providing consistency and reliability across different database systems. Key aspects of DBAPI v2.0 compliance include:
 
- **Connection Objects**: Establishing and managing connections to the database.
- **Cursor Objects**: Executing SQL commands and retrieving results.
- **Transaction Management**: Supporting commit and rollback operations to ensure data integrity.
- **Error Handling**: Providing a consistent set of exceptions for handling database errors.
- **Parameter Substitution**: Allowing the use of placeholders in SQL queries to prevent SQL injection attacks.
 
By adhering to the DB API 2.0 specification, the mssql-python module ensures compatibility with a wide range of Python applications and frameworks, making it a versatile choice for developers working with Microsoft SQL Server, Azure SQL Database, and Azure SQL Managed Instance.
 
### Error classification with SQLSTATE and native error numbers

DB-API exceptions expose `sqlstate: str | None` and `native_error: int | None`.
Synchronous ODBC errors retain the SQLSTATE and native number from the same
selected diagnostic record, including SQLSTATE codes not recognized by the
driver's exception mapping. `native_error` is returned directly by ODBC, not
parsed from message text: SQL Server errors commonly provide engine numbers
(for example, 208 for a missing table), while driver errors can provide 0 or
provider-specific numbers. A genuine 0 is retained; unavailable metadata is
`None`. Python-created errors default to `None`. Async Rust/TDS errors do not
currently provide this metadata and also default to `None`.

```python
from mssql_python import Error

try:
    cursor.execute("SELECT * FROM missing_table")
except Error as exc:
    # getattr also supports driver versions without the sqlstate attribute.
    if getattr(exc, "sqlstate", None) == "42S02":
        print("The requested table was not found.")
    raise
```

Existing exception types, two-argument constructors, messages, and `args` are
unchanged. Constructors also accept optional keyword-only `sqlstate` and
`native_error` arguments, and both survive pickling and copying. Neither
attribute alone guarantees that retrying an operation is safe.

### Enhanced Pythonic Features
 
The driver offers a suite of Pythonic enhancements that streamline database interactions, making it easier for developers to execute queries, manage connections, and handle data more efficiently.
 
### Text encoding

SQL statements and Python `str` parameters always use UTF-16LE. Text parameters are
bound as ODBC `SQL_C_WCHAR` on every supported platform, for both `execute()` and
`executemany()`, including calls that use `setinputsizes()`. Declaring a `VARCHAR`
SQL type does not switch to narrow C buffers; SQL Server performs the conversion
to the destination column's character set.

`Connection.setencoding()` retains requested settings for compatibility, but does
not change statement encoding or parameter binding. Requests that pass validation
but differ from `encoding="utf-16le", ctype=SQL_WCHAR` emit `UserWarning`, including
an explicitly requested or automatically selected `SQL_CHAR`. Invalid codec names,
invalid ctypes, and incompatible combinations (such as UTF-8 with `SQL_WCHAR`)
raise `ProgrammingError` before any warning is emitted or settings are stored.
`getencoding()` returns the requested settings, not the effective binding. For
example, requesting ASCII with `SQL_CHAR` does not cause non-ASCII parameters to
raise encoding errors. Use `setencoding()` with no arguments to restore the
supported defaults. `setdecoding()` independently controls how result data is read.

### Native vector values

The synchronous ODBC API accepts and returns immutable `Vector` values. SQL NULL
is represented by `None`. FLOAT32 supports 1–1998 dimensions; FLOAT16 supports
1–3996. A vector's dimensions must match its SQL column or parameter. Where
supported, SQL Server converts between FLOAT32 and FLOAT16 base types.

Enable native vector negotiation **when connecting**: `VectorTypeSupport=v1`
for FLOAT32, or `VectorTypeSupport=v2` for both subtypes. The default remains
`off` (the ODBC driver's existing JSON/text behavior). FLOAT32 requires Microsoft
ODBC Driver 18.6.1.1+; FLOAT16 requires **18.7.1.1+** and a server that
supports FLOAT16 vectors. FLOAT16 is generally available in Azure SQL Database,
Azure SQL Managed Instance and SQL database in Fabric; in SQL Server 2025 it is a
preview feature that requires `ALTER DATABASE SCOPED CONFIGURATION SET
PREVIEW_FEATURES = ON`. FLOAT32 works on all of these. The currently published `mssql-python-odbc` 18.6.2.2
package does not meet the FLOAT16 requirement. These Python changes do not
upgrade the bundled driver or add support to the alternate Rust ODBC provider.

| Connection setting | Native FLOAT32 | Native FLOAT16 |
|---|---|---|
| `VectorTypeSupport=off` (default) | No; reads return JSON text | No; reads return JSON text |
| `VectorTypeSupport=v1` | Yes | No; FLOAT16 inputs are rejected by ODBC and FLOAT16 columns are read as JSON text |
| `VectorTypeSupport=v2` | Yes | Yes |

`Vector(values, base_type="float16")` selects the value's SQL base type; it does not upgrade a
connection from v1 to v2, and on a v1 connection it is not sent as FLOAT32
instead: ODBC rejects it with an error. The connection setting is passed through to ODBC for
negotiation rather than emulated in Python.

The driver never converts between FLOAT32 and FLOAT16. A `Vector` is sent with
its own base type and SQL Server converts it to the column's base type,
rounding to nearest; FLOAT32 values outside the FLOAT16 range fail with a server
error. This matches Microsoft.Data.SqlClient.

```python
from mssql_python import connect, Vector, SQL_SS_VECTOR

connection = connect(connection_string, VectorTypeSupport="v1")
cursor = connection.cursor()
cursor.execute("CREATE TABLE #embeddings (embedding VECTOR(3) NULL)")
vector = Vector([1.25, -2.5, 0])  # FLOAT16 values require a v2 connection
cursor.execute("INSERT INTO #embeddings VALUES (?)", vector)
cursor.executemany("INSERT INTO #embeddings VALUES (?)", [(None,), (vector,)])
cursor.setinputsizes([(SQL_SS_VECTOR, 3, 0)])  # dimensions, subtype: 0=FLOAT32, 1=FLOAT16
cursor.execute("INSERT INTO #embeddings VALUES (?)", None)
cursor.execute("SELECT embedding FROM #embeddings")
rows = cursor.fetchall()  # fetchone(), fetchmany(), and iteration work too
values = list(vector)  # Independent list; the Vector remains immutable
```

`Vector(values, base_type="float32")` snapshots a numeric iterable and normalizes
each element to float32 immediately. Its `.base_type` and `.dimensions`
properties describe the SQL value; iteration and indexing return Python floats.
Ordinary float32 rounding is allowed, including subnormal values and underflow
to zero. Exact float32 representability is not required.

Non-numeric elements (including bool and None) raise `TypeError`. NaN, infinity,
and magnitudes above the finite float32 maximum (approximately 3.402823466e38)
raise `ValueError` in the constructor, before binding or network I/O. FLOAT16
values additionally reject magnitudes above 65504 conservatively, before
float32 rounding. Errors identify the element index without including its value.
Values are never clamped, promoted to float64, or silently changed to JSON.

An explicit vector input hint also accepts a numeric **list**, applying the
same checks during binding. Without a hint, ordinary lists retain their existing
parameter meanings and are not automatically vectors. Hints are consumed by a
successful execution; reapply them for subsequent calls.

```python
cursor.setinputsizes([(SQL_SS_VECTOR, 3, 0)])
cursor.execute("INSERT INTO #embeddings VALUES (?)", [[0.1, 0.2, 0.3]])
cursor.setinputsizes([(SQL_SS_VECTOR, 3, 0)])
cursor.executemany(
    "INSERT INTO #embeddings VALUES (?)",
    [([0.1, 0.2, 0.3],), (None,), (vector,)],
)
```

Non-NULL values must match the hinted dimensions and base type. A conflicting
`Vector` is rejected, not reinterpreted. Batch vector columns must agree in
dimensions/base type; hinted columns may mix lists, matching Vectors, and None.

Both subtypes are marshaled to ODBC as `SQL_C_SS_VECTOR` structures containing
C float32 arrays and bound with SQL type `SQL_SS_VECTOR`. ODBC encodes and
decodes FLOAT16 elements (half-precision rounding) and reports backend
diagnostics; Python performs only the early finite/range checks above. Python does not
pack TDS headers, encode half floats, or serialize vectors as binary/JSON.

**Stored procedures:** `callproc()` accepts ordinary input parameters and
`OutputParameter(sql_type, size, scale)` / `InputOutputParameter(vector)`
wrappers. Use `execute()` to pass table-valued parameters. `OutputParameter`
takes the same triple as `setinputsizes()`; today
only `SQL_SS_VECTOR` is supported (size is dimensions, scale is 0 for FLOAT32 or
1 for FLOAT16), and other SQL types raise `NotSupportedError`.
`InputOutputParameter` infers its type from the initial `Vector`; if its
`.value` is later `None` (for example after a NULL output), it is sent as a NULL
of that type. `callproc()`
returns a modified parameter sequence and also fills each wrapper's
`.value`. Result sets are buffered in memory so output values are final when the
call returns; use normal row fetches and `nextset()` to read them.
Input parameters may be `None`; parameter-description metadata is not exposed as
a procedure result set, including when reusing the cursor. Procedure names have
1-3 parts; parts may already be bracket-quoted, e.g. `[dbo].[my.proc]`.

```python
from mssql_python import OutputParameter, InputOutputParameter

# dbo.EchoEmbedding: @input VECTOR(3), @output VECTOR(3) OUTPUT,
#                    @inout VECTOR(3) OUTPUT
output = OutputParameter(SQL_SS_VECTOR, 3, 0)
inout = InputOutputParameter(vector)
parameters = cursor.callproc("dbo.EchoEmbedding", [vector, output, inout])
print(output.value)
```

Procedure text/binary inputs
must fit the inline binding limits. Table-valued parameters are not part of this
vector support; vector TVP columns will build on the shared TVP definition.
Arrow vector fetches and Arrow access to
buffered procedure results are not supported; use row fetch methods instead.
**Vector bulk copy is pending the pycore vector input contract.** `bulkcopy()`
explicitly rejects `Vector` cells rather than guessing a serialization format.
Once that backend contract is available, pycore will encode FLOAT16 for bulk
copy, and SQL Server will convert between base types as it does for parameters.

Vector unit tests run without SQL Server. End-to-end tests in
`tests/test_025_vector.py` exercise v1/FLOAT32, v2/FLOAT32, and v2/FLOAT16:
scalar and batch binding (including explicitly hinted lists, pyformat,
typed/all-NULL arrays and streamed text), numeric boundaries and error recovery,
every row fetch method including `fetchval()`, metadata/row access, output converters,
commit/rollback, stored procedure outputs/result sets, and error recovery.
Negotiation tests assert that v1 rejects native FLOAT16, `off` rejects native
vector inputs, and `off` retains JSON/NULL reads. Arrow and bulk-copy cases assert
the current explicit unsupported boundaries; they are not positive vector
bulk-copy tests.

Set `DB_CONNECTION_STRING` outside source control to enable the live tests.
The database principal needs permission to create tables, procedures, and table
types in `dbo`; named test objects use unique names and are dropped in cleanup.
The v2 cases additionally require `MSSQL_TEST_VECTOR_FLOAT16=1`, ODBC 18.7.1.1+,
and a FLOAT16-capable server. For example, from PowerShell:

```powershell
python -m pytest tests\test_025_vector.py -k live -v
# Enable the full v2 matrix only after provisioning a compatible driver/server:
$env:MSSQL_TEST_VECTOR_FLOAT16 = "1"
python -m pytest tests\test_025_vector.py -v
```

Servers without the built-in vector type and the unqualified alternate provider
are explicitly skipped, so existing SQL Server 2022 validation legs remain
usable. The server check reads `sys.types`, not a version banner, and does not
suppress connection or query errors.

Missing database/capability configuration causes explicit skips. Once enabled,
connection failures and unsupported backend behavior fail the tests rather than
being converted to skips.

### Batch parameter inference

For automatically inferred `executemany()` text columns, any non-ASCII value
makes the entire parameter column `NVARCHAR`; ASCII-only columns retain `VARCHAR`
inference. Buffer sizes and the streaming threshold use the maximum UTF-16
code-unit length across all rows, not just the longest Python string. Embedded
NUL characters are preserved using explicit text lengths. Batch text metadata
is computed natively without allocating encoded copies of each string; BMP-only
strings do not require a character-by-character scan.
Wide-character retrieval of `VARCHAR(MAX)` values uses a non-consuming binary
length probe to size the conversion buffer, avoiding character loss at truncated
UTF-16 conversion boundaries while preserving embedded and trailing NULs.
Explicit `setinputsizes()` SQL types and sizes still take precedence. An explicit
`VARCHAR` parameter can lose characters outside the database collation's code
page even when inserting into an `NVARCHAR` destination; use `SQL_WVARCHAR` when
Unicode preservation is required.

Automatically inferred Decimal columns use a `NUMERIC` precision and scale
covering every row. Their formatted text buffer capacity is separate from SQL
precision, so signs and decimal points do not change the declared numeric type.
If individually valid Decimals cannot share a lossless precision/scale within SQL
Server's 38-digit limit, the batch uses individual `execute()` calls. Columns
mixing incompatible Python types (for example, integers and Decimals, or dates
and datetimes) or naive and timezone-aware datetimes also use individual calls
rather than coercing every value through one sample's type. Boolean/integer and
bytes/bytearray mixtures, and mixtures of strings with `str` subclasses remain
eligible for array binding. Explicit `setinputsizes()` overrides retain their
specified SQL types, precision and scale, including numeric normalization during
individual execution.
Individual execution is slower than array binding; known affected-row counts are
summed across the batch, or `rowcount` is `-1` if any execution's count is unknown.

## Getting Started Examples
Connect to SQL Server and execute a simple query:
 
```python
import mssql_python

# Establish a connection
# Specify connection string (semicolon-delimited key=value format preserved)
# Uses Azure Entra ID Interactive authentication — no password in the string.
connection_string = "SERVER=tcp:mssql-python-driver-eastus01.database.windows.net,1433;DATABASE=AdventureWorksLT;Authentication=ActiveDirectoryInteractive;Encrypt=yes;"
connection = mssql_python.connect(connection_string)

# Execute a realistic query against AdventureWorksLT:
# Top 10 customers by number of orders, with their total spend
cursor = connection.cursor()
cursor.execute("""
    SELECT TOP 10
        c.CustomerID,
        c.FirstName,
        c.LastName,
        COUNT(h.SalesOrderID) AS OrderCount,
        SUM(h.TotalDue) AS TotalSpend
    FROM SalesLT.Customer AS c
    INNER JOIN SalesLT.SalesOrderHeader AS h
        ON c.CustomerID = h.CustomerID
    GROUP BY c.CustomerID, c.FirstName, c.LastName
    ORDER BY OrderCount DESC, TotalSpend DESC
""")
rows = cursor.fetchall()

for row in rows:
    print(row)

# Close the connection
connection.close()
 
```
 
### Table-valued parameters

Declare a table type and an input `READONLY` parameter on SQL Server:

```sql
CREATE TYPE dbo.ItemBatch AS TABLE (id int, label nvarchar(100));
GO
CREATE PROCEDURE dbo.ProcessItems @items dbo.ItemBatch READONLY
AS SELECT id, label FROM @items;
```

Pass one explicit value to synchronous `execute()`; ordinary nested lists/tuples are **not**
implicitly treated as TVPs:

```python
from mssql_python import TableValuedParameter, SQL_INTEGER, SQL_WVARCHAR

items = TableValuedParameter(
    type_name="dbo.ItemBatch",
    columns=[("id", SQL_INTEGER), ("label", SQL_WVARCHAR, 100)],
    rows=[(1, "Keyboard"), (2, None)],
)
cursor.execute("EXEC dbo.ProcessItems ?", items)
```

Column tuples are `(name, sql_type[, size_or_precision[, scale]])`. Use the
driver's integer `SQL_*` constants. Columns bind **positionally**; names are
diagnostic labels, not a schema lookup. Character and binary columns require a
size; `0` means MAX for variable-length types. Decimal columns require both
precision and scale, e.g. `("amount", SQL_DECIMAL, 20, 6)`. Precision is 1–38;
scale is 0–precision. Temporal scale defaults to 7 (0–7 supported).
Sizes for Unicode columns are UTF-16 code units; binary sizes are bytes.
Rows must contain one cell per column. Supported cells include integers, floats,
bools, `Decimal`, strings, bytes/bytearray, date/time/datetime, UUID and `None`.

Use an unquoted `schema.Type` or `type_name="ItemBatch", schema="dbo"` (default
schema: `dbo`); SQL quoting/bracket syntax and database-qualified names are not
parsed. The constructor does not consume rows. `rows=None` is invalid: use `[]`
for an empty table; individual cells may be `None`.

With the default **msodbcsql18** sync provider, `rows` can also be a synchronous
iterator/generator. It is pulled during `execute()`, with one row of additional
driver-side binding storage regardless of total input length. A large cell still
occupies memory for that cell. This is one SQL execution, **not** `executemany()`;
multiple TVPs, scalar parameters and named parameters can be combined. The
native row-count descriptor permits up to 2,147,483,647 rows. Materialized sources
can be reused; iterators are one-shot and may be partially consumed on failure.
Iterator exceptions, including `KeyboardInterrupt`, cancel/reset the statement
before releasing its buffers. `executemany()` and `setinputsizes()` overrides
for statements containing TVPs are not supported. The `mssql-odbc` provider does
not currently support this ODBC TVP path.

## Still have questions?
 
Check out our [FAQ](https://github.com/microsoft/mssql-python/wiki/Frequently-Asked-Questions). Still not answered? Create an [issue](https://github.com/microsoft/mssql-python/issues/new/choose) to ask a question.
 
## Contributing
 
This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.
 
When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.
 
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### Performance profiling (internal)

The driver ships with an optional, compile-time-gated profiler that times both the
Python and native (C++) layers of a query. The **native (C++) instrumentation is off
by default and compiled out of released wheels**; the thin Python-layer markers
(`perf_timer.py` and the `perf_phase(...)` calls) do ship, but are a no-op unless
profiling is explicitly enabled. It is intended for contributors diagnosing where time
goes on the execute/fetch paths. See [`profiler/README.md`](profiler/README.md) for how
to build with profiling enabled and run it.

## License
The mssql-python driver for SQL Server is licensed under the MIT license, except the dynamic-link libraries (DLLs) in the [libs](https://github.com/microsoft/mssql-python/tree/main/mssql_python_odbc/libs) folder 
that are licensed under MICROSOFT SOFTWARE LICENSE TERMS.
 
Please review the [LICENSE](LICENSE) file for more details.
 
## Trademarks
 
This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
