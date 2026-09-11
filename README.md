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
> - Current version: **18.6.2.1**
>
> `mssql-python` depends on `mssql-python-odbc==18.6.2.1`. The ODBC driver is loaded lazily when the first connection is created. `pip install mssql-python` transparently pulls the companion package alongside it — no separate install step is required.
>
> Starting with v1.13.0, the bundled `libs/` fallback that shipped in v1.12.0 has been removed. For pip installations, creating a connection will fail if `mssql-python-odbc` is not installed. If you install `mssql-python` from a private index or with `--no-deps`, make sure `mssql-python-odbc==18.6.2.1` is installed alongside it.
>
> The **temporary Conda candidate** instead combines the code and ODBC payload in one `mssql-python` Conda package. It does not require a separately installed Conda ODBC package. This is not an announcement of public channel availability or release qualification; see the [Conda installation, migration, and readiness guide](conda/README.md).
>
> ### ODBC Provider Selection (opt-in)
> `mssql-python` also supports selecting an alternate native ODBC provider before the first connection, via the `mssql_python.native_provider` module property or the `MSSQL_PYTHON_NATIVE_PROVIDER` environment variable (which takes precedence). A conflicting property assignment emits a `RuntimeWarning`. The default, `"msodbcsql18"`, is unchanged; opting into `"mssql-odbc"` requires the `mssql-python-rs` package (which bundles the Rust ODBC driver alongside the Rust TDS core). Call `mssql_python.get_native_provider_info()` to check the selected provider, source, package version, and resolved driver path.

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
includes the ODBC Driver 18 payload and required bulk-copy core. Linux requires
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

**Conda release maintainers:** The dependent [release-additions PR](https://github.com/microsoft/mssql-python/pull/720)
supplies `OneBranchPipelines/conda-release-pipeline.yml` and the publication/provenance
validators; they are not part of this native-packaging change. The workflow described
below requires those additions and does not establish that production setup or publication
has occurred. Its default is `publishToConda=false`.
Select the exact completed Conda build and expected package version.
The pipeline verifies its recorded upstream wheel run, checks the 28-package matrix and
ELF/PE/Mach-O payloads, and logs archive SHA-256 values and the upload plan without publishing.
Feature-branch candidates are allowed only for validation; production requires release,
Conda producer, and wheel sources on `refs/heads/main`. Recipe and wheel commits may differ,
but each must match its authoritative run record. This is static release readiness, not
live SQL/TLS, bulk-copy, or Arrow feature certification.

**Production prerequisite (administrator setup, not performed by a dry run):** in the
`SqlClientDrivers/mssql-python` ADO project, protect the existing **Anaconda Publishing**
variable group **117** with an enabled native **Exclusive lock** check and a designated
**Approval** check. Authorize only release definition **2322**, and grant its build identity
read access to group/check configuration and run-check evidence. Record the installed check
IDs as non-secret group variables `CONDA_PUBLICATION_LOCK_CHECK_ID` and
`CONDA_PUBLICATION_APPROVAL_CHECK_ID`; absent or mismatched IDs block publication. Every writer to
`microsoft/mssql-python` label `main` must use this same protected resource; other credentials
or pipelines must not bypass it. The dependent workflow's publish-only `CondaRelease` stage references this group
with `lockBehavior: sequential`, and refuses upload or promotion without matching,
successful current-stage checks. YAML `lockBehavior` alone does not create the lock.
The server-enforced stage lock must cover snapshot, upload, promotion, rollback, and cleanup.
Rollback is compensating, not atomic; a killed process can leave partial labels, which a
subsequent authorized run must re-verify. Metadata/label API calls use 15-second connect
and 60-second read timeouts; the publishing job, including CLI uploads, is capped at 60 minutes.
Until resource setup and a controlled positive
lock/approval evaluation are verified, production remains blocked; validate-only success
does not prove or authorize production publication.

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
 
### Enhanced Pythonic Features
 
The driver offers a suite of Pythonic enhancements that streamline database interactions, making it easier for developers to execute queries, manage connections, and handle data more efficiently.
 
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
