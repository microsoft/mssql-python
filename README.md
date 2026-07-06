# Microsoft Python Driver for SQL Server
 
**mssql-python** is a Python driver for Microsoft SQL Server and the Azure SQL family of databases. It leverages Direct Database Connectivity (DDBC) that enables direct connections to SQL Server without requiring an external driver manager. Designed to comply with the [DB API 2.0](https://peps.python.org/pep-0249/) specification, this driver also introduces Pythonic enhancements for improved usability and functionality. It supports a full range of database operations, including connection management, query execution, and transaction handling.
 
The driver is compatible with all the Python versions >= 3.10
 
[Documentation](https://github.com/microsoft/mssql-python/wiki) | [Release Notes](https://github.com/microsoft/mssql-python/releases) | [Roadmap](https://github.com/microsoft/mssql-python/blob/main/ROADMAP.md)
 
> **Note:**
> This project is now Generally Available (GA) and ready for production use. We’ve completed core functionality and incorporated feedback from the preview phase.
> 
## Installation
 
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
 
## License
The mssql-python driver for SQL Server is licensed under the MIT license, except the dynamic-link libraries (DLLs) in the [libs](https://github.com/microsoft/mssql-python/tree/alphaChanges/mssql_python/libs) folder 
that are licensed under MICROSOFT SOFTWARE LICENSE TERMS.
 
Please review the [LICENSE](LICENSE) file for more details.
 
## Trademarks
 
This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
