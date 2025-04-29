# Microsoft Python Driver for SQL Server

**mssql-python** is a Python driver for Microsoft SQL Server and the Azure SQL family of databases. It leverages Direct Database Connectivity (DDBC) that enables direct connections to SQL Server without requiring an external driver manager. Designed to comply with the [DB API 2.0](https://peps.python.org/pep-0249/) specification, this driver also introduces Pythonic enhancements for improved usability and functionality. It supports a full range of database operations, including connection management, query execution, and transaction handling.

[Documentation](https://github.com/microsoft/mssql-python/wiki) | [Release Notes](https://github.com/microsoft/mssql-python/releases) | [Roadmap](https://github.com/microsoft/mssql-python/blob/main/ROADMAP.md)

> **Note:**
> This project is currently in an alpha phase, meaning it is still under active development. We are looking to validate core functionalities and gather initial feedback before a broader public launch. Please use with caution and avoid production environments.
> 
## Installation

mssql-python can be installed with [pip](http://pypi.python.org/pypi/pip)
```bash
pip install mssql-python
```
## Key Features
### Supported Platforms 

Windows

> **Note:**
> Support for macOS and Linux is coming soon
> 

### DBAPI v2.0 Compliance

The Microsoft **mssql-python** module is designed to be fully compliant with the DB API 2.0 specification. This ensures that the driver adheres to a standardized interface for database access in Python, providing consistency and reliability across different database systems. Key aspects of DBAPI v2.0 compliance include:

- **Connection Objects**: Establishing and managing connections to the database.
- **Cursor Objects**: Executing SQL commands and retrieving results.
- **Transaction Management**: Supporting commit and rollback operations to ensure data integrity.
- **Error Handling**: Providing a consistent set of exceptions for handling database errors.
- **Parameter Substitution**: Allowing the use of placeholders in SQL queries to prevent SQL injection attacks.

By adhering to the DB API 2.0 specification, the mssql-python module ensures compatibility with a wide range of Python applications and frameworks, making it a versatile choice for developers working with Microsoft SQL Server, Azure SQL Database, and Azure SQL Managed Instance.

### Support for Microsoft Entra ID Authentication

The Microsoft mssql-python driver enables Python applications to connect to Microsoft SQL Server, Azure SQL Database, or Azure SQL Managed Instance using Microsoft Entra ID identities. It supports various authentication methods, including username and password, Microsoft Entra managed identity, and Integrated Windows Authentication in a federated, domain-joined environment. Additionally, the driver supports Microsoft Entra interactive authentication and Microsoft Entra managed identity authentication for both system-assigned and user-assigned managed identities.

### Enhanced Pythonic Features

The driver offers a suite of Pythonic enhancements that streamline database interactions, making it easier for developers to execute queries, manage connections, and handle data more efficiently.

## Getting Started Examples
  
Connect to SQL Server and execute a simple query:

```python
import mssql_python

# Establish a connection
# Specify connection string
connection_string = "SERVER=<your_server_name>;DATABASE=<your_database_name>;UID=<your_user_name>;PWD=<your_password>;Encrypt=yes;"
connection = mssql_python.connect(connection_string)

# Execute a query
cursor = connection.cursor()
cursor.execute("SELECT * from customer")
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
