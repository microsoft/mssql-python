# Project

> This repo has been populated by an initial template to help get you started. Please
> make sure to update the content to build a great experience for community-building.

As the maintainer of this project, please make a few updates:

- Improving this README.MD file to provide a great experience
- Updating SUPPORT.MD with content about this project's support experience
- Understanding the security reporting process in SECURITY.MD
- Remove this section from the README

# Python Connector for SQL Server and Azure SQL

The Microsoft **mssql-python** module is a powerful, user-friendly Python connector designed for seamless interaction with Microsoft SQL Server, Azure SQL Database, and Azure SQL Managed Instance databases using Direct Database Connectivity (DDBC). DDBC allows Python applications to connect directly to SQL Server, bypassing the need for an external driver manager. This driver is crafted to adhere to the [DB API 2.0](https://peps.python.org/pep-0249/) specification while incorporating additional Pythonic features that enhance its ease of use and functionality. The driver offers comprehensive functionalities, including establishing connections, executing queries, and managing transactions among many other features.

## Key Features

### DBAPI v2.0 Compliance

The Microsoft **mssql-python** module is designed to be fully compliant with the DB API 2.0 specification. This ensures that the driver adheres to a standardized interface for database access in Python, providing consistency and reliability across different database systems. Key aspects of DBAPI v2.0 compliance include:

- **Connection Objects**: Establishing and managing connections to the database.
- **Cursor Objects**: Executing SQL commands and retrieving results.
- **Transaction Management**: Supporting commit and rollback operations to ensure data integrity.
- **Error Handling**: Providing a consistent set of exceptions for handling database errors.
- **Parameter Substitution**: Allowing the use of placeholders in SQL queries to prevent SQL injection attacks.

By adhering to the DB API 2.0 specification, the mssql-python module ensures compatibility with a wide range of Python applications and frameworks, making it a versatile choice for developers working with Microsoft SQL Server, Azure SQL Database, and Azure SQL Managed Instance.

### Ease of Installation

- **pip install**: The easiest way to install mssql-python is through the Python package manager, pip, irrespective of the Operating System. This ensures a quick and straightforward setup process without the need for additional tools, software, or compilers.
    ```bash
    pip install mssql-python
    ```

- **Offline install using zip file**: Another way of installing the driver is using a .zip file which can be downloaded from here [TODO: Link of the zip file]. 
  - [TODO] - Steps to follow to install the driver using .zip file.

### Platform Compatibility

The current release of mssql-python is tailored for the Windows platform. Future updates will extend compatibility to Mac and Linux, ensuring a consistent installation experience and feature set across all platforms. Stay tuned for more updates in this space.

### Support for Microsoft Entra ID Authentication

The Microsoft mssql-python driver enables Python applications to connect to Microsoft SQL Server, Azure SQL Database, or Azure SQL Managed Instance using Microsoft Entra ID identities. It supports various authentication methods, including username and password, Microsoft Entra managed identity, and Integrated Windows Authentication in a federated, domain-joined environment. Additionally, the driver supports Microsoft Entra interactive authentication and Microsoft Entra managed identity authentication for both system-assigned and user-assigned managed identities.

### Enhanced Pythonic Features

The driver offers a suite of Pythonic enhancements that streamline database interactions, making it easier for developers to execute queries, manage connections, and handle data more efficiently.

### Example Usage

Below is a simple example demonstrating how to establish a connection and execute a query using mssql-python:

```python
import mssql_python

# Establish a connection
connection = mssql_python.connect(
    server='your_server',
    database='your_database',
    username='your_username',
    password='your_password'
)

# Execute a query
cursor = connection.cursor()
cursor.execute("SELECT * FROM your_table")
rows = cursor.fetchall()

for row in rows:
    print(row)

# Close the connection
connection.close()
```

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
The mssql-python driver for SQL Server is licensed under the MIT license, except the dynamic-link libraries (DLLs) in the [libs](libs) folder.
These files are subject to the [Microsoft ODBC Driver 18 End User License Agreement](libs\win\MICROSOFT_ODBC_DRIVER_FOR_SQL_SERVER_LICENSE.txt) and the [Microsoft Visual C++ Redistributable](libs\win\MICROSOFT_VISUAL_STUDIO_LICENSE.txt) licensing terms.

Please review the [LICENSE](LICENSE.txt) file for more details.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
