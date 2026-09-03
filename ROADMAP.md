# Roadmap for Python Driver for SQL Server

The following roadmap summarizes the features planned for the Python Driver for SQL Server.

| Feature                        | Description                                                       | Status       | Target Timeline          |
| ------------------------------ | ----------------------------------------------------------------- | ------------ | ------------------------ |
| Return Rows as Dictionaries    | Fetch rows as dictionaries for more Pythonic access               | Planned      | Q3 2026                  |
| Asynchronous Query Execution   | Non-blocking queries with asyncio support                         | Planned      | Q4 2026                  |
| Vector Datatype Support        | Native binding for the SQL Server 2025 `vector` type. Vector columns are already readable and writable today as JSON array strings | In Progress  | Q3 2026                  |
| Table-Valued Parameters (TVPs) | Pass tabular data structures into stored procedures               | Planned      | Q3 2026                  |
| JSON Datatype Support          | Automatic mapping of JSON datatype to Python dicts/lists          | Planned      | Q4 2026                  |
