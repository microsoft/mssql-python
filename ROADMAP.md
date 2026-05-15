# Roadmap for Python Driver for SQL Server

The following roadmap summarizes the features planned for the Python Driver for SQL Server.

| Feature                        | Description                                                       | Status       | Target Timeline          |
| ------------------------------ | ----------------------------------------------------------------- | ------------ | ------------------------ |
| Parameter Dictionaries         | Allow parameters to be supplied as Python dicts                   | Planned      | Q4 2025                  |
| Return Rows as Dictionaries    | Fetch rows as dictionaries for more Pythonic access               | Planned      | Q4 2025                  |
| Bulk Copy (BCP)                | High-throughput ingestion API for ETL workloads                   | Under Design | Q1 2026                  |
| Asynchronous Query Execution   | Non-blocking queries with asyncio support                         | Planned      | Q1 2026                  |
| Vector Datatype Support        | Native support for SQL Server vector datatype                     | Planned      | Q1 2026                  |
| Table-Valued Parameters (TVPs) | Pass tabular data structures into stored procedures               | Planned      | Q1 2026                  |
| C++ Abstraction                | Modular separation via pybind11 for performance & maintainability | In Progress  | ETA will be updated soon |
| JSON Datatype Support          | Automatic mapping of JSON datatype to Python dicts/lists          | Planned      | ETA will be updated soon |
| callproc()                     | Full DBAPI compliance & stored procedure enhancements             | Planned      | ETA will be updated soon |
| setinputsize()                 | Full DBAPI compliance & stored procedure enhancements             | Planned      | ETA will be updated soon |
| setoutputsize()                | Full DBAPI compliance & stored procedure enhancements             | Planned      | ETA will be updated soon |
| Output/InputOutput Params      | Full DBAPI compliance & stored procedure enhancements             | Planned      | ETA will be updated soon |
