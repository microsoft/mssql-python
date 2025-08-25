# Benchmarks

This directory contains benchmark scripts for testing the performance of various database operations using `pyodbc` and `mssql_python`. The goal is to evaluate and compare the performance of these libraries for common database operations.

## Why Benchmarks?
- To measure the efficiency of `pyodbc` and `mssql_python` in handling database operations.
- To identify performance bottlenecks and optimize database interactions.
- To ensure the reliability and scalability of the libraries under different workloads.

## How to Run Benchmarks
1. **Set Up the Environment Variable**:
   - Ensure you have a running SQL Server instance.
   - Set the `DB_CONNECTION_STRING` environment variable with the connection string to your database. For example:
     ```cmd
     set DB_CONNECTION_STRING=Server=your_server;Database=your_database;UID=your_user;PWD=your_password;
     ```

2. **Install Richbench - Benchmarking Tool**:
   - Install richbench :
     ```cmd
     pip install richbench
     ```

3. **Run the Benchmarks**:
   - Execute richbench from the parent folder (mssql-python) :
     ```cmd
     richbench benchmarks
     ```
     Results will be displayed in the terminal with detailed performance metrics.

## Key Features of `bench_mssql.py`
- **Comprehensive Benchmarks**: Includes SELECT, INSERT, UPDATE, DELETE, complex queries, stored procedures, and transaction handling.
- **Error Handling**: Each benchmark function is wrapped with error handling to ensure smooth execution.
- **Progress Messages**: Clear progress messages are printed during execution for better visibility.
- **Automated Setup and Cleanup**: The script automatically sets up and cleans up the database environment before and after the benchmarks.

## Notes
- Ensure the database user has the necessary permissions to create and drop tables and stored procedures.
- The script uses permanent tables prefixed with `perfbenchmark_` for benchmarking purposes.
- A stored procedure named `perfbenchmark_stored_procedure` is created and used during the benchmarks.