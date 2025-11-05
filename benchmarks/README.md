# Benchmarks

This directory contains benchmark scripts for testing the performance of various database operations using `pyodbc` and `mssql_python`. The goal is to evaluate and compare the performance of these libraries for common database operations.

## Benchmark Scripts

### 1. `bench_mssql.py` - Richbench Framework Benchmarks
Comprehensive benchmarks using the richbench framework for detailed performance analysis.

### 2. `perf-benchmarking.py` - Real-World Query Benchmarks
Standalone script that tests real-world queries against AdventureWorks2022 database with statistical analysis.

## Why Benchmarks?
- To measure the efficiency of `pyodbc` and `mssql_python` in handling database operations.
- To identify performance bottlenecks and optimize database interactions.
- To ensure the reliability and scalability of the libraries under different workloads.

## How to Run Benchmarks

### Running bench_mssql.py (Richbench Framework)

1. **Set Up the Environment Variable**:
   - Ensure you have a running SQL Server instance.
   - Set the `DB_CONNECTION_STRING` environment variable with the connection string to your database. For example:
     ```bash
     export DB_CONNECTION_STRING="Server=your_server;Database=your_database;UID=your_user;PWD=your_password;"
     ```

2. **Install Richbench - Benchmarking Tool**:
   ```bash
   pip install richbench
   ```

3. **Run the Benchmarks**:
   - Execute richbench from the parent folder (mssql-python):
     ```bash
     richbench benchmarks
     ```
   - Results will be displayed in the terminal with detailed performance metrics.

### Running perf-benchmarking.py (Real-World Queries)

This script tests performance with real-world queries from the AdventureWorks2022 database.

1. **Prerequisites**:
   - AdventureWorks2022 database must be available
   - Both `pyodbc` and `mssql-python` must be installed
   - Update the connection string in the script if needed

2. **Run from project root**:
   ```bash
   python benchmarks/perf-benchmarking.py
   ```

3. **Features**:
   - Runs each query multiple times (default: 5 iterations)
   - Calculates average, min, max, and standard deviation
   - Provides speedup comparisons between libraries
   - Tests various query patterns:
     - Complex joins with aggregations
     - Large dataset retrieval (10K+ rows)
     - Very large dataset (1.2M rows)
     - CTEs and subqueries
   - Detailed summary tables and conclusions

4. **Output**:
   The script provides:
   - Progress indicators during execution
   - Detailed results for each benchmark
   - Summary comparison table
   - Overall performance conclusion with speedup factors

## Key Features of `bench_mssql.py`
- **Comprehensive Benchmarks**: Includes SELECT, INSERT, UPDATE, DELETE, complex queries, stored procedures, and transaction handling.
- **Error Handling**: Each benchmark function is wrapped with error handling to ensure smooth execution.
- **Progress Messages**: Clear progress messages are printed during execution for better visibility.
- **Automated Setup and Cleanup**: The script automatically sets up and cleans up the database environment before and after the benchmarks.

## Key Features of `perf-benchmarking.py`
- **Statistical Analysis**: Multiple iterations with avg/min/max/stddev calculations
- **Real-World Queries**: Tests against AdventureWorks2022 with production-like queries
- **Automatic Import Resolution**: Correctly imports local `mssql_python` package
- **Comprehensive Reporting**: Detailed comparison tables and performance summaries
- **Speedup Calculations**: Clear indication of performance differences

## Notes
- Ensure the database user has the necessary permissions to create and drop tables and stored procedures.
- The `bench_mssql.py` script uses permanent tables prefixed with `perfbenchmark_` for benchmarking purposes.
- A stored procedure named `perfbenchmark_stored_procedure` is created and used during the benchmarks.
- The `perf-benchmarking.py` script connects to AdventureWorks2022 and requires read permissions only.