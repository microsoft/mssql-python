#!/usr/bin/env python3
"""
Debug script for Very Large Dataset performance
Focus on the specific query that's still 1.2x slower than pyodbc
"""

import os
import sys
import time

# Add parent directory to path to import local mssql_python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import pyodbc
from mssql_python import connect
import mssql_python.ddbc_bindings as ddbc_bindings

# Configuration
CONN_STR = os.getenv("DB_CONNECTION_STRING")

if not CONN_STR:
    print("Error: The environment variable DB_CONNECTION_STRING is not set.")
    sys.exit(1)

# Ensure pyodbc connection string has ODBC driver specified
if CONN_STR and 'Driver=' not in CONN_STR:
    CONN_STR_PYODBC = f"Driver={{ODBC Driver 18 for SQL Server}};{CONN_STR}"

# The problematic query
VERY_LARGE_DATASET = """
SELECT
    sod.SalesOrderID,
    sod.SalesOrderDetailID,
    sod.ProductID,
    sod.OrderQty,
    sod.UnitPrice,
    sod.LineTotal,
    p.Name AS ProductName,
    p.ProductNumber,
    p.Color,
    p.ListPrice,
    n1.number AS RowMultiplier1
FROM Sales.SalesOrderDetail sod
CROSS JOIN (SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
            FROM Sales.SalesOrderDetail) n1
INNER JOIN Production.Product p ON sod.ProductID = p.ProductID;
"""

def test_pyodbc():
    """Test with pyodbc"""
    print("Testing pyodbc...")
    start_time = time.time()
    
    conn = pyodbc.connect(CONN_STR_PYODBC)
    cursor = conn.cursor()
    
    query_start = time.time()
    cursor.execute(VERY_LARGE_DATASET)
    rows = cursor.fetchall()
    query_end = time.time()
    
    conn.close()
    end_time = time.time()
    
    print(f"  Rows fetched: {len(rows):,}")
    print(f"  Total time: {end_time - start_time:.3f}s")
    print(f"  Query + fetch time: {query_end - query_start:.3f}s")
    return end_time - start_time

def test_mssql_python():
    """Test with mssql_python with profiling"""
    print("Testing mssql-python...")
    
    # Enable profiling
    ddbc_bindings.profiling.enable()
    ddbc_bindings.profiling.clear()
    
    start_time = time.time()
    
    conn = connect(CONN_STR)
    cursor = conn.cursor()
    
    query_start = time.time()
    cursor.execute(VERY_LARGE_DATASET)
    rows = cursor.fetchall()
    query_end = time.time()
    
    conn.close()
    end_time = time.time()
    
    # Get profiling results
    profiling_results = ddbc_bindings.profiling.get_results()
    
    print(f"  Rows fetched: {len(rows):,}")
    print(f"  Total time: {end_time - start_time:.3f}s")
    print(f"  Query + fetch time: {query_end - query_start:.3f}s")
    
    # Print top 10 most time-consuming operations
    print("\n" + "=" * 60)
    print("TOP PERFORMANCE BOTTLENECKS")
    print("=" * 60)
    
    sorted_results = sorted(profiling_results.items(), key=lambda x: x[1]['total_time'], reverse=True)
    
    for operation, stats in sorted_results[:10]:  # Top 10
        total_time_ms = stats['total_time'] / 1000.0
        count = stats['count']
        avg_time_us = stats['total_time'] / count if count > 0 else 0
        
        print(f"{operation:<40} {total_time_ms:>8.1f}ms ({count:>8} calls)")
    
    total_measured = sum(stats['total_time'] for stats in profiling_results.values()) / 1000.0
    query_time_ms = (query_end - query_start) * 1000
    unmeasured = query_time_ms - total_measured
    
    print("-" * 60)
    print(f"Total measured: {total_measured:.1f}ms | Unmeasured: {unmeasured:.1f}ms ({unmeasured/query_time_ms*100:.1f}%)")
    
    return end_time - start_time

def main():
    print("=" * 60)
    print("DEBUGGING VERY LARGE DATASET PERFORMANCE")
    print("=" * 60)
    
    # Run both tests
    pyodbc_time = test_pyodbc()
    print()
    mssql_time = test_mssql_python()
    
    print()
    print("=" * 60)
    print("COMPARISON")
    print("=" * 60)
    print(f"pyodbc:        {pyodbc_time:.3f}s")
    print(f"mssql-python:  {mssql_time:.3f}s")
    print(f"Difference:    {mssql_time - pyodbc_time:.3f}s")
    print(f"Ratio:         {mssql_time / pyodbc_time:.2f}x")

if __name__ == "__main__":
    main()