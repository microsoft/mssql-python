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

# Configuration
CONN_STR = os.getenv("DB_CONNECTION_STRING")

if not CONN_STR:
    print("Error: The environment variable DB_CONNECTION_STRING is not set.")
    sys.exit(1)

# Ensure pyodbc connection string has ODBC driver specified
if CONN_STR and 'Driver=' not in CONN_STR:
    CONN_STR = f"Driver={{ODBC Driver 18 for SQL Server}};{CONN_STR}"

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
    
    conn = pyodbc.connect(CONN_STR)
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
    """Test with mssql_python"""
    print("Testing mssql-python...")
    start_time = time.time()
    
    conn = connect(CONN_STR)
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