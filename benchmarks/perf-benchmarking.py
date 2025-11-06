"""
Performance Benchmarking Script for mssql-python vs pyodbc

This script runs comprehensive performance tests comparing mssql-python with pyodbc
across multiple query types and scenarios. Each test is run multiple times to calculate
average execution times, minimum, maximum, and standard deviation.

Usage:
    python benchmarks/perf-benchmarking.py

Requirements:
    - pyodbc
    - mssql_python
    - Valid SQL Server connection
"""

import os
import sys
import time
import statistics
from typing import List, Tuple

# Add parent directory to path to import local mssql_python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pyodbc
from mssql_python import connect

# Configuration
CONN_STR = os.getenv("DB_CONNECTION_STRING")

if not CONN_STR:
    print("Error: The environment variable DB_CONNECTION_STRING is not set. Please set it to a valid SQL Server connection string and try again.")
    sys.exit(1)

# Ensure pyodbc connection string has ODBC driver specified
if CONN_STR and 'Driver=' not in CONN_STR:
    CONN_STR = f"Driver={{ODBC Driver 18 for SQL Server}};{CONN_STR}"

NUM_ITERATIONS = 5  # Number of times to run each test for averaging

# SQL Queries
COMPLEX_JOIN_AGGREGATION = """
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        pc.Name AS Category,
        psc.Name AS Subcategory,
        COUNT(sod.SalesOrderDetailID) AS TotalOrders,
        SUM(sod.OrderQty) AS TotalQuantity,
        SUM(sod.LineTotal) AS TotalRevenue,
        AVG(sod.UnitPrice) AS AvgPrice
    FROM Sales.SalesOrderDetail sod
    INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
    INNER JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    INNER JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
    GROUP BY p.ProductID, p.Name, pc.Name, psc.Name
    HAVING SUM(sod.LineTotal) > 10000
    ORDER BY TotalRevenue DESC;
"""

LARGE_DATASET = """
    SELECT
        soh.SalesOrderID,
        soh.OrderDate,
        soh.DueDate,
        soh.ShipDate,
        soh.Status,
        soh.SubTotal,
        soh.TaxAmt,
        soh.Freight,
        soh.TotalDue,
        c.CustomerID,
        p.FirstName,
        p.LastName,
        a.AddressLine1,
        a.City,
        sp.Name AS StateProvince,
        cr.Name AS Country
    FROM Sales.SalesOrderHeader soh
    INNER JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
    INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    INNER JOIN Person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
    INNER JOIN Person.Address a ON bea.AddressID = a.AddressID
    INNER JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
    INNER JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
    WHERE soh.OrderDate >= '2013-01-01';
"""

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

SUBQUERY_WITH_CTE = """
    WITH SalesSummary AS (
        SELECT
            soh.SalesPersonID,
            YEAR(soh.OrderDate) AS OrderYear,
            SUM(soh.TotalDue) AS YearlyTotal
        FROM Sales.SalesOrderHeader soh
        WHERE soh.SalesPersonID IS NOT NULL
        GROUP BY soh.SalesPersonID, YEAR(soh.OrderDate)
    ),
    RankedSales AS (
        SELECT
            SalesPersonID,
            OrderYear,
            YearlyTotal,
            RANK() OVER (PARTITION BY OrderYear ORDER BY YearlyTotal DESC) AS SalesRank
        FROM SalesSummary
    )
    SELECT
        rs.SalesPersonID,
        p.FirstName,
        p.LastName,
        rs.OrderYear,
        rs.YearlyTotal,
        rs.SalesRank
    FROM RankedSales rs
    INNER JOIN Person.Person p ON rs.SalesPersonID = p.BusinessEntityID
    WHERE rs.SalesRank <= 10
    ORDER BY rs.OrderYear DESC, rs.SalesRank;
"""


class BenchmarkResult:
    """Class to store and calculate benchmark statistics"""
    
    def __init__(self, name: str):
        self.name = name
        self.times: List[float] = []
        self.row_count: int = 0
    
    def add_time(self, elapsed: float, rows: int = 0):
        """Add a timing result"""
        self.times.append(elapsed)
        if rows > 0:
            self.row_count = rows
    
    @property
    def avg_time(self) -> float:
        """Calculate average time"""
        return statistics.mean(self.times) if self.times else 0.0
    
    @property
    def min_time(self) -> float:
        """Get minimum time"""
        return min(self.times) if self.times else 0.0
    
    @property
    def max_time(self) -> float:
        """Get maximum time"""
        return max(self.times) if self.times else 0.0
    
    @property
    def std_dev(self) -> float:
        """Calculate standard deviation"""
        return statistics.stdev(self.times) if len(self.times) > 1 else 0.0
    
    def __str__(self) -> str:
        """Format results as string"""
        return (f"{self.name}:\n"
                f"  Avg: {self.avg_time:.4f}s | Min: {self.min_time:.4f}s | "
                f"Max: {self.max_time:.4f}s | StdDev: {self.std_dev:.4f}s | "
                f"Rows: {self.row_count}")


def run_benchmark_pyodbc(query: str, name: str, iterations: int) -> BenchmarkResult:
    """Run a benchmark using pyodbc"""
    result = BenchmarkResult(f"{name} (pyodbc)")
    
    for i in range(iterations):
        try:
            start_time = time.time()
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            elapsed = time.time() - start_time
            
            result.add_time(elapsed, len(rows))
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"  Error in iteration {i+1}: {e}")
            continue
    
    return result


def run_benchmark_mssql_python(query: str, name: str, iterations: int) -> BenchmarkResult:
    """Run a benchmark using mssql-python"""
    result = BenchmarkResult(f"{name} (mssql-python)")
    
    for i in range(iterations):
        try:
            start_time = time.time()
            conn = connect(CONN_STR)
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            elapsed = time.time() - start_time
            
            result.add_time(elapsed, len(rows))
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"  Error in iteration {i+1}: {e}")
            continue
    
    return result


def calculate_speedup(pyodbc_result: BenchmarkResult, mssql_python_result: BenchmarkResult) -> float:
    """Calculate speedup factor"""
    if mssql_python_result.avg_time == 0:
        return 0.0
    return pyodbc_result.avg_time / mssql_python_result.avg_time


def print_comparison(pyodbc_result: BenchmarkResult, mssql_python_result: BenchmarkResult):
    """Print detailed comparison of results"""
    speedup = calculate_speedup(pyodbc_result, mssql_python_result)
    
    print(f"\n{'='*80}")
    print(f"BENCHMARK: {pyodbc_result.name.split(' (')[0]}")
    print(f"{'='*80}")
    print(f"\npyodbc:")
    print(f"  Avg: {pyodbc_result.avg_time:.4f}s")
    print(f"  Min: {pyodbc_result.min_time:.4f}s")
    print(f"  Max: {pyodbc_result.max_time:.4f}s")
    print(f"  StdDev: {pyodbc_result.std_dev:.4f}s")
    print(f"  Rows: {pyodbc_result.row_count}")
    
    print(f"\nmssql-python:")
    print(f"  Avg: {mssql_python_result.avg_time:.4f}s")
    print(f"  Min: {mssql_python_result.min_time:.4f}s")
    print(f"  Max: {mssql_python_result.max_time:.4f}s")
    print(f"  StdDev: {mssql_python_result.std_dev:.4f}s")
    print(f"  Rows: {mssql_python_result.row_count}")
    
    print(f"\nPerformance:")
    if speedup > 1:
        print(f"  mssql-python is {speedup:.2f}x FASTER than pyodbc")
    elif speedup < 1 and speedup > 0:
        print(f"  mssql-python is {1/speedup:.2f}x SLOWER than pyodbc")
    else:
        print(f"  Unable to calculate speedup")
    
    print(f"  Time difference: {(pyodbc_result.avg_time - mssql_python_result.avg_time):.4f}s")


def main():
    """Main benchmark runner"""
    print("="*80)
    print("PERFORMANCE BENCHMARKING: mssql-python vs pyodbc")
    print("="*80)
    print(f"\nConfiguration:")
    print(f"  Iterations per test: {NUM_ITERATIONS}")
    print(f"  Database: AdventureWorks2022")
    print(f"\n")
    
    # Define benchmarks
    benchmarks = [
        (COMPLEX_JOIN_AGGREGATION, "Complex Join Aggregation"),
        (LARGE_DATASET, "Large Dataset Retrieval"),
        (VERY_LARGE_DATASET, "Very Large Dataset (1.2M rows)"),
        (SUBQUERY_WITH_CTE, "Subquery with CTE"),
    ]
    
    # Store all results for summary
    all_results: List[Tuple[BenchmarkResult, BenchmarkResult]] = []
    
    # Run each benchmark
    for query, name in benchmarks:
        print(f"\nRunning: {name}")
        print(f"  Testing with pyodbc... ", end="", flush=True)
        pyodbc_result = run_benchmark_pyodbc(query, name, NUM_ITERATIONS)
        print(f"OK (avg: {pyodbc_result.avg_time:.4f}s)")
        
        print(f"  Testing with mssql-python... ", end="", flush=True)
        mssql_python_result = run_benchmark_mssql_python(query, name, NUM_ITERATIONS)
        print(f"OK (avg: {mssql_python_result.avg_time:.4f}s)")
        
        all_results.append((pyodbc_result, mssql_python_result))
    
    # Print detailed comparisons
    print("\n\n" + "="*80)
    print("DETAILED RESULTS")
    print("="*80)
    
    for pyodbc_result, mssql_python_result in all_results:
        print_comparison(pyodbc_result, mssql_python_result)
    
    # Print summary table
    print("\n\n" + "="*80)
    print("SUMMARY TABLE")
    print("="*80)
    print(f"\n{'Benchmark':<35} {'pyodbc (s)':<15} {'mssql-python (s)':<20} {'Speedup'}")
    print("-" * 80)
    
    total_pyodbc = 0.0
    total_mssql_python = 0.0
    
    for pyodbc_result, mssql_python_result in all_results:
        name = pyodbc_result.name.split(' (')[0]
        speedup = calculate_speedup(pyodbc_result, mssql_python_result)
        
        total_pyodbc += pyodbc_result.avg_time
        total_mssql_python += mssql_python_result.avg_time
        
        print(f"{name:<35} {pyodbc_result.avg_time:<15.4f} {mssql_python_result.avg_time:<20.4f} {speedup:.2f}x")
    
    print("-" * 80)
    print(f"{'TOTAL':<35} {total_pyodbc:<15.4f} {total_mssql_python:<20.4f} "
          f"{total_pyodbc/total_mssql_python if total_mssql_python > 0 else 0:.2f}x")
    
    # Overall conclusion
    overall_speedup = total_pyodbc / total_mssql_python if total_mssql_python > 0 else 0
    print(f"\n{'='*80}")
    print("OVERALL CONCLUSION")
    print("="*80)
    if overall_speedup > 1:
        print(f"\nmssql-python is {overall_speedup:.2f}x FASTER than pyodbc on average")
        print(f"Total time saved: {total_pyodbc - total_mssql_python:.4f}s ({((total_pyodbc - total_mssql_python)/total_pyodbc*100):.1f}%)")
    elif overall_speedup < 1 and overall_speedup > 0:
        print(f"\nmssql-python is {1/overall_speedup:.2f}x SLOWER than pyodbc on average")
        print(f"Total time difference: {total_mssql_python - total_pyodbc:.4f}s ({((total_mssql_python - total_pyodbc)/total_mssql_python*100):.1f}%)")
    
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
