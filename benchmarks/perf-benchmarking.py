"""
Performance Benchmarking: mssql-python vs pyodbc

Runs scenarios (fetch queries + insertmanyvalues), compares mssql-python against pyodbc,
and optionally against a baseline JSON from the main branch.

Usage:
    python benchmarks/perf-benchmarking.py                          # 2-col: PR vs pyodbc
    python benchmarks/perf-benchmarking.py --baseline baseline.json # 3-col: main vs PR vs pyodbc
    python benchmarks/perf-benchmarking.py --json results.json      # save results to JSON

Environment:
    DB_CONNECTION_STRING  — required, e.g. Server=localhost;Database=...;Uid=sa;Pwd=...;TrustServerCertificate=yes
"""

import argparse
import json
import os
import sys
import time
import statistics
from datetime import datetime, timezone
from typing import List, Optional

# Add parent directory to path to import local mssql_python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pyodbc
from mssql_python import connect

# Configuration
CONN_STR = os.getenv("DB_CONNECTION_STRING")
CONN_STR_PYODBC = None

NUM_ITERATIONS = 10
INSERTMANY_ROWS = 100_000
INSERTMANY_BATCH_SIZE = 1000


def _init_conn_strings():
    global CONN_STR, CONN_STR_PYODBC
    if not CONN_STR:
        print(
            "Error: The environment variable DB_CONNECTION_STRING is not set. "
            "Please set it to a valid SQL Server connection string and try again."
        )
        sys.exit(1)
    if "Driver=" not in CONN_STR:
        CONN_STR_PYODBC = f"Driver={{ODBC Driver 18 for SQL Server}};{CONN_STR}"
    else:
        CONN_STR_PYODBC = CONN_STR
        # mssql-python manages its own driver and rejects Driver= in the
        # connection string.  Strip it so both drivers can share one env var.
        parts = [p for p in CONN_STR.split(";") if not p.strip().lower().startswith("driver=")]
        CONN_STR = ";".join(parts)



class BenchmarkResult:
    def __init__(self, name: str):
        self.name = name
        self.times: List[float] = []
        self.row_count: int = 0

    def add_time(self, elapsed: float, rows: int = 0):
        self.times.append(elapsed)
        if rows > 0:
            self.row_count = rows

    @property
    def avg(self) -> float:
        return statistics.mean(self.times) if self.times else 0.0

    @property
    def min(self) -> float:
        return min(self.times) if self.times else 0.0

    @property
    def max(self) -> float:
        return max(self.times) if self.times else 0.0

    @property
    def stddev(self) -> float:
        return statistics.stdev(self.times) if len(self.times) > 1 else 0.0

    def to_dict(self) -> dict:
        return {
            "avg": round(self.avg, 6),
            "min": round(self.min, 6),
            "max": round(self.max, 6),
            "stddev": round(self.stddev, 6),
            "rows": self.row_count,
            "iterations": len(self.times),
        }


# ---------------------------------------------------------------------------
# Fetch scenario runners
# ---------------------------------------------------------------------------

def run_fetch_pyodbc(query: str, name: str, iterations: int) -> BenchmarkResult:
    result = BenchmarkResult(name)
    for _ in range(iterations):
        conn = None
        try:
            conn = pyodbc.connect(CONN_STR_PYODBC)
            cursor = conn.cursor()
            start = time.perf_counter()
            cursor.execute(query)
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - start
            result.add_time(elapsed, len(rows))
        except Exception as e:
            print(f"    pyodbc error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    return result


def run_fetch_mssql(query: str, name: str, iterations: int) -> BenchmarkResult:
    result = BenchmarkResult(name)
    for _ in range(iterations):
        conn = None
        try:
            conn = connect(CONN_STR)
            cursor = conn.cursor()
            start = time.perf_counter()
            cursor.execute(query)
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - start
            result.add_time(elapsed, len(rows))
        except Exception as e:
            print(f"    mssql-python error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    return result


# ---------------------------------------------------------------------------
# Insertmanyvalues scenario
# ---------------------------------------------------------------------------

def _build_batch_sql(batch_size: int) -> str:
    placeholders = ",".join(["(?,?)"] * batch_size)
    return f"INSERT INTO #bench_insert VALUES {placeholders}"


def _generate_rows(total: int) -> list:
    return [(i, f"value_{i}") for i in range(total)]


def _run_insertmany(conn_factory, conn_str, name: str, iterations: int) -> BenchmarkResult:
    result = BenchmarkResult(name)
    batch_sql = _build_batch_sql(INSERTMANY_BATCH_SIZE)
    all_rows = _generate_rows(INSERTMANY_ROWS)

    # Pre-build flat param lists per batch
    batches = []
    for start in range(0, INSERTMANY_ROWS, INSERTMANY_BATCH_SIZE):
        chunk = all_rows[start : start + INSERTMANY_BATCH_SIZE]
        flat = []
        for row in chunk:
            flat.extend(row)
        batches.append(flat)

    for _ in range(iterations):
        conn = None
        try:
            conn = conn_factory(conn_str)
            cursor = conn.cursor()
            cursor.execute(
                "IF OBJECT_ID('tempdb..#bench_insert') IS NOT NULL DROP TABLE #bench_insert; "
                "CREATE TABLE #bench_insert (id INT, val VARCHAR(100))"
            )

            start = time.perf_counter()
            for flat_params in batches:
                cursor.execute(batch_sql, flat_params)
            elapsed = time.perf_counter() - start

            result.add_time(elapsed, INSERTMANY_ROWS)
        except Exception as e:
            print(f"    {name} error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    return result


def run_insertmany_pyodbc(iterations: int) -> BenchmarkResult:
    return _run_insertmany(
        lambda cs: pyodbc.connect(cs), CONN_STR_PYODBC,
        "Insertmanyvalues (100K rows)", iterations,
    )


def run_insertmany_mssql(iterations: int) -> BenchmarkResult:
    return _run_insertmany(
        lambda cs: connect(cs), CONN_STR,
        "Insertmanyvalues (100K rows)", iterations,
    )


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _ratio_str(a: float, b: float) -> str:
    """Return 'Nx faster/slower' comparing a to b (lower is better)."""
    if a == 0 or b == 0:
        return "N/A"
    if a <= b:
        factor = b / a
        return f"{factor:.1f}x faster"
    else:
        factor = a / b
        return f"{factor:.1f}x slower"


def print_results(
    results: List[tuple],
    baseline: Optional[dict],
):
    has_baseline = baseline is not None

    # Header
    print("\n" + "=" * 100)
    if has_baseline:
        print("RESULTS: main (baseline) vs this PR vs pyodbc")
    else:
        print("RESULTS: this PR vs pyodbc")
    print("=" * 100)

    if has_baseline:
        hdr = (
            f"\n{'Scenario':<40} {'main':<10} {'this PR':<10} {'pyodbc':<10} "
            f"{'vs main':<16} {'vs pyodbc':<16}"
        )
    else:
        hdr = f"\n{'Scenario':<40} {'this PR':<10} {'pyodbc':<10} {'vs pyodbc':<16}"
    print(hdr)
    print("-" * 100)

    regressions = []
    highlights = []

    for name, mssql_result, pyodbc_result in results:
        pr_avg = mssql_result.avg
        py_avg = pyodbc_result.avg

        if has_baseline and name in baseline:
            main_avg = baseline[name]["avg"]
            vs_main = _ratio_str(pr_avg, main_avg)
            vs_pyodbc = _ratio_str(pr_avg, py_avg)
            print(
                f"{name:<40} {main_avg:<10.4f} {pr_avg:<10.4f} {py_avg:<10.4f} "
                f"{vs_main:<16} {vs_pyodbc:<16}"
            )
            if main_avg > 0 and pr_avg > main_avg * 1.05:
                regressions.append((name, main_avg, pr_avg))
            if main_avg > 0 and pr_avg < main_avg * 0.90:
                highlights.append((name, main_avg, pr_avg))
        else:
            vs_pyodbc = _ratio_str(pr_avg, py_avg)
            if has_baseline:
                print(
                    f"{name:<40} {'N/A':<10} {pr_avg:<10.4f} {py_avg:<10.4f} "
                    f"{'N/A':<16} {vs_pyodbc:<16}"
                )
            else:
                print(f"{name:<40} {pr_avg:<10.4f} {py_avg:<10.4f} {vs_pyodbc:<16}")

    print("-" * 100)

    if has_baseline:
        print(f"\n{'='*100}")
        if regressions:
            print("REGRESSIONS (>5% slower than main)")
            print("=" * 100)
            for name, main_avg, pr_avg in regressions:
                factor = pr_avg / main_avg
                print(f"  {name}: {main_avg:.4f}s -> {pr_avg:.4f}s ({factor:.1f}x slower)")
        else:
            print("REGRESSIONS (>5% slower than main): None detected")

        print(f"\n{'='*100}")
        if highlights:
            print("HIGHLIGHTS (>10% faster than main)")
            print("=" * 100)
            for name, main_avg, pr_avg in highlights:
                factor = main_avg / pr_avg
                print(f"  {name}: {main_avg:.4f}s -> {pr_avg:.4f}s ({factor:.1f}x faster)")
        else:
            print("HIGHLIGHTS (>10% faster than main): None")

    print(f"\n{'='*100}\n")


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------

def save_json(results: List[tuple], path: str):
    data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "iterations": NUM_ITERATIONS,
        "scenarios": {},
    }
    for name, mssql_result, pyodbc_result in results:
        data["scenarios"][name] = {
            "mssql_python": mssql_result.to_dict(),
            "pyodbc": pyodbc_result.to_dict(),
        }
    # For baseline consumption, also store flat avg per scenario at top level
    data["baseline"] = {name: mssql_result.to_dict() for name, mssql_result, _ in results}
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {path}")


def load_baseline(path: str) -> Optional[dict]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        baseline = data.get("baseline")
        return baseline if baseline else None
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  Warning: could not parse baseline {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

FETCH_SCENARIOS = [
    (
        "Complex Join Aggregation",
        """
        SELECT p.ProductID, p.Name AS ProductName, pc.Name AS Category,
               psc.Name AS Subcategory, COUNT(sod.SalesOrderDetailID) AS TotalOrders,
               SUM(sod.OrderQty) AS TotalQuantity, SUM(sod.LineTotal) AS TotalRevenue,
               AVG(sod.UnitPrice) AS AvgPrice
        FROM Sales.SalesOrderDetail sod
        INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
        INNER JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        INNER JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        GROUP BY p.ProductID, p.Name, pc.Name, psc.Name
        HAVING SUM(sod.LineTotal) > 10000
        ORDER BY TotalRevenue DESC
        """,
    ),
    (
        "Large Dataset Retrieval",
        """
        SELECT soh.SalesOrderID, soh.OrderDate, soh.DueDate, soh.ShipDate, soh.Status,
               soh.SubTotal, soh.TaxAmt, soh.Freight, soh.TotalDue, c.CustomerID,
               p.FirstName, p.LastName, a.AddressLine1, a.City,
               sp.Name AS StateProvince, cr.Name AS Country
        FROM Sales.SalesOrderHeader soh
        INNER JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
        INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
        INNER JOIN Person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
        INNER JOIN Person.Address a ON bea.AddressID = a.AddressID
        INNER JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        INNER JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        WHERE soh.OrderDate >= '2013-01-01'
        """,
    ),
    (
        "Very Large Dataset (1.2M rows)",
        """
        SELECT sod.SalesOrderID, sod.SalesOrderDetailID, sod.ProductID,
               sod.OrderQty, sod.UnitPrice, sod.LineTotal,
               p.Name AS ProductName, p.ProductNumber, p.Color, p.ListPrice,
               n1.number AS RowMultiplier1
        FROM Sales.SalesOrderDetail sod
        CROSS JOIN (SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
                    FROM Sales.SalesOrderDetail) n1
        INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
        """,
    ),
    (
        "Subquery with CTE",
        """
        WITH SalesSummary AS (
            SELECT soh.SalesPersonID, YEAR(soh.OrderDate) AS OrderYear,
                   SUM(soh.TotalDue) AS YearlyTotal
            FROM Sales.SalesOrderHeader soh
            WHERE soh.SalesPersonID IS NOT NULL
            GROUP BY soh.SalesPersonID, YEAR(soh.OrderDate)
        ),
        RankedSales AS (
            SELECT SalesPersonID, OrderYear, YearlyTotal,
                   RANK() OVER (PARTITION BY OrderYear ORDER BY YearlyTotal DESC) AS SalesRank
            FROM SalesSummary
        )
        SELECT rs.SalesPersonID, p.FirstName, p.LastName,
               rs.OrderYear, rs.YearlyTotal, rs.SalesRank
        FROM RankedSales rs
        INNER JOIN Person.Person p ON rs.SalesPersonID = p.BusinessEntityID
        WHERE rs.SalesRank <= 10
        ORDER BY rs.OrderYear DESC, rs.SalesRank
        """,
    ),
]


def main():
    parser = argparse.ArgumentParser(description="mssql-python performance benchmarks")
    parser.add_argument("--json", metavar="PATH", help="Save results to JSON file")
    parser.add_argument("--baseline", metavar="PATH", help="Load baseline JSON from main branch")
    args = parser.parse_args()

    _init_conn_strings()

    baseline = load_baseline(args.baseline)

    print("=" * 100)
    if baseline:
        print("PERFORMANCE BENCHMARKING: mssql-python PR vs main vs pyodbc")
    else:
        if args.baseline:
            print("PERFORMANCE BENCHMARKING: mssql-python vs pyodbc")
            print("  (baseline file not found — showing 2-column comparison)")
        else:
            print("PERFORMANCE BENCHMARKING: mssql-python vs pyodbc")
    print("=" * 100)
    print(f"  Iterations: {NUM_ITERATIONS}")
    if baseline:
        print(f"  Baseline: {args.baseline}")
    print()

    all_results: List[tuple] = []

    # Fetch scenarios (require AdventureWorks)
    for name, query in FETCH_SCENARIOS:
        print(f"Running: {name}")
        print(f"  pyodbc...       ", end="", flush=True)
        py_result = run_fetch_pyodbc(query, name, NUM_ITERATIONS)
        if py_result.times:
            print(f"OK ({py_result.avg:.4f}s)")
        else:
            print("FAILED")

        print(f"  mssql-python... ", end="", flush=True)
        ms_result = run_fetch_mssql(query, name, NUM_ITERATIONS)
        if ms_result.times:
            print(f"OK ({ms_result.avg:.4f}s)")
        else:
            print("FAILED")

        all_results.append((name, ms_result, py_result))

    # Insertmanyvalues scenario (uses temp table, no AdventureWorks needed)
    insert_name = "Insertmanyvalues (100K rows)"
    print(f"\nRunning: {insert_name}")
    print(f"  pyodbc...       ", end="", flush=True)
    py_insert = run_insertmany_pyodbc(NUM_ITERATIONS)
    if py_insert.times:
        print(f"OK ({py_insert.avg:.4f}s)")
    else:
        print("FAILED")

    print(f"  mssql-python... ", end="", flush=True)
    ms_insert = run_insertmany_mssql(NUM_ITERATIONS)
    if ms_insert.times:
        print(f"OK ({ms_insert.avg:.4f}s)")
    else:
        print("FAILED")

    all_results.append((insert_name, ms_insert, py_insert))

    # Output
    print_results(all_results, baseline)

    if args.json:
        save_json(all_results, args.json)


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
