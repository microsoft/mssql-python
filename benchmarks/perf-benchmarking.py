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

Methodology (variance control):
    Each scenario runs NUM_ITERATIONS timed passes and reports the median, discarding
    the first WARMUP_ITERATIONS samples (cold cache / plan compile). The "vs main"
    comparison uses a normalized score instead of raw time: each run expresses
    mssql-python's time relative to pyodbc measured on the same runner (see
    normalized_score()). Because pyodbc is a pinned build, dividing by it cancels the
    runner's raw speed, so scores are comparable across CI runs even on different
    hardware -- which is the dominant source of run-to-run noise. Measured on ~11
    historical main runs, normalization roughly halves variance on the I/O-bound
    scenarios (CV 18%->8%, 11%->5%) and trims it on the rest; the gate threshold is
    set above the residual.
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
WARMUP_ITERATIONS = 1  # first N timed samples are discarded (no extra DB passes)
MIN_SAMPLES = 3        # need at least this many successful samples to gate
INSERTMANY_ROWS = 100_000
INSERTMANY_BATCH_SIZE = 1000

# Regression/highlight gate thresholds, applied to the normalized score (see
# normalized_score()). Because that score cancels machine-to-machine speed, the
# residual historical CI variance is only ~5-15% CV per scenario, so the threshold
# is set above that; real perf changes move numbers by 30%+, well clear of it. Tunable.
REGRESSION_THRESHOLD = 0.20
HIGHLIGHT_THRESHOLD = 0.20


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
    def median(self) -> float:
        return statistics.median(self.times) if self.times else 0.0

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
            "median": round(self.median, 6),
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
    for i in range(iterations):
        conn = None
        try:
            conn = pyodbc.connect(CONN_STR_PYODBC)
            cursor = conn.cursor()
            start = time.perf_counter()
            cursor.execute(query)
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - start
            if i >= WARMUP_ITERATIONS:
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
    for i in range(iterations):
        conn = None
        try:
            conn = connect(CONN_STR)
            cursor = conn.cursor()
            start = time.perf_counter()
            cursor.execute(query)
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - start
            if i >= WARMUP_ITERATIONS:
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

    for i in range(iterations):
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

            if i >= WARMUP_ITERATIONS:
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


def normalized_score(mssql_median: float, pyodbc_median: float) -> Optional[float]:
    """Runner-independent score for one scenario: how long mssql-python took
    relative to pyodbc on the same runner (mssql-python median / pyodbc median).

    pyodbc is a pinned build, so it soaks up the runner's raw speed. Expressing
    mssql-python's time as a multiple of pyodbc's cancels machine-to-machine speed
    differences, so two scores are comparable across CI runs even on different
    hardware. Lower is better; None when pyodbc has no valid measurement.
    """
    return (mssql_median / pyodbc_median) if pyodbc_median > 0 else None


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
        print("(vs main uses each run's normalized score = mssql-python time relative to")
        print(" pyodbc on the same runner, which cancels runner speed; metric = median)")
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
        pr_med = mssql_result.median
        py_med = pyodbc_result.median

        if has_baseline and name in baseline:
            b = baseline[name]
            main_med = b.get("median", b.get("avg", 0.0))
            # main's normalized score, stored in the baseline by save_json().
            main_score = b.get("norm")
            # this PR's normalized score, computed from this run's own numbers.
            pr_score = normalized_score(pr_med, py_med)
            enough = (len(mssql_result.times) >= MIN_SAMPLES
                      and len(pyodbc_result.times) >= MIN_SAMPLES)
            vs_pyodbc = _ratio_str(pr_med, py_med)

            if main_score is not None and pr_score is not None and enough:
                # Both scores are runner-independent (each divides out its own
                # runner's pyodbc), so comparing them is a fair PR-vs-main check
                # even when the two runs landed on different hardware.
                vs_main = _ratio_str(pr_score, main_score)
                if pr_score > main_score * (1 + REGRESSION_THRESHOLD):
                    regressions.append((name, main_score, pr_score, True))
                if pr_score < main_score * (1 - HIGHLIGHT_THRESHOLD):
                    highlights.append((name, main_score, pr_score, True))
            elif main_med > 0 and pr_med > 0 and len(mssql_result.times) >= MIN_SAMPLES:
                # Fallback to raw wall-clock: baseline predates the normalized
                # score, or pyodbc had no valid run to normalize against.
                vs_main = _ratio_str(pr_med, main_med)
                if pr_med > main_med * (1 + REGRESSION_THRESHOLD):
                    regressions.append((name, main_med, pr_med, False))
                if pr_med < main_med * (1 - HIGHLIGHT_THRESHOLD):
                    highlights.append((name, main_med, pr_med, False))
            else:
                # Too few samples / no usable baseline -> show, don't gate.
                vs_main = "inconclusive"

            print(
                f"{name:<40} {main_med:<10.4f} {pr_med:<10.4f} {py_med:<10.4f} "
                f"{vs_main:<16} {vs_pyodbc:<16}"
            )
        else:
            vs_pyodbc = _ratio_str(pr_med, py_med)
            if has_baseline:
                print(
                    f"{name:<40} {'N/A':<10} {pr_med:<10.4f} {py_med:<10.4f} "
                    f"{'N/A':<16} {vs_pyodbc:<16}"
                )
            else:
                print(f"{name:<40} {pr_med:<10.4f} {py_med:<10.4f} {vs_pyodbc:<16}")

    print("-" * 100)

    if has_baseline:
        rpct = int(REGRESSION_THRESHOLD * 100)
        hpct = int(HIGHLIGHT_THRESHOLD * 100)

        print(f"\n{'='*100}")
        if regressions:
            print(f"REGRESSIONS (>{rpct}% slower than main, normalized)")
            print("=" * 100)
            for name, old, new, normed in regressions:
                factor = new / old if old else 0
                unit = "" if normed else "s"
                print(f"  {name}: {old:.4f}{unit} -> {new:.4f}{unit} ({factor:.2f}x slower)")
        else:
            print(f"REGRESSIONS (>{rpct}% slower than main, normalized): None detected")

        print(f"\n{'='*100}")
        if highlights:
            print(f"HIGHLIGHTS (>{hpct}% faster than main, normalized)")
            print("=" * 100)
            for name, old, new, normed in highlights:
                factor = old / new if new else 0
                unit = "" if normed else "s"
                print(f"  {name}: {old:.4f}{unit} -> {new:.4f}{unit} ({factor:.2f}x faster)")
        else:
            print(f"HIGHLIGHTS (>{hpct}% faster than main, normalized): None")

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
    # For baseline consumption, store per-scenario metrics plus the normalized score
    # (see normalized_score()). PR runs compare their own normalized score against the
    # stored `norm` so that runner-to-runner speed differences cancel out.
    data["baseline"] = {}
    for name, mssql_result, pyodbc_result in results:
        entry = mssql_result.to_dict()
        py_med = pyodbc_result.median
        entry["pyodbc_median"] = round(py_med, 6)
        score = normalized_score(mssql_result.median, py_med)
        entry["norm"] = round(score, 6) if score is not None else None
        data["baseline"][name] = entry
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
    print(f"  Iterations: {NUM_ITERATIONS} (first {WARMUP_ITERATIONS} discarded as warmup, metric: median)")
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
            print(f"OK ({py_result.median:.4f}s)")
        else:
            print("FAILED")

        print(f"  mssql-python... ", end="", flush=True)
        ms_result = run_fetch_mssql(query, name, NUM_ITERATIONS)
        if ms_result.times:
            print(f"OK ({ms_result.median:.4f}s)")
        else:
            print("FAILED")

        all_results.append((name, ms_result, py_result))

    # Insertmanyvalues scenario (uses temp table, no AdventureWorks needed)
    insert_name = "Insertmanyvalues (100K rows)"
    print(f"\nRunning: {insert_name}")
    print(f"  pyodbc...       ", end="", flush=True)
    py_insert = run_insertmany_pyodbc(NUM_ITERATIONS)
    if py_insert.times:
        print(f"OK ({py_insert.median:.4f}s)")
    else:
        print("FAILED")

    print(f"  mssql-python... ", end="", flush=True)
    ms_insert = run_insertmany_mssql(NUM_ITERATIONS)
    if ms_insert.times:
        print(f"OK ({ms_insert.median:.4f}s)")
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
