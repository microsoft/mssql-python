"""Reproduce the wide, high-row-count Windows fetch reported in GH-554."""

import argparse
import gc
import json
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyodbc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import mssql_python

MEASURED_PAIRS = 3
WARMUP_PAIRS = 1

FROM_CLAUSE = """
FROM Sales.SalesOrderDetail AS sod
CROSS JOIN (
    SELECT TOP (14) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS copy_number
    FROM Sales.SalesOrderDetail
) AS copies
INNER JOIN Production.Product AS p ON p.ProductID = sod.ProductID
"""

QUERY = """
SELECT
    sod.SalesOrderID,
    sod.SalesOrderDetailID,
    sod.CarrierTrackingNumber,
    sod.OrderQty,
    sod.ProductID,
    sod.SpecialOfferID,
    sod.UnitPrice,
    sod.UnitPriceDiscount,
    sod.LineTotal,
    sod.rowguid,
    sod.ModifiedDate,
    p.Name,
    p.ProductNumber,
    p.MakeFlag,
    p.FinishedGoodsFlag,
    p.Color,
    p.SafetyStockLevel,
    p.ReorderPoint,
    p.StandardCost,
    p.ListPrice,
    p.Size,
    p.Weight,
    p.DaysToManufacture,
    p.SellStartDate
""" + FROM_CLAUSE

EXPECTED_QUERY = """
SELECT
    COUNT_BIG(*),
    SUM(CONVERT(BIGINT, sod.SalesOrderDetailID))
""" + FROM_CLAUSE


def connection_strings():
    value = os.environ.get("DB_CONNECTION_STRING")
    if not value:
        raise RuntimeError("DB_CONNECTION_STRING is required")
    pyodbc_value = (
        value if "driver=" in value.lower() else f"Driver={{ODBC Driver 18 for SQL Server}};{value}"
    )
    mssql_value = ";".join(
        part for part in value.split(";") if not part.strip().lower().startswith("driver=")
    )
    return mssql_value, pyodbc_value


def expected_result(pyodbc_connection_string):
    with pyodbc.connect(pyodbc_connection_string) as connection:
        row = connection.cursor().execute(EXPECTED_QUERY).fetchone()
    return int(row[0]), int(row[1])


def measure(connect, connection_string, expected_rows, expected_sum):
    with connect(connection_string) as connection:
        cursor = connection.cursor()
        started = time.perf_counter()
        rows = cursor.execute(QUERY).fetchall()
        elapsed = time.perf_counter() - started

    if len(rows) != expected_rows:
        raise RuntimeError(f"Expected {expected_rows} rows, received {len(rows)}")
    if rows and len(rows[0]) != 24:
        raise RuntimeError(f"Expected 24 columns, received {len(rows[0])}")
    actual_sum = sum(int(row[1]) for row in rows)
    if actual_sum != expected_sum:
        raise RuntimeError(f"Expected detail-id sum {expected_sum}, received {actual_sum}")

    del rows
    gc.collect()
    return elapsed


def run():
    mssql_connection_string, pyodbc_connection_string = connection_strings()
    expected_rows, expected_sum = expected_result(pyodbc_connection_string)
    timings = {"mssql-python": [], "pyodbc": []}
    drivers = {
        "mssql-python": (mssql_python.connect, mssql_connection_string),
        "pyodbc": (pyodbc.connect, pyodbc_connection_string),
    }

    for pair in range(WARMUP_PAIRS + MEASURED_PAIRS):
        order = ("pyodbc", "mssql-python") if pair % 2 == 0 else ("mssql-python", "pyodbc")
        for name in order:
            elapsed = measure(*drivers[name], expected_rows, expected_sum)
            print(f"pair={pair + 1} driver={name} seconds={elapsed:.6f}", flush=True)
            if pair >= WARMUP_PAIRS:
                timings[name].append(elapsed)

    mssql_median = statistics.median(timings["mssql-python"])
    pyodbc_median = statistics.median(timings["pyodbc"])
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "os": platform.platform(),
            "python": platform.python_version(),
        },
        "rows": expected_rows,
        "columns": 24,
        "measured_pairs": MEASURED_PAIRS,
        "warmup_pairs": WARMUP_PAIRS,
        "timings_seconds": timings,
        "median_seconds": {
            "mssql-python": mssql_median,
            "pyodbc": pyodbc_median,
        },
        "mssql_to_pyodbc_ratio": mssql_median / pyodbc_median,
    }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    result = run()
    print(json.dumps(result, indent=2))
    if args.json:
        args.json.write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
