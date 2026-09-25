"""Fixed workloads shared by the base and candidate profiler benchmark builds."""

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Barrier, Event
import time

from profiler import scenarios

# Preserve the AdventureWorks workloads from the previous CI benchmark.
QUERIES = {
    "join_aggregation": """
        SELECT p.ProductID, p.Name AS ProductName, pc.Name AS Category,
               psc.Name AS Subcategory, COUNT(sod.SalesOrderDetailID) AS TotalOrders,
               SUM(sod.OrderQty) AS TotalQuantity, SUM(sod.LineTotal) AS TotalRevenue,
               AVG(sod.UnitPrice) AS AvgPrice
        FROM Sales.SalesOrderDetail sod
        INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
        INNER JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        INNER JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        GROUP BY p.ProductID, p.Name, pc.Name, psc.Name
        HAVING SUM(sod.LineTotal) > 10000 ORDER BY TotalRevenue DESC
    """,
    "large_fetch": """
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
    "fetch_1_2m": """
        SELECT sod.SalesOrderID, sod.SalesOrderDetailID, sod.ProductID,
               sod.OrderQty, sod.UnitPrice, sod.LineTotal,
               p.Name AS ProductName, p.ProductNumber, p.Color, p.ListPrice,
               n1.number AS RowMultiplier1
        FROM Sales.SalesOrderDetail sod
        CROSS JOIN (SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
                    FROM Sales.SalesOrderDetail) n1
        INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
    """,
    "cte": """
        WITH SalesSummary AS (
            SELECT soh.SalesPersonID, YEAR(soh.OrderDate) AS OrderYear,
                   SUM(soh.TotalDue) AS YearlyTotal
            FROM Sales.SalesOrderHeader soh WHERE soh.SalesPersonID IS NOT NULL
            GROUP BY soh.SalesPersonID, YEAR(soh.OrderDate)
        ), RankedSales AS (
            SELECT SalesPersonID, OrderYear, YearlyTotal,
                   RANK() OVER (PARTITION BY OrderYear ORDER BY YearlyTotal DESC) AS SalesRank
            FROM SalesSummary
        )
        SELECT rs.SalesPersonID, p.FirstName, p.LastName,
               rs.OrderYear, rs.YearlyTotal, rs.SalesRank
        FROM RankedSales rs INNER JOIN Person.Person p ON rs.SalesPersonID = p.BusinessEntityID
        WHERE rs.SalesRank <= 10 ORDER BY rs.OrderYear DESC, rs.SalesRank
    """,
}


def query(conn, ctx, sql):
    with conn.cursor() as cursor:
        ctx.enable()
        try:
            start = time.perf_counter()
            cursor.execute(sql)
            rows = cursor.fetchall()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            return dict(
                title="AdventureWorks query",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=f"Rows: {len(rows)}",
            )
        finally:
            ctx.disable()


def parameter_execution(conn, ctx, named=False):
    with conn.cursor() as cursor:
        ctx.enable()
        try:
            start = time.perf_counter()
            for value in range(100):
                if named:
                    cursor.execute("SELECT %(value)s", {"value": value})
                else:
                    cursor.execute("SELECT ?", (value,))
                assert cursor.fetchone()[0] == value
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            return dict(
                title="Parameterized execution", wall_ms=wall_ms, cpp=cpp, py=py, detail="Rows: 100"
            )
        finally:
            ctx.disable()


def legacy_insertmany(conn, ctx, input_sizes=False):
    from mssql_python import SQL_INTEGER, SQL_VARCHAR

    sql = "INSERT INTO #ci_insert VALUES " + ",".join(["(?,?)"] * 1000)
    batches = [
        [value for i in range(start, start + 1000) for value in (i, f"value_{i}")]
        for start in range(0, 100_000, 1000)
    ]
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                "DROP TABLE IF EXISTS #ci_insert; "
                "CREATE TABLE #ci_insert (id INT, val VARCHAR(100))"
            )
            sizes = [(SQL_INTEGER, 0, 0), (SQL_VARCHAR, 100, 0)] * 1000
            ctx.enable()
            start = time.perf_counter()
            for params in batches:
                if input_sizes:
                    cursor.setinputsizes(sizes)
                cursor.execute(sql, params)
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            return dict(
                title="Batched insert", wall_ms=wall_ms, cpp=cpp, py=py, detail="Rows: 100000"
            )
        finally:
            ctx.disable()
            conn.rollback()


def lob_fetch(conn, ctx):
    """Fetch one multi-chunk value; setup and exact-value validation are not timed."""
    payload_bytes = 256 * 1024
    expression = f"REPLICATE(CAST('x' AS VARCHAR(MAX)), {payload_bytes})"
    expected = "x" * payload_bytes

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT {expression} AS payload")
        ctx.enable()
        try:
            start = time.perf_counter()
            rows = cursor.fetchall()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert len(rows) == 1
            row = rows[0]
            assert row is not None and len(row) == 1
            assert type(row[0]) is type(expected) and row[0] == expected
            assert not cursor.messages, "Clean LOB fetch unexpectedly produced diagnostics"
            return dict(
                title="Multi-chunk LOB fetch",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=f"Rows: 1; type: varchar; payload bytes: {payload_bytes}; API: fetchall",
            )
        finally:
            ctx.disable()


def _connect_close(connection_string):
    from mssql_python import connect

    connect(connection_string).close()


def _prewarm_pool(connection_string, connections):
    from mssql_python import connect

    opened = []
    try:
        for _ in range(connections):
            opened.append(connect(connection_string))
    finally:
        for connection in opened:
            connection.close()


def pooled_connect_close(connection_string, ctx, connections=1000):
    """Measure steady-state pooled checkout/check-in, including close."""
    _prewarm_pool(connection_string, 1)
    ctx.enable()
    try:
        start = time.perf_counter()
        for _ in range(connections):
            _connect_close(connection_string)
        wall_ms = (time.perf_counter() - start) * 1000
        cpp, py = ctx.collect()
        return dict(
            title="Pooled connection lifecycle",
            wall_ms=wall_ms,
            cpp=cpp,
            py=py,
            detail=f"Connections: {connections}; workers: 1",
        )
    finally:
        ctx.disable()


def pooled_parallel_connect_close(connection_string, ctx, workers=10, connections_per_worker=100):
    """Measure concurrent steady-state pooled checkout/check-in."""
    _prewarm_pool(connection_string, workers)
    ready = Barrier(workers + 1)
    start_workers = Event()

    def worker():
        ready.wait()
        start_workers.wait()
        for _ in range(connections_per_worker):
            _connect_close(connection_string)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker) for _ in range(workers)]
        ready.wait()
        ctx.enable()
        try:
            start = time.perf_counter()
            start_workers.set()
            for future in futures:
                future.result()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            return dict(
                title="Parallel pooled connection lifecycle",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=f"Connections: {workers * connections_per_worker}; workers: {workers}",
            )
        finally:
            start_workers.set()
            ctx.disable()


def registry():
    """Keep every PR #552 scenario, including its existing timing boundaries."""
    result = dict(scenarios.SCENARIOS)
    result.update(
        fetchmany_100=(partial(scenarios.fetchmany, batch_size=100), True),
        fetchmany_10000=(partial(scenarios.fetchmany, batch_size=10000), True),
        prepared_qmark=(parameter_execution, False),
        prepared_named=(partial(parameter_execution, named=True), False),
        legacy_insertmany=(legacy_insertmany, False),
        setinputsizes=(partial(legacy_insertmany, input_sizes=True), False),
    )
    result.update((name, (partial(query, sql=sql), False)) for name, sql in QUERIES.items())
    result["lob_varchar_256k_fetchall"] = (lob_fetch, False)
    result["pooled_connect_close"] = (pooled_connect_close, False)
    result["pooled_parallel_connect_close"] = (pooled_parallel_connect_close, False)
    return result
