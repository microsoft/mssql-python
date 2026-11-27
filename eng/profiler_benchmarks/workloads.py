"""Fixed workloads shared by the base and candidate profiler benchmark builds."""

from functools import partial
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


def scalar_fetchval(conn, table, ctx):
    """Measure scalar fetching with debug disabled, excluding setup and validation."""
    from mssql_python.logging import logger

    if logger.is_debug_enabled:
        raise ValueError("scalar_fetchval requires debug logging to be disabled")

    expected = list(range(10_000))
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT TOP (10000) int_col FROM {table} ORDER BY id")
        ctx.enable()
        try:
            start = time.perf_counter()
            values = [cursor.fetchval() for _ in expected]
            eof = cursor.fetchval()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert values == expected and all(type(value) is int for value in values)
            assert eof is None, "Scalar fetch did not reach EOF"
            assert not cursor.messages, "Clean scalar fetch unexpectedly produced diagnostics"
            return dict(
                title="Scalar fetchval",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail="Rows: 10000; type: int; API: fetchval; debug: disabled",
            )
        finally:
            ctx.disable()


def fetchmany_one_text(conn, table, ctx):
    """Collect mixed rows one at a time; setup and validation are not timed."""
    expected = [(index, f"unicode_row_{index}") for index in range(10_000)]
    with conn.cursor() as cursor:
        try:
            cursor.execute(f"SELECT TOP (10000) int_col, nvarchar_col FROM {table} ORDER BY id")
            rows = []
            ctx.enable()
            start = time.perf_counter()
            while batch := cursor.fetchmany(1):
                rows.extend(batch)
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert [tuple(row) for row in rows] == expected
            assert all(type(row[0]) is int and type(row[1]) is str for row in rows)
            assert not cursor.messages, "Clean size-one fetch unexpectedly produced diagnostics"
            return dict(
                title="Mixed text / fetchmany(1)",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail="Rows: 10000; types: int,nvarchar; API: fetchmany(1)",
            )
        finally:
            ctx.disable()


def output_converter_fetch(conn, table, ctx, batched=False):
    """Time fetching with a UTF-16LE converter; validate retained rows afterward."""
    from mssql_python import SQL_WVARCHAR

    texts = [f"unicode_row_{index}" for index in range(10_000)]
    expected = [(index, text.upper()) for index, text in enumerate(texts)]
    api = "fetchmany(1000)" if batched else "fetchall()"

    def converter(raw):
        return raw.decode("utf-16le").upper()

    with conn.cursor() as cursor:
        previous = conn.get_output_converter(SQL_WVARCHAR)
        try:
            conn.add_output_converter(SQL_WVARCHAR, converter)
            cursor.execute(f"SELECT TOP (10000) int_col, nvarchar_col FROM {table} ORDER BY id")
            rows = []
            ctx.enable()
            start = time.perf_counter()
            if batched:
                while batch := cursor.fetchmany(1000):
                    rows.extend(batch)
            else:
                rows = cursor.fetchall()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert [tuple(row) for row in rows] == expected
            assert all(type(row[0]) is int and type(row[1]) is str for row in rows)
            assert not cursor.messages, "Clean converter fetch unexpectedly produced diagnostics"
            return dict(
                title=f"Output converter / {api}",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=f"Rows: 10000; types: int,nvarchar; converter: UTF-16LE upper; API: {api}",
            )
        finally:
            try:
                ctx.disable()
            finally:
                if previous is None:
                    conn.remove_output_converter(SQL_WVARCHAR)
                else:
                    conn.add_output_converter(SQL_WVARCHAR, previous)


def text_conversion_fetchall(conn, ctx, storage, payload_size="medium"):
    """Fetch declared-wide text with stored-byte evidence and explicit UTF-16 decoding."""
    from mssql_python import SQL_CHAR, SQL_WCHAR

    data_type, collation, encoding, text = {
        "ascii": ("varchar(8000)", "Latin1_General_100_BIN2", "ascii", "Abcd1234"),
        "cp1252": (
            "varchar(8000)",
            "Latin1_General_100_BIN2",
            "cp1252",
            "caf\u00e9 \u20ac\u00df!",
        ),
        "utf8": (
            "varchar(8000)",
            "Latin1_General_100_BIN2_UTF8",
            "utf-8",
            "A\u00e9\u4e2d\U0001f642Z\u6f22\u03a9!",
        ),
        "nvarchar": (
            "nvarchar(4000)",
            "Latin1_General_100_BIN2",
            "utf-16le",
            "A\u00e9\u4e2d\U0001f642Z\u6f22\u03a9!",
        ),
    }[storage]
    for sql_type in (SQL_CHAR, SQL_WCHAR):
        if conn.getdecoding(sql_type) != {"encoding": "utf-16le", "ctype": SQL_WCHAR}:
            raise ValueError("Text conversion benchmarks require UTF-16LE / SQL_WCHAR decoding")
    repeats = {"short": 1, "medium": 32, "full": 8000 // len(text.encode(encoding))}
    expected = text * repeats[payload_size]
    row_count = 200 if payload_size == "full" else 5000
    expected_bytes = expected.encode(encoding)
    literal = f"CAST(0x{expected.encode('utf-16le').hex()} AS nvarchar(max))"
    with conn.cursor() as cursor:
        created = False
        try:
            cursor.execute(
                "CREATE TABLE #ci_text_conversion "
                f"(id INT PRIMARY KEY, payload {data_type} COLLATE {collation})"
            )
            created = True
            cursor.execute(
                "WITH d(n) AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),"
                "(5),(6),(7),(8),(9)) AS v(n)), "
                "nums(n) AS (SELECT a.n+10*b.n+100*c.n+1000*e.n "
                "FROM d a CROSS JOIN d b CROSS JOIN d c CROSS JOIN d e) "
                f"INSERT INTO #ci_text_conversion SELECT n, {literal} "
                f"FROM nums WHERE n < {row_count}"
            )
            cursor.execute(
                "SELECT DATALENGTH(payload), CAST(payload AS varbinary(8000)) "
                "FROM #ci_text_conversion WHERE id = 0"
            )
            assert tuple(cursor.fetchone()) == (len(expected_bytes), expected_bytes)
            cursor.execute(
                "SELECT "
                + ", ".join(f"payload AS c{column}" for column in range(8))
                + " FROM #ci_text_conversion ORDER BY id"
            )
            ctx.enable()
            start = time.perf_counter()
            rows = cursor.fetchall()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert len(rows) == row_count
            assert all(
                tuple(row) == (expected,) * 8 and all(type(value) is str for value in row)
                for row in rows
            )
            assert not cursor.messages, "Clean text conversion unexpectedly produced diagnostics"
            return dict(
                title="Wide-declared text conversion / fetchall",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=(
                    f"Rows: {row_count}; columns: 8; storage: {data_type}/{encoding}; "
                    f"payload chars: {len(expected)}; fetch: UTF-16LE; API: fetchall"
                ),
            )
        finally:
            ctx.disable()
            if created:
                cursor.execute("DROP TABLE #ci_text_conversion")


def narrow_fetchall_control(conn, ctx, integers=False):
    """Fetch 200,000 narrow rows; only fetching and row wrapping are timed."""
    expression = "n" if integers else "CAST('control' AS varchar(32))"
    with conn.cursor() as cursor:
        cursor.execute(
            "WITH d(n) AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),"
            "(5),(6),(7),(8),(9)) AS v(n)), "
            "nums(n) AS (SELECT a.n+10*b.n+100*c.n+1000*e.n+10000*f.n+100000*g.n "
            "FROM d a CROSS JOIN d b CROSS JOIN d c CROSS JOIN d e "
            "CROSS JOIN d f CROSS JOIN d g) SELECT "
            + ", ".join(f"{expression} AS c{column}" for column in range(8))
            + " FROM nums WHERE n < 200000 ORDER BY n"
        )
        ctx.enable()
        try:
            start = time.perf_counter()
            rows = cursor.fetchall()
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert len(rows) == 200_000
            expected_type = int if integers else str
            assert all(
                tuple(row) == ((index if integers else "control"),) * 8
                and all(type(value) is expected_type for value in row)
                for index, row in enumerate(rows)
            )
            assert not cursor.messages, "Clean narrow fetch unexpectedly produced diagnostics"
            return dict(
                title="Narrow-result fetchall control",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=(
                    "Rows: 200000; columns: 8; "
                    f"type: {'int' if integers else 'varchar(32)'}; API: fetchall"
                ),
            )
        finally:
            ctx.disable()


def row_to_dict(conn, table, ctx):
    """Convert fetched rows with warm mapping metadata; fetch and validation are not timed."""
    names = tuple(f"c{column}" for column in range(32))
    sql = "SELECT TOP (10000) " + ", ".join(
        f"CASE WHEN int_col % 7 = 0 THEN CAST(NULL AS INT) "
        f"ELSE int_col + {column} END AS {name}"
        for column, name in enumerate(names)
    )
    sql += f" FROM {table} ORDER BY id"
    expected = [
        tuple(None if value % 7 == 0 else value + column for column in range(32))
        for value in range(10_000)
    ]

    with conn.cursor() as cursor:
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            assert len(rows) == len(
                expected
            ), "Row-to-dictionary fetch returned the wrong row count"
            for row in rows:
                dict(row._mapping)

            ctx.enable()
            start = time.perf_counter()
            dictionaries = [dict(row._mapping) for row in rows]
            wall_ms = (time.perf_counter() - start) * 1000
            cpp, py = ctx.collect()
            assert len(dictionaries) == len(expected)
            for dictionary, values in zip(dictionaries, expected):
                assert tuple(dictionary) == names
                assert all(type(key) is str for key in dictionary)
                actual = tuple(dictionary.values())
                assert actual == values and all(
                    type(value) is type(wanted) for value, wanted in zip(actual, values)
                )
            assert not cursor.messages, "Clean row-to-dictionary conversion produced diagnostics"
            return dict(
                title="Warm row-to-dictionary conversion",
                wall_ms=wall_ms,
                cpp=cpp,
                py=py,
                detail=(
                    "Rows: 10000; columns: 32; types: int/NULL; "
                    "API: dict(row._mapping); mode: warm"
                ),
            )
        finally:
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
    result["scalar_fetchval"] = (scalar_fetchval, True)
    result["output_converter_fetchall"] = (output_converter_fetch, True)
    result["output_converter_fetchmany1000"] = (partial(output_converter_fetch, batched=True), True)
    result["fetchmany_one_text"] = (fetchmany_one_text, True)
    for storage in ("ascii", "cp1252", "utf8", "nvarchar"):
        result[f"text_conversion_{storage}_fetchall"] = (
            partial(text_conversion_fetchall, storage=storage),
            False,
        )
    for storage in ("ascii", "cp1252", "utf8", "nvarchar"):
        for size in ("short", "full"):
            result[f"text_conversion_{storage}_{size}_fetchall"] = (
                partial(text_conversion_fetchall, storage=storage, payload_size=size),
                False,
            )
    result["narrow_varchar_fetchall"] = (narrow_fetchall_control, False)
    result["narrow_integer_fetchall"] = (partial(narrow_fetchall_control, integers=True), False)
    result["row_to_dict"] = (row_to_dict, True)
    return result
