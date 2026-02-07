"""Tests for SQL Server spatial types (geography, geometry, hierarchyid)."""

import pytest
import mssql_python
from mssql_python.constants import ConstantsDDBC

# ==================== GEOGRAPHY TYPE TESTS ====================

POINT_WKT = "POINT(-122.34900 47.65100)"  # Seattle coordinates
LINESTRING_WKT = "LINESTRING(-122.360 47.656, -122.343 47.656)"
POLYGON_WKT = "POLYGON((-122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.653))"
MULTIPOINT_WKT = "MULTIPOINT((-122.34900 47.65100), (-122.11100 47.67700))"
COLLECTION_WKT = "GEOMETRYCOLLECTION(POINT(-122.34900 47.65100))"


def test_geography_basic_insert_fetch(cursor, db_connection):
    """Insert and fetch a geography Point as binary."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_basic (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_basic (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute("SELECT geo_col FROM #pytest_geography_basic;").fetchone()
        assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_basic;")
        db_connection.commit()


def test_geography_as_text(cursor, db_connection):
    """Fetch geography as WKT text using STAsText()."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_text (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_text (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT geo_col.STAsText() as wkt FROM #pytest_geography_text;"
        ).fetchone()
        assert row[0] is not None
        assert row[0].startswith("POINT")
        assert "-122.349" in row[0] and "47.651" in row[0]

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_text;")
        db_connection.commit()


def test_geography_various_types(cursor, db_connection):
    """Insert and fetch various geography types (Point, LineString, Polygon, etc.)."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_types (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL, description NVARCHAR(100));"
        )
        db_connection.commit()

        test_cases = [
            (POINT_WKT, "Point", "POINT"),
            (LINESTRING_WKT, "LineString", "LINESTRING"),
            (POLYGON_WKT, "Polygon", "POLYGON"),
            (MULTIPOINT_WKT, "MultiPoint", "MULTIPOINT"),
            (COLLECTION_WKT, "GeometryCollection", "GEOMETRYCOLLECTION"),
        ]

        for wkt, desc, _ in test_cases:
            cursor.execute(
                "INSERT INTO #pytest_geography_types (geo_col, description) VALUES (geography::STGeomFromText(?, 4326), ?);",
                (wkt, desc),
            )
        db_connection.commit()

        rows = cursor.execute(
            "SELECT geo_col.STAsText() as wkt, description FROM #pytest_geography_types ORDER BY id;"
        ).fetchall()

        for i, (_, expected_desc, expected_type) in enumerate(test_cases):
            assert rows[i][0].startswith(
                expected_type
            ), f"{expected_desc} should start with {expected_type}"
            assert rows[i][1] == expected_desc

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_types;")
        db_connection.commit()


def test_geography_null_value(cursor, db_connection):
    """Insert and fetch NULL geography value."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_null (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute("INSERT INTO #pytest_geography_null (geo_col) VALUES (?);", None)
        db_connection.commit()

        row = cursor.execute("SELECT geo_col FROM #pytest_geography_null;").fetchone()
        assert row[0] is None

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_null;")
        db_connection.commit()


def test_geography_fetchone(cursor, db_connection):
    """Test fetchone with geography columns."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_fetchone (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_fetchone (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
        db_connection.commit()

        cursor.execute("SELECT geo_col FROM #pytest_geography_fetchone;")
        row = cursor.fetchone()
        assert row is not None
        assert isinstance(row[0], bytes)
        assert cursor.fetchone() is None

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_fetchone;")
        db_connection.commit()


def test_geography_fetchmany(cursor, db_connection):
    """Test fetchmany with geography columns."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_fetchmany (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        for _ in range(5):
            cursor.execute(
                "INSERT INTO #pytest_geography_fetchmany (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                POINT_WKT,
            )
        db_connection.commit()

        cursor.execute("SELECT geo_col FROM #pytest_geography_fetchmany;")
        rows = cursor.fetchmany(3)
        assert len(rows) == 3
        for row in rows:
            assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_fetchmany;")
        db_connection.commit()


def test_geography_fetchall(cursor, db_connection):
    """Test fetchall with geography columns."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_fetchall (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        num_rows = 10
        for _ in range(num_rows):
            cursor.execute(
                "INSERT INTO #pytest_geography_fetchall (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                POINT_WKT,
            )
        db_connection.commit()

        cursor.execute("SELECT geo_col FROM #pytest_geography_fetchall;")
        rows = cursor.fetchall()
        assert len(rows) == num_rows
        for row in rows:
            assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_fetchall;")
        db_connection.commit()


def test_geography_executemany(cursor, db_connection):
    """Test batch insert (executemany) of multiple geography values."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_batch (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL, name NVARCHAR(50));"
        )
        db_connection.commit()

        test_data = [
            (POINT_WKT, "Point1"),
            (LINESTRING_WKT, "Line1"),
            (POLYGON_WKT, "Poly1"),
        ]

        cursor.executemany(
            "INSERT INTO #pytest_geography_batch (geo_col, name) "
            "VALUES (geography::STGeomFromText(?, 4326), ?);",
            [(wkt, name) for wkt, name in test_data],
        )
        db_connection.commit()

        rows = cursor.execute(
            "SELECT geo_col, name FROM #pytest_geography_batch ORDER BY id;"
        ).fetchall()
        assert len(rows) == len(test_data)
        for (_, expected_name), (geo_value, name_value) in zip(test_data, rows):
            assert isinstance(geo_value, bytes)
            assert name_value == expected_name

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_batch;")
        db_connection.commit()


def test_geography_large_value_lob_streaming(cursor, db_connection):
    """Test large geography values to verify LOB/streaming behavior."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_large (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        large_polygon = (
            "POLYGON(("
            + ", ".join([f"{-122.5 + i*0.0001} {47.5 + i*0.0001}" for i in range(100)])
            + ", -122.5 47.5))"
        )

        cursor.execute(
            "INSERT INTO #pytest_geography_large (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            large_polygon,
        )
        db_connection.commit()

        row = cursor.execute("SELECT geo_col FROM #pytest_geography_large;").fetchone()
        assert isinstance(row[0], bytes)
        assert len(row[0]) > 0

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_large;")
        db_connection.commit()


def test_geography_mixed_with_other_types(cursor, db_connection):
    """Test geography columns mixed with other data types."""
    try:
        cursor.execute("""CREATE TABLE #pytest_geography_mixed (
                id INT PRIMARY KEY IDENTITY(1,1),
                name NVARCHAR(100),
                geo_col GEOGRAPHY NULL,
                created_date DATETIME,
                score FLOAT
            );""")
        db_connection.commit()

        cursor.execute(
            """INSERT INTO #pytest_geography_mixed (name, geo_col, created_date, score)
               VALUES (?, geography::STGeomFromText(?, 4326), ?, ?);""",
            ("Seattle", POINT_WKT, "2025-11-26", 95.5),
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT name, geo_col, created_date, score FROM #pytest_geography_mixed;"
        ).fetchone()
        assert row[0] == "Seattle"
        assert isinstance(row[1], bytes)
        assert row[3] == 95.5

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_mixed;")
        db_connection.commit()


def test_geography_null_and_empty_mixed(cursor, db_connection):
    """Test mix of NULL and valid geography values."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_null_mixed (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute("INSERT INTO #pytest_geography_null_mixed (geo_col) VALUES (?);", None)
        cursor.execute(
            "INSERT INTO #pytest_geography_null_mixed (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
        cursor.execute("INSERT INTO #pytest_geography_null_mixed (geo_col) VALUES (?);", None)
        db_connection.commit()

        rows = cursor.execute(
            "SELECT geo_col FROM #pytest_geography_null_mixed ORDER BY id;"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0][0] is None
        assert isinstance(rows[1][0], bytes)
        assert rows[2][0] is None

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_null_mixed;")
        db_connection.commit()


def test_geography_with_srid(cursor, db_connection):
    """Test geography with SRID value verification."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_srid (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL, srid INT);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_srid (geo_col, srid) VALUES (geography::STGeomFromText(?, 4326), 4326);",
            POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT geo_col.STSrid as srid FROM #pytest_geography_srid;"
        ).fetchone()
        assert row[0] == 4326

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_srid;")
        db_connection.commit()


def test_geography_methods(cursor, db_connection):
    """Test geography methods (STArea, STLength)."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_methods (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_methods (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POLYGON_WKT,
        )
        db_connection.commit()

        # STArea on polygon
        row = cursor.execute(
            "SELECT geo_col.STArea() as area FROM #pytest_geography_methods;"
        ).fetchone()
        assert row[0] is not None
        assert row[0] > 0

        # STLength on linestring
        cursor.execute(
            "UPDATE #pytest_geography_methods SET geo_col = geography::STGeomFromText(?, 4326);",
            LINESTRING_WKT,
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT geo_col.STLength() as length FROM #pytest_geography_methods;"
        ).fetchone()
        assert row[0] is not None
        assert row[0] > 0

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_methods;")
        db_connection.commit()


def test_geography_output_converter(cursor, db_connection):
    """Test output converter registration for geography (SQL_SS_UDT)."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_converter (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_converter (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
        db_connection.commit()

        converted = []

        def geography_converter(value):
            if value is None:
                return None
            converted.append(True)
            return value

        db_connection.add_output_converter(ConstantsDDBC.SQL_SS_UDT.value, geography_converter)

        try:
            row = cursor.execute("SELECT geo_col FROM #pytest_geography_converter;").fetchone()
            assert len(converted) > 0
            assert isinstance(row[0], bytes)
        finally:
            db_connection.remove_output_converter(ConstantsDDBC.SQL_SS_UDT.value)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_converter;")
        db_connection.commit()


def test_geography_description_metadata(cursor, db_connection):
    """Test cursor.description for geography columns reports SQL_SS_UDT."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_desc (id INT PRIMARY KEY, geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute("SELECT id, geo_col FROM #pytest_geography_desc;")
        desc = cursor.description

        assert len(desc) == 2
        assert desc[0][0] == "id"
        assert desc[1][0] == "geo_col"
        assert int(desc[1][1]) == ConstantsDDBC.SQL_SS_UDT.value

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_desc;")
        db_connection.commit()


def test_geography_complex_operations(cursor, db_connection):
    """Test STDistance between two geography points."""
    try:
        cursor.execute("""CREATE TABLE #pytest_geography_complex (
                id INT PRIMARY KEY IDENTITY(1,1),
                geo1 GEOGRAPHY NULL,
                geo2 GEOGRAPHY NULL
            );""")
        db_connection.commit()

        point1 = "POINT(-122.34900 47.65100)"  # Seattle
        point2 = "POINT(-73.98500 40.75800)"  # New York

        cursor.execute(
            """INSERT INTO #pytest_geography_complex (geo1, geo2)
               VALUES (geography::STGeomFromText(?, 4326), geography::STGeomFromText(?, 4326));""",
            (point1, point2),
        )
        db_connection.commit()

        row = cursor.execute("""SELECT geo1.STDistance(geo2) as distance_meters
               FROM #pytest_geography_complex;""").fetchone()

        assert row[0] is not None
        assert row[0] > 0
        # Seattle to New York is approximately 3,900 km
        assert row[0] > 3000000

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_complex;")
        db_connection.commit()


def test_geography_binary_parameter_round_trip(cursor, db_connection):
    """Test binary round-trip: fetch as bytes, reinsert via STGeomFromWKB, compare WKT."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geography_binary (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geography_binary (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute("SELECT geo_col FROM #pytest_geography_binary;").fetchone()
        original_binary = row[0]
        assert isinstance(original_binary, bytes)

        # Reinsert the binary via STGeomFromWKB
        cursor.execute(
            "INSERT INTO #pytest_geography_binary (geo_col) VALUES (geography::STGeomFromWKB(?, 4326));",
            original_binary,
        )
        db_connection.commit()

        rows = cursor.execute(
            "SELECT geo_col.STAsText() FROM #pytest_geography_binary ORDER BY id;"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == rows[1][0]

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_binary;")
        db_connection.commit()


# ==================== GEOMETRY TYPE TESTS ====================

GEOMETRY_POINT_WKT = "POINT(100 200)"
GEOMETRY_LINESTRING_WKT = "LINESTRING(0 0, 100 100, 200 0)"
GEOMETRY_POLYGON_WKT = "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"
GEOMETRY_MULTIPOINT_WKT = "MULTIPOINT((0 0), (100 100))"


def test_geometry_basic_insert_fetch(cursor, db_connection):
    """Insert and fetch a geometry Point as binary."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_basic (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geometry_basic (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
            GEOMETRY_POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute("SELECT geom_col FROM #pytest_geometry_basic;").fetchone()
        assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_basic;")
        db_connection.commit()


def test_geometry_as_text(cursor, db_connection):
    """Fetch geometry as WKT text using STAsText()."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_text (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geometry_text (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
            GEOMETRY_POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT geom_col.STAsText() as wkt FROM #pytest_geometry_text;"
        ).fetchone()
        assert row[0] is not None
        assert row[0].startswith("POINT")
        assert "100" in row[0] and "200" in row[0]

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_text;")
        db_connection.commit()


def test_geometry_various_types(cursor, db_connection):
    """Insert and fetch various geometry types (Point, LineString, Polygon, MultiPoint)."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_types (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL, description NVARCHAR(100));"
        )
        db_connection.commit()

        test_cases = [
            (GEOMETRY_POINT_WKT, "Point", "POINT"),
            (GEOMETRY_LINESTRING_WKT, "LineString", "LINESTRING"),
            (GEOMETRY_POLYGON_WKT, "Polygon", "POLYGON"),
            (GEOMETRY_MULTIPOINT_WKT, "MultiPoint", "MULTIPOINT"),
        ]

        for wkt, desc, _ in test_cases:
            cursor.execute(
                "INSERT INTO #pytest_geometry_types (geom_col, description) VALUES (geometry::STGeomFromText(?, 0), ?);",
                (wkt, desc),
            )
        db_connection.commit()

        rows = cursor.execute(
            "SELECT geom_col.STAsText() as wkt, description FROM #pytest_geometry_types ORDER BY id;"
        ).fetchall()

        for i, (_, expected_desc, expected_type) in enumerate(test_cases):
            assert rows[i][0].startswith(
                expected_type
            ), f"{expected_desc} should start with {expected_type}"
            assert rows[i][1] == expected_desc

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_types;")
        db_connection.commit()


def test_geometry_null_value(cursor, db_connection):
    """Insert and fetch NULL geometry value."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_null (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        cursor.execute("INSERT INTO #pytest_geometry_null (geom_col) VALUES (?);", None)
        db_connection.commit()

        row = cursor.execute("SELECT geom_col FROM #pytest_geometry_null;").fetchone()
        assert row[0] is None

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_null;")
        db_connection.commit()


def test_geometry_fetchall(cursor, db_connection):
    """Test fetchall with geometry columns."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_fetchall (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        num_rows = 5
        for _ in range(num_rows):
            cursor.execute(
                "INSERT INTO #pytest_geometry_fetchall (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
                GEOMETRY_POINT_WKT,
            )
        db_connection.commit()

        cursor.execute("SELECT geom_col FROM #pytest_geometry_fetchall;")
        rows = cursor.fetchall()
        assert len(rows) == num_rows
        for row in rows:
            assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_fetchall;")
        db_connection.commit()


def test_geometry_methods(cursor, db_connection):
    """Test geometry methods (STArea, STLength)."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_methods (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geometry_methods (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
            GEOMETRY_POLYGON_WKT,
        )
        db_connection.commit()

        # STArea — 100x100 square = 10000 sq units
        row = cursor.execute(
            "SELECT geom_col.STArea() as area FROM #pytest_geometry_methods;"
        ).fetchone()
        assert row[0] == 10000

        # STLength on linestring
        cursor.execute(
            "UPDATE #pytest_geometry_methods SET geom_col = geometry::STGeomFromText(?, 0);",
            GEOMETRY_LINESTRING_WKT,
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT geom_col.STLength() as length FROM #pytest_geometry_methods;"
        ).fetchone()
        assert row[0] > 0

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_methods;")
        db_connection.commit()


def test_geometry_description_metadata(cursor, db_connection):
    """Test cursor.description for geometry columns reports SQL_SS_UDT."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_desc (id INT PRIMARY KEY, geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        cursor.execute("SELECT id, geom_col FROM #pytest_geometry_desc;")
        desc = cursor.description

        assert len(desc) == 2
        assert desc[0][0] == "id"
        assert desc[1][0] == "geom_col"
        assert int(desc[1][1]) == ConstantsDDBC.SQL_SS_UDT.value

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_desc;")
        db_connection.commit()


def test_geometry_mixed_with_other_types(cursor, db_connection):
    """Test geometry columns mixed with other data types."""
    try:
        cursor.execute("""CREATE TABLE #pytest_geometry_mixed (
                id INT PRIMARY KEY IDENTITY(1,1),
                name NVARCHAR(100),
                geom_col GEOMETRY NULL,
                area FLOAT
            );""")
        db_connection.commit()

        cursor.execute(
            """INSERT INTO #pytest_geometry_mixed (name, geom_col, area)
               VALUES (?, geometry::STGeomFromText(?, 0), ?);""",
            ("Square", GEOMETRY_POLYGON_WKT, 10000.0),
        )
        db_connection.commit()

        row = cursor.execute("SELECT name, geom_col, area FROM #pytest_geometry_mixed;").fetchone()
        assert row[0] == "Square"
        assert isinstance(row[1], bytes)
        assert row[2] == 10000.0

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_mixed;")
        db_connection.commit()


def test_geometry_binary_parameter_round_trip(cursor, db_connection):
    """Test binary round-trip: fetch as bytes, reinsert via STGeomFromWKB, compare WKT."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_geometry_binary (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_geometry_binary (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
            GEOMETRY_POINT_WKT,
        )
        db_connection.commit()

        row = cursor.execute("SELECT geom_col FROM #pytest_geometry_binary;").fetchone()
        original_binary = row[0]
        assert isinstance(original_binary, bytes)

        # Reinsert the binary via STGeomFromWKB
        cursor.execute(
            "INSERT INTO #pytest_geometry_binary (geom_col) VALUES (geometry::STGeomFromWKB(?, 0));",
            original_binary,
        )
        db_connection.commit()

        rows = cursor.execute(
            "SELECT geom_col.STAsText() FROM #pytest_geometry_binary ORDER BY id;"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == rows[1][0]

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_binary;")
        db_connection.commit()


# ==================== HIERARCHYID TYPE TESTS ====================


def test_hierarchyid_basic_insert_fetch(cursor, db_connection):
    """Insert and fetch a hierarchyid value as binary."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_hierarchyid_basic (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_hierarchyid_basic (node) VALUES (hierarchyid::Parse(?));",
            "/1/2/3/",
        )
        db_connection.commit()

        row = cursor.execute("SELECT node FROM #pytest_hierarchyid_basic;").fetchone()
        assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_basic;")
        db_connection.commit()


def test_hierarchyid_as_string(cursor, db_connection):
    """Fetch hierarchyid as string using ToString()."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_hierarchyid_string (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_hierarchyid_string (node) VALUES (hierarchyid::Parse(?));",
            "/1/2/3/",
        )
        db_connection.commit()

        row = cursor.execute(
            "SELECT node.ToString() as path FROM #pytest_hierarchyid_string;"
        ).fetchone()
        assert row[0] == "/1/2/3/"

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_string;")
        db_connection.commit()


def test_hierarchyid_null_value(cursor, db_connection):
    """Insert and fetch NULL hierarchyid value."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_hierarchyid_null (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
        )
        db_connection.commit()

        cursor.execute("INSERT INTO #pytest_hierarchyid_null (node) VALUES (?);", None)
        db_connection.commit()

        row = cursor.execute("SELECT node FROM #pytest_hierarchyid_null;").fetchone()
        assert row[0] is None

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_null;")
        db_connection.commit()


def test_hierarchyid_fetchall(cursor, db_connection):
    """Test fetchall with hierarchyid columns."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_hierarchyid_fetchall (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
        )
        db_connection.commit()

        paths = ["/1/", "/1/1/", "/1/2/", "/2/", "/2/1/"]
        for path in paths:
            cursor.execute(
                "INSERT INTO #pytest_hierarchyid_fetchall (node) VALUES (hierarchyid::Parse(?));",
                path,
            )
        db_connection.commit()

        cursor.execute("SELECT node FROM #pytest_hierarchyid_fetchall;")
        rows = cursor.fetchall()
        assert len(rows) == len(paths)
        for row in rows:
            assert isinstance(row[0], bytes)

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_fetchall;")
        db_connection.commit()


def test_hierarchyid_methods(cursor, db_connection):
    """Test hierarchyid methods (GetLevel, GetAncestor, IsDescendantOf)."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_hierarchyid_methods (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
        )
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_hierarchyid_methods (node) VALUES (hierarchyid::Parse(?));",
            "/1/2/3/",
        )
        db_connection.commit()

        # GetLevel — /1/2/3/ is at level 3
        row = cursor.execute(
            "SELECT node.GetLevel() as level FROM #pytest_hierarchyid_methods;"
        ).fetchone()
        assert row[0] == 3

        # GetAncestor — parent of /1/2/3/ is /1/2/
        row = cursor.execute(
            "SELECT node.GetAncestor(1).ToString() as parent FROM #pytest_hierarchyid_methods;"
        ).fetchone()
        assert row[0] == "/1/2/"

        # IsDescendantOf
        row = cursor.execute(
            "SELECT node.IsDescendantOf(hierarchyid::Parse('/1/')) as is_descendant FROM #pytest_hierarchyid_methods;"
        ).fetchone()
        assert row[0] == 1

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_methods;")
        db_connection.commit()


def test_hierarchyid_description_metadata(cursor, db_connection):
    """Test cursor.description for hierarchyid columns reports SQL_SS_UDT."""
    try:
        cursor.execute(
            "CREATE TABLE #pytest_hierarchyid_desc (id INT PRIMARY KEY, node HIERARCHYID NULL);"
        )
        db_connection.commit()

        cursor.execute("SELECT id, node FROM #pytest_hierarchyid_desc;")
        desc = cursor.description

        assert len(desc) == 2
        assert desc[0][0] == "id"
        assert desc[1][0] == "node"
        assert int(desc[1][1]) == ConstantsDDBC.SQL_SS_UDT.value

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_desc;")
        db_connection.commit()


def test_hierarchyid_tree_structure(cursor, db_connection):
    """Test hierarchyid with an org chart tree structure."""
    try:
        cursor.execute("""CREATE TABLE #pytest_hierarchyid_tree (
                id INT PRIMARY KEY IDENTITY(1,1),
                name NVARCHAR(100),
                node HIERARCHYID NULL
            );""")
        db_connection.commit()

        org_data = [
            ("CEO", "/"),
            ("VP Engineering", "/1/"),
            ("VP Sales", "/2/"),
            ("Dev Manager", "/1/1/"),
            ("QA Manager", "/1/2/"),
            ("Senior Dev", "/1/1/1/"),
            ("Junior Dev", "/1/1/2/"),
        ]

        for name, path in org_data:
            cursor.execute(
                "INSERT INTO #pytest_hierarchyid_tree (name, node) VALUES (?, hierarchyid::Parse(?));",
                (name, path),
            )
        db_connection.commit()

        # All descendants of VP Engineering (including self)
        rows = cursor.execute("""SELECT name, node.ToString() as path 
               FROM #pytest_hierarchyid_tree 
               WHERE node.IsDescendantOf(hierarchyid::Parse('/1/')) = 1
               ORDER BY node;""").fetchall()

        assert len(rows) == 5
        assert rows[0][0] == "VP Engineering"

        # Direct reports of Dev Manager
        rows = cursor.execute("""SELECT name, node.ToString() as path 
               FROM #pytest_hierarchyid_tree 
               WHERE node.GetAncestor(1) = hierarchyid::Parse('/1/1/')
               ORDER BY node;""").fetchall()

        assert len(rows) == 2
        names = [r[0] for r in rows]
        assert "Senior Dev" in names and "Junior Dev" in names

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_tree;")
        db_connection.commit()


def test_hierarchyid_mixed_with_other_types(cursor, db_connection):
    """Test hierarchyid columns mixed with other data types."""
    try:
        cursor.execute("""CREATE TABLE #pytest_hierarchyid_mixed (
                id INT PRIMARY KEY IDENTITY(1,1),
                name NVARCHAR(100),
                node HIERARCHYID NULL,
                salary DECIMAL(10,2)
            );""")
        db_connection.commit()

        cursor.execute(
            "INSERT INTO #pytest_hierarchyid_mixed (name, node, salary) VALUES (?, hierarchyid::Parse(?), ?);",
            ("Manager", "/1/", 75000.00),
        )
        db_connection.commit()

        row = cursor.execute("SELECT name, node, salary FROM #pytest_hierarchyid_mixed;").fetchone()
        assert row[0] == "Manager"
        assert isinstance(row[1], bytes)
        assert float(row[2]) == 75000.00

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_mixed;")
        db_connection.commit()


# ==================== SPATIAL TYPE ERROR HANDLING TESTS ====================


def test_geography_invalid_wkt_parsing(cursor, db_connection):
    """Test that invalid WKT raises DatabaseError."""
    cursor.execute(
        "CREATE TABLE #pytest_geography_invalid (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    try:
        # Missing closing paren
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_geography_invalid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                "POINT(-122.34900 47.65100",
            )
        db_connection.rollback()

        # Not a valid geometry type
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_geography_invalid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                "INVALIDTYPE(0 0)",
            )
        db_connection.rollback()

        # Latitude > 90 is invalid for geography (geodetic coordinates)
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_geography_invalid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                "POINT(0 100)",
            )
        db_connection.rollback()

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geography_invalid;")
        db_connection.commit()


def test_geometry_invalid_wkt_parsing(cursor, db_connection):
    """Test that invalid geometry WKT raises DatabaseError."""
    cursor.execute(
        "CREATE TABLE #pytest_geometry_invalid (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    try:
        # Missing coordinates
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_geometry_invalid (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
                "POINT()",
            )
        db_connection.rollback()

        # Unclosed polygon (first/last points differ)
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_geometry_invalid (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
                "POLYGON((0 0, 100 0, 100 100))",
            )
        db_connection.rollback()

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_geometry_invalid;")
        db_connection.commit()


def test_hierarchyid_invalid_parsing(cursor, db_connection):
    """Test that invalid hierarchyid paths raise DatabaseError."""
    cursor.execute(
        "CREATE TABLE #pytest_hierarchyid_invalid (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
    )
    db_connection.commit()

    try:
        # Letters where numbers expected
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_hierarchyid_invalid (node) VALUES (hierarchyid::Parse(?));",
                "/abc/",
            )
        db_connection.rollback()

        # Missing leading slash
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #pytest_hierarchyid_invalid (node) VALUES (hierarchyid::Parse(?));",
                "1/2/",
            )
        db_connection.rollback()

    finally:
        cursor.execute("DROP TABLE IF EXISTS #pytest_hierarchyid_invalid;")
        db_connection.commit()
