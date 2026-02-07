"""Tests for SQL Server spatial types (geography, geometry, hierarchyid)."""

import pytest
from decimal import Decimal
import mssql_python

# ==================== GEOGRAPHY TYPE TESTS ====================

POINT_WKT = "POINT(-122.34900 47.65100)"  # Seattle coordinates
LINESTRING_WKT = "LINESTRING(-122.360 47.656, -122.343 47.656)"
POLYGON_WKT = "POLYGON((-122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.653))"
MULTIPOINT_WKT = "MULTIPOINT((-122.34900 47.65100), (-122.11100 47.67700))"
COLLECTION_WKT = "GEOMETRYCOLLECTION(POINT(-122.34900 47.65100))"


def test_geography_basic_insert_fetch(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_basic (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_basic (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geo_col FROM #geo_basic;").fetchone()
    assert isinstance(row[0], bytes)


def test_geography_as_text(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_text (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_text (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geo_col.STAsText() as wkt FROM #geo_text;").fetchone()
    assert row[0] is not None
    assert row[0].startswith("POINT")
    assert "-122.349" in row[0] and "47.651" in row[0]


def test_geography_various_types(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_types (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL, description NVARCHAR(100));"
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
            "INSERT INTO #geo_types (geo_col, description) VALUES (geography::STGeomFromText(?, 4326), ?);",
            (wkt, desc),
        )
    db_connection.commit()

    rows = cursor.execute(
        "SELECT geo_col.STAsText() as wkt, description FROM #geo_types ORDER BY id;"
    ).fetchall()

    for i, (_, expected_desc, expected_type) in enumerate(test_cases):
        assert rows[i][0].startswith(
            expected_type
        ), f"{expected_desc} should start with {expected_type}"
        assert rows[i][1] == expected_desc


def test_geography_null_value(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_null (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute("INSERT INTO #geo_null (geo_col) VALUES (?);", None)
    db_connection.commit()

    row = cursor.execute("SELECT geo_col FROM #geo_null;").fetchone()
    assert row[0] is None


def test_geography_fetchone(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_fetchone (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_fetchone (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    db_connection.commit()

    cursor.execute("SELECT geo_col FROM #geo_fetchone;")
    row = cursor.fetchone()
    assert row is not None
    assert isinstance(row[0], bytes)
    assert cursor.fetchone() is None


def test_geography_fetchmany(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_fetchmany (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    for _ in range(5):
        cursor.execute(
            "INSERT INTO #geo_fetchmany (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
    db_connection.commit()

    cursor.execute("SELECT geo_col FROM #geo_fetchmany;")
    rows = cursor.fetchmany(3)
    assert len(rows) == 3
    for row in rows:
        assert isinstance(row[0], bytes)


def test_geography_fetchall(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_fetchall (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    num_rows = 10
    for _ in range(num_rows):
        cursor.execute(
            "INSERT INTO #geo_fetchall (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
            POINT_WKT,
        )
    db_connection.commit()

    cursor.execute("SELECT geo_col FROM #geo_fetchall;")
    rows = cursor.fetchall()
    assert len(rows) == num_rows
    for row in rows:
        assert isinstance(row[0], bytes)


def test_geography_executemany(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_batch (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL, name NVARCHAR(50));"
    )
    db_connection.commit()

    test_data = [
        (POINT_WKT, "Point1"),
        (LINESTRING_WKT, "Line1"),
        (POLYGON_WKT, "Poly1"),
    ]

    cursor.executemany(
        "INSERT INTO #geo_batch (geo_col, name) " "VALUES (geography::STGeomFromText(?, 4326), ?);",
        test_data,
    )
    db_connection.commit()

    rows = cursor.execute("SELECT geo_col, name FROM #geo_batch ORDER BY id;").fetchall()
    assert len(rows) == len(test_data)
    for (_, expected_name), (fetched_geo, fetched_name) in zip(test_data, rows):
        assert isinstance(fetched_geo, bytes)
        assert fetched_name == expected_name


def test_geography_large_polygon_fetch(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_large (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    large_polygon = (
        "POLYGON(("
        + ", ".join([f"{-122.5 + i*0.0001} {47.5 + i*0.0001}" for i in range(100)])
        + ", -122.5 47.5))"
    )

    cursor.execute(
        "INSERT INTO #geo_large (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        large_polygon,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geo_col FROM #geo_large;").fetchone()
    assert isinstance(row[0], bytes)
    assert len(row[0]) > 100


def test_geography_mixed_with_other_types(cursor, db_connection):
    cursor.execute("""CREATE TABLE #geo_mixed (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(100),
            geo_col GEOGRAPHY NULL,
            created_date DATETIME,
            score FLOAT
        );""")
    db_connection.commit()

    cursor.execute(
        """INSERT INTO #geo_mixed (name, geo_col, created_date, score)
           VALUES (?, geography::STGeomFromText(?, 4326), ?, ?);""",
        ("Seattle", POINT_WKT, "2025-11-26", 95.5),
    )
    db_connection.commit()

    row = cursor.execute("SELECT name, geo_col, created_date, score FROM #geo_mixed;").fetchone()
    assert row[0] == "Seattle"
    assert isinstance(row[1], bytes)
    assert row[3] == 95.5


def test_geography_null_and_valid_mixed(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_null_mixed (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute("INSERT INTO #geo_null_mixed (geo_col) VALUES (?);", None)
    cursor.execute(
        "INSERT INTO #geo_null_mixed (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    cursor.execute("INSERT INTO #geo_null_mixed (geo_col) VALUES (?);", None)
    db_connection.commit()

    rows = cursor.execute("SELECT geo_col FROM #geo_null_mixed ORDER BY id;").fetchall()
    assert len(rows) == 3
    assert rows[0][0] is None
    assert isinstance(rows[1][0], bytes)
    assert rows[2][0] is None


def test_geography_with_srid(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_srid (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_srid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geo_col.STSrid as srid FROM #geo_srid;").fetchone()
    assert row[0] == 4326


def test_geography_methods(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_methods (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_methods (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POLYGON_WKT,
    )
    db_connection.commit()

    # STArea on polygon
    row = cursor.execute("SELECT geo_col.STArea() as area FROM #geo_methods;").fetchone()
    assert row[0] > 0

    # STLength on linestring
    cursor.execute(
        "UPDATE #geo_methods SET geo_col = geography::STGeomFromText(?, 4326);",
        LINESTRING_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geo_col.STLength() as length FROM #geo_methods;").fetchone()
    assert row[0] > 0


def test_geography_output_converter(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_converter (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_converter (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    db_connection.commit()

    def geography_converter(value):
        if value is None:
            return None
        return b"CONVERTED:" + value

    db_connection.add_output_converter(bytes, geography_converter)

    try:
        row = cursor.execute("SELECT geo_col FROM #geo_converter;").fetchone()
        assert isinstance(row[0], bytes)
        assert row[0].startswith(b"CONVERTED:")
    finally:
        db_connection.remove_output_converter(bytes)


def test_geography_description_metadata(cursor, db_connection):
    cursor.execute("CREATE TABLE #geo_desc (id INT PRIMARY KEY, geo_col GEOGRAPHY NULL);")
    db_connection.commit()

    cursor.execute("SELECT id, geo_col FROM #geo_desc;")
    desc = cursor.description

    assert len(desc) == 2
    assert desc[0][0] == "id"
    assert desc[1][0] == "geo_col"
    assert desc[1][1] == bytes


def test_geography_stdistance(cursor, db_connection):
    cursor.execute("""CREATE TABLE #geo_distance (
            id INT PRIMARY KEY IDENTITY(1,1),
            geo1 GEOGRAPHY NULL,
            geo2 GEOGRAPHY NULL
        );""")
    db_connection.commit()

    point2 = "POINT(-73.98500 40.75800)"  # New York

    cursor.execute(
        """INSERT INTO #geo_distance (geo1, geo2)
           VALUES (geography::STGeomFromText(?, 4326), geography::STGeomFromText(?, 4326));""",
        (POINT_WKT, point2),
    )
    db_connection.commit()

    row = cursor.execute("""SELECT geo1.STDistance(geo2) as distance_meters
           FROM #geo_distance;""").fetchone()

    # Seattle to New York is approximately 3,870 km
    assert 3_500_000 < row[0] < 4_500_000


def test_geography_binary_output_consistency(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geo_binary (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geo_binary (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    cursor.execute(
        "INSERT INTO #geo_binary (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
        POINT_WKT,
    )
    db_connection.commit()

    rows = cursor.execute("SELECT geo_col FROM #geo_binary ORDER BY id;").fetchall()
    assert len(rows) == 2
    assert isinstance(rows[0][0], bytes)
    assert isinstance(rows[1][0], bytes)
    assert rows[0][0] == rows[1][0]


# ==================== GEOMETRY TYPE TESTS ====================

GEOMETRY_POINT_WKT = "POINT(100 200)"
GEOMETRY_LINESTRING_WKT = "LINESTRING(0 0, 100 100, 200 0)"
GEOMETRY_POLYGON_WKT = "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"
GEOMETRY_MULTIPOINT_WKT = "MULTIPOINT((0 0), (100 100))"


def test_geometry_basic_insert_fetch(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_basic (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geom_basic (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
        GEOMETRY_POINT_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geom_col FROM #geom_basic;").fetchone()
    assert isinstance(row[0], bytes)


def test_geometry_as_text(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_text (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geom_text (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
        GEOMETRY_POINT_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geom_col.STAsText() as wkt FROM #geom_text;").fetchone()
    assert row[0] is not None
    assert row[0].startswith("POINT")
    assert "100" in row[0] and "200" in row[0]


def test_geometry_various_types(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_types (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL, description NVARCHAR(100));"
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
            "INSERT INTO #geom_types (geom_col, description) VALUES (geometry::STGeomFromText(?, 0), ?);",
            (wkt, desc),
        )
    db_connection.commit()

    rows = cursor.execute(
        "SELECT geom_col.STAsText() as wkt, description FROM #geom_types ORDER BY id;"
    ).fetchall()

    for i, (_, expected_desc, expected_type) in enumerate(test_cases):
        assert rows[i][0].startswith(
            expected_type
        ), f"{expected_desc} should start with {expected_type}"
        assert rows[i][1] == expected_desc


def test_geometry_null_value(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_null (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    cursor.execute("INSERT INTO #geom_null (geom_col) VALUES (?);", None)
    db_connection.commit()

    row = cursor.execute("SELECT geom_col FROM #geom_null;").fetchone()
    assert row[0] is None


def test_geometry_fetchall(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_fetchall (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    num_rows = 5
    for _ in range(num_rows):
        cursor.execute(
            "INSERT INTO #geom_fetchall (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
            GEOMETRY_POINT_WKT,
        )
    db_connection.commit()

    cursor.execute("SELECT geom_col FROM #geom_fetchall;")
    rows = cursor.fetchall()
    assert len(rows) == num_rows
    for row in rows:
        assert isinstance(row[0], bytes)


def test_geometry_methods(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_methods (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geom_methods (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
        GEOMETRY_POLYGON_WKT,
    )
    db_connection.commit()

    # STArea — 100x100 square = 10000 sq units
    row = cursor.execute("SELECT geom_col.STArea() as area FROM #geom_methods;").fetchone()
    assert row[0] == 10000

    # STLength on linestring
    cursor.execute(
        "UPDATE #geom_methods SET geom_col = geometry::STGeomFromText(?, 0);",
        GEOMETRY_LINESTRING_WKT,
    )
    db_connection.commit()

    row = cursor.execute("SELECT geom_col.STLength() as length FROM #geom_methods;").fetchone()
    assert row[0] > 0


def test_geometry_description_metadata(cursor, db_connection):
    cursor.execute("CREATE TABLE #geom_desc (id INT PRIMARY KEY, geom_col GEOMETRY NULL);")
    db_connection.commit()

    cursor.execute("SELECT id, geom_col FROM #geom_desc;")
    desc = cursor.description

    assert len(desc) == 2
    assert desc[0][0] == "id"
    assert desc[1][0] == "geom_col"
    assert desc[1][1] == bytes


def test_geometry_mixed_with_other_types(cursor, db_connection):
    cursor.execute("""CREATE TABLE #geom_mixed (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(100),
            geom_col GEOMETRY NULL,
            area FLOAT
        );""")
    db_connection.commit()

    cursor.execute(
        """INSERT INTO #geom_mixed (name, geom_col, area)
           VALUES (?, geometry::STGeomFromText(?, 0), ?);""",
        ("Square", GEOMETRY_POLYGON_WKT, 10000.0),
    )
    db_connection.commit()

    row = cursor.execute("SELECT name, geom_col, area FROM #geom_mixed;").fetchone()
    assert row[0] == "Square"
    assert isinstance(row[1], bytes)
    assert row[2] == 10000.0


def test_geometry_binary_output_consistency(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #geom_binary (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #geom_binary (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
        GEOMETRY_POINT_WKT,
    )
    cursor.execute(
        "INSERT INTO #geom_binary (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
        GEOMETRY_POINT_WKT,
    )
    db_connection.commit()

    rows = cursor.execute("SELECT geom_col FROM #geom_binary ORDER BY id;").fetchall()
    assert len(rows) == 2
    assert isinstance(rows[0][0], bytes)
    assert isinstance(rows[1][0], bytes)
    assert rows[0][0] == rows[1][0]


# ==================== HIERARCHYID TYPE TESTS ====================


def test_hierarchyid_basic_insert_fetch(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #hid_basic (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #hid_basic (node) VALUES (hierarchyid::Parse(?));",
        "/1/2/3/",
    )
    db_connection.commit()

    row = cursor.execute("SELECT node FROM #hid_basic;").fetchone()
    assert isinstance(row[0], bytes)


def test_hierarchyid_as_string(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #hid_string (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #hid_string (node) VALUES (hierarchyid::Parse(?));",
        "/1/2/3/",
    )
    db_connection.commit()

    row = cursor.execute("SELECT node.ToString() as path FROM #hid_string;").fetchone()
    assert row[0] == "/1/2/3/"


def test_hierarchyid_null_value(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #hid_null (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
    )
    db_connection.commit()

    cursor.execute("INSERT INTO #hid_null (node) VALUES (?);", None)
    db_connection.commit()

    row = cursor.execute("SELECT node FROM #hid_null;").fetchone()
    assert row[0] is None


def test_hierarchyid_fetchall(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #hid_fetchall (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
    )
    db_connection.commit()

    paths = ["/1/", "/1/1/", "/1/2/", "/2/", "/2/1/"]
    for path in paths:
        cursor.execute(
            "INSERT INTO #hid_fetchall (node) VALUES (hierarchyid::Parse(?));",
            path,
        )
    db_connection.commit()

    cursor.execute("SELECT node FROM #hid_fetchall;")
    rows = cursor.fetchall()
    assert len(rows) == len(paths)
    for row in rows:
        assert isinstance(row[0], bytes)


def test_hierarchyid_methods(cursor, db_connection):
    cursor.execute(
        "CREATE TABLE #hid_methods (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
    )
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #hid_methods (node) VALUES (hierarchyid::Parse(?));",
        "/1/2/3/",
    )
    db_connection.commit()

    # GetLevel — /1/2/3/ is at level 3
    row = cursor.execute("SELECT node.GetLevel() as level FROM #hid_methods;").fetchone()
    assert row[0] == 3

    # GetAncestor — parent of /1/2/3/ is /1/2/
    row = cursor.execute(
        "SELECT node.GetAncestor(1).ToString() as parent FROM #hid_methods;"
    ).fetchone()
    assert row[0] == "/1/2/"

    # IsDescendantOf
    row = cursor.execute(
        "SELECT node.IsDescendantOf(hierarchyid::Parse('/1/')) as is_descendant FROM #hid_methods;"
    ).fetchone()
    assert row[0] == 1


def test_hierarchyid_description_metadata(cursor, db_connection):
    cursor.execute("CREATE TABLE #hid_desc (id INT PRIMARY KEY, node HIERARCHYID NULL);")
    db_connection.commit()

    cursor.execute("SELECT id, node FROM #hid_desc;")
    desc = cursor.description

    assert len(desc) == 2
    assert desc[0][0] == "id"
    assert desc[1][0] == "node"
    assert desc[1][1] == bytes


def test_hierarchyid_tree_structure(cursor, db_connection):
    cursor.execute("""CREATE TABLE #hid_tree (
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
            "INSERT INTO #hid_tree (name, node) VALUES (?, hierarchyid::Parse(?));",
            (name, path),
        )
    db_connection.commit()

    # All descendants of VP Engineering (including self)
    rows = cursor.execute("""SELECT name, node.ToString() as path
           FROM #hid_tree
           WHERE node.IsDescendantOf(hierarchyid::Parse('/1/')) = 1
           ORDER BY node;""").fetchall()

    assert len(rows) == 5
    names = [r[0] for r in rows]
    assert names == ["VP Engineering", "Dev Manager", "Senior Dev", "Junior Dev", "QA Manager"]

    # Direct reports of Dev Manager
    rows = cursor.execute("""SELECT name, node.ToString() as path
           FROM #hid_tree
           WHERE node.GetAncestor(1) = hierarchyid::Parse('/1/1/')
           ORDER BY node;""").fetchall()

    assert len(rows) == 2
    names = [r[0] for r in rows]
    assert "Senior Dev" in names and "Junior Dev" in names


def test_hierarchyid_mixed_with_other_types(cursor, db_connection):
    cursor.execute("""CREATE TABLE #hid_mixed (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(100),
            node HIERARCHYID NULL,
            salary DECIMAL(10,2)
        );""")
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #hid_mixed (name, node, salary) VALUES (?, hierarchyid::Parse(?), ?);",
        ("Manager", "/1/", 75000.00),
    )
    db_connection.commit()

    row = cursor.execute("SELECT name, node, salary FROM #hid_mixed;").fetchone()
    assert row[0] == "Manager"
    assert isinstance(row[1], bytes)
    assert row[2] == Decimal("75000.00")


# ==================== SPATIAL TYPE ERROR HANDLING TESTS ====================


def test_geography_invalid_wkt_parsing(cursor, db_connection):
    try:
        cursor.execute(
            "CREATE TABLE #geo_invalid (id INT PRIMARY KEY IDENTITY(1,1), geo_col GEOGRAPHY NULL);"
        )
        db_connection.commit()

        # Missing closing paren
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #geo_invalid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                "POINT(-122.34900 47.65100",
            )
        db_connection.rollback()

        # Not a valid geometry type
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #geo_invalid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                "INVALIDTYPE(0 0)",
            )
        db_connection.rollback()

        # Latitude > 90 is invalid for geography (geodetic coordinates)
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #geo_invalid (geo_col) VALUES (geography::STGeomFromText(?, 4326));",
                "POINT(0 100)",
            )
        db_connection.rollback()

    finally:
        cursor.execute("DROP TABLE IF EXISTS #geo_invalid;")
        db_connection.commit()


def test_geometry_invalid_wkt_parsing(cursor, db_connection):
    try:
        cursor.execute(
            "CREATE TABLE #geom_invalid (id INT PRIMARY KEY IDENTITY(1,1), geom_col GEOMETRY NULL);"
        )
        db_connection.commit()

        # Missing coordinates
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #geom_invalid (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
                "POINT()",
            )
        db_connection.rollback()

        # Unclosed polygon (first/last points differ)
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #geom_invalid (geom_col) VALUES (geometry::STGeomFromText(?, 0));",
                "POLYGON((0 0, 100 0, 100 100))",
            )
        db_connection.rollback()

    finally:
        cursor.execute("DROP TABLE IF EXISTS #geom_invalid;")
        db_connection.commit()


def test_hierarchyid_invalid_parsing(cursor, db_connection):
    try:
        cursor.execute(
            "CREATE TABLE #hid_invalid (id INT PRIMARY KEY IDENTITY(1,1), node HIERARCHYID NULL);"
        )
        db_connection.commit()

        # Letters where numbers expected
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #hid_invalid (node) VALUES (hierarchyid::Parse(?));",
                "/abc/",
            )
        db_connection.rollback()

        # Missing leading slash
        with pytest.raises(mssql_python.DatabaseError):
            cursor.execute(
                "INSERT INTO #hid_invalid (node) VALUES (hierarchyid::Parse(?));",
                "1/2/",
            )
        db_connection.rollback()

    finally:
        cursor.execute("DROP TABLE IF EXISTS #hid_invalid;")
        db_connection.commit()
