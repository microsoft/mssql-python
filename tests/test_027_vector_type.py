"""Tests for the SQL Server 2025 vector type.

The driver does not bind the vector type natively yet. Vector values are written
by passing a JSON array string and converting it server side with CAST, and they
come back as a JSON array string. These tests pin that behaviour so it cannot
regress silently, and pin the failure modes on servers that do not have the type.

Everything that needs the type is gated on the supports_vector fixture, so the
SQL Server 2022 and LocalDB legs skip rather than fail.
"""

import json
import pytest
import mssql_python

# float32 is the only base type SQL Server 2025 currently accepts, and it caps
# vector columns at 1998 dimensions.
MAX_FLOAT32_DIMENSION = 1998

SAMPLE = [1.0, 2.0, 3.0]
SAMPLE_JSON = "[1.0, 2.0, 3.0]"


def _vector_literal(dimension, value="1.5"):
    """Build a JSON array string of the given dimension."""
    return "[" + ",".join([value] * dimension) + "]"


@pytest.fixture
def requires_vector(supports_vector):
    """Skip a test when the target server has no vector type."""
    if not supports_vector:
        pytest.skip("server does not support the vector type (requires SQL Server 2025+)")


@pytest.fixture
def requires_no_vector(supports_vector):
    """Skip a test when the target server does have the vector type."""
    if supports_vector:
        pytest.skip("server supports the vector type, degradation test does not apply")


# ==================== ROUND TRIP ====================


def test_vector_insert_and_fetch(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_basic (id INT, v VECTOR(3));")
    db_connection.commit()

    cursor.execute(
        "INSERT INTO #vec_basic VALUES (?, CAST(? AS VECTOR(3)));",
        (1, SAMPLE_JSON),
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_basic;").fetchone()
    assert isinstance(row[0], str)
    assert json.loads(row[0]) == SAMPLE


def test_vector_returned_as_string_not_bytes(cursor, db_connection, requires_vector):
    """The value arrives as text, not as a binary blob."""
    cursor.execute("CREATE TABLE #vec_str (v VECTOR(3));")
    cursor.execute("INSERT INTO #vec_str VALUES (CAST(? AS VECTOR(3)));", SAMPLE_JSON)
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_str;").fetchone()
    assert isinstance(row[0], str)
    assert row[0].startswith("[")
    assert row[0].endswith("]")


def test_vector_description_reports_string_type(cursor, db_connection, requires_vector):
    """cursor.description advertises a string column, since binding is not native yet."""
    cursor.execute("CREATE TABLE #vec_desc (v VECTOR(3));")
    cursor.execute("INSERT INTO #vec_desc VALUES (CAST(? AS VECTOR(3)));", SAMPLE_JSON)
    db_connection.commit()

    cursor.execute("SELECT v FROM #vec_desc;")
    assert cursor.description[0][0] == "v"
    assert cursor.description[0][1] is str


def test_vector_json_loads_round_trip(cursor, db_connection, requires_vector):
    """The returned text parses with json.loads, which is the documented pattern."""
    cursor.execute("CREATE TABLE #vec_json (v VECTOR(4));")
    values = [0.5, -2.5, 0.25, 4.0]  # exactly representable in float32
    cursor.execute(
        "INSERT INTO #vec_json VALUES (CAST(? AS VECTOR(4)));",
        json.dumps(values),
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_json;").fetchone()
    assert json.loads(row[0]) == values


# ==================== DIMENSIONS ====================


@pytest.mark.parametrize("dimension", [1, 2, 3, 100, 512, MAX_FLOAT32_DIMENSION])
def test_vector_supported_dimensions(cursor, db_connection, requires_vector, dimension):
    cursor.execute(f"CREATE TABLE #vec_dim (v VECTOR({dimension}));")
    db_connection.commit()

    literal = _vector_literal(dimension)
    cursor.execute(
        f"INSERT INTO #vec_dim VALUES (CAST(? AS VECTOR({dimension})));",
        literal,
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_dim;").fetchone()
    parsed = json.loads(row[0])
    assert len(parsed) == dimension
    assert all(value == 1.5 for value in parsed)

    cursor.execute("DROP TABLE #vec_dim;")
    db_connection.commit()


def test_vector_dimension_above_maximum_is_rejected(cursor, db_connection, requires_vector):
    """1999 dimensions exceeds the float32 limit and the server says so."""
    with pytest.raises(mssql_python.ProgrammingError) as exc:
        cursor.execute(f"CREATE TABLE #vec_too_big (v VECTOR({MAX_FLOAT32_DIMENSION + 1}));")
    assert str(MAX_FLOAT32_DIMENSION) in str(exc.value)


def test_vector_dimension_mismatch_is_rejected(cursor, db_connection, requires_vector):
    """A 2 element value cannot be cast into a 3 dimension vector."""
    cursor.execute("CREATE TABLE #vec_mismatch (v VECTOR(3));")
    db_connection.commit()

    with pytest.raises(mssql_python.ProgrammingError) as exc:
        cursor.execute(
            "INSERT INTO #vec_mismatch VALUES (CAST(? AS VECTOR(3)));",
            "[1.0, 2.0]",
        )
    assert "dimension" in str(exc.value).lower()


# ==================== PRECISION ====================


def test_vector_float32_precision_is_lossy(cursor, db_connection, requires_vector):
    """Values are stored as float32, so extra decimal places are not preserved.

    This is documented behaviour rather than a bug, and it is pinned here so the
    README claim stays honest.
    """
    cursor.execute("CREATE TABLE #vec_prec (v VECTOR(1));")
    cursor.execute(
        "INSERT INTO #vec_prec VALUES (CAST(? AS VECTOR(1)));",
        "[3.14159265]",
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_prec;").fetchone()
    stored = json.loads(row[0])[0]
    assert stored != 3.14159265
    assert stored == pytest.approx(3.14159265, rel=1e-6)


def test_vector_exact_float32_values_survive(cursor, db_connection, requires_vector):
    """Powers of two round trip exactly, which isolates the loss to precision."""
    cursor.execute("CREATE TABLE #vec_exact (v VECTOR(4));")
    values = [1.0, 0.5, 0.25, 8.0]
    cursor.execute(
        "INSERT INTO #vec_exact VALUES (CAST(? AS VECTOR(4)));",
        json.dumps(values),
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_exact;").fetchone()
    assert json.loads(row[0]) == values


def test_vector_negative_and_zero_values(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_signs (v VECTOR(3));")
    values = [-1.5, 0.0, 2.5]
    cursor.execute(
        "INSERT INTO #vec_signs VALUES (CAST(? AS VECTOR(3)));",
        json.dumps(values),
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_signs;").fetchone()
    assert json.loads(row[0]) == values


# ==================== NULLS ====================


def test_vector_null_literal(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_null (id INT, v VECTOR(3) NULL);")
    cursor.execute("INSERT INTO #vec_null VALUES (1, NULL);")
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_null;").fetchone()
    assert row[0] is None


def test_vector_null_parameter(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_null_param (id INT, v VECTOR(3) NULL);")
    cursor.execute(
        "INSERT INTO #vec_null_param VALUES (1, CAST(? AS VECTOR(3)));",
        None,
    )
    db_connection.commit()

    row = cursor.execute("SELECT v FROM #vec_null_param;").fetchone()
    assert row[0] is None


def test_vector_mixed_null_and_values(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_mixed (id INT, v VECTOR(3) NULL);")
    cursor.execute("INSERT INTO #vec_mixed VALUES (1, CAST(? AS VECTOR(3)));", SAMPLE_JSON)
    cursor.execute("INSERT INTO #vec_mixed VALUES (2, NULL);")
    db_connection.commit()

    rows = cursor.execute("SELECT id, v FROM #vec_mixed ORDER BY id;").fetchall()
    assert json.loads(rows[0][1]) == SAMPLE
    assert rows[1][1] is None


# ==================== BATCHING AND FETCHING ====================


def test_vector_executemany(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_many (id INT, v VECTOR(3));")
    db_connection.commit()

    rows = [(i, f"[{i}.0,{i}.0,{i}.0]") for i in range(1, 6)]
    cursor.executemany("INSERT INTO #vec_many VALUES (?, CAST(? AS VECTOR(3)));", rows)
    db_connection.commit()

    fetched = cursor.execute("SELECT id, v FROM #vec_many ORDER BY id;").fetchall()
    assert len(fetched) == 5
    for index, row in enumerate(fetched, start=1):
        assert json.loads(row[1]) == [float(index)] * 3


def test_vector_fetchall_multiple_rows(cursor, db_connection, requires_vector):
    cursor.execute("CREATE TABLE #vec_multi (id INT, v VECTOR(2));")
    for i in range(1, 4):
        cursor.execute(
            "INSERT INTO #vec_multi VALUES (?, CAST(? AS VECTOR(2)));",
            (i, f"[{i}.0,{i}.0]"),
        )
    db_connection.commit()

    rows = cursor.execute("SELECT v FROM #vec_multi ORDER BY id;").fetchall()
    assert len(rows) == 3
    assert all(isinstance(row[0], str) for row in rows)


# ==================== VECTOR FUNCTIONS ====================


def test_vector_distance_cosine(cursor, db_connection, requires_vector):
    """VECTOR_DISTANCE works against a parameter, which is the search use case."""
    cursor.execute("CREATE TABLE #vec_dist (v VECTOR(3));")
    cursor.execute("INSERT INTO #vec_dist VALUES (CAST(? AS VECTOR(3)));", SAMPLE_JSON)
    db_connection.commit()

    row = cursor.execute(
        "SELECT VECTOR_DISTANCE('cosine', CAST(? AS VECTOR(3)), v) FROM #vec_dist;",
        SAMPLE_JSON,
    ).fetchone()
    # Identical vectors, so cosine distance is zero within float tolerance.
    assert row[0] == pytest.approx(0.0, abs=1e-6)


def test_vector_distance_orders_results(cursor, db_connection, requires_vector):
    """The nearest vector sorts first, which is what a similarity query relies on."""
    cursor.execute("CREATE TABLE #vec_order (id INT, v VECTOR(2));")
    cursor.execute("INSERT INTO #vec_order VALUES (1, CAST(? AS VECTOR(2)));", "[1.0, 0.0]")
    cursor.execute("INSERT INTO #vec_order VALUES (2, CAST(? AS VECTOR(2)));", "[0.0, 1.0]")
    db_connection.commit()

    rows = cursor.execute(
        "SELECT id FROM #vec_order " "ORDER BY VECTOR_DISTANCE('cosine', CAST(? AS VECTOR(2)), v);",
        "[1.0, 0.0]",
    ).fetchall()
    assert rows[0][0] == 1


# ==================== INPUT VALIDATION ====================


def test_vector_malformed_input_is_rejected(cursor, db_connection, requires_vector):
    """Text that is not a JSON array fails as a normal query error."""
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("SELECT CAST(? AS VECTOR(3));", "not-a-vector")


def test_vector_empty_array_is_rejected(cursor, db_connection, requires_vector):
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("SELECT CAST(? AS VECTOR(3));", "[]")


def test_vector_float16_base_type_is_rejected(cursor, db_connection, requires_vector):
    """float16 is not a recognised base type yet, so only float32 is documented."""
    with pytest.raises(mssql_python.ProgrammingError) as exc:
        cursor.execute("CREATE TABLE #vec_f16 (v VECTOR(3, float16));")
    assert "float16" in str(exc.value).lower()


def test_connection_usable_after_vector_error(cursor, db_connection, requires_vector):
    """A rejected vector statement must not poison the connection."""
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("SELECT CAST(? AS VECTOR(3));", "[1.0, 2.0]")

    assert cursor.execute("SELECT 1;").fetchone()[0] == 1


# ==================== SERVERS WITHOUT THE VECTOR TYPE ====================


def test_vector_type_rejected_on_older_server(cursor, db_connection, requires_no_vector):
    """On SQL Server 2022 and earlier the type does not exist.

    The point of this test is that the failure is an ordinary query error rather
    than a crash or a hung connection.
    """
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("SELECT CAST(? AS VECTOR(3));", SAMPLE_JSON)


def test_connection_usable_after_unsupported_vector(cursor, db_connection, requires_no_vector):
    """The connection survives the unsupported type error and keeps working."""
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("SELECT CAST(? AS VECTOR(3));", SAMPLE_JSON)

    assert cursor.execute("SELECT 1;").fetchone()[0] == 1
