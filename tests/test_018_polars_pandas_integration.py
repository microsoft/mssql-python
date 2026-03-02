"""Tests that polars and pandas correctly infer schemas from cursor.description type codes,
and that setinputsizes works with ODBC 3.x date/time type codes."""

import datetime
import inspect
import pytest

from mssql_python.constants import ConstantsDDBC

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ── cursor.description type_code verification ─────────────────────────────


class TestCursorDescriptionTypeCodes:
    """Verify cursor.description returns isclass-compatible Python types."""

    def test_date_type_code_is_datetime_date(self, cursor):
        """DATE columns must report datetime.date, not str."""
        cursor.execute("SELECT CAST('2024-01-15' AS DATE) AS d")
        type_code = cursor.description[0][1]
        assert type_code is datetime.date
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_time_type_code_is_datetime_time(self, cursor):
        """TIME columns must report datetime.time."""
        cursor.execute("SELECT CAST('13:45:30' AS TIME) AS t")
        type_code = cursor.description[0][1]
        assert type_code is datetime.time
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_datetime_type_code_is_datetime_datetime(self, cursor):
        """DATETIME columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:30' AS DATETIME) AS dt")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_datetime2_type_code_is_datetime_datetime(self, cursor):
        """DATETIME2 columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:30.1234567' AS DATETIME2) AS dt2")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_smalldatetime_type_code_is_datetime_datetime(self, cursor):
        """SMALLDATETIME columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_datetimeoffset_type_code_is_datetime_datetime(self, cursor):
        """DATETIMEOFFSET columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:30.123 +05:30' AS DATETIMEOFFSET) AS dto")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_all_types_are_isclass(self, cursor):
        """Every type_code in cursor.description must pass inspect.isclass()."""
        cursor.execute("""
            SELECT
                CAST(1 AS INT) AS i,
                CAST(1 AS SMALLINT) AS si,
                CAST(1 AS TINYINT) AS ti,
                CAST(1 AS BIGINT) AS bi,
                CAST('x' AS CHAR(1)) AS c,
                CAST('x' AS VARCHAR(10)) AS vc,
                CAST('x' AS NCHAR(1)) AS nc,
                CAST('x' AS NVARCHAR(10)) AS nvc,
                CAST(1.5 AS FLOAT) AS f,
                CAST(1.5 AS REAL) AS r,
                CAST(1.5 AS DECIMAL(10,2)) AS dec,
                CAST(1.5 AS NUMERIC(10,2)) AS num,
                CAST(1 AS BIT) AS b,
                CAST('2024-01-15' AS DATE) AS d,
                CAST('13:45:30' AS TIME) AS t,
                CAST('2024-01-15 13:45:30' AS DATETIME) AS dt,
                CAST('2024-01-15 13:45:30' AS DATETIME2) AS dt2,
                CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt,
                CAST('2024-01-15 13:45:30 +05:30' AS DATETIMEOFFSET) AS dto,
                CAST(0x01 AS BINARY(1)) AS bin,
                CAST(0x01 AS VARBINARY(10)) AS vbin,
                NEWID() AS guid,
                CAST('<r/>' AS XML) AS x
            """)
        for desc in cursor.description:
            col_name = desc[0]
            type_code = desc[1]
            assert inspect.isclass(
                type_code
            ), f"Column '{col_name}': type_code={type_code!r} fails isclass()"
        cursor.fetchall()


# ── Polars integration ────────────────────────────────────────────────────


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestPolarsIntegration:
    """Polars read_database must infer correct dtypes from cursor.description."""

    def test_polars_date_column(self, db_connection):
        """Issue #352: DATE columns caused ComputeError in polars."""
        df = pl.read_database(
            query="SELECT CAST('2024-01-15' AS DATE) AS d",
            connection=db_connection,
        )
        assert df.schema["d"] == pl.Date
        assert df["d"][0] == datetime.date(2024, 1, 15)

    def test_polars_all_datetime_types(self, db_connection):
        """All date/time types must produce correct polars dtypes."""
        df = pl.read_database(
            query="""
                SELECT
                    CAST('2024-01-15' AS DATE) AS d,
                    CAST('13:45:30' AS TIME) AS t,
                    CAST('2024-01-15 13:45:30' AS DATETIME) AS dt,
                    CAST('2024-01-15 13:45:30.123' AS DATETIME2) AS dt2,
                    CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt,
                    CAST('2024-01-15 13:45:30.123 +05:30' AS DATETIMEOFFSET) AS dto
            """,
            connection=db_connection,
        )
        assert df.schema["d"] == pl.Date
        assert df.schema["t"] == pl.Time
        assert df.schema["dt"] == pl.Datetime
        assert df.schema["dt2"] == pl.Datetime
        assert df.schema["sdt"] == pl.Datetime
        assert df.schema["dto"] == pl.Datetime

    def test_polars_mixed_types(self, db_connection):
        """Mixed column types with DATE must not cause schema mismatch."""
        df = pl.read_database(
            query="""
                SELECT
                    CAST(42 AS INT) AS i,
                    CAST('hello' AS NVARCHAR(50)) AS s,
                    CAST('2024-06-15' AS DATE) AS d,
                    CAST(99.95 AS DECIMAL(10,2)) AS amount
            """,
            connection=db_connection,
        )
        assert df["i"][0] == 42
        assert df["s"][0] == "hello"
        assert df["d"][0] == datetime.date(2024, 6, 15)
        assert df.schema["d"] == pl.Date

    def test_polars_date_with_nulls(self, db_connection):
        """DATE columns with NULLs must still infer Date dtype."""
        cursor = db_connection.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS #polars_null_test")
            cursor.execute("""
                CREATE TABLE #polars_null_test (
                    id INT,
                    d DATE
                )
                """)
            cursor.execute("""
                INSERT INTO #polars_null_test VALUES
                (1, '2024-01-15'),
                (2, NULL),
                (3, '2024-03-20')
                """)
            db_connection.commit()

            df = pl.read_database(
                query="SELECT * FROM #polars_null_test ORDER BY id",
                connection=db_connection,
            )
            assert df.schema["d"] == pl.Date
            assert df["d"][0] == datetime.date(2024, 1, 15)
            assert df["d"][1] is None
            assert df["d"][2] == datetime.date(2024, 3, 20)
        finally:
            try:
                cursor.execute("DROP TABLE IF EXISTS #polars_null_test")
                db_connection.commit()
            except Exception:
                # Intentionally ignore cleanup errors: the temp table may not exist
                # or the connection may already be closed, and this should not fail the test.
                pass
            cursor.close()


# ── Pandas integration ────────────────────────────────────────────────────


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@pytest.mark.filterwarnings("ignore:pandas only supports SQLAlchemy connectable:UserWarning")
class TestPandasIntegration:
    """Pandas read_sql must handle date/time columns correctly."""

    def test_pandas_date_column(self, db_connection):
        """DATE columns must be readable by pandas without error."""
        df = pd.read_sql(
            "SELECT CAST('2024-01-15' AS DATE) AS d",
            db_connection,
        )
        assert len(df) == 1
        val = df["d"].iloc[0]
        # pandas may return datetime or date depending on version
        if isinstance(val, datetime.datetime):
            assert val.date() == datetime.date(2024, 1, 15)
        else:
            assert val == datetime.date(2024, 1, 15)

    def test_pandas_all_datetime_types(self, db_connection):
        """All date/time types must be readable by pandas."""
        df = pd.read_sql(
            """
            SELECT
                CAST('2024-01-15' AS DATE) AS d,
                CAST('13:45:30' AS TIME) AS t,
                CAST('2024-01-15 13:45:30' AS DATETIME) AS dt,
                CAST('2024-01-15 13:45:30.123' AS DATETIME2) AS dt2,
                CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt,
                CAST('2024-01-15 13:45:30.123 +05:30' AS DATETIMEOFFSET) AS dto
            """,
            db_connection,
        )
        assert len(df) == 1
        assert len(df.columns) == 6
        # Verify date column value (pandas may return datetime or date)
        val = df["d"].iloc[0]
        if isinstance(val, datetime.datetime):
            assert val.date() == datetime.date(2024, 1, 15)
        else:
            assert val == datetime.date(2024, 1, 15)
        # Verify time column is a time object
        t_val = df["t"].iloc[0]
        assert isinstance(t_val, datetime.time)

    def test_pandas_mixed_types_with_date(self, db_connection):
        """Mixed column types including DATE must work correctly."""
        df = pd.read_sql(
            """
            SELECT
                CAST(42 AS INT) AS i,
                CAST('hello' AS NVARCHAR(50)) AS s,
                CAST('2024-06-15' AS DATE) AS d,
                CAST(99.95 AS DECIMAL(10,2)) AS amount
            """,
            db_connection,
        )
        assert df["i"].iloc[0] == 42
        assert df["s"].iloc[0] == "hello"
        val = df["d"].iloc[0]
        if isinstance(val, datetime.datetime):
            assert val.date() == datetime.date(2024, 6, 15)
        else:
            assert val == datetime.date(2024, 6, 15)


# ── setinputsizes with ODBC 3.x date/time type codes ─────────────────────


class TestSetInputSizesDateTimeTypes:
    """setinputsizes must accept ODBC 3.x date/time codes and round-trip values."""

    def test_setinputsizes_sql_type_date(self, db_connection):
        """SQL_TYPE_DATE (91) round-trips a date value."""
        cursor = db_connection.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS #sis_date")
            cursor.execute("CREATE TABLE #sis_date (d DATE)")
            cursor.setinputsizes([(ConstantsDDBC.SQL_TYPE_DATE.value, 0, 0)])
            cursor.execute("INSERT INTO #sis_date VALUES (?)", datetime.date(2024, 1, 15))
            db_connection.commit()
            cursor.execute("SELECT d FROM #sis_date")
            row = cursor.fetchone()
            assert row[0] == datetime.date(2024, 1, 15)
        finally:
            cursor.execute("DROP TABLE IF EXISTS #sis_date")
            cursor.close()

    def test_setinputsizes_sql_type_time(self, db_connection):
        """SQL_TYPE_TIME (92) round-trips a time value."""
        cursor = db_connection.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS #sis_time")
            cursor.execute("CREATE TABLE #sis_time (t TIME)")
            cursor.setinputsizes([(ConstantsDDBC.SQL_TYPE_TIME.value, 0, 0)])
            cursor.execute("INSERT INTO #sis_time VALUES (?)", datetime.time(13, 45, 30))
            db_connection.commit()
            cursor.execute("SELECT t FROM #sis_time")
            row = cursor.fetchone()
            assert row[0].hour == 13
            assert row[0].minute == 45
            assert row[0].second == 30
        finally:
            cursor.execute("DROP TABLE IF EXISTS #sis_time")
            cursor.close()

    def test_setinputsizes_sql_type_timestamp(self, db_connection):
        """SQL_TYPE_TIMESTAMP (93) round-trips a datetime value."""
        cursor = db_connection.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS #sis_ts")
            cursor.execute("CREATE TABLE #sis_ts (dt DATETIME2)")
            cursor.setinputsizes([(ConstantsDDBC.SQL_TYPE_TIMESTAMP.value, 0, 0)])
            cursor.execute(
                "INSERT INTO #sis_ts VALUES (?)",
                datetime.datetime(2024, 1, 15, 13, 45, 30),
            )
            db_connection.commit()
            cursor.execute("SELECT dt FROM #sis_ts")
            row = cursor.fetchone()
            assert row[0] == datetime.datetime(2024, 1, 15, 13, 45, 30)
        finally:
            cursor.execute("DROP TABLE IF EXISTS #sis_ts")
            cursor.close()

    def test_setinputsizes_sql_ss_time2(self, db_connection):
        """SQL_SS_TIME2 (-154) round-trips a time value."""
        cursor = db_connection.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS #sis_time2")
            cursor.execute("CREATE TABLE #sis_time2 (t TIME)")
            cursor.setinputsizes([(ConstantsDDBC.SQL_SS_TIME2.value, 0, 0)])
            cursor.execute("INSERT INTO #sis_time2 VALUES (?)", datetime.time(9, 30, 0))
            db_connection.commit()
            cursor.execute("SELECT t FROM #sis_time2")
            row = cursor.fetchone()
            assert row[0].hour == 9
            assert row[0].minute == 30
        finally:
            cursor.execute("DROP TABLE IF EXISTS #sis_time2")
            cursor.close()

    def test_setinputsizes_sql_datetimeoffset(self, db_connection):
        """SQL_DATETIMEOFFSET (-155) round-trips a datetime with timezone."""
        cursor = db_connection.cursor()
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        dt_in = datetime.datetime(2024, 1, 15, 13, 45, 30, tzinfo=tz)
        try:
            cursor.execute("DROP TABLE IF EXISTS #sis_dto")
            cursor.execute("CREATE TABLE #sis_dto (dto DATETIMEOFFSET)")
            cursor.setinputsizes([(ConstantsDDBC.SQL_DATETIMEOFFSET.value, 0, 0)])
            cursor.execute("INSERT INTO #sis_dto VALUES (?)", dt_in)
            db_connection.commit()
            cursor.execute("SELECT dto FROM #sis_dto")
            row = cursor.fetchone()
            assert row[0].year == 2024
            assert row[0].month == 1
            assert row[0].day == 15
            assert row[0].hour == 13
            assert row[0].minute == 45
        finally:
            cursor.execute("DROP TABLE IF EXISTS #sis_dto")
            cursor.close()
