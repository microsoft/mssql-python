"""Live regression coverage for query timeout and subsequent cursor recovery."""

import pytest

from mssql_python import OperationalError
from mssql_python.cursor import Cursor


def test_query_timeout_and_recovery(db_connection):
    with Cursor(db_connection, timeout=1) as cursor:
        with pytest.raises(OperationalError, match="[Tt]imeout"):
            cursor.execute("WAITFOR DELAY '00:00:03'; SELECT 1")
        assert cursor.execute("SELECT 42").fetchone()[0] == 42
    with Cursor(db_connection, timeout=0) as cursor:
        assert cursor.execute("SELECT 43").fetchone()[0] == 43
