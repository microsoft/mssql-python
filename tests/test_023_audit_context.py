"""
Tests for the session context API
(set_session_context / get_session_context / clear_session_context).

Functions:
- test_set_and_get_session_context: Set named fields and verify via server query.
- test_session_context_server_roundtrip: Verify values are readable via SESSION_CONTEXT().
- test_session_context_extra_keys: Test arbitrary extra key-value pairs.
- test_session_context_merge: Successive calls merge, not replace.
- test_session_context_empty_call: Calling with no arguments is a no-op.
- test_session_context_clear_value: Setting a key to None clears it server-side.
- test_session_context_read_only: read_only=True prevents subsequent changes.
- test_session_context_closed_connection: Raises InterfaceError when connection is closed.
- test_session_context_key_too_long: Raises ProgrammingError for oversized keys.
- test_session_context_value_too_long: Raises ProgrammingError for oversized values.
- test_session_context_non_string_value: Raises ProgrammingError for non-string values.
- test_clear_session_context_single_key: Clear one key.
- test_clear_session_context_multiple_keys: Clear several keys.
- test_clear_session_context_all: Clear all keys.
- test_clear_session_context_read_only_raises: Clearing a read-only key raises ProgrammingError.
- test_clear_session_context_closed: Raises InterfaceError on closed connection.
- test_clear_session_context_noop: No-op when nothing has been set.
- test_get_session_context_reflects_server: Getter fetches live server values.
- test_pool_return_clears_context: Session context is cleared when pooled connection is closed.
"""

import pytest
from mssql_python import connect
from mssql_python.exceptions import InterfaceError, ProgrammingError, DatabaseError


@pytest.fixture()
def audit_conn(conn_str):
    """Dedicated connection for session context tests (module-scoped fixtures
    would share session state, so we create a fresh connection per test)."""
    conn = connect(conn_str)
    yield conn
    conn.close()


class TestSessionContext:
    """Tests for Connection.set_session_context / get_session_context."""

    def test_set_and_get_session_context(self, audit_conn):
        """Named fields are reflected in the local cache."""
        audit_conn.set_session_context(
            application_name="BillingAPI",
            module_name="InvoiceProcessor",
            action_name="GenerateInvoice",
            user_id="123",
        )
        ctx = audit_conn.get_session_context()
        assert ctx["application_name"] == "BillingAPI"
        assert ctx["module_name"] == "InvoiceProcessor"
        assert ctx["action_name"] == "GenerateInvoice"
        assert ctx["user_id"] == "123"

    def test_session_context_server_roundtrip(self, audit_conn):
        """Values set via set_session_context are readable with SESSION_CONTEXT()."""
        audit_conn.set_session_context(application_name="RoundTrip", user_id="42")
        cursor = audit_conn.cursor()
        try:
            cursor.execute("SELECT SESSION_CONTEXT(N'application_name')")
            row = cursor.fetchone()
            assert row[0] == "RoundTrip"

            cursor.execute("SELECT SESSION_CONTEXT(N'user_id')")
            row = cursor.fetchone()
            assert row[0] == "42"
        finally:
            cursor.close()

    def test_session_context_extra_keys(self, audit_conn):
        """Arbitrary extra keys are stored via sp_set_session_context."""
        audit_conn.set_session_context(tenant_id="ACME", correlation_id="abc-def")
        ctx = audit_conn.get_session_context()
        assert ctx["tenant_id"] == "ACME"
        assert ctx["correlation_id"] == "abc-def"

        # Verify server-side
        cursor = audit_conn.cursor()
        try:
            cursor.execute("SELECT SESSION_CONTEXT(N'tenant_id')")
            assert cursor.fetchone()[0] == "ACME"
        finally:
            cursor.close()

    def test_session_context_merge(self, audit_conn):
        """Successive calls merge values, not replace."""
        audit_conn.set_session_context(application_name="App1")
        audit_conn.set_session_context(module_name="Mod1")
        ctx = audit_conn.get_session_context()
        assert ctx["application_name"] == "App1"
        assert ctx["module_name"] == "Mod1"

    def test_session_context_overwrite(self, audit_conn):
        """A second call with the same key overwrites the previous value."""
        audit_conn.set_session_context(action_name="First")
        audit_conn.set_session_context(action_name="Second")
        assert audit_conn.get_session_context()["action_name"] == "Second"

    def test_session_context_empty_call(self, audit_conn):
        """Calling with no arguments is a silent no-op."""
        audit_conn.set_session_context()
        assert audit_conn.get_session_context() == {}

    def test_session_context_clear_value(self, audit_conn):
        """Setting a key to None clears it (sends NULL to the server)."""
        audit_conn.set_session_context(user_id="99")
        audit_conn.set_session_context(user_id=None)
        assert "user_id" not in audit_conn.get_session_context()

    def test_session_context_read_only(self, audit_conn):
        """read_only=True makes the key immutable for the session."""
        audit_conn.set_session_context(action_name="Locked", read_only=True)
        # Attempting to change a read-only key should raise a DatabaseError
        # from SQL Server (error 15664).
        with pytest.raises(DatabaseError):
            audit_conn.set_session_context(action_name="Changed")

    def test_session_context_read_only_clear_via_none(self, audit_conn):
        """Setting a read-only key to None raises ProgrammingError."""
        audit_conn.set_session_context(action_name="Locked", read_only=True)
        with pytest.raises(ProgrammingError):
            audit_conn.set_session_context(action_name=None)

    def test_session_context_closed_connection_set(self, audit_conn):
        """set_session_context raises InterfaceError on a closed connection."""
        audit_conn.close()
        with pytest.raises(InterfaceError):
            audit_conn.set_session_context(application_name="X")

    def test_session_context_closed_connection_get(self, audit_conn):
        """get_session_context raises InterfaceError on a closed connection."""
        audit_conn.close()
        with pytest.raises(InterfaceError):
            audit_conn.get_session_context()

    def test_session_context_key_max_length(self, audit_conn):
        """A key at exactly 128 characters is accepted."""
        key = "k" * 128
        audit_conn.set_session_context(**{key: "val"})
        ctx = audit_conn.get_session_context()
        assert ctx[key] == "val"

    def test_session_context_key_one_over_max(self, audit_conn):
        """A key at 129 characters is rejected."""
        with pytest.raises(ProgrammingError):
            audit_conn.set_session_context(**{"k" * 129: "v"})

    def test_session_context_key_too_long(self, audit_conn):
        """Keys longer than 128 characters are rejected."""
        with pytest.raises(ProgrammingError):
            audit_conn.set_session_context(**{"x" * 200: "v"})

    def test_session_context_value_max_length(self, audit_conn):
        """A value at exactly 4000 characters is accepted."""
        val = "v" * 4000
        audit_conn.set_session_context(user_id=val)
        ctx = audit_conn.get_session_context()
        assert ctx["user_id"] == val

    def test_session_context_value_one_over_max(self, audit_conn):
        """A value at 4001 characters is rejected."""
        with pytest.raises(ProgrammingError):
            audit_conn.set_session_context(user_id="v" * 4001)

    def test_session_context_non_string_value(self, audit_conn):
        """Non-string values are rejected with ProgrammingError."""
        with pytest.raises(ProgrammingError):
            audit_conn.set_session_context(user_id=123)  # type: ignore[arg-type]

    def test_get_session_context_returns_fresh(self, audit_conn):
        """get_session_context queries the server and returns a fresh dict each time."""
        audit_conn.set_session_context(application_name="Fresh")
        ctx1 = audit_conn.get_session_context()
        ctx2 = audit_conn.get_session_context()
        assert ctx1 == ctx2
        assert ctx1 is not ctx2  # distinct dict objects

    def test_get_session_context_reflects_server(self, audit_conn):
        """Getter fetches live values from the server, not stale cache."""
        audit_conn.set_session_context(user_id="original")
        # Mutate the value directly via T-SQL (bypassing the Python API)
        cursor = audit_conn.cursor()
        try:
            cursor.execute(
                "EXEC sp_set_session_context @key=N'user_id', @value=N'mutated'"
            )
        finally:
            cursor.close()
        # The getter should reflect the server-side mutation
        ctx = audit_conn.get_session_context()
        assert ctx["user_id"] == "mutated"

    # ---- clear_session_context tests ----

    def test_clear_session_context_single_key(self, audit_conn):
        """Clearing a single key removes it from the server."""
        audit_conn.set_session_context(user_id="1", module_name="Mod")
        audit_conn.clear_session_context("user_id")
        ctx = audit_conn.get_session_context()
        assert "user_id" not in ctx
        assert ctx["module_name"] == "Mod"

    def test_clear_session_context_multiple_keys(self, audit_conn):
        """Clearing multiple keys removes them all."""
        audit_conn.set_session_context(
            user_id="1", module_name="Mod", action_name="Act"
        )
        audit_conn.clear_session_context("user_id", "action_name")
        ctx = audit_conn.get_session_context()
        assert "user_id" not in ctx
        assert "action_name" not in ctx
        assert ctx["module_name"] == "Mod"

    def test_clear_session_context_all(self, audit_conn):
        """Calling with no args clears all non-read-only keys."""
        audit_conn.set_session_context(user_id="1", module_name="Mod")
        audit_conn.clear_session_context()
        assert audit_conn.get_session_context() == {}

    def test_clear_session_context_read_only_raises(self, audit_conn):
        """Explicitly clearing a read-only key raises ProgrammingError."""
        audit_conn.set_session_context(user_id="locked", read_only=True)
        with pytest.raises(ProgrammingError):
            audit_conn.clear_session_context("user_id")

    def test_clear_session_context_all_skips_read_only(self, audit_conn):
        """clear_session_context() without args skips read-only keys."""
        audit_conn.set_session_context(user_id="locked", read_only=True)
        audit_conn.set_session_context(module_name="clearable")
        audit_conn.clear_session_context()
        ctx = audit_conn.get_session_context()
        assert ctx["user_id"] == "locked"
        assert "module_name" not in ctx

    def test_clear_session_context_closed(self, audit_conn):
        """clear_session_context raises InterfaceError on a closed connection."""
        audit_conn.close()
        with pytest.raises(InterfaceError):
            audit_conn.clear_session_context("user_id")

    def test_clear_session_context_noop(self, audit_conn):
        """Clearing when nothing has been set is a silent no-op."""
        audit_conn.clear_session_context()  # should not raise

    # ---- Pool return tests ----

    def test_pool_return_clears_context(self, conn_str):
        """When pooling is enabled, close() clears session context server-side."""
        conn = connect(conn_str)
        conn.set_session_context(application_name="PoolTest", module_name="Mod")
        # Simulate pooling enabled
        conn._pooling = True
        conn.close()
        # After close the cache should be empty
        assert not getattr(conn, "_session_context", {})

    def test_pool_return_skips_without_pooling(self, conn_str):
        """Without pooling, close() does not attempt to clear session context."""
        conn = connect(conn_str)
        conn.set_session_context(application_name="NoPools")
        conn._pooling = False
        conn.close()
        # Cache is left as-is (object is closed, no pool reuse)
        assert conn._session_context.get("application_name") == "NoPools"