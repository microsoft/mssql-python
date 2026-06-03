"""
Tests for the session metadata / auditing API (set_audit_context / get_audit_context).

Functions:
- test_set_and_get_audit_context: Set named fields and verify local cache.
- test_audit_context_server_roundtrip: Verify values are readable via SESSION_CONTEXT().
- test_audit_context_extra_keys: Test arbitrary extra key-value pairs.
- test_audit_context_merge: Successive calls merge, not replace.
- test_audit_context_empty_call: Calling with no arguments is a no-op.
- test_audit_context_clear_value: Setting a key to "" clears it server-side.
- test_audit_context_read_only: read_only=True prevents subsequent changes.
- test_audit_context_closed_connection: Raises InterfaceError when connection is closed.
- test_audit_context_key_too_long: Raises ProgrammingError for oversized keys.
- test_audit_context_value_too_long: Raises ProgrammingError for oversized values.
- test_audit_context_non_string_value: Raises ProgrammingError for non-string values.
"""

import pytest
from mssql_python import connect
from mssql_python.exceptions import InterfaceError, ProgrammingError, DatabaseError


@pytest.fixture()
def audit_conn(conn_str):
    """Dedicated connection for audit context tests (module-scoped fixtures
    would share session state, so we create a fresh connection per test)."""
    conn = connect(conn_str)
    yield conn
    conn.close()


class TestAuditContext:
    """Tests for Connection.set_audit_context / get_audit_context."""

    def test_set_and_get_audit_context(self, audit_conn):
        """Named fields are reflected in the local cache."""
        audit_conn.set_audit_context(
            application="BillingAPI",
            module="InvoiceProcessor",
            action="GenerateInvoice",
            user_id="123",
        )
        ctx = audit_conn.get_audit_context()
        assert ctx["application_name"] == "BillingAPI"
        assert ctx["module_name"] == "InvoiceProcessor"
        assert ctx["action_name"] == "GenerateInvoice"
        assert ctx["user_id"] == "123"

    def test_audit_context_server_roundtrip(self, audit_conn):
        """Values set via set_audit_context are readable with SESSION_CONTEXT()."""
        audit_conn.set_audit_context(application="RoundTrip", user_id="42")
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

    def test_audit_context_extra_keys(self, audit_conn):
        """Arbitrary extra keys are stored via sp_set_session_context."""
        audit_conn.set_audit_context(tenant_id="ACME", correlation_id="abc-def")
        ctx = audit_conn.get_audit_context()
        assert ctx["tenant_id"] == "ACME"
        assert ctx["correlation_id"] == "abc-def"

        # Verify server-side
        cursor = audit_conn.cursor()
        try:
            cursor.execute("SELECT SESSION_CONTEXT(N'tenant_id')")
            assert cursor.fetchone()[0] == "ACME"
        finally:
            cursor.close()

    def test_audit_context_merge(self, audit_conn):
        """Successive calls merge values, not replace."""
        audit_conn.set_audit_context(application="App1")
        audit_conn.set_audit_context(module="Mod1")
        ctx = audit_conn.get_audit_context()
        assert ctx["application_name"] == "App1"
        assert ctx["module_name"] == "Mod1"

    def test_audit_context_overwrite(self, audit_conn):
        """A second call with the same key overwrites the previous value."""
        audit_conn.set_audit_context(action="First")
        audit_conn.set_audit_context(action="Second")
        assert audit_conn.get_audit_context()["action_name"] == "Second"

    def test_audit_context_empty_call(self, audit_conn):
        """Calling with no arguments is a silent no-op."""
        audit_conn.set_audit_context()
        assert audit_conn.get_audit_context() == {}

    def test_audit_context_clear_value(self, audit_conn):
        """Setting a key to '' clears it (sends NULL to the server)."""
        audit_conn.set_audit_context(user_id="99")
        audit_conn.set_audit_context(user_id="")
        assert "user_id" not in audit_conn.get_audit_context()

    def test_audit_context_read_only(self, audit_conn):
        """read_only=True makes the key immutable for the session."""
        audit_conn.set_audit_context(action="Locked", read_only=True)
        # Attempting to change a read-only key should raise a DatabaseError
        # from SQL Server (error 15664).
        with pytest.raises(DatabaseError):
            audit_conn.set_audit_context(action="Changed")

    def test_audit_context_closed_connection_set(self, audit_conn):
        """set_audit_context raises InterfaceError on a closed connection."""
        audit_conn.close()
        with pytest.raises(InterfaceError):
            audit_conn.set_audit_context(application="X")

    def test_audit_context_closed_connection_get(self, audit_conn):
        """get_audit_context raises InterfaceError on a closed connection."""
        audit_conn.close()
        with pytest.raises(InterfaceError):
            audit_conn.get_audit_context()

    def test_audit_context_key_too_long(self, audit_conn):
        """Keys longer than 128 characters are rejected."""
        with pytest.raises(ProgrammingError):
            audit_conn.set_audit_context(**{"x" * 200: "v"})

    def test_audit_context_value_too_long(self, audit_conn):
        """Values longer than 8000 characters are rejected."""
        with pytest.raises(ProgrammingError):
            audit_conn.set_audit_context(user_id="v" * 8001)

    def test_audit_context_non_string_value(self, audit_conn):
        """Non-string values are rejected with ProgrammingError."""
        with pytest.raises((ProgrammingError, TypeError)):
            audit_conn.set_audit_context(user_id=123)  # type: ignore[arg-type]

    def test_get_audit_context_returns_copy(self, audit_conn):
        """get_audit_context returns a copy, not the internal dict."""
        audit_conn.set_audit_context(application="Copy")
        ctx = audit_conn.get_audit_context()
        ctx["application_name"] = "Mutated"
        assert audit_conn.get_audit_context()["application_name"] == "Copy"
