"""
Subprocess-isolated tests for Connection context manager transaction semantics.

Validates:
- Commit on clean exit (autocommit=False)
- Rollback on exception (autocommit=False)
- No commit/rollback when autocommit=True
- No segfault or hang on broken connection
- Exception masking: user exception preserved, not replaced by cleanup errors
- Double context manager usage
- GC safety: no crash if connection is collected without __exit__

Each test runs in a subprocess to catch segfaults (SIGSEGV) that would
otherwise kill the test runner.
"""

import os
import sys
import subprocess
import textwrap
import signal
import pytest

CONN_STR = os.getenv("DB_CONNECTION_STRING")
PYTHON = sys.executable
TIMEOUT = 30


def _run_script(script: str, timeout: int = TIMEOUT) -> subprocess.CompletedProcess:
    """Run a Python script in a subprocess, return the result."""
    env = os.environ.copy()
    env["DB_CONNECTION_STRING"] = CONN_STR or ""
    result = subprocess.run(
        [PYTHON, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    return result


def _assert_no_crash(result: subprocess.CompletedProcess, context: str = ""):
    """Assert the subprocess did not segfault."""
    if result.returncode < 0:
        sig = -result.returncode
        try:
            sig_name = signal.Signals(sig).name
        except ValueError:
            sig_name = str(sig)
        pytest.fail(
            f"CRASH ({sig_name}) {context}\n"
            f"stdout: {result.stdout[-500:]}\n"
            f"stderr: {result.stderr[-500:]}"
        )


@pytest.mark.skipif(not CONN_STR, reason="DB_CONNECTION_STRING not set")
class TestContextManagerCommit:
    """Test that context manager commits on clean exit."""

    def test_commit_on_clean_exit(self):
        """Data inserted inside `with` block should persist after exit."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Setup: create table
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_commit') IS NOT NULL DROP TABLE pytest_ctx_commit")
            setup.cursor().execute("CREATE TABLE pytest_ctx_commit (id INT, val VARCHAR(50))")
            setup.commit()
            setup.close()

            # Test: insert inside context manager, no manual commit
            with connect(cs) as conn:
                conn.cursor().execute("INSERT INTO pytest_ctx_commit VALUES (1, 'auto_committed')")
            # __exit__ should have committed

            # Verify: data should exist
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT val FROM pytest_ctx_commit WHERE id = 1")
            row = cur.fetchone()
            assert row is not None, "FAIL: data was not committed by context manager"
            assert row[0] == "auto_committed", f"FAIL: unexpected value {row[0]}"
            verify.close()

            # Cleanup
            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_commit")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_commit_on_clean_exit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_rollback_on_exception(self):
        """Data inserted inside `with` block should NOT persist if exception raised."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Setup
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_rollback') IS NOT NULL DROP TABLE pytest_ctx_rollback")
            setup.cursor().execute("CREATE TABLE pytest_ctx_rollback (id INT, val VARCHAR(50))")
            setup.commit()
            setup.close()

            # Test: insert then raise
            try:
                with connect(cs) as conn:
                    conn.cursor().execute("INSERT INTO pytest_ctx_rollback VALUES (1, 'should_vanish')")
                    raise ValueError("intentional error")
            except ValueError:
                pass

            # Verify: data should NOT exist
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT val FROM pytest_ctx_rollback WHERE id = 1")
            row = cur.fetchone()
            assert row is None, f"FAIL: data was NOT rolled back, got {row}"
            verify.close()

            # Cleanup
            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_rollback")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_rollback_on_exception")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_autocommit_no_explicit_commit(self):
        """When autocommit=True, no commit/rollback call in __exit__."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Setup
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_autocommit') IS NOT NULL DROP TABLE pytest_ctx_autocommit")
            setup.cursor().execute("CREATE TABLE pytest_ctx_autocommit (id INT)")
            setup.commit()
            setup.close()

            # Test: autocommit=True, each statement auto-commits
            with connect(cs, autocommit=True) as conn:
                conn.cursor().execute("INSERT INTO pytest_ctx_autocommit VALUES (1)")

            # Verify: data persists (auto-committed per statement)
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT id FROM pytest_ctx_autocommit WHERE id = 1")
            row = cur.fetchone()
            assert row is not None, "FAIL: autocommit data missing"
            verify.close()

            # Cleanup
            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_autocommit")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_autocommit_no_explicit_commit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_exception_not_masked(self):
        """User exception should propagate, not be replaced by cleanup errors."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            caught = None
            try:
                with connect(cs) as conn:
                    conn.cursor().execute("SELECT 1")
                    raise RuntimeError("user_error_42")
            except RuntimeError as e:
                caught = e

            assert caught is not None, "FAIL: exception not raised"
            assert "user_error_42" in str(caught), f"FAIL: wrong exception: {caught}"
            print("PASS")
        """)
        _assert_no_crash(result, "test_exception_not_masked")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_multiple_operations_single_commit(self):
        """Multiple inserts in one context block get committed together."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Setup
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_multi') IS NOT NULL DROP TABLE pytest_ctx_multi")
            setup.cursor().execute("CREATE TABLE pytest_ctx_multi (id INT)")
            setup.commit()
            setup.close()

            # Test: multiple inserts, one implicit commit
            with connect(cs) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO pytest_ctx_multi VALUES (1)")
                cur.execute("INSERT INTO pytest_ctx_multi VALUES (2)")
                cur.execute("INSERT INTO pytest_ctx_multi VALUES (3)")

            # Verify
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_multi")
            count = cur.fetchone()[0]
            assert count == 3, f"FAIL: expected 3 rows, got {count}"
            verify.close()

            # Cleanup
            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_multi")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_multiple_operations_single_commit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_partial_rollback_on_exception(self):
        """All inserts roll back if exception raised after multiple ops."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Setup
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_partial') IS NOT NULL DROP TABLE pytest_ctx_partial")
            setup.cursor().execute("CREATE TABLE pytest_ctx_partial (id INT)")
            setup.commit()
            setup.close()

            # Test: insert then raise
            try:
                with connect(cs) as conn:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO pytest_ctx_partial VALUES (1)")
                    cur.execute("INSERT INTO pytest_ctx_partial VALUES (2)")
                    raise ValueError("boom")
            except ValueError:
                pass

            # Verify: nothing committed
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_partial")
            count = cur.fetchone()[0]
            assert count == 0, f"FAIL: expected 0 rows, got {count}"
            verify.close()

            # Cleanup
            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_partial")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_partial_rollback_on_exception")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    @pytest.mark.stress
    def test_no_segfault_on_rapid_context_managers(self):
        """Rapid open/close cycles should not segfault."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            for i in range(50):
                with connect(cs) as conn:
                    conn.cursor().execute("SELECT 1")
            print("PASS")
        """)
        _assert_no_crash(result, "test_no_segfault_on_rapid_context_managers")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_no_segfault_gc_without_exit(self):
        """Connection created in context-like pattern but GC'd without __exit__ should not crash."""
        result = _run_script("""
            import os
            import gc
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            def make_connection():
                conn = connect(cs)
                conn.cursor().execute("SELECT 1")
                # No close, no context manager — just let it get GC'd
                return None

            for _ in range(20):
                make_connection()
                gc.collect()

            print("PASS")
        """)
        _assert_no_crash(result, "test_no_segfault_gc_without_exit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_nested_context_managers(self):
        """Nested connection context managers should each commit independently."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Setup
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_nested') IS NOT NULL DROP TABLE pytest_ctx_nested")
            setup.cursor().execute("CREATE TABLE pytest_ctx_nested (id INT, source VARCHAR(10))")
            setup.commit()
            setup.close()

            # Outer commits, inner commits independently
            with connect(cs) as outer:
                outer.cursor().execute("INSERT INTO pytest_ctx_nested VALUES (1, 'outer')")
                with connect(cs) as inner:
                    inner.cursor().execute("INSERT INTO pytest_ctx_nested VALUES (2, 'inner')")
                # inner committed here

            # Verify both
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_nested")
            count = cur.fetchone()[0]
            assert count == 2, f"FAIL: expected 2 rows, got {count}"
            verify.close()

            # Cleanup
            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_nested")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_nested_context_managers")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    # ── Edge cases: hardened scenarios ──

    def test_manual_commit_then_clean_exit(self):
        """Manual commit() inside block, then clean exit commits again (no-op). No crash."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_manual_commit') IS NOT NULL DROP TABLE pytest_ctx_manual_commit")
            setup.cursor().execute("CREATE TABLE pytest_ctx_manual_commit (id INT)")
            setup.commit()
            setup.close()

            with connect(cs) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO pytest_ctx_manual_commit VALUES (1)")
                conn.commit()
                cur.execute("INSERT INTO pytest_ctx_manual_commit VALUES (2)")
                # __exit__ commits the second insert

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_manual_commit")
            count = cur.fetchone()[0]
            assert count == 2, f"FAIL: expected 2 rows, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_manual_commit")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_manual_commit_then_clean_exit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_manual_rollback_then_clean_exit(self):
        """Manual rollback() inside block, then clean exit. Commit on exit is a no-op."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_manual_rb') IS NOT NULL DROP TABLE pytest_ctx_manual_rb")
            setup.cursor().execute("CREATE TABLE pytest_ctx_manual_rb (id INT)")
            setup.commit()
            setup.close()

            with connect(cs) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO pytest_ctx_manual_rb VALUES (1)")
                conn.rollback()
                # block exits cleanly, __exit__ commits (but nothing pending)

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_manual_rb")
            count = cur.fetchone()[0]
            assert count == 0, f"FAIL: expected 0 rows, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_manual_rb")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_manual_rollback_then_clean_exit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_pending_results_at_exit(self):
        """Cursor with unfetched rows at context exit. Commit then close must work."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_pending') IS NOT NULL DROP TABLE pytest_ctx_pending")
            setup.cursor().execute("CREATE TABLE pytest_ctx_pending (id INT)")
            setup.cursor().execute("INSERT INTO pytest_ctx_pending VALUES (1)")
            setup.cursor().execute("INSERT INTO pytest_ctx_pending VALUES (2)")
            setup.cursor().execute("INSERT INTO pytest_ctx_pending VALUES (3)")
            setup.commit()
            setup.close()

            with connect(cs) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO pytest_ctx_pending VALUES (4)")
                # open a SELECT and only fetch one row — leave results pending
                cur2 = conn.cursor()
                cur2.execute("SELECT * FROM pytest_ctx_pending")
                cur2.fetchone()
                # exit with pending results on cur2

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_pending")
            count = cur.fetchone()[0]
            assert count == 4, f"FAIL: expected 4 rows, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_pending")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_pending_results_at_exit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_autocommit_toggled_mid_block(self):
        """Autocommit toggled from False to True mid-block. Exit should skip commit."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_toggle') IS NOT NULL DROP TABLE pytest_ctx_toggle")
            setup.cursor().execute("CREATE TABLE pytest_ctx_toggle (id INT)")
            setup.commit()
            setup.close()

            with connect(cs) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO pytest_ctx_toggle VALUES (1)")
                conn.commit()  # commit first insert
                conn.autocommit = True
                cur.execute("INSERT INTO pytest_ctx_toggle VALUES (2)")
                # autocommit=True at exit, so __exit__ skips commit (already auto-committed)

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_toggle")
            count = cur.fetchone()[0]
            assert count == 2, f"FAIL: expected 2 rows, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_toggle")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_autocommit_toggled_mid_block")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_autocommit_false_to_true_rollback(self):
        """Start autocommit=True, switch to False, raise. Rollback should apply."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_toggle_rb') IS NOT NULL DROP TABLE pytest_ctx_toggle_rb")
            setup.cursor().execute("CREATE TABLE pytest_ctx_toggle_rb (id INT)")
            setup.commit()
            setup.close()

            try:
                with connect(cs, autocommit=True) as conn:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO pytest_ctx_toggle_rb VALUES (1)")
                    # row 1 auto-committed
                    conn.setautocommit(False)
                    cur.execute("INSERT INTO pytest_ctx_toggle_rb VALUES (2)")
                    raise RuntimeError("boom")
            except RuntimeError:
                pass

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_toggle_rb")
            count = cur.fetchone()[0]
            # row 1 was auto-committed, row 2 should be rolled back
            assert count == 1, f"FAIL: expected 1 row, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_toggle_rb")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_autocommit_false_to_true_rollback")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_ddl_and_dml_rollback(self):
        """DDL + DML in one block, exception raised. Both should roll back on SQL Server."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            # Make sure table doesn't exist
            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_ddl_test') IS NOT NULL DROP TABLE pytest_ctx_ddl_test")
            setup.commit()
            setup.close()

            try:
                with connect(cs) as conn:
                    cur = conn.cursor()
                    cur.execute("CREATE TABLE pytest_ctx_ddl_test (id INT)")
                    cur.execute("INSERT INTO pytest_ctx_ddl_test VALUES (1)")
                    raise ValueError("rollback everything")
            except ValueError:
                pass

            # Both DDL and DML should be rolled back
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT OBJECT_ID('pytest_ctx_ddl_test')")
            obj_id = cur.fetchone()[0]
            assert obj_id is None, f"FAIL: table still exists after rollback (id={obj_id})"
            verify.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_ddl_and_dml_rollback")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_ddl_and_dml_commit(self):
        """DDL + DML in one block, clean exit. Both should commit."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_ddl_commit') IS NOT NULL DROP TABLE pytest_ctx_ddl_commit")
            setup.commit()
            setup.close()

            with connect(cs) as conn:
                cur = conn.cursor()
                cur.execute("CREATE TABLE pytest_ctx_ddl_commit (id INT)")
                cur.execute("INSERT INTO pytest_ctx_ddl_commit VALUES (1)")

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_ddl_commit")
            count = cur.fetchone()[0]
            assert count == 1, f"FAIL: expected 1 row, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_ddl_commit")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_ddl_and_dml_commit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_conn_closed_inside_block(self):
        """User closes connection inside with block. __exit__ should not crash."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            with connect(cs) as conn:
                conn.cursor().execute("SELECT 1")
                conn.close()
                # __exit__ fires on already-closed connection

            assert conn.closed, "FAIL: should be closed"
            print("PASS")
        """)
        _assert_no_crash(result, "test_conn_closed_inside_block")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_doomed_transaction_xact_abort(self):
        """XACT_ABORT ON + constraint violation = doomed txn. Commit should fail."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_doomed') IS NOT NULL DROP TABLE pytest_ctx_doomed")
            setup.cursor().execute("CREATE TABLE pytest_ctx_doomed (id INT PRIMARY KEY)")
            setup.cursor().execute("INSERT INTO pytest_ctx_doomed VALUES (1)")
            setup.commit()
            setup.close()

            got_error = False
            try:
                with connect(cs) as conn:
                    cur = conn.cursor()
                    cur.execute("SET XACT_ABORT ON")
                    try:
                        cur.execute("INSERT INTO pytest_ctx_doomed VALUES (1)")  # PK violation
                    except Exception:
                        pass  # swallow the SQL error
                    # block exits cleanly, but txn is doomed
                    # __exit__ will try to commit, which should fail
            except Exception as e:
                got_error = True

            # Either the commit raised (good) or the data didn't persist (also acceptable)
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_doomed")
            count = cur.fetchone()[0]
            assert count == 1, f"FAIL: expected only the original row, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_doomed")
            cleanup.commit()
            cleanup.close()
            print(f"PASS (commit_error={got_error})")
        """)
        _assert_no_crash(result, "test_doomed_transaction_xact_abort")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_double_exit_idempotent(self):
        """Calling __exit__ twice should not crash."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            conn = connect(cs)
            conn.__enter__()
            conn.cursor().execute("SELECT 1")
            conn.__exit__(None, None, None)
            conn.__exit__(None, None, None)  # second call, should be no-op
            assert conn.closed, "FAIL: should be closed"
            print("PASS")
        """)
        _assert_no_crash(result, "test_double_exit_idempotent")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_keyboard_interrupt_in_block(self):
        """KeyboardInterrupt (BaseException) should trigger rollback."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_kbint') IS NOT NULL DROP TABLE pytest_ctx_kbint")
            setup.cursor().execute("CREATE TABLE pytest_ctx_kbint (id INT)")
            setup.commit()
            setup.close()

            try:
                with connect(cs) as conn:
                    conn.cursor().execute("INSERT INTO pytest_ctx_kbint VALUES (1)")
                    raise KeyboardInterrupt()
            except KeyboardInterrupt:
                pass

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_kbint")
            count = cur.fetchone()[0]
            # KeyboardInterrupt is BaseException, not Exception
            # exc_type will be set, so rollback should happen
            assert count == 0, f"FAIL: expected 0 rows, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_kbint")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_keyboard_interrupt_in_block")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_generator_abandonment(self):
        """Context manager inside generator that gets closed mid-yield."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_gen') IS NOT NULL DROP TABLE pytest_ctx_gen")
            setup.cursor().execute("CREATE TABLE pytest_ctx_gen (id INT)")
            setup.commit()
            setup.close()

            def gen():
                with connect(cs) as conn:
                    conn.cursor().execute("INSERT INTO pytest_ctx_gen VALUES (1)")
                    yield  # suspend here
                    conn.cursor().execute("INSERT INTO pytest_ctx_gen VALUES (2)")

            g = gen()
            next(g)      # run to yield
            g.close()    # force GeneratorExit, triggers __exit__

            # GeneratorExit is BaseException, so rollback should happen
            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_gen")
            count = cur.fetchone()[0]
            assert count == 0, f"FAIL: expected 0 rows (rolled back), got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_gen")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """)
        _assert_no_crash(result, "test_generator_abandonment")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    @pytest.mark.stress
    def test_large_transaction_commit(self):
        """10k inserts in one block, all committed on exit."""
        result = _run_script(
            """
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            setup = connect(cs)
            setup.cursor().execute("IF OBJECT_ID('pytest_ctx_large') IS NOT NULL DROP TABLE pytest_ctx_large")
            setup.cursor().execute("CREATE TABLE pytest_ctx_large (id INT)")
            setup.commit()
            setup.close()

            with connect(cs) as conn:
                cur = conn.cursor()
                for i in range(10000):
                    cur.execute(f"INSERT INTO pytest_ctx_large VALUES ({i})")

            verify = connect(cs)
            cur = verify.cursor()
            cur.execute("SELECT COUNT(*) FROM pytest_ctx_large")
            count = cur.fetchone()[0]
            assert count == 10000, f"FAIL: expected 10000 rows, got {count}"
            verify.close()

            cleanup = connect(cs)
            cleanup.cursor().execute("DROP TABLE pytest_ctx_large")
            cleanup.commit()
            cleanup.close()
            print("PASS")
        """,
            timeout=120,
        )
        _assert_no_crash(result, "test_large_transaction_commit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout

    def test_connection_closed_after_exit(self):
        """Connection should be closed after __exit__ regardless of commit/rollback."""
        result = _run_script("""
            import os
            from mssql_python import connect

            cs = os.environ["DB_CONNECTION_STRING"]

            conn_ref = None
            with connect(cs) as conn:
                conn_ref = conn
                assert not conn.closed, "FAIL: should be open inside block"

            assert conn_ref.closed, "FAIL: should be closed after block"
            print("PASS")
        """)
        _assert_no_crash(result, "test_connection_closed_after_exit")
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"
        assert "PASS" in result.stdout
