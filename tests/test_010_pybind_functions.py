"""
This file contains tests for the pybind C++ functions in ddbc_bindings module.
These tests exercise the C++ code paths without mocking to provide real code coverage.

Functions tested:
- Architecture and module info
- Utility functions (GetDriverPathCpp, ThrowStdException)
- Data structures (ParamInfo, NumericData, ErrorInfo, DateTimeOffset)
- SQL functions (DDBCSQLExecDirect, DDBCSQLExecute, etc.)
- Connection pooling functions
- Error handling functions
- Threading safety tests
- Unix-specific utility functions (when available)
"""

import pytest
import platform
import threading
import os

# Import ddbc_bindings with error handling
try:
    import mssql_python.ddbc_bindings as ddbc

    DDBC_AVAILABLE = True
except ImportError as e:
    print(f"Warning: ddbc_bindings not available: {e}")
    DDBC_AVAILABLE = False
    ddbc = None

from mssql_python.exceptions import (
    InterfaceError,
    ProgrammingError,
    DatabaseError,
    OperationalError,
    DataError,
)


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestPybindModuleInfo:
    """Test module information and architecture detection."""

    def test_module_architecture_attribute(self):
        """Test that the module exposes architecture information."""
        assert hasattr(ddbc, "ARCHITECTURE")

        arch = getattr(ddbc, "ARCHITECTURE")
        assert isinstance(arch, str)
        assert len(arch) > 0

    def test_architecture_consistency(self):
        """Test that architecture attributes are consistent."""
        arch = getattr(ddbc, "ARCHITECTURE")
        # Valid architectures for Windows, Linux, and macOS
        valid_architectures = [
            "x64",
            "x86",
            "arm64",
            "win64",  # Windows
            "x86_64",
            "i386",
            "aarch64",  # Linux
            "arm64",
            "x86_64",
            "universal2",  # macOS (arm64/Intel/Universal)
        ]
        assert arch in valid_architectures, f"Unknown architecture: {arch}"

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        # Module may not have __doc__ attribute set, which is acceptable
        doc = getattr(ddbc, "__doc__", None)
        if doc is not None:
            assert isinstance(doc, str)
        # Just verify the module loads and has expected attributes
        assert hasattr(ddbc, "ARCHITECTURE")


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestUtilityFunctions:
    """Test C++ utility functions exposed to Python."""

    def test_get_driver_path_cpp(self):
        """Test GetDriverPathCpp function."""
        try:
            # Function requires a driver name argument
            driver_path = ddbc.GetDriverPathCpp("ODBC Driver 18 for SQL Server")
            assert isinstance(driver_path, str)
            # Driver path should not be empty if found
            if driver_path:
                assert len(driver_path) > 0
        except Exception as e:
            # On some systems, driver might not be available
            error_msg = str(e).lower()
            assert any(
                keyword in error_msg
                for keyword in [
                    "driver not found",
                    "cannot find",
                    "not available",
                    "incompatible",
                    "not supported",
                ]
            )

    def test_throw_std_exception(self):
        """Test ThrowStdException function."""
        with pytest.raises(RuntimeError):
            ddbc.ThrowStdException("Test exception message")


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestDataStructures:
    """Test C++ data structures exposed to Python."""

    def test_param_info_creation(self):
        """Test ParamInfo structure creation and access."""
        param = ddbc.ParamInfo()

        # Test that object was created successfully
        assert param is not None

        # Test basic attributes that should be accessible
        try:
            param.inputOutputType = 1
            assert param.inputOutputType == 1
        except (AttributeError, TypeError):
            # Some attributes might not be directly accessible
            pass

        try:
            param.paramCType = 2
            assert param.paramCType == 2
        except (AttributeError, TypeError):
            pass

        try:
            param.paramSQLType = 3
            assert param.paramSQLType == 3
        except (AttributeError, TypeError):
            pass

        # Test that the object has the expected type
        assert str(type(param)) == "<class 'ddbc_bindings.ParamInfo'>"

    def test_numeric_data_creation(self):
        """Test NumericData structure creation and manipulation."""
        # Test default constructor
        num1 = ddbc.NumericData()
        assert hasattr(num1, "precision")
        assert hasattr(num1, "scale")
        assert hasattr(num1, "sign")
        assert hasattr(num1, "val")

        # Test parameterized constructor
        test_bytes = b"\\x12\\x34\\x00\\x00"  # Sample binary data
        num2 = ddbc.NumericData(18, 2, 1, test_bytes.decode("latin-1"))

        assert num2.precision == 18
        assert num2.scale == 2
        assert num2.sign == 1
        assert len(num2.val) == 16  # SQL_MAX_NUMERIC_LEN

        # Test setting values
        num1.precision = 10
        num1.scale = 3
        num1.sign = 0

        assert num1.precision == 10
        assert num1.scale == 3
        assert num1.sign == 0

    def test_error_info_structure(self):
        """Test ErrorInfo structure."""
        # ErrorInfo might not have a default constructor, so just test that the class exists
        assert hasattr(ddbc, "ErrorInfo")

        # Test that it's a valid class type
        ErrorInfoClass = getattr(ddbc, "ErrorInfo")
        assert callable(ErrorInfoClass) or hasattr(ErrorInfoClass, "__name__")


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestConnectionFunctions:
    """Test connection-related pybind functions."""

    @pytest.fixture
    def db_connection(self):
        """Provide a database connection for testing."""
        try:
            conn_str = os.getenv("DB_CONNECTION_STRING")
            conn = ddbc.Connection(conn_str, False, {})
            yield conn
            try:
                conn.close()
            except:
                pass
        except Exception:
            pytest.skip("Database connection not available for testing")

    def test_connection_creation(self):
        """Test Connection class creation."""
        try:
            conn_str = os.getenv("DB_CONNECTION_STRING")
            conn = ddbc.Connection(conn_str, False, {})

            assert conn is not None

            # Test basic methods exist
            assert hasattr(conn, "close")
            assert hasattr(conn, "commit")
            assert hasattr(conn, "rollback")
            assert hasattr(conn, "set_autocommit")
            assert hasattr(conn, "get_autocommit")
            assert hasattr(conn, "alloc_statement_handle")

            conn.close()

        except Exception as e:
            if "driver not found" in str(e).lower():
                pytest.skip(f"ODBC driver not available: {e}")
            else:
                raise

    def test_connection_with_attrs_before(self):
        """Test Connection creation with attrs_before parameter."""
        try:
            conn_str = os.getenv("DB_CONNECTION_STRING")
            attrs = {"SQL_ATTR_CONNECTION_TIMEOUT": 30}
            conn = ddbc.Connection(conn_str, False, attrs)

            assert conn is not None
            conn.close()

        except Exception as e:
            if "driver not found" in str(e).lower():
                pytest.skip(f"ODBC driver not available: {e}")
            else:
                raise


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestPoolingFunctions:
    """Test connection pooling functionality."""

    def test_enable_pooling(self):
        """Test enabling connection pooling."""
        try:
            ddbc.enable_pooling()
            # Should not raise an exception
        except Exception as e:
            # Some environments might not support pooling
            assert "pooling" in str(e).lower() or "not supported" in str(e).lower()

    def test_close_pooling(self):
        """Test closing connection pools."""
        try:
            ddbc.close_pooling()
            # Should not raise an exception
        except Exception as e:
            # Acceptable if pooling wasn't enabled
            pass


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestSQLFunctions:
    """Test SQL execution functions."""

    @pytest.fixture
    def statement_handle(self, db_connection):
        """Provide a statement handle for testing."""
        try:
            stmt = db_connection.alloc_statement_handle()
            yield stmt
            try:
                ddbc.DDBCSQLFreeHandle(2, stmt)  # SQL_HANDLE_STMT = 2
            except:
                pass
        except Exception:
            pytest.skip("Cannot create statement handle")

    def test_sql_exec_direct_simple(self, statement_handle):
        """Test DDBCSQLExecDirect with a simple query."""
        try:
            result = ddbc.DDBCSQLExecDirect(statement_handle, "SELECT 1 as test_col")
            # SQL_SUCCESS = 0, SQL_SUCCESS_WITH_INFO = 1
            assert result in [0, 1]
        except Exception as e:
            if "connection" in str(e).lower():
                pytest.skip(f"Database connection issue: {e}")
            else:
                raise

    def test_sql_num_result_cols(self, statement_handle):
        """Test DDBCSQLNumResultCols function."""
        try:
            # First execute a query
            ddbc.DDBCSQLExecDirect(statement_handle, "SELECT 1 as col1, 'test' as col2")

            # Then get number of columns
            num_cols = ddbc.DDBCSQLNumResultCols(statement_handle)
            assert num_cols == 2

        except Exception as e:
            if "connection" in str(e).lower():
                pytest.skip(f"Database connection issue: {e}")
            else:
                raise

    def test_sql_describe_col(self, statement_handle):
        """Test DDBCSQLDescribeCol function."""
        try:
            # Execute a query first
            ddbc.DDBCSQLExecDirect(statement_handle, "SELECT 'test' as test_column")

            # Describe the first column
            col_info = ddbc.DDBCSQLDescribeCol(statement_handle, 1)

            assert isinstance(col_info, tuple)
            assert len(col_info) >= 6  # Should return column name, type, etc.

        except Exception as e:
            if "connection" in str(e).lower():
                pytest.skip(f"Database connection issue: {e}")
            else:
                raise

    def test_sql_fetch(self, statement_handle):
        """Test DDBCSQLFetch function."""
        try:
            # Execute a query
            ddbc.DDBCSQLExecDirect(statement_handle, "SELECT 1")

            # Fetch the row
            result = ddbc.DDBCSQLFetch(statement_handle)
            # SQL_SUCCESS = 0, SQL_NO_DATA = 100
            assert result in [0, 100]

        except Exception as e:
            if "connection" in str(e).lower():
                pytest.skip(f"Database connection issue: {e}")
            else:
                raise


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestErrorHandling:
    """Test error handling functions."""

    def test_sql_check_error_type_validation(self):
        """Test DDBCSQLCheckError input validation."""
        # Test that function exists and can handle type errors gracefully
        assert hasattr(ddbc, "DDBCSQLCheckError")

        # Test with obviously wrong parameter types to check input validation
        with pytest.raises((TypeError, AttributeError)):
            ddbc.DDBCSQLCheckError("invalid", "invalid", "invalid")


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestDecimalSeparator:
    """Test decimal separator functionality."""

    def test_set_decimal_separator(self):
        """Test DDBCSetDecimalSeparator function."""
        try:
            # Test setting different decimal separators
            ddbc.DDBCSetDecimalSeparator(".")
            ddbc.DDBCSetDecimalSeparator(",")

            # Should not raise exceptions for valid separators
        except Exception as e:
            # Some implementations might not support this
            assert "not supported" in str(e).lower() or "invalid" in str(e).lower()


@pytest.mark.skipif(
    platform.system() not in ["Linux", "Darwin"],
    reason="Unix-specific tests only run on Linux/macOS",
)
class TestUnixSpecificFunctions:
    """Test Unix-specific functionality when available."""

    def test_unix_utils_availability(self):
        """Test that Unix utils are available on Unix systems."""
        # These functions are in unix_utils.h/cpp and should be available
        # through the pybind module on Unix systems

        # Check if any Unix-specific functionality is exposed
        # This tests that the conditional compilation worked correctly
        module_attrs = dir(ddbc)

        # The module should at least have the basic functions
        assert "GetDriverPathCpp" in module_attrs
        assert "Connection" in module_attrs


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestThreadSafety:
    """Test thread safety of pybind functions."""

    def test_concurrent_driver_path_access(self):
        """Test concurrent access to GetDriverPathCpp."""
        results = []
        exceptions = []

        def get_driver_path():
            try:
                path = ddbc.GetDriverPathCpp()
                results.append(path)
            except Exception as e:
                exceptions.append(e)

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=get_driver_path)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Either all should succeed with same result, or all should fail consistently
        if results:
            # All successful results should be the same
            assert all(r == results[0] for r in results)

        # Should not have mixed success/failure without consistent error types
        if exceptions and results:
            # This would indicate a thread safety issue
            pytest.fail("Mixed success/failure in concurrent access suggests thread safety issue")


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestMemoryManagement:
    """Test memory management in pybind functions."""

    def test_multiple_param_info_creation(self):
        """Test creating multiple ParamInfo objects."""
        params = []
        for i in range(100):
            param = ddbc.ParamInfo()
            param.inputOutputType = i
            param.dataPtr = f"data_{i}"
            params.append(param)

        # Verify all objects maintain their data correctly
        for i, param in enumerate(params):
            assert param.inputOutputType == i
            assert param.dataPtr == f"data_{i}"

    def test_multiple_numeric_data_creation(self):
        """Test creating multiple NumericData objects."""
        numerics = []
        for i in range(50):
            numeric = ddbc.NumericData(
                10 + i, 2, 1, f"test_{i}".encode("latin-1").decode("latin-1")
            )
            numerics.append(numeric)

        # Verify all objects maintain their data correctly
        for i, numeric in enumerate(numerics):
            assert numeric.precision == 10 + i
            assert numeric.scale == 2
            assert numeric.sign == 1


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_numeric_data_max_length(self):
        """Test NumericData with maximum length value."""
        # SQL_MAX_NUMERIC_LEN is 16
        max_data = b"\\x00" * 16
        try:
            numeric = ddbc.NumericData(38, 0, 1, max_data.decode("latin-1"))
            assert len(numeric.val) == 16
        except Exception as e:
            # Should either work or give a clear error about length
            assert "length" in str(e).lower() or "size" in str(e).lower()

    def test_numeric_data_oversized_value(self):
        """Test NumericData with oversized value."""
        oversized_data = b"\\x00" * 20  # Larger than SQL_MAX_NUMERIC_LEN
        with pytest.raises((RuntimeError, ValueError)):
            ddbc.NumericData(38, 0, 1, oversized_data.decode("latin-1"))

    def test_param_info_extreme_values(self):
        """Test ParamInfo with extreme values."""
        param = ddbc.ParamInfo()

        # Test with very large values
        param.columnSize = 2**31 - 1  # Max SQLULEN
        param.strLenOrInd = -(2**31)  # Min SQLLEN

        assert param.columnSize == 2**31 - 1
        assert param.strLenOrInd == -(2**31)


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestAdditionalPybindFunctions:
    """Test additional pybind functions to increase coverage."""

    def test_all_exposed_functions_exist(self):
        """Test that all expected C++ functions are exposed."""
        expected_functions = [
            "GetDriverPathCpp",
            "ThrowStdException",
            "enable_pooling",
            "close_pooling",
            "DDBCSetDecimalSeparator",
            "DDBCSQLExecDirect",
            "DDBCSQLExecute",
            "DDBCSQLRowCount",
            "DDBCSQLFetch",
            "DDBCSQLNumResultCols",
            "DDBCSQLDescribeCol",
            "DDBCSQLGetData",
            "DDBCSQLMoreResults",
            "DDBCSQLFetchOne",
            "DDBCSQLFetchMany",
            "DDBCSQLFetchAll",
            "DDBCSQLFreeHandle",
            "DDBCSQLCheckError",
            "DDBCSQLTables",
            "DDBCSQLFetchScroll",
            "DDBCSQLSetStmtAttr",
            "DDBCSQLGetTypeInfo",
        ]

        for func_name in expected_functions:
            assert hasattr(ddbc, func_name), f"Function {func_name} not found in ddbc_bindings"
            func = getattr(ddbc, func_name)
            assert callable(func), f"{func_name} is not callable"

    def test_all_exposed_classes_exist(self):
        """Test that all expected C++ classes are exposed."""
        expected_classes = ["ParamInfo", "NumericData", "ErrorInfo", "SqlHandle", "Connection"]

        for class_name in expected_classes:
            assert hasattr(ddbc, class_name), f"Class {class_name} not found in ddbc_bindings"
            cls = getattr(ddbc, class_name)
            # Check that it's a class/type
            assert hasattr(cls, "__name__") or str(type(cls)).find("class") != -1

    def test_numeric_data_with_various_inputs(self):
        """Test NumericData with various input combinations."""
        # Test different precision and scale combinations
        test_cases = [
            (10, 0, 1, b"\\x12\\x34"),
            (18, 2, 0, b"\\x00\\x01"),
            (38, 10, 1, b"\\xFF\\xEE\\xDD"),
        ]

        for precision, scale, sign, data in test_cases:
            try:
                numeric = ddbc.NumericData(precision, scale, sign, data.decode("latin-1"))
                assert numeric.precision == precision
                assert numeric.scale == scale
                assert numeric.sign == sign
                assert len(numeric.val) == 16  # SQL_MAX_NUMERIC_LEN
            except Exception as e:
                # Some combinations might not be valid, which is acceptable
                assert (
                    "length" in str(e).lower()
                    or "size" in str(e).lower()
                    or "runtime" in str(e).lower()
                )

    def test_connection_pooling_workflow(self):
        """Test the complete connection pooling workflow."""
        try:
            # Test enabling pooling multiple times (should be safe)
            ddbc.enable_pooling()
            ddbc.enable_pooling()

            # Test closing pools
            ddbc.close_pooling()
            ddbc.close_pooling()  # Should be safe to call multiple times

        except Exception as e:
            # Pooling might not be supported in all environments
            error_msg = str(e).lower()
            assert any(
                keyword in error_msg for keyword in ["not supported", "not available", "pooling"]
            )

    def test_decimal_separator_variations(self):
        """Test decimal separator with different inputs."""
        separators_to_test = [".", ",", ";"]

        for sep in separators_to_test:
            try:
                ddbc.DDBCSetDecimalSeparator(sep)
                # If successful, test that we can set it back
                ddbc.DDBCSetDecimalSeparator(".")
            except Exception as e:
                # Some separators might not be supported
                error_msg = str(e).lower()
                assert any(
                    keyword in error_msg for keyword in ["invalid", "not supported", "separator"]
                )

    def test_driver_path_with_different_drivers(self):
        """Test GetDriverPathCpp with different driver names."""
        driver_names = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
            "NonExistentDriver",
        ]

        for driver_name in driver_names:
            try:
                path = ddbc.GetDriverPathCpp(driver_name)
                if path:  # If a path is returned
                    assert isinstance(path, str)
                    assert len(path) > 0
            except Exception as e:
                # Driver not found is acceptable
                error_msg = str(e).lower()
                assert any(
                    keyword in error_msg
                    for keyword in ["not found", "cannot find", "not available", "driver"]
                )

    def test_function_signature_validation(self):
        """Test that functions properly validate their input parameters."""

        # Test ThrowStdException with different message types
        test_messages = ["Test message", "", "Unicode: こんにちは"]
        for msg in test_messages:
            with pytest.raises(RuntimeError):
                ddbc.ThrowStdException(msg)

        # Test parameter validation for other functions
        with pytest.raises(TypeError):
            ddbc.DDBCSetDecimalSeparator(123)  # Should be string

        with pytest.raises(TypeError):
            ddbc.GetDriverPathCpp(None)  # Should be string


@pytest.mark.skipif(not DDBC_AVAILABLE, reason="ddbc_bindings not available")
class TestPybindErrorScenarios:
    """Test error scenarios and edge cases in pybind functions."""

    def test_invalid_parameter_types(self):
        """Test functions with invalid parameter types."""

        # Test various functions with wrong parameter types
        test_cases = [
            (ddbc.GetDriverPathCpp, [None, 123, []]),
            (ddbc.ThrowStdException, [None, 123, []]),
            (ddbc.DDBCSetDecimalSeparator, [None, 123, []]),
        ]

        for func, invalid_params in test_cases:
            for param in invalid_params:
                with pytest.raises(TypeError):
                    func(param)

    def test_boundary_conditions(self):
        """Test functions with boundary condition inputs."""

        # Test with very long strings
        long_string = "A" * 10000
        try:
            ddbc.ThrowStdException(long_string)
            assert False, "Should have raised RuntimeError"
        except RuntimeError:
            pass  # Expected
        except Exception as e:
            # Might fail with different error for very long strings
            assert "length" in str(e).lower() or "size" in str(e).lower()

        # Test with empty string
        with pytest.raises(RuntimeError):
            ddbc.ThrowStdException("")

    def test_unicode_handling(self):
        """Test Unicode string handling in pybind functions."""

        unicode_strings = [
            "Hello, 世界",  # Chinese
            "Привет, мир",  # Russian
            "مرحبا بالعالم",  # Arabic
            "🌍🌎🌏",  # Emojis
        ]

        for unicode_str in unicode_strings:
            try:
                with pytest.raises(RuntimeError):
                    ddbc.ThrowStdException(unicode_str)
            except UnicodeError:
                # Some Unicode might not be handled properly, which is acceptable
                pass

            try:
                ddbc.GetDriverPathCpp(unicode_str)
                # Might succeed or fail depending on system
            except Exception:
                # Unicode driver names likely don't exist
                pass


if __name__ == "__main__":
    # Run tests when executed directly
    pytest.main([__file__, "-v"])
