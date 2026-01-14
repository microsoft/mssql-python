"""
BCP Rust Wrapper Module
Provides Python interface to the Rust-based mssql_py_core library
"""

from typing import Optional, List, Tuple, Dict, Any, Iterable
from mssql_python.logging import logger

try:
    import mssql_py_core

    RUST_CORE_AVAILABLE = True
except ImportError:
    RUST_CORE_AVAILABLE = False
    mssql_py_core = None


class BCPRustWrapper:
    """
    Wrapper class for Rust-based BCP operations using mssql_py_core.
    Supports context manager for automatic resource cleanup.

    Example:
        with BCPRustWrapper(connection_string) as wrapper:
            wrapper.connect()
            result = wrapper.bulkcopy('TableName', data)
    """

    def __init__(self, connection_string: Optional[str] = None):
        if not RUST_CORE_AVAILABLE:
            raise ImportError(
                "mssql_py_core is not installed. "
                "Please install it from the BCPRustWheel directory."
            )
        self._core = mssql_py_core
        self._rust_connection = None
        self._connection_string = connection_string

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection is closed"""
        self.close()
        return False

    def __del__(self):
        """Destructor - cleanup resources if not already closed"""
        try:
            if self._rust_connection is not None:
                logger.warning(
                    "BCPRustWrapper connection was not explicitly closed, cleaning up in destructor"
                )
                self.close()
        except Exception:
            # Ignore errors during cleanup in destructor
            pass

    @property
    def is_connected(self) -> bool:
        """Check if connection is active"""
        return self._rust_connection is not None

    def close(self):
        """Close the connection and cleanup resources"""
        if self._rust_connection:
            try:
                logger.info("Closing Rust connection")
                # If the connection has a close method, call it
                if hasattr(self._rust_connection, "close"):
                    self._rust_connection.close()
            except Exception as e:
                logger.warning("Error closing connection: %s", str(e))
            finally:
                # Always set to None to prevent reuse
                self._rust_connection = None

    def connect(self, connection_string: Optional[str] = None):
        """
        Create a connection using the Rust-based PyCoreConnection

        Args:
            connection_string: SQL Server connection string

        Returns:
            PyCoreConnection instance

        Raises:
            ValueError: If connection string is missing or invalid
            RuntimeError: If connection fails
        """
        conn_str = connection_string or self._connection_string
        if not conn_str:
            raise ValueError("Connection string is required")

        # Close existing connection if any
        if self._rust_connection:
            logger.warning("Closing existing connection before creating new one")
            self.close()

        try:
            # Parse connection string into dictionary
            params = {}
            for pair in conn_str.split(";"):
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    params[key.strip().lower()] = value.strip()

            # Validate required parameters
            if not params.get("server"):
                raise ValueError("SERVER parameter is required in connection string")

            # PyCoreConnection expects a dictionary with specific keys
            python_client_context = {
                "server": params.get("server", "localhost"),
                "database": params.get("database", "master"),
                "user_name": params.get("uid", ""),
                "password": params.get("pwd", ""),
                "trust_server_certificate": params.get("trustservercertificate", "yes").lower()
                in ["yes", "true"],
                "encryption": "Optional",
            }

            logger.info(
                "Attempting to connect to server: %s, database: %s",
                python_client_context["server"],
                python_client_context["database"],
            )

            self._rust_connection = self._core.PyCoreConnection(python_client_context)

            logger.info("Connection established successfully")
            return self._rust_connection

        except ValueError as ve:
            logger.error("Connection string validation error: %s", str(ve))
            raise
        except Exception as e:
            logger.error("Failed to create connection: %s - %s", type(e).__name__, str(e))
            raise RuntimeError(f"Connection failed: {str(e)}") from e

    def bulkcopy(
        self,
        table_name: str,
        data: Iterable,
        batch_size: int = 1000,
        timeout: int = 30,
        column_mappings: Optional[List[Tuple[int, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Perform bulk copy operation to insert data into SQL Server table.

        Args:
            table_name: Target table name
            data: Iterable of tuples/lists containing row data
            batch_size: Number of rows per batch (default: 1000)
            timeout: Timeout in seconds (default: 30)
            column_mappings: List of tuples mapping source column index to target column name
                           e.g., [(0, "id"), (1, "name")]

        Returns:
            Dictionary with bulk copy results containing:
                - rows_copied: Number of rows successfully copied
                - batch_count: Number of batches processed
                - elapsed_time: Time taken for the operation

        Raises:
            RuntimeError: If no active connection or cursor creation fails
            ValueError: If parameters are invalid
        """
        # Validate inputs
        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")

        if timeout <= 0:
            raise ValueError(f"timeout must be positive, got {timeout}")

        if not self._rust_connection:
            raise RuntimeError("No active connection. Call connect() first.")

        rust_cursor = None
        try:
            # Create cursor
            rust_cursor = self._rust_connection.cursor()
        except Exception as e:
            logger.error("Failed to create cursor: %s - %s", type(e).__name__, str(e))
            raise RuntimeError(f"Cursor creation failed: {str(e)}") from e

        try:
            # Build kwargs for bulkcopy
            kwargs = {
                "batch_size": batch_size,
                "timeout": timeout,
            }

            if column_mappings:
                kwargs["column_mappings"] = column_mappings

            # Execute bulk copy with error handling
            logger.info(
                "Starting bulk copy to table '%s' - batch_size=%d, timeout=%d",
                table_name,
                batch_size,
                timeout,
            )
            result = rust_cursor.bulkcopy(table_name, iter(data), kwargs=kwargs)

            logger.info(
                "Bulk copy completed successfully - rows_copied=%d, batch_count=%d, elapsed_time=%s",
                result.get("rows_copied", 0),
                result.get("batch_count", 0),
                result.get("elapsed_time", "unknown"),
            )
            return result
        except AttributeError as ae:
            logger.error("Invalid cursor or method call for table '%s': %s", table_name, str(ae))
            raise RuntimeError(f"Bulk copy method error: {str(ae)}") from ae
        except TypeError as te:
            logger.error("Invalid data type or parameters for table '%s': %s", table_name, str(te))
            raise ValueError(f"Invalid bulk copy parameters: {str(te)}") from te
        except Exception as e:
            logger.error(
                "Bulk copy failed for table '%s': %s - %s", table_name, type(e).__name__, str(e)
            )
            raise
        finally:
            # Always close cursor to prevent resource leak
            if rust_cursor is not None:
                try:
                    if hasattr(rust_cursor, "close"):
                        rust_cursor.close()
                        logger.debug("Cursor closed successfully")
                except Exception as e:
                    logger.warning("Error closing cursor: %s", str(e))


def is_rust_core_available() -> bool:
    """
    Check if the Rust core library is available

    Returns:
        bool: True if mssql_py_core is installed, False otherwise
    """
    return RUST_CORE_AVAILABLE
