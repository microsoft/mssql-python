"""
Microsoft SQL Server Bulk Copy Program (BCP) client implementation.

This module provides functionality to perform bulk copy operations to and from
SQL Server databases using the BCP protocol. It wraps the native BCP functionality
and provides a Pythonic interface for configuring and executing BCP operations.
"""

# Standard library imports
import logging

# Third party imports
try:
    from ddbc_bindings import BCPWrapper
except ImportError:
    # Mock for static analysis
    class BCPWrapper:
        """Mock BCPWrapper for static analysis."""
        def __init__(self, *args):
            """Initialize mock wrapper."""
            # Empty init is fine for a mock

# First party imports
from mssql_python.bcp_options import BCPOptions
from mssql_python.constants import BCPControlOptions

logger = logging.getLogger(__name__)

# defining constants for BCP control options
SUPPORTED_DIRECTIONS = ("in", "out", "queryout")
SQL_CHAR = 1
SQL_VARLEN_DATA = -10
SQL_NULL_DATA = -1


class BCPClient:
    """
    A client for performing bulk copy operations using the BCP (Bulk Copy Program) utility.
    
    This class provides methods to initialize and execute BCP operations.
    """

    def __init__(self, connection):
        """
        Initializes the BCPClient with a database connection.
        
        Args:
            connection: A mssql_python.connection.Connection object.
        """
        logger.info("Initializing BCPClient.")
        if connection is None:
            logger.error("Connection object is None during BCPClient initialization.")
            raise ValueError(
                "A valid connection object is required to initialize BCPClient."
            )

        # Access the underlying C++ ddbc_bindings.Connection object
        if not hasattr(connection, "_conn"):
            logger.error(
                "The provided Python connection object does not have the expected "
                "'_conn' attribute."
            )
            raise TypeError(
                "The Python Connection object is missing the '_conn' attribute "
                "holding the native C++ connection."
            )

        self.wrapper = BCPWrapper(connection._conn)
        print(f"connection: {connection._conn}")
        logger.info("BCPClient initialized successfully.")

    def sql_bulk_copy(self, options: BCPOptions, table: str = ""):
        """
        Executes a bulk copy operation to or from a specified table or using a query.

        Args:
            table (str): The name of the table (for 'in', 'out', 'format') or the query string.
            options (BCPOptions): Configuration for the bulk copy operation.
                                  The options.direction field dictates the BCP operation.

        Raises:
            ValueError: If 'table' is not provided, or if 'options' are invalid
                      or use a direction not supported by this client.
            TypeError: If 'options' is not an instance of BCPOptions.
            RuntimeError: If the BCPWrapper was not initialized.
        """
        logger.info(
            "Starting sql_bulk_copy for table/query: '%s', direction: '%s'.",
            table,
            options.direction
        )
        options.direction = options.direction.lower()
        if not table and options.direction != "queryout":
            # If 'table' is empty and direction is not 'queryout', raise an error.
            logger.error(
                "Validation failed: 'table' (or query) not provided for sql_bulk_copy."
            )
            raise ValueError(
                "The 'table' name (or query for queryout) must be provided."
            )

        if not isinstance(options, BCPOptions):
            logger.error(
                "Validation failed: 'options' is not an instance of BCPOptions. "
                "Got type: %s.",
                type(options)
            )
            # This check is good practice, though type hints help statically.
            raise TypeError("The 'options' argument must be an instance of BCPOptions.")

        # BCPClient can add its own operational constraints:
        if options.direction not in SUPPORTED_DIRECTIONS:
            logger.error(
                "Validation failed: Unsupported BCP direction '%s'. Supported: %s",
                options.direction,
                SUPPORTED_DIRECTIONS
            )
            raise ValueError(
                f"BCPClient currently only supports directions: "
                f"{', '.join(SUPPORTED_DIRECTIONS)}. Got '{options.direction}'."
            )

        current_options = options  # Use the validated options directly
        logger.debug("Using BCPOptions: %s", current_options)

        if not self.wrapper:  # Should be caught by __init__ ideally
            logger.error("BCPWrapper was not initialized before calling sql_bulk_copy.")
            raise RuntimeError("BCPWrapper was not initialized.")

        try:
            if not current_options.use_memory_bcp or not current_options.bind_data:
                # Standard file-based BCP initialization
                logger.info(
                    "Initializing BCP operation: table='%s', data_file='%s', "
                    "error_file='%s', direction='%s'",
                    table,
                    current_options.data_file,
                    current_options.error_file,
                    current_options.direction
                )
                # Initialize BCP operation for file-based operations
                self.wrapper.bcp_initialize_operation(
                    table,
                    current_options.data_file,
                    current_options.error_file,
                    current_options.direction,
                )
                logger.debug("BCP operation initialized with BCPWrapper.")
            else:
                # In-memory BCP initialization (for bind/sendrow)
                logger.info("Initializing in-memory BCP operation for table: '%s'", table)
                # For in-memory BCP, initialize with no data file
                self.wrapper.bcp_initialize_operation(
                    table,
                    "",  # No data file for in-memory BCP
                    current_options.error_file or "",
                    "in",  # Always use "in" for in-memory BCP
                )
                logger.debug("In-memory BCP operation initialized with BCPWrapper.")

            if current_options.query:
                logger.debug("Setting BCPControlOptions.HINTS to '%s'", current_options.query)
                print("current_options.query: %s", current_options.query)
                self.wrapper.bcp_control(
                    BCPControlOptions.HINTS.value, current_options.query
                )

            if current_options.batch_size:
                logger.debug(
                    "Setting BCPControlOptions.BATCH_SIZE to '%s'",
                    current_options.batch_size
                )
                self.wrapper.bcp_control(
                    BCPControlOptions.BATCH_SIZE.value,
                    current_options.batch_size
                )
            if current_options.max_errors:
                logger.debug(
                    "Setting BCPControlOptions.MAX_ERRORS to '%s'",
                    current_options.max_errors
                )
                self.wrapper.bcp_control(
                    BCPControlOptions.MAX_ERRORS.value,
                    current_options.max_errors
                )
            if current_options.first_row:
                logger.debug(
                    "Setting BCPControlOptions.FIRST_ROW to '%s'",
                    current_options.first_row
                )
                self.wrapper.bcp_control(
                    BCPControlOptions.FIRST_ROW.value,
                    current_options.first_row
                )
            if current_options.last_row:
                logger.debug(
                    "Setting BCPControlOptions.LAST_ROW to '%s'",
                    current_options.last_row
                )
                self.wrapper.bcp_control(
                    BCPControlOptions.LAST_ROW.value,
                    current_options.last_row
                )
            if current_options.keep_identity:
                logger.debug(
                    "Setting BCPControlOptions.KEEP_IDENTITY to '%s'",
                    current_options.keep_identity
                )
                self.wrapper.bcp_control(
                    BCPControlOptions.KEEP_IDENTITY.value,
                    current_options.keep_identity
                )
            if current_options.keep_nulls:
                logger.debug(
                    "Setting BCPControlOptions.KEEP_NULLS to '%s'",
                    current_options.keep_nulls
                )
                self.wrapper.bcp_control(
                    BCPControlOptions.KEEP_NULLS.value,
                    current_options.keep_nulls
                )

            # Handle format file or column definitions
            if current_options.format_file:
                logger.info("Reading format file: '%s'", current_options.format_file)
                self.wrapper.read_format_file(current_options.format_file)
            elif current_options.columns:
                logger.info(
                    "Defining %s columns programmatically.",
                    len(current_options.columns)
                )
                self.wrapper.define_columns(len(current_options.columns))
                for col_fmt_obj in current_options.columns:
                    logger.debug(
                        "Defining column format for file column %s: %s",
                        col_fmt_obj.file_col,
                        col_fmt_obj
                    )
                    print("col_fmt_obj: %s", col_fmt_obj)

                    self.wrapper.define_column_format(
                        file_col_idx=col_fmt_obj.file_col,
                        user_data_type=col_fmt_obj.user_data_type,
                        indicator_length=col_fmt_obj.prefix_len,
                        user_data_length=col_fmt_obj.data_len,
                        terminator_bytes=col_fmt_obj.field_terminator,
                        terminator_length=col_fmt_obj.terminator_len,
                        server_col_idx=col_fmt_obj.server_col,
                    )
            else:
                logger.info(
                    "No format file or explicit column definitions provided. "
                    "Relying on BCP defaults or server types."
                )

            # Handle in-memory BCP binding
            if current_options.use_memory_bcp and current_options.bind_data:
                # Check if bind_data is a list of lists (multiple rows)
                is_multi_row = (
                    len(current_options.bind_data) > 0 and
                    isinstance(current_options.bind_data[0], list)
                )

                if is_multi_row:
                    # Process multiple rows
                    row_count = len(current_options.bind_data)
                    logger.info("Processing %s rows in memory", row_count)

                    for row_idx, row_data in enumerate(current_options.bind_data):
                        logger.info("Processing row %s of %s", row_idx+1, row_count)

                        # Bind each column in this row
                        col_count = len(row_data)
                        logger.info("Binding %s columns for row %s", col_count, row_idx+1)

                        for bind_data in row_data:
                            logger.debug(
                                "Binding column %s with data type %s",
                                bind_data.server_col,
                                bind_data.data_type
                            )
                            self.wrapper.bind_column(
                                data=bind_data.data,
                                indicator_length=bind_data.indicator_length,
                                data_length=bind_data.data_length,
                                terminator=bind_data.terminator,
                                terminator_length=bind_data.terminator_length,
                                data_type=bind_data.data_type,
                                server_col_idx=bind_data.server_col,
                            )

                        # Send this row to the server
                        logger.info("Sending row %s to server", row_idx+1)
                        self.wrapper.send_row()

                else:
                    # Original single-row logic
                    logger.info(
                        "Binding data for %s columns (single row)",
                        len(current_options.bind_data)
                    )
                    for bind_data in current_options.bind_data:
                        logger.debug(
                            "Binding column %s with data type %s",
                            bind_data.server_col,
                            bind_data.data_type
                        )
                        self.wrapper.bind_column(
                            data=bind_data.data,
                            indicator_length=bind_data.indicator_length,
                            data_length=bind_data.data_length,
                            terminator=bind_data.terminator,
                            terminator_length=bind_data.terminator_length,
                            data_type=bind_data.data_type,
                            server_col_idx=bind_data.server_col,
                        )

                    # For single-row in-memory BCP, send the row to the server
                    logger.info("Sending row to server")
                    self.wrapper.send_row()

                # Call finish to complete the batch
                logger.info("Finishing BCP batch")
                self.wrapper.finish()
            else:
                # For file-based BCP, execute and finish
                logger.info("Executing BCP operation via wrapper.exec_bcp().")
                self.wrapper.exec_bcp()
                logger.info("BCP operation executed successfully.")

        except Exception as e:
            logger.exception(
                "An error occurred during BCP operation for table '%s': %s",
                table,
                e
            )
            raise  # Re-raise the exception after logging
        finally:
            if self.wrapper:
                logger.info("Finishing and closing BCPWrapper.")
                # self.wrapper.finish()
                # self.wrapper.close()
                logger.debug("BCPWrapper finished and closed.")
            logger.info("sql_bulk_copy for table/query: '%s' completed.", table)
