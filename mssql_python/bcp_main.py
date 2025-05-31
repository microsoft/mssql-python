import logging 
from mssql_python.bcp_options import (
    BCPOptions,
)
from ddbc_bindings import BCPWrapper 
from mssql_python.constants import BCPControlOptions
from typing import Optional  # Import Optional for type hints

logger = logging.getLogger(__name__) # Add a logger instance

# defining constants for BCP control options
SUPPORTED_DIRECTIONS = ("in", "out")

class BCPClient:
    """
    A client for performing bulk copy operations using the BCP (Bulk Copy Program) utility.
    This class provides methods to initialize and execute BCP operations.
    """

    def __init__(self, connection): # connection is an instance of mssql_python.connection.Connection
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
        # stored in the _conn attribute of your Python Connection wrapper.
        if not hasattr(connection, '_conn'):
            logger.error("The provided Python connection object does not have the expected '_conn' attribute.")
            raise TypeError("The Python Connection object is missing the '_conn' attribute holding the native C++ connection.")

        self.wrapper = BCPWrapper(connection._conn)
        logger.info("BCPClient initialized successfully.")

    def sql_bulk_copy(self, table: str, options: BCPOptions):  # options is no longer Optional
        """
        Executes a bulk copy operation to or from a specified table or using a query.

        Args:
            table (str): The name of the table (for 'in', 'out', 'format') or the query string (for 'queryout').
            options (BCPOptions): Configuration for the bulk copy operation. Must be provided.
                                  The options.direction field dictates the BCP operation.
        Raises:
            ValueError: If 'table' is not provided, or if 'options' are invalid
                        or use a direction not supported by this client.
            TypeError: If 'options' is not an instance of BCPOptions.
            RuntimeError: If the BCPWrapper was not initialized.
        """
        logger.info(f"Starting sql_bulk_copy for table/query: '{table}', direction: '{options.direction}'.")
        if not table:
            logger.error("Validation failed: 'table' (or query) not provided for sql_bulk_copy.")
            raise ValueError(
                "The 'table' name (or query for queryout) must be provided."
            )

        if not isinstance(options, BCPOptions):
            logger.error(f"Validation failed: 'options' is not an instance of BCPOptions. Got type: {type(options)}.")
            # This check is good practice, though type hints help statically.
            raise TypeError("The 'options' argument must be an instance of BCPOptions.")

        # BCPOptions.__post_init__ has already performed its internal validation.
        # BCPClient can add its own operational constraints:
        if options.direction not in SUPPORTED_DIRECTIONS:
            logger.error(f"Validation failed: Unsupported BCP direction '{options.direction}'. Supported: {SUPPORTED_DIRECTIONS}")
            raise ValueError(
                f"BCPClient currently only supports directions: {', '.join(SUPPORTED_DIRECTIONS)}. "
                f"Got '{options.direction}'."
            )

        current_options = options  # Use the validated options directly
        logger.debug(f"Using BCPOptions: {current_options}")

        if not self.wrapper:  # Should be caught by __init__ ideally
            logger.error("BCPWrapper was not initialized before calling sql_bulk_copy.")
            raise RuntimeError("BCPWrapper was not initialized.")

        try:
            logger.info(
                f"Initializing BCP operation: table='{table}', data_file='{current_options.data_file}', "
                f"error_file='{current_options.error_file}', direction='{current_options.direction}'"
            )
            # 'table' here is used as szTable for bcp_init, which can be a table name or view.
            # For 'queryout', the C++ wrapper would need to handle 'table' as the query string
            # if bcp_init is used, or use bcp_queryout directly if that's the chosen C++ API.
            # Assuming bcp_initialize_operation is flexible or maps to bcp_init.
            self.wrapper.bcp_initialize_operation(
                table,
                current_options.data_file,
                current_options.error_file,
                current_options.direction,
            )
            logger.debug("BCP operation initialized with BCPWrapper.")

            # Set BCP control options
            if current_options.batch_size is not None:
                logger.debug(f"Setting BCPControlOptions.BATCH_SIZE to {current_options.batch_size}")
                self.wrapper.bcp_control(
                    BCPControlOptions.BATCH_SIZE.value, current_options.batch_size
                )
            if current_options.max_errors is not None:
                logger.debug(f"Setting BCPControlOptions.MAX_ERRORS to {current_options.max_errors}")
                self.wrapper.bcp_control(
                    BCPControlOptions.MAX_ERRORS.value, current_options.max_errors
                )
            if current_options.first_row is not None:
                logger.debug(f"Setting BCPControlOptions.FIRST_ROW to {current_options.first_row}")
                self.wrapper.bcp_control(
                    BCPControlOptions.FIRST_ROW.value, current_options.first_row
                )
            if current_options.last_row is not None:
                logger.debug(f"Setting BCPControlOptions.LAST_ROW to {current_options.last_row}")
                self.wrapper.bcp_control(
                    BCPControlOptions.LAST_ROW.value, current_options.last_row
                )
            if current_options.code_page is not None:
                logger.debug(f"Setting BCPControlOptions.FILE_CODE_PAGE to {current_options.code_page}")
                self.wrapper.bcp_control(
                    BCPControlOptions.FILE_CODE_PAGE.value, current_options.code_page
                )
            if current_options.keep_identity:
                logger.debug("Setting BCPControlOptions.KEEP_IDENTITY to 1")
                self.wrapper.bcp_control(BCPControlOptions.KEEP_IDENTITY.value, 1)
            if current_options.keep_nulls:
                logger.debug("Setting BCPControlOptions.KEEP_NULLS to 1")
                self.wrapper.bcp_control(BCPControlOptions.KEEP_NULLS.value, 1)
            if current_options.hints:
                logger.debug(f"Setting BCPControlOptions.HINTS to '{current_options.hints}'")
                self.wrapper.bcp_control(
                    BCPControlOptions.HINTS.value, current_options.hints
                )
            # if (
            #     current_options.columns
            #     and current_options.columns[0].row_terminator is not None
            # ):  # Check if columns list is not empty
            #     logger.debug(f"Setting BCPControlOptions.SET_ROW_TERMINATOR to '{current_options.columns[0].row_terminator}'")
            #     self.wrapper.bcp_control(
            #         BCPControlOptions.SET_ROW_TERMINATOR.value,
            #         current_options.columns[0].row_terminator,
            #     )

            if current_options.bulk_mode:
                logger.debug(f"Setting bulk mode to '{current_options.bulk_mode}'")
                field_term_bytes: Optional[bytes] = None
                row_term_bytes: Optional[bytes] = None

                if current_options.columns: # Check if the list is not empty
                    # Use the field terminator from the first column as a default for the file
                    if current_options.columns[0].field_terminator is not None:
                        field_term_bytes = current_options.columns[0].field_terminator
                    
                    # Find the row terminator from any column that defines it
                    # Often, it's defined on the last column's ColumnFormat object
                    for col_fmt in current_options.columns:
                        if col_fmt.row_terminator is not None:
                            row_term_bytes = col_fmt.row_terminator
                            break # Use the first one found
                
                self.wrapper.set_bulk_mode(
                    current_options.bulk_mode,
                    field_terminator=field_term_bytes, 
                    row_terminator=row_term_bytes
                )

            # Handle format file or column definitions
            if current_options.format_file:
                logger.info(f"Reading format file: '{current_options.format_file}'")
                # This implies direction is "format" or "in"/"out" with a format file
                self.wrapper.read_format_file(current_options.format_file)
            elif current_options.columns:  # Check if columns list is not empty
                logger.info(f"Defining {len(current_options.columns)} columns programmatically.")
                self.wrapper.define_columns(len(current_options.columns))
                for i, col_format_obj in enumerate(current_options.columns):
                    logger.debug(f"Defining column format for file column {col_format_obj.file_col}: {col_format_obj}")
                    self.wrapper.define_column_format(
                        file_col_idx=col_format_obj.file_col,
                        user_data_type=col_format_obj.user_data_type, # New field from ColumnFormat
                        indicator_length=col_format_obj.prefix_len,
                        user_data_length=col_format_obj.data_len,
                        terminator_bytes=col_format_obj.field_terminator, # Pass bytes directly
                        server_col_idx=col_format_obj.server_col
                    )
            else:
                logger.info("No format file or explicit column definitions provided. Relying on BCP defaults or server types.")


            logger.info("Executing BCP operation via wrapper.exec_bcp().")
            self.wrapper.exec_bcp()
            logger.info("BCP operation executed successfully.")

        except Exception as e:
            logger.exception(f"An error occurred during BCP operation for table '{table}': {e}")
            raise # Re-raise the exception after logging
        finally:
            if self.wrapper:
                logger.info("Finishing and closing BCPWrapper.")
                self.wrapper.finish()
                self.wrapper.close()
                logger.debug("BCPWrapper finished and closed.")
            logger.info(f"sql_bulk_copy for table/query: '{table}' completed.")
