from mssql_python.bcp_options import (
    BCPOptions,
    ColumnFormat,
)  # BCPOptions now handles more validation
from typing import (
    Optional,
)  # Optional might still be used elsewhere, but not for run_bcp options
from ddbc_bindings import BCPWrapper
from mssql_python.constants import BCPControlOptions


class BCPClient:
    """
    A client for performing bulk copy operations using the BCP (Bulk Copy Program) utility.
    This class provides methods to initialize and execute BCP operations.
    """

    SUPPORTED_DIRECTIONS = {
        "in",
        "out",
    }  # This client might only support a subset of BCP directions

    def __init__(self, connection):
        """
        Initializes the BCPClient with a database connection.
        Args:
            connection: A database connection object that will be used by BCPWrapper.
        """
        if connection is None:
            raise ValueError(
                "A valid connection object is required to initialize BCPClient."
            )
        self.wrapper = BCPWrapper(connection)

    def run_bcp(self, table: str, options: BCPOptions):  # options is no longer Optional
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
        if not table:
            raise ValueError(
                "The 'table' name (or query for queryout) must be provided."
            )

        if not isinstance(options, BCPOptions):
            # This check is good practice, though type hints help statically.
            raise TypeError("The 'options' argument must be an instance of BCPOptions.")

        # BCPOptions.__post_init__ has already performed its internal validation.
        # BCPClient can add its own operational constraints:
        if options.direction not in self.SUPPORTED_DIRECTIONS:
            raise ValueError(
                f"BCPClient currently only supports directions: {', '.join(self.SUPPORTED_DIRECTIONS)}. "
                f"Got '{options.direction}'."
            )

        current_options = options  # Use the validated options directly

        if not self.wrapper:  # Should be caught by __init__ ideally
            raise RuntimeError("BCPWrapper was not initialized.")

        try:
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

            # Set BCP control options
            if current_options.batch_size is not None:
                self.wrapper.bcp_control(
                    BCPControlOptions.BATCH_SIZE.value, current_options.batch_size
                )
            if current_options.max_errors is not None:
                self.wrapper.bcp_control(
                    BCPControlOptions.MAX_ERRORS.value, current_options.max_errors
                )
            if current_options.first_row is not None:
                self.wrapper.bcp_control(
                    BCPControlOptions.FIRST_ROW.value, current_options.first_row
                )
            if current_options.last_row is not None:
                self.wrapper.bcp_control(
                    BCPControlOptions.LAST_ROW.value, current_options.last_row
                )
            if current_options.code_page is not None:
                self.wrapper.bcp_control(
                    BCPControlOptions.FILE_CODE_PAGE.value, current_options.code_page
                )
            if current_options.keep_identity:
                self.wrapper.bcp_control(BCPControlOptions.KEEP_IDENTITY.value, 1)
            if current_options.keep_nulls:
                self.wrapper.bcp_control(BCPControlOptions.KEEP_NULLS.value, 1)
            if current_options.hints:
                self.wrapper.bcp_control(
                    BCPControlOptions.HINTS.value, current_options.hints
                )
            if (
                current_options.columns
                and current_options.columns[0].row_terminator is not None
            ):  # Check if columns list is not empty
                self.wrapper.bcp_control(
                    BCPControlOptions.SET_ROW_TERMINATOR.value,
                    current_options.columns[0].row_terminator,
                )

            if current_options.bulk_mode:
                self.wrapper.set_bulk_mode(current_options.bulk_mode)

            # Handle format file or column definitions
            if current_options.format_file:
                # This implies direction is "format" or "in"/"out" with a format file
                self.wrapper.read_format_file(current_options.format_file)
            elif current_options.columns:  # Check if columns list is not empty
                self.wrapper.define_columns(len(current_options.columns))
                for i, col_format_obj in enumerate(current_options.columns):
                    self.wrapper.define_column_format(
                        col_num_ordinal=i + 1,
                        prefix_len=col_format_obj.prefix_len,
                        data_len=col_format_obj.data_len,
                        terminator_wstr=col_format_obj.field_terminator,
                        col_name=col_format_obj.col_name,
                        server_col=col_format_obj.server_col,
                        file_col=col_format_obj.file_col,
                    )

            self.wrapper.exec_bcp()

        finally:
            if self.wrapper:
                self.wrapper.finish()
                self.wrapper.close()
