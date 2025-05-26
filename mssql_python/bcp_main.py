from mssql_python.bcp_options import BCPOptions, ColumnFormat
from typing import Optional
from ddbc_bindings import BCPWrapper

class BCPClient:
    """
    A client for performing bulk copy operations using the BCP (Bulk Copy Program) utility.
    This class provides methods to initialize and execute BCP operations for 'in' and 'out' directions.
    """

    def __init__(self, connection):
        """
        Initializes the BCPClient with a database connection.
        Args:
            connection: A database connection object.
        """
        self.wrapper = BCPWrapper(connection)

    def _validate_options(self, options: Optional[BCPOptions]) -> BCPOptions:
        """
        Validates the provided BCP options or provides a default.
        Ensures options are suitable for 'in' or 'out' operations.
        """
        if options is None:
            # Default options for 'in' operation
            return BCPOptions(direction="in", data_file="default_data.bcp", error_file="default_error.err", bulk_mode="native")
        
        if not isinstance(options, BCPOptions):
            raise TypeError("options must be an instance of BCPOptions or None.")

        if options.direction not in ["in", "out"]:
            raise ValueError("BCPClient currently only supports 'in' or 'out' directions.")
        
        # Further validation is handled by BCPOptions.__post_init__
        # (e.g., data_file and error_file are mandatory for 'in'/'out')
        return options


    def run_bcp(self, table: str, options: Optional[BCPOptions] = None):
        """
        Executes a bulk copy operation ('in' or 'out') to or from a specified table.
        Args:
            table (str): The name of the table.
            options (BCPOptions, optional): Options for the bulk copy operation.
                If None, default options for an 'in' operation will be used.
                options.direction must be 'in' or 'out'.
        """
        current_options = self._validate_options(options)

        # BCPOptions.__post_init__ ensures data_file and error_file are set for 'in'/'out'
        self.wrapper.bcp_initialize_operation(table, current_options.data_file, current_options.error_file, current_options.direction)

        # Set BCP control options
        if current_options.batch_size is not None:
            self.wrapper.bcp_control("BCPBATCH", current_options.batch_size)
        if current_options.max_errors is not None:
            self.wrapper.bcp_control("BCPMAXERRS", current_options.max_errors)
        if current_options.first_row is not None:
            self.wrapper.bcp_control("BCPFIRST", current_options.first_row)
        if current_options.last_row is not None:
            self.wrapper.bcp_control("BCPLAST", current_options.last_row)
        if current_options.code_page is not None:
            self.wrapper.bcp_control("BCPFILECP", current_options.code_page)
        if hasattr(current_options, 'keep_identity') and current_options.keep_identity:
            self.wrapper.bcp_control("BCPKEEPIDENTITY", 1)
        if hasattr(current_options, 'keep_nulls') and current_options.keep_nulls:
            self.wrapper.bcp_control("BCPKEEPNULLS", 1)
        if current_options.hints:
            self.wrapper.bcp_control("BCPHINTS", current_options.hints)
        
        if current_options.columns and current_options.columns[0].row_terminator is not None:
             self.wrapper.bcp_control("BCPSETROWTERM", current_options.columns[0].row_terminator)

        if current_options.bulk_mode:
            self.wrapper.set_bulk_mode(current_options.bulk_mode)

        # Handle format file or column definitions for 'in' or 'out'
        if current_options.format_file:
            self.wrapper.read_format_file(current_options.format_file)
        elif current_options.columns:
            self.wrapper.define_columns(len(current_options.columns))
            for i, col in enumerate(current_options.columns):
                self.wrapper.define_column_format(
                    col_num=i + 1, 
                    prefix_len=col.prefix_len,
                    data_len=col.data_len,
                    terminator=col.field_terminator, 
                    server_col_type=getattr(col, 'server_col_type', 0), # Assuming 0 if not present
                    col_name=getattr(col, 'col_name', None), # Assuming None if not present
                    server_col=col.server_col,
                    file_col=col.file_col
                )
        
        self.wrapper.exec_bcp()
        self.wrapper.finish()
        
        if hasattr(self.wrapper, 'close'):
            self.wrapper.close()