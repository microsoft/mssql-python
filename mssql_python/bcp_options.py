from dataclasses import dataclass, field
from typing import List, Optional, Union
from mssql_python.constants import BCPControlOptions


@dataclass
class ColumnFormat:
    """
    Represents the format of a column in a bulk copy operation.
    Attributes:
        prefix_len (int): Option: (format_file) or (prefix_len, data_len).
            The length of the prefix for fixed-length data types. Must be non-negative.
        data_len (int): Option: (format_file) or (prefix_len, data_len).
            The length of the data. Must be non-negative.
        field_terminator (Optional[bytes]): Option: (-t). The field terminator string.
            e.g., b',' for comma-separated values.
        row_terminator (Optional[bytes]): Option: (-r). The row terminator string.
            e.g., b'\\n' for newline-terminated rows.
        server_col (int): Option: (format_file) or (server_col). The 1-based column number
            in the SQL Server table. Defaults to 1, representing the first column.
            Must be a positive integer.
        file_col (int): Option: (format_file) or (file_col). The 1-based column number
            in the data file. Defaults to 1, representing the first column.
            Must be a positive integer.
    """

    prefix_len: int = 0
    data_len: int = 0
    field_terminator: Optional[bytes] = None
    row_terminator: Optional[bytes] = None
    server_col: int = 1
    file_col: int = 1
    col_name: Optional[str] = None

    def __post_init__(self):
        if self.prefix_len < 0:
            raise ValueError("prefix_len must be a non-negative integer.")
        if self.data_len < 0:
            raise ValueError("data_len must be a non-negative integer.")
        if self.server_col <= 0:
            raise ValueError("server_col must be a positive integer (1-based).")
        if self.file_col <= 0:
            raise ValueError("file_col must be a positive integer (1-based).")
        if self.field_terminator is not None and not isinstance(
            self.field_terminator, bytes
        ):
            raise TypeError("field_terminator must be bytes or None.")
        if self.row_terminator is not None and not isinstance(
            self.row_terminator, bytes
        ):
            raise TypeError("row_terminator must be bytes or None.")


@dataclass
class BCPOptions:
    """
    Represents the options for a bulk copy operation.
    Attributes:
        direction (Literal[str]): 'in' or 'out'. Option: (-i or -o).
        data_file (str): The data file. Option: (positional argument).
        error_file (Optional[str]): The error file. Option: (-e).
        format_file (Optional[str]): The format file to use for 'in'/'out'. Option: (-f).
        batch_size (Optional[int]): The batch size. Option: (-b).
        max_errors (Optional[int]): The maximum number of errors allowed. Option: (-m).
        first_row (Optional[int]): The first row to process. Option: (-F).
        last_row (Optional[int]): The last row to process. Option: (-L).
        code_page (Optional[str]): The code page. Option: (-C).
        keep_identity (bool): Keep identity values. Option: (-E).
        keep_nulls (bool): Keep null values. Option: (-k).
        hints (Optional[str]): Additional hints. Option: (-h).
        bulk_mode (str): Bulk mode ('native', 'char', 'unicode'). Option: (-n, -c, -w).
            Defaults to "native".
        columns (List[ColumnFormat]): Column formats.
    """

    direction: str
    data_file: str  # data_file is mandatory for 'in' and 'out'
    error_file: Optional[str] = None
    format_file: Optional[str] = None
    # write_format_file is removed as 'format' direction is not actively supported
    bulk_mode: Optional[str] = "native"  # Default to 'native' mode
    batch_size: Optional[int] = None
    max_errors: Optional[int] = None
    first_row: Optional[int] = None
    last_row: Optional[int] = None
    code_page: Optional[Union[int, str]] = None
    hints: Optional[str] = None
    columns: Optional[List[ColumnFormat]] = field(default_factory=list)
    keep_identity: bool = False
    keep_nulls: bool = False

    def __post_init__(self):
        if (
            not self.direction
        ):  # Should be caught by dataclass if no default, but good for explicit check
            raise ValueError("BCPOptions.direction is a required field.")

        if self.direction not in BCPControlOptions.ALLOWED_DIRECTIONS:
            raise ValueError(
                f"BCPOptions.direction '{self.direction}' is invalid. "
                f"Allowed directions are: {', '.join(BCPControlOptions.ALLOWED_DIRECTIONS)}."
            )

        if self.direction in ["in", "out"]:
            if not self.data_file:
                raise ValueError(
                    f"BCPOptions.data_file is required for BCP direction '{self.direction}'."
                )

        if not self.data_file:
            raise ValueError(
                "data_file must be provided and non-empty for 'in' or 'out' directions."
            )
        if (
            self.error_file is None or not self.error_file
        ):  # Making error_file mandatory for in/out
            raise ValueError(
                "error_file must be provided and non-empty for 'in' or 'out' directions."
            )

        if self.columns and self.format_file:
            raise ValueError(
                "Cannot specify both 'columns' (for bcp_colfmt) and 'format_file' (for bcp_readfmt). Choose one."
            )

        if isinstance(self.code_page, int) and self.code_page < 0:
            raise ValueError(
                "BCPOptions.code_page, if an integer, must be non-negative."
            )
        
        if self.bulk_mode not in BCPControlOptions.ALLOWED_FILE_MODES:
            raise ValueError(
                f"BCPOptions.bulk_mode '{self.bulk_mode}' is invalid. "
                f"Allowed modes are: {', '.join(BCPControlOptions.ALLOWED_FILE_MODES)}."
            )
        for attr_name in ["batch_size", "max_errors", "first_row", "last_row"]:
            attr_value = getattr(self, attr_name)
            if attr_value is not None and attr_value < 0:
                raise ValueError(
                    f"BCPOptions.{attr_name} must be non-negative if specified. Got {attr_value}"
                )

        if (
            self.first_row is not None
            and self.last_row is not None
            and self.first_row > self.last_row
        ):
            raise ValueError(
                "BCPOptions.first_row cannot be greater than BCPOptions.last_row."
            )