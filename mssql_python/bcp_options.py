"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
BCPOptions and ColumnFormat classes for Bulk Copy Program (BCP) operations.
"""
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Union

# Removed unused import: BCPControlOptions

# defining constants for BCP control options
ALLOWED_DIRECTIONS = ("in", "out", "queryout")
ALLOWED_FILE_MODES = ("native", "char", "unicode")


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

    file_col: int = 1
    user_data_type: int = 0
    prefix_len: int = 0
    data_len: int = 0
    field_terminator: Optional[bytes] = None
    terminator_len: int = 0
    server_col: int = 1

    def __post_init__(self):
        logging.debug("Initializing ColumnFormat: %r", self)
        if self.prefix_len < 0:
            logging.error("prefix_len must be a non-negative integer.")
            raise ValueError("prefix_len must be a non-negative integer.")
        if self.data_len < 0:
            logging.error("data_len must be a non-negative integer.")
            raise ValueError("data_len must be a non-negative integer.")
        if self.server_col <= 0:
            logging.error("server_col must be a positive integer (1-based).")
            raise ValueError("server_col must be a positive integer (1-based).")
        if self.file_col <= 0:
            logging.error("file_col must be a positive integer (1-based).")
            raise ValueError("file_col must be a positive integer (1-based).")
        if self.field_terminator is not None and not isinstance(
            self.field_terminator, bytes
        ):
            logging.error("field_terminator must be bytes or None.")
            raise TypeError("field_terminator must be bytes or None.")

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
    data_file: Optional[str] = None  # data_file is mandatory for 'in' and 'out'
    error_file: Optional[str] = None
    format_file: Optional[str] = None
    query: Optional[str] = None  # For 'query' direction
    bulk_mode: Optional[str] = "native"  # Default to 'native' mode
    batch_size: Optional[int] = None
    max_errors: Optional[int] = None
    first_row: Optional[int] = None
    last_row: Optional[int] = None
    code_page: Optional[Union[int, str]] = None
    hints: Optional[str] = None
    columns: Optional[List[ColumnFormat]] = field(default_factory=list)
    row_terminator: Optional[bytes] = None
    keep_identity: bool = False
    keep_nulls: bool = False

    def __post_init__(self):
        logging.debug("Initializing BCPOptions: %r", self)
        if not self.direction:
            logging.error("BCPOptions.direction is a required field.")
            raise ValueError("BCPOptions.direction is a required field.")

        if self.direction not in ALLOWED_DIRECTIONS:
            logging.error(
                "BCPOptions.direction '%s' is invalid. Allowed directions are: %s.",
                self.direction, ", ".join(ALLOWED_DIRECTIONS)
            )
            raise ValueError(
                f"BCPOptions.direction '{self.direction}' is invalid.
                Allowed directions are: {', '.join(ALLOWED_DIRECTIONS)}."
            )

        if self.direction in ["in", "out"]:
            if not self.data_file:
                logging.error(
                    "BCPOptions.data_file is required for BCP direction '%s'.",
                    self.direction
                )
                raise ValueError(
                    f"BCPOptions.data_file is required for BCP direction '{self.direction}'."
                )
        if self.direction == "queryout" and not self.query:
            logging.error("BCPOptions.query is required for BCP direction 'query'.")
            raise ValueError("BCPOptions.query is required for BCP direction 'query'.")

        if not self.data_file:
            logging.error(
                "data_file must be provided and non-empty for 'in' or 'out' directions."
            )
            raise ValueError(
                "data_file must be provided and non-empty for 'in' or 'out' directions."
            )
        if self.error_file is None or not self.error_file:
            logging.error(
                "error_file must be provided and non-empty for 'in' or 'out' directions."
            )
            raise ValueError(
                "error_file must be provided and non-empty for 'in' or 'out' directions."
            )

        if self.columns and self.format_file:
            logging.error(
                "Cannot specify both 'columns' (for bcp_colfmt) and 'format_file' "
                "(for bcp_readfmt). Choose one."
            )
            raise ValueError(
                "Cannot specify both 'columns' (for bcp_colfmt) and 'format_file' "
                "(for bcp_readfmt). Choose one."
            )

        if isinstance(self.code_page, int) and self.code_page < 0:
            logging.error(
                "BCPOptions.code_page, if an integer, must be non-negative."
            )
            raise ValueError(
                "BCPOptions.code_page, if an integer, must be non-negative."
            )

        if self.bulk_mode not in ALLOWED_FILE_MODES:
            logging.error(
                "BCPOptions.bulk_mode '%s' is invalid. Allowed modes are: %s.",
                self.bulk_mode, ", ".join(ALLOWED_FILE_MODES)
            )
            raise ValueError(
                f"BCPOptions.bulk_mode '{self.bulk_mode}' is invalid.
                Allowed modes are: {', '.join(ALLOWED_FILE_MODES)}."
            )
        for attr_name in ["batch_size", "max_errors", "first_row", "last_row"]:
            attr_value = getattr(self, attr_name)
            if attr_value is not None and attr_value < 0:
                logging.error(
                    "BCPOptions.%s must be non-negative if specified. Got %r",
                    attr_name, attr_value
                )
                raise ValueError(
                    f"BCPOptions.{attr_name} must be non-negative if specified. Got {attr_value!r}"
                )

        if (
            self.first_row is not None
            and self.last_row is not None
            and self.first_row > self.last_row
        ):
            logging.error(
                "BCPOptions.first_row cannot be greater than BCPOptions.last_row."
            )
            raise ValueError(
                "BCPOptions.first_row cannot be greater than BCPOptions.last_row."
            )

        if self.row_terminator is not None and not isinstance(
            self.row_terminator, bytes
        ):
            logging.error("row_terminator must be bytes or None.")
            raise TypeError("row_terminator must be bytes or None.")
