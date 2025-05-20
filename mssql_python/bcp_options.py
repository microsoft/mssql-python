from dataclasses import dataclass, field
from typing import List, Optional, Literal

@dataclass
class ColumnFormat:
    """
    Represents the format of a column in a bulk copy operation.
    Attributes:
        prefix_len (int): Option: (format_file) or (prefix_len, data_len).
        data_len (int): Option: (format_file) or (prefix_len, data_len).
        field_terminator (Optional[str]): Option: (-t). The field terminator string.
        row_terminator (Optional[str]): Option: (-r). The row terminator string.
        server_col (int): Option: (format_file) or (server_col). The server column number.
        file_col (int): Option: (format_file) or (file_col). The file column number.
    """
    prefix_len: int
    data_len: int
    field_terminator: Optional[str] = None
    row_terminator: Optional[str] = None
    server_col: int = 1
    file_col: int = 1

@dataclass
class BCPOptions:
    """
    Represents the options for a bulk copy operation.
    Attributes:
        direction (Literal(str)): 'in' or 'out'. Option: (-i or -o).
        data_file (str): The data file. Option: (positional argument).
        error_file (Optional[str]): The error file. Option: (-e).
        format_file (Optional[str]): The format file. Option: (-f).
        write_format_file (Optional[str]): Write a format file. Option: (-x).
        batch_size (Optional[int]): The batch size. Option: (-b).
        max_errors (Optional[int]): The maximum number of errors allowed. Option: (-m).
        first_row (Optional[int]): The first row to process. Option: (-F).
        last_row (Optional[int]): The last row to process. Option: (-L).
        code_page (Optional[str]): The code page. Option: (-C).
        keep_identity (bool): Keep identity values. Option: (-E).
        keep_nulls (bool): Keep null values. Option: (-k).
        hints (Optional[str]): Additional hints. Option: (-h).
        bulk_mode (str): Bulk mode ('native', 'char', 'unicode'). Option: (-n, -c, -w).
        columns (List[ColumnFormat]): Column formats. Option: (format_file) or (columns).
    """
    direction: Literal["in", "out"]
    data_file: str
    error_file: Optional[str] = None
    format_file: Optional[str] = None
    write_format_file: Optional[str] = None
    batch_size: Optional[int] = None
    max_errors: Optional[int] = None
    first_row: Optional[int] = None
    last_row: Optional[int] = None
    code_page: Optional[str] = None
    keep_identity: bool = False
    keep_nulls: bool = False
    hints: Optional[str] = None
    bulk_mode: Literal["native", "char", "unicode"] = "native"
    columns: List[ColumnFormat] = field(default_factory=list)