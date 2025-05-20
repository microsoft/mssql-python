from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ColumnFormat:
    prefix_len: int # Option: (format_file) or (prefix_len, data_len)
    data_len: int # Option: (format_file) or (prefix_len, data_len)
    field_terminator: Optional[str] = None # Option: (-t)
    row_terminator: Optional[str] = None # Option: (-r)
    server_col: int = 1 # Option: (format_file) or (server_col)
    file_col: int = 1 # Option: (format_file) or (file_col)

@dataclass
class BCPOptions:
    direction: str  # 'in' or 'out' Option: (-i or -o)
    data_file: str # Option: (positional argument)
    error_file: Optional[str] = None # Option: (-e)
    format_file: Optional[str] = None # Option: (-f)
    write_format_file: Optional[str] = None # Option: (-x)
    batch_size: Optional[int] = None # Option: (-b)
    max_errors: Optional[int] = None # Option: (-m)
    first_row: Optional[int] = None # Option: (-F)
    last_row: Optional[int] = None # Option: (-L)
    code_page: Optional[str] = None # Option: (-C)
    keep_identity: bool = False # Option: (-E)
    keep_nulls: bool = False # Option: (-k)
    hints: Optional[str] = None # Option: (-h)
    bulk_mode: str = "native"  # native, char, unicode Option: (-n, -c, -w)
    columns: Optional[List[ColumnFormat]] = field(default_factory=list) # Option: (format_file) or (columns)