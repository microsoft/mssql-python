class Row:
    """
    A row of data from a cursor fetch operation. Provides both tuple-like indexing
    and attribute access to column values.
    
    Example:
        row = cursor.fetchone()
        print(row[0])           # Access by index
        print(row.column_name)  # Access by column name
    """
    
    def __init__(self, cursor, description, values, column_map=None):
        """
        Initialize a Row object with values and cursor description.
        
        Args:
            cursor: The cursor object
            description: The cursor description containing column metadata
            values: List of values for this row
            column_map: Optional pre-built column map (for optimization)
        """
        self._cursor = cursor
        self._values = values
        
        # TODO: ADO task - Optimize memory usage by sharing column map across rows
        # Instead of storing the full cursor_description in each Row object:
        # 1. Build the column map once at the cursor level after setting description
        # 2. Pass only this map to each Row instance
        # 3. Remove cursor_description from Row objects entirely
        
        # Create mapping of column names to indices
        # If column_map is not provided, build it from description
        if column_map is None:
            column_map = {}
            for i, col_desc in enumerate(description):
                col_name = col_desc[0]  # Name is first item in description tuple
                column_map[col_name] = i
                
        self._column_map = column_map
    
    def __getitem__(self, index):
        """Allow accessing by numeric index: row[0]"""
        return self._values[index]
    
    def __getattr__(self, name):
        """Allow accessing by column name as attribute: row.column_name"""
        # Handle lowercase attribute access - if lowercase is enabled,
        # try to match attribute names case-insensitively
        if name in self._column_map:
            return self._values[self._column_map[name]]
        
        # If lowercase is enabled on the cursor, try case-insensitive lookup
        if hasattr(self._cursor, 'lowercase') and self._cursor.lowercase:
            name_lower = name.lower()
            for col_name in self._column_map:
                if col_name.lower() == name_lower:
                    return self._values[self._column_map[col_name]]
        
        raise AttributeError(f"Row has no attribute '{name}'")
    
    def __eq__(self, other):
        """
        Support comparison with lists for test compatibility.
        This is the key change needed to fix the tests.
        """
        if isinstance(other, list):
            return self._values == other
        elif isinstance(other, Row):
            return self._values == other._values
        return super().__eq__(other)
    
    def __len__(self):
        """Return the number of values in the row"""
        return len(self._values)
    
    def __iter__(self):
        """Allow iteration through values"""
        return iter(self._values)
    
    def __str__(self):
        """Return string representation of the row"""
        from decimal import Decimal
        from mssql_python import getDecimalSeparator
        
        parts = []
        for value in self:
            if isinstance(value, Decimal):
                # Apply custom decimal separator for display
                sep = getDecimalSeparator()
                if sep != '.' and value is not None:
                    s = str(value)
                    if '.' in s:
                        s = s.replace('.', sep)
                    parts.append(s)
                else:
                    parts.append(str(value))
            else:
                parts.append(repr(value))
        
        return "(" + ", ".join(parts) + ")"

    def __repr__(self):
        """Return a detailed string representation for debugging"""
        return repr(tuple(self._values))