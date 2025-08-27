class Row:
    """
    A row of data from a cursor fetch operation. Provides both tuple-like indexing
    and attribute access to column values.
    
    Example:
        row = cursor.fetchone()
        print(row[0])           # Access by index
        print(row.column_name)  # Access by column name
    """
    
    def __init__(self, values, description, column_map=None):
        """
        Initialize a Row object with values and description.
        
        Args:
            values: List of values for this row.
            description: Description of the columns (from cursor.description).
            column_map: Optional mapping of column names to indices.
        """
        self._values = values
        self._description = description
        
        # Build column map if not provided
        if column_map is None:
            self._column_map = {}
            for i, desc in enumerate(description):
                col_name = desc[0]
                self._column_map[col_name] = i
                self._column_map[col_name.lower()] = i  # Add lowercase for case-insensitivity
        else:
            self._column_map = column_map
    
    def __getitem__(self, index):
        """Allow accessing by numeric index: row[0]"""
        return self._values[index]
    
    def __getattr__(self, name):
        """Allow accessing by column name as attribute: row.column_name"""
        if name in self._column_map:
            return self._values[self._column_map[name]]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
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
        return str(tuple(self._values))

    def __repr__(self):
        """Return a detailed string representation for debugging"""
        return repr(tuple(self._values))