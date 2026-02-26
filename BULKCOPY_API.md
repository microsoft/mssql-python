# Bulkcopy API Documentation

## Overview

The `bulkcopy()` method provides high-performance bulk data loading into Microsoft SQL Server, Azure SQL Database, and Azure SQL Managed Instance. This operation is optimized for inserting large volumes of data much faster than individual INSERT statements.

**Key Benefits:**
- High-performance batch inserts
- Configurable batch sizes for optimal throughput
- Flexible column mapping (ordinal or explicit)
- Support for identity columns, constraints, and triggers
- Transaction control per batch

## Method Signature

```python
cursor.bulkcopy(
    table_name: str,
    data: Iterable[Union[Tuple, List]],
    batch_size: int = 0,
    timeout: int = 30,
    column_mappings: Optional[Union[List[str], List[Tuple[int, str]]]] = None,
    keep_identity: bool = False,
    check_constraints: bool = False,
    table_lock: bool = False,
    keep_nulls: bool = False,
    fire_triggers: bool = False,
    use_internal_transaction: bool = False,
) -> dict
```

## Parameters

### Required Parameters

#### `table_name` (str)
Target table name where data will be inserted.

- Can include schema qualifier: `'dbo.MyTable'` or `'MyTable'`
- Can use fully qualified name: `'[database].[schema].[table]'`
- The table must exist before calling bulkcopy
- User must have INSERT permission on the target table

**Examples:**
```python
table_name = "Users"                    # Simple table name
table_name = "dbo.Users"                # With schema
table_name = "[MyDB].[dbo].[Users]"     # Fully qualified
```

#### `data` (Iterable[Union[Tuple, List]])
Iterable containing row data to be inserted.

**Data Format Requirements:**
- Each element represents one row
- Each row must be a tuple or list of column values
- Column order must match the table's column order (by ordinal position) unless `column_mappings` is specified
- Number of values per row must match the number of columns in the target table

**Examples:**
```python
# List of tuples (recommended for immutability)
data = [
    (1, "Alice", 100.5),
    (2, "Bob", 200.75),
    (3, "Charlie", 300.25),
]

# Generator (memory-efficient for large datasets)
def row_generator():
    for i in range(1000):
        yield (i, f"User{i}", i * 10.0)

data = row_generator()
```

### Optional Parameters

#### `batch_size` (int, default: 0)
Number of rows to send per batch.

- `0` (default): Server determines optimal batch size
- Positive integer: Explicit batch size

**Performance Tip:** For most scenarios, the default (0) provides optimal performance. Use explicit batch sizes when:
- Memory constrained environments (smaller batches)
- Very large rows (smaller batches)
- Network latency issues (larger batches)

#### `timeout` (int, default: 30)
Operation timeout in seconds.

- Sets the maximum time for the entire bulkcopy operation
- Raises timeout error if operation exceeds this duration

#### `column_mappings` (Optional[Union[List[str], List[Tuple[int, str]]]], default: None)
Maps source data columns to target table columns.

**Format 1: Simple List (List[str])**
- Position in list corresponds to source data index
- Value is the destination column name

```python
# Source data: (value1, value2, value3)
# Map to columns: UserID, FirstName, Email
column_mappings = ['UserID', 'FirstName', 'Email']
# Result: value1 → UserID, value2 → FirstName, value3 → Email
```

**Format 2: Explicit Tuples (List[Tuple[int, str]])**
- Each tuple is `(source_index, target_column_name)`
- Allows skipping or reordering columns

```python
# Source data has 4 values, but we only want to insert 3 columns
column_mappings = [
    (0, 'UserID'),      # First value → UserID
    (1, 'FirstName'),   # Second value → FirstName
    (3, 'Email')        # Fourth value → Email (skip index 2)
]
```

**When omitted (None):**
- Columns mapped by ordinal position
- First data value → first table column, second → second, etc.

#### `keep_identity` (bool, default: False)
Preserve identity values from source data.

- `False`: Server generates identity values (default behavior)
- `True`: Use identity values from source data (requires SET IDENTITY_INSERT ON permission)

**Use Case:** Migrating data while preserving original IDs

#### `check_constraints` (bool, default: False)
Enable constraint checking during bulk copy.

- `False`: Constraints are not checked (faster, default behavior)
- `True`: Check constraints and foreign keys during insert

**Use Case:** Ensure data integrity during import, but slower performance

#### `table_lock` (bool, default: False)
Use table-level lock instead of row-level locks.

- `False`: Row-level locks (default)
- `True`: Table-level lock (better performance for large imports)

**Performance Tip:** Enable for large bulk operations when table is not in active use

#### `keep_nulls` (bool, default: False)
Preserve null values from source data.

- `False`: Use column default values for nulls
- `True`: Insert null values as-is

**Use Case:** When source data null values should override table defaults

#### `fire_triggers` (bool, default: False)
Execute INSERT triggers on the target table.

- `False`: Triggers not fired (default, faster)
- `True`: Fire triggers for each batch

**Use Case:** When business logic in triggers must execute during bulk load

#### `use_internal_transaction` (bool, default: False)
Use internal transaction for each batch.

- `False`: No internal transaction (use external transaction if needed)
- `True`: Each batch wrapped in its own transaction

**Use Case:** Partial success scenarios where batches should commit independently

## Return Value

Returns a dictionary with bulk copy operation results:

```python
{
    "rows_copied": int,      # Number of rows successfully copied
    "batch_count": int,      # Number of batches processed
    "elapsed_time": float    # Time taken in seconds
}
```

**Example:**
```python
result = cursor.bulkcopy("Users", data)
print(f"Copied {result['rows_copied']} rows in {result['elapsed_time']:.2f}s")
# Output: Copied 10000 rows in 2.35s
```

## Exceptions

### `ImportError`
Raised when `mssql_py_core` library is not installed.

```python
try:
    cursor.bulkcopy("Users", data)
except ImportError as e:
    print("mssql_py_core not available - bulkcopy not supported")
```

### `TypeError`
Raised when data parameter is invalid:
- `data` is None
- `data` is not iterable
- `data` is a string or bytes object

```python
try:
    cursor.bulkcopy("Users", "invalid_data")
except TypeError as e:
    print(f"Invalid data type: {e}")
```

### `ValueError`
Raised for invalid parameters:
- `table_name` is empty or None
- SERVER parameter missing from connection string
- Invalid column mapping format
- Invalid parameter values (negative batch_size, etc.)

```python
try:
    cursor.bulkcopy("", data)
except ValueError as e:
    print(f"Invalid parameter: {e}")
```

### `RuntimeError`
Raised when connection string is not available or connection issues occur.

```python
try:
    cursor.bulkcopy("Users", data)
except RuntimeError as e:
    print(f"Connection error: {e}")
```

## Usage Examples

### Example 1: Basic Bulkcopy (Ordinal Mapping)

```python
import mssql_python

# Connect to database
conn = mssql_python.connect(
    "SERVER=localhost;DATABASE=TestDB;UID=user;PWD=password;"
)
cursor = conn.cursor()

# Create table
cursor.execute("""
    CREATE TABLE Users (
        id INT,
        name VARCHAR(50),
        email VARCHAR(100)
    )
""")
conn.commit()

# Prepare data - columns match table order (id, name, email)
data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com"),
]

# Perform bulkcopy
result = cursor.bulkcopy("Users", data)

print(f"Rows copied: {result['rows_copied']}")
# Output: Rows copied: 3

cursor.close()
conn.close()
```

### Example 2: Bulkcopy with Simple Column Mapping

```python
# Table has columns: UserID, FirstName, LastName, Email
cursor.execute("""
    CREATE TABLE Users (
        UserID INT,
        FirstName VARCHAR(50),
        LastName VARCHAR(50),
        Email VARCHAR(100)
    )
""")
conn.commit()

# Data columns: ID, First, Last, Email
data = [
    (1, "Alice", "Smith", "alice@example.com"),
    (2, "Bob", "Jones", "bob@example.com"),
]

# Map data positions to table column names
column_mappings = ['UserID', 'FirstName', 'LastName', 'Email']

result = cursor.bulkcopy(
    table_name="Users",
    data=data,
    column_mappings=column_mappings
)

print(f"Rows copied: {result['rows_copied']}")
```

### Example 3: Bulkcopy with Explicit Column Mapping (Reordering)

```python
# Table columns: UserID, Email, FirstName
cursor.execute("""
    CREATE TABLE Users (
        UserID INT,
        Email VARCHAR(100),
        FirstName VARCHAR(50)
    )
""")
conn.commit()

# Source data columns: ID, First, Last, Email
# We'll map: ID → UserID, First → FirstName, Email → Email
# (skipping Last name)
data = [
    (1, "Alice", "Smith", "alice@example.com"),
    (2, "Bob", "Jones", "bob@example.com"),
]

# Explicit mapping: (source_index, target_column)
column_mappings = [
    (0, 'UserID'),      # data[0] → UserID
    (3, 'Email'),       # data[3] → Email
    (1, 'FirstName'),   # data[1] → FirstName
]

result = cursor.bulkcopy(
    table_name="Users",
    data=data,
    column_mappings=column_mappings
)

print(f"Rows copied: {result['rows_copied']}")
```

### Example 4: Bulkcopy with Identity Column

```python
# Table with identity column
cursor.execute("""
    CREATE TABLE Users (
        UserID INT IDENTITY(1,1) PRIMARY KEY,
        FirstName VARCHAR(50),
        Email VARCHAR(100)
    )
""")
conn.commit()

# Data includes specific ID values we want to preserve
data = [
    (100, "Alice", "alice@example.com"),
    (200, "Bob", "bob@example.com"),
]

# Enable keep_identity to use provided ID values
result = cursor.bulkcopy(
    table_name="Users",
    data=data,
    keep_identity=True,  # Preserve identity values from data
    column_mappings=['UserID', 'FirstName', 'Email']
)

# Verify IDs were preserved
cursor.execute("SELECT UserID FROM Users ORDER BY UserID")
ids = cursor.fetchall()
assert ids[0][0] == 100  # Original ID preserved
assert ids[1][0] == 200
```

### Example 5: Bulkcopy with Performance Options

```python
# Large dataset bulk import with optimized settings
def generate_large_dataset():
    """Generator for memory-efficient large dataset."""
    for i in range(1000000):
        yield (i, f"User{i}", f"user{i}@example.com")

data = generate_large_dataset()

result = cursor.bulkcopy(
    table_name="Users",
    data=data,
    batch_size=10000,           # Process 10k rows per batch
    timeout=300,                # 5 minute timeout
    table_lock=True,            # Use table lock for performance
    check_constraints=False,    # Skip constraint checking
    fire_triggers=False,        # Skip triggers for speed
    use_internal_transaction=True  # Commit each batch independently
)

print(f"Copied {result['rows_copied']} rows in {result['elapsed_time']:.2f}s")
print(f"Processed {result['batch_count']} batches")
print(f"Average: {result['rows_copied']/result['elapsed_time']:.0f} rows/sec")
```

### Example 6: Bulkcopy with Schema and Database Qualifiers

```python
# Fully qualified table name including database and schema
result = cursor.bulkcopy(
    table_name="[ProductionDB].[dbo].[Users]",
    data=data,
    timeout=60
)

# Or use schema qualifier only
result = cursor.bulkcopy(
    table_name="dbo.Users",
    data=data,
    timeout=60
)
```

### Example 7: Bulkcopy with Error Handling

```python
import mssql_python

try:
    # Prepare large dataset
    data = [(i, f"User{i}", f"user{i}@example.com") for i in range(10000)]
    
    # Perform bulkcopy with error handling
    result = cursor.bulkcopy(
        table_name="Users",
        data=data,
        batch_size=1000,
        timeout=120
    )
    
    print(f"Success! Copied {result['rows_copied']} rows")
    
except ImportError:
    print("Error: mssql_py_core not available - bulkcopy not supported")
    
except TypeError as e:
    print(f"Invalid data format: {e}")
    
except ValueError as e:
    print(f"Invalid parameter: {e}")
    
except RuntimeError as e:
    print(f"Connection or operation error: {e}")
    
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Example 8: Bulkcopy Without DATABASE Parameter

```python
# Connection string without DATABASE parameter
# (uses default database for the user)
conn_str = "SERVER=localhost;UID=user;PWD=password;"
conn = mssql_python.connect(conn_str)
cursor = conn.cursor()

# Switch to target database
cursor.execute("USE TestDB")

# Use fully qualified table name for bulkcopy
# (bulkcopy creates its own connection)
result = cursor.bulkcopy(
    table_name="[TestDB].[dbo].[Users]",
    data=data
)

print(f"Rows copied: {result['rows_copied']}")
```

### Example 9: Bulkcopy with SERVER Synonyms

```python
# All these connection string formats work:

# Using SERVER keyword
conn_str = "SERVER=localhost;DATABASE=TestDB;UID=user;PWD=password;"

# Using ADDR keyword (synonym)
conn_str = "ADDR=localhost;DATABASE=TestDB;UID=user;PWD=password;"

# Using ADDRESS keyword (synonym)
conn_str = "ADDRESS=localhost;DATABASE=TestDB;UID=user;PWD=password;"

conn = mssql_python.connect(conn_str)
cursor = conn.cursor()

# Bulkcopy works with any SERVER synonym
result = cursor.bulkcopy("Users", data)
```

## Best Practices

### Performance Optimization

1. **Use Generators for Large Datasets**
   ```python
   def data_generator():
       for row in read_large_file():
           yield row
   
   cursor.bulkcopy("Users", data_generator())
   ```

2. **Enable Table Lock for Large Imports**
   ```python
   cursor.bulkcopy("Users", data, table_lock=True)
   ```

3. **Adjust Batch Size Based on Row Size**
   - Small rows (<100 bytes): Use larger batches (10000-50000)
   - Large rows (>1KB): Use smaller batches (1000-5000)
   - Default (0) is recommended for most cases

4. **Disable Constraints and Triggers During Import**
   ```python
   cursor.bulkcopy(
       "Users", 
       data,
       check_constraints=False,
       fire_triggers=False
   )
   ```

### Data Integrity

1. **Enable Constraints for Data Validation**
   ```python
   cursor.bulkcopy("Users", data, check_constraints=True)
   ```

2. **Use Transactions for Consistency**
   ```python
   try:
       result = cursor.bulkcopy("Users", data)
       conn.commit()
   except Exception as e:
       conn.rollback()
       raise
   ```

3. **Validate Data Before Bulkcopy**
   ```python
   # Check data format
   for row in data_sample:
       assert len(row) == expected_columns
       assert all(isinstance(v, expected_types[i]) for i, v in enumerate(row))
   ```

### Error Handling

1. **Always Use Try-Except Blocks**
   ```python
   try:
       result = cursor.bulkcopy("Users", data)
   except ValueError as e:
       print(f"Parameter error: {e}")
   except RuntimeError as e:
       print(f"Operation failed: {e}")
   ```

2. **Monitor Progress for Large Operations**
   ```python
   start_time = time.time()
   result = cursor.bulkcopy("Users", data, timeout=600)
   elapsed = time.time() - start_time
   rate = result['rows_copied'] / elapsed
   print(f"Import rate: {rate:.0f} rows/second")
   ```

### Schema Management

1. **Use Fully Qualified Names for Clarity**
   ```python
   cursor.bulkcopy("[MyDB].[dbo].[Users]", data)
   ```

2. **Create and Verify Table Schema First**
   ```python
   cursor.execute("CREATE TABLE Users (...)")
   conn.commit()
   
   # Verify table exists
   cursor.execute("""
       SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
       WHERE TABLE_NAME = 'Users'
   """)
   assert cursor.fetchone()[0] == 1
   
   # Now perform bulkcopy
   cursor.bulkcopy("Users", data)
   ```

## Requirements

- **mssql_py_core**: Native extension library must be installed
- **Permissions**: INSERT permission on target table
- **Table Existence**: Target table must exist before bulkcopy
- **Connection**: Valid connection with SERVER parameter

## Compatibility

- **Python**: 3.10+
- **Databases**: 
  - Microsoft SQL Server 2012+
  - Azure SQL Database
  - Azure SQL Managed Instance
  - SQL Server in Microsoft Fabric

## See Also

- [Connection String Builder API](mssql_python/connection_string_builder.py)
- [Connection String Parser API](mssql_python/connection_string_parser.py)
- [DB API 2.0 Specification](https://peps.python.org/pep-0249/)
- [Microsoft SQL Server Bulk Copy](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/bulk-import-and-export-of-data-sql-server)

## Support

For questions, issues, or feedback:
- GitHub Issues: https://github.com/microsoft/mssql-python/issues
- Email: mssql-python@microsoft.com
