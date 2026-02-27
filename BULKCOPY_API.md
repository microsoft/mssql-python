# Bulk Copy (BCP) API Reference

## Overview

The `bulkcopy()` method on the `Cursor` object provides high-performance bulk data loading into SQL Server, Azure SQL Database, Azure SQL Managed Instance, SQL database in Fabric, and Microsoft Fabric. It is designed for ETL workloads and scenarios that require inserting large volumes of data far more efficiently than individual `INSERT` statements.

---
## Key Benefits

-	High-Performance Batch Inserts
-	Configurable Batch Sizes for Optimal Throughput
-	Flexible Column Mapping (Ordinal or Explicit)
-	Support for Identity Columns, Constraints, and Triggers
-	Transaction Control per Batch

## Quick Start

```python
import mssql_python

conn = mssql_python.connect(
   "Server=<server>.database.windows.net;"
   "Database=<db>;"
   "Authentication=ActiveDirectoryIntegrated;"
   "Encrypt=yes;"
)
cursor = conn.cursor()

# bulkcopy() requires the target table to already exist; this CREATE TABLE is just for the example
cursor.execute("""
    CREATE TABLE Products (
        id    INT,
        name  VARCHAR(100),
        price FLOAT
    )
""")
conn.commit()

data = [
    (1, "Widget",  9.99),
    (2, "Gadget", 24.50),
    (3, "Gizmo",  14.75),
]

result = cursor.bulkcopy("Products", data)
print(result)
# {'rows_copied': 3, 'batch_count': 1, 'elapsed_time': 0.12}

cursor.close()
conn.close()
```

---

## Method Signature

```python
Cursor.bulkcopy(
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

---

## Parameters

### Required

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | `str` | Target table name. May include schema (`dbo.MyTable`) or be fully qualified (`[MyDB].[dbo].[MyTable]`). The table must already exist and the caller must have `INSERT` permission. |
| `data` | `Iterable[Tuple \| List]` | Iterable of rows. Each row is a tuple or list of column values. Column order must match the table's ordinal column order unless `column_mappings` is provided. |

### Optional

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | `int` | `0` | Rows per batch. `0` lets the server choose the optimal size. |
| `timeout` | `int` | `30` | BCP Operation timeout in seconds. |
| `column_mappings` | `List[str]` or `List[Tuple[int, str]]` | `None` | Column mapping specification (see [Column Mappings](#column-mappings) below). When `None`, columns are mapped by ordinal position. |
| `keep_identity` | `bool` | `False` | When `True`, identity values from the source data are preserved. Requires `IDENTITY_INSERT` permission. |
| `check_constraints` | `bool` | `False` | When `True`, CHECK and FOREIGN KEY constraints are enforced during the insert. |
| `table_lock` | `bool` | `False` | When table_lock=True, SQL Server acquires a table-level BULK UPDATE lock instead of row or page locks. This reduces locking overhead and can significantly improve bulk insert throughput, especially when the table is not being accessed concurrently. |
| `keep_nulls` | `bool` | `False` | When `True`, source `NULL` values are inserted as-is. When `False`, the column's default value is used instead. |
| `fire_triggers` | `bool` | `False` | When `True`, INSERT triggers defined on the target table are executed. |
| `use_internal_transaction` | `bool` | `False` | When `True`, each batch is committed in its own transaction, enabling partial-success scenarios. |

---

## Column Mappings

Column mappings control how columns in the source data line up with columns in the target table.

### Ordinal (Default)

When `column_mappings` is omitted, columns are mapped by position:

```
source index 0 → first table column
source index 1 → second table column
…
```

### Simple Format — `List[str]`

A list of destination column names. Position in the list equals the source index.

```python
column_mappings = ["UserID", "FirstName", "Email"]
# index 0 → UserID, index 1 → FirstName, index 2 → Email
```

### Advanced Format — `List[Tuple[int, str]]`

Explicit `(source_index, target_column_name)` tuples. Allows skipping or reordering columns.

```python
column_mappings = [
    (0, "UserID"),       # source[0] → UserID
    (1, "FirstName"),    # source[1] → FirstName
    (3, "Email"),        # source[3] → Email  (source[2] skipped)
]
```

---

## Return Value

A dictionary with operation metrics:

| Key | Type | Description |
|-----|------|-------------|
| `rows_copied` | `int` | Total number of rows successfully inserted. |
| `batch_count` | `int` | Number of batches processed. |
| `elapsed_time` | `float` | Total elapsed time for the bulk copy operation in seconds. |
| `rows_per_second` | `float` | Throughput in rows per second. |

```python
result = cursor.bulkcopy("Products", data)
# result == {'rows_copied': 3, 'batch_count': 1, 'elapsed_time': 0.12, 'rows_per_second': 25.0}
```

---

## Exceptions

| Exception | When |
|-----------|------|
| `ImportError` | `mssql_py_core` native library is not available. |
| `TypeError` | `data` is `None`, not iterable, or is a `str`/`bytes`; `batch_size` or `timeout` is not `int`. |
| `ValueError` | `table_name` is empty or not a string; `batch_size` is negative; `timeout` is not positive; `SERVER` parameter missing from connection string. |
| `RuntimeError` | Connection string is unavailable; Azure AD token acquisition fails. |

---

## Usage Examples

### 1. Basic Insert (Ordinal Mapping)

```python
data = [
    (1, "Alice", 100.5),
    (2, "Bob",   200.75),
    (3, "Charlie", 300.25),
]

result = cursor.bulkcopy("Sales", data)
assert result["rows_copied"] == 3
```

### 2. Named Column Mapping

```python
# Table: Users (UserID INT, FirstName VARCHAR, LastName VARCHAR, Email VARCHAR)
data = [
    (1, "Alice", "Smith",  "alice@example.com"),
    (2, "Bob",   "Jones",  "bob@example.com"),
]

result = cursor.bulkcopy(
    "Users",
    data,
    column_mappings=["UserID", "FirstName", "LastName", "Email"],
)
```

### 3. Selective Column Mapping (Skip & Reorder)

```python
# Source rows have 4 values; only 3 are inserted (index 2 is skipped)
data = [
    (1, "Alice", "SKIP_ME", "alice@example.com"),
    (2, "Bob",   "SKIP_ME", "bob@example.com"),
]

result = cursor.bulkcopy(
    "Users",
    data,
    column_mappings=[
        (0, "UserID"),
        (1, "FirstName"),
        (3, "Email"),       # index 2 intentionally skipped
    ],
)
```

### 4. Preserving Identity Values

```python
# Table: Users (UserID INT IDENTITY PRIMARY KEY, Name VARCHAR)
data = [
    (100, "Alice"),
    (200, "Bob"),
]

result = cursor.bulkcopy(
    "Users",
    data,
    keep_identity=True,
    column_mappings=["UserID", "Name"],
)
```

### 5. High-Throughput Import with Performance Options

```python
def generate_rows():
    """Memory-efficient generator for large datasets."""
    for i in range(1_000_000):
        yield (i, f"User{i}", f"user{i}@example.com")

result = cursor.bulkcopy(
    "Users",
    generate_rows(),
    batch_size=10_000,
    timeout=300,
    table_lock=True,
    use_internal_transaction=True,
)

print(f"{result['rows_copied']} rows in {result['elapsed_time']:.1f}s "
      f"({result['rows_copied'] / result['elapsed_time']:.0f} rows/sec)")
```

### 6. Fully Qualified Table Name (Cross-Database)

```python
# Useful when DATABASE is omitted from the connection string
result = cursor.bulkcopy("[ProductionDB].[dbo].[Users]", data)
```

### 7. Connection with SERVER Synonym

```python
# All three keywords resolve identically:
conn = mssql_python.connect("ADDR=localhost,1433;DATABASE=TestDB;UID=sa;PWD=pass;")
cursor = conn.cursor()
result = cursor.bulkcopy("Users", data)
```

### 8. Comprehensive Error Handling

```python
try:
    result = cursor.bulkcopy("Users", data, batch_size=1000, timeout=120)
    print(f"Copied {result['rows_copied']} rows")

except ImportError:
    print("mssql_py_core is not installed — bulkcopy unavailable")

except TypeError as e:
    print(f"Bad data format: {e}")

except ValueError as e:
    print(f"Invalid parameter: {e}")

except RuntimeError as e:
    print(f"Connection / auth error: {e}")
```

---

## Performance Tips

| Tip | Rationale |
|-----|-----------|
| Use generators instead of lists for large datasets | Avoids loading the entire dataset into memory. |
| Set `table_lock=True` for exclusive inserts | Eliminates row-lock overhead. |
| Leave `batch_size=0` unless tuning | Server-optimal batching is generally the best default. |
| Set `check_constraints=False` and `fire_triggers=False` | Reduces per-row overhead; validate constraints after load. |
| Increase `timeout` proportionally to data volume | Prevents premature timeout for million-row imports. |
| Use `use_internal_transaction=True` for partial-commit semantics | Each batch commits independently — useful when you want to keep rows already loaded even if a later batch fails. |

---
