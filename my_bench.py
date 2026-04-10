# Cursor reuse benchmark — matches github issue pattern
# Usage: python -m profiler --script my_bench.py
# Usage: python -m profiler --timeline --script my_bench.py

# Setup: create a wide table (15 columns, like SalesLT.Customer)
cursor.execute(
    """
IF OBJECT_ID('tempdb..#wide_table', 'U') IS NOT NULL DROP TABLE #wide_table;
CREATE TABLE #wide_table (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName NVARCHAR(50), LastName NVARCHAR(50),
    CompanyName NVARCHAR(128), EmailAddress NVARCHAR(128),
    Phone NVARCHAR(25), PasswordHash NVARCHAR(128),
    PasswordSalt NVARCHAR(10), Title NVARCHAR(8),
    Suffix NVARCHAR(10), MiddleName NVARCHAR(50),
    SalesPerson NVARCHAR(256), ModifiedDate DATETIME2,
    rowguid NVARCHAR(36), NameStyle BIT
)
"""
)
conn.commit()

cursor.execute(
    """
INSERT INTO #wide_table
    (FirstName, LastName, CompanyName, EmailAddress, Phone,
     PasswordHash, PasswordSalt, Title, Suffix, MiddleName,
     SalesPerson, ModifiedDate, rowguid, NameStyle)
VALUES
    ('Orlando', 'Gee', 'A Bike Store', 'orlando0@adventure-works.com', '245-555-0173',
     'L/Rlwxzp4w7RWmEgXX+/A7cXaePEPcp+KwQhl2fJL7w=', '1KjXYs4=',
     'Mr.', NULL, 'N.', 'adventure-works\\pamela0',
     '2024-06-15 00:00:00', 'CBA964E0-A478-4EFF-B9D1-32F23A6F1F68', 0)
"""
)
conn.commit()

ITERATIONS = 1
QUERY = "SELECT TOP 1 * FROM #wide_table"

print(f"  Cursor reuse: {ITERATIONS}x [{QUERY}]")
print(f"  15 columns, simulating github issue pattern")

for _ in range(ITERATIONS):
    cursor.execute(QUERY)
    cursor.fetchall()
