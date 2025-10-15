#!/usr/bin/env python3
"""
Query Analysis Tool - Analyze data patterns, types, and volumes for performance optimization
"""

import pyodbc
import time
from collections import Counter

# Connection string
conn_str = "DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:sqlsumitsardb.database.windows.net,1433;Database=AdventureWorks2022;Uid=sqladmin;Pwd=SoftMicro$123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# All benchmark queries
queries = {
    "Query 1 (Complex Join Aggregation)": """
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    pc.Name AS Category,
    psc.Name AS Subcategory,
    COUNT(sod.SalesOrderDetailID) AS TotalOrders,
    SUM(sod.OrderQty) AS TotalQuantity,
    SUM(sod.LineTotal) AS TotalRevenue,
    AVG(sod.UnitPrice) AS AvgPrice
FROM Sales.SalesOrderDetail sod
INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
INNER JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
INNER JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
GROUP BY p.ProductID, p.Name, pc.Name, psc.Name
HAVING SUM(sod.LineTotal) > 10000
ORDER BY TotalRevenue DESC;
""",
    
    "Query 2 (Large Dataset)": """
SELECT
    soh.SalesOrderID,
    soh.OrderDate,
    soh.DueDate,
    soh.ShipDate,
    soh.Status,
    soh.SubTotal,
    soh.TaxAmt,
    soh.Freight,
    soh.TotalDue,
    c.CustomerID,
    p.FirstName,
    p.LastName,
    a.AddressLine1,
    a.City,
    sp.Name AS StateProvince,
    cr.Name AS Country
FROM Sales.SalesOrderHeader soh
INNER JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
INNER JOIN Person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
INNER JOIN Person.Address a ON bea.AddressID = a.AddressID
INNER JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
INNER JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
WHERE soh.OrderDate >= '2013-01-01';
""",

    "Query 3 (Very Large Dataset)": """
SELECT
    sod.SalesOrderID,
    sod.SalesOrderDetailID,
    sod.ProductID,
    sod.OrderQty,
    sod.UnitPrice,
    sod.LineTotal,
    p.Name AS ProductName,
    p.ProductNumber,
    p.Color,
    p.ListPrice,
    n1.number AS RowMultiplier1
FROM Sales.SalesOrderDetail sod
CROSS JOIN (SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
            FROM Sales.SalesOrderDetail) n1
INNER JOIN Production.Product p ON sod.ProductID = p.ProductID;
""",

    "Query 4 (CTE)": """
WITH SalesSummary AS (
    SELECT
        soh.SalesPersonID,
        YEAR(soh.OrderDate) AS OrderYear,
        SUM(soh.TotalDue) AS YearlyTotal
    FROM Sales.SalesOrderHeader soh
    WHERE soh.SalesPersonID IS NOT NULL
    GROUP BY soh.SalesPersonID, YEAR(soh.OrderDate)
),
RankedSales AS (
    SELECT
        SalesPersonID,
        OrderYear,
        YearlyTotal,
        RANK() OVER (PARTITION BY OrderYear ORDER BY YearlyTotal DESC) AS SalesRank
    FROM SalesSummary
)
SELECT
    rs.SalesPersonID,
    p.FirstName,
    p.LastName,
    rs.OrderYear,
    rs.YearlyTotal,
    rs.SalesRank
FROM RankedSales rs
INNER JOIN Person.Person p ON rs.SalesPersonID = p.BusinessEntityID
WHERE rs.SalesRank <= 10
ORDER BY rs.OrderYear DESC, rs.SalesRank;
"""
}

def analyze_query(name, query):
    """Analyze a single query's data characteristics"""
    print(f"\n=== {name} ===")
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Execute query and get metadata
        cursor.execute(query)
        
        # Get column information
        columns = cursor.description
        print(f"Columns: {len(columns)}")
        
        # Analyze column types
        column_types = Counter()
        for col in columns:
            column_types[col[1]] += 1
        
        print("Column type distribution:")
        for type_code, count in column_types.most_common():
            # Map pyodbc type codes to readable names
            type_names = {
                str: "STRING", 
                int: "INTEGER", 
                float: "DECIMAL", 
                bytes: "BINARY",
                type(None): "NULL"
            }
            type_name = type_names.get(type_code, str(type_code))
            print(f"  {type_name}: {count}")
        
        # Count rows and sample data sizes
        print("Fetching data for analysis...")
        start_time = time.perf_counter()
        rows = cursor.fetchall()
        fetch_time = time.perf_counter() - start_time
        
        print(f"Total rows: {len(rows)}")
        print(f"Fetch time: {fetch_time:.4f} seconds")
        
        if rows:
            # Sample first row for data analysis
            first_row = rows[0]
            print(f"Sample row length: {len(first_row)} columns")
            
            # Analyze data sizes by type
            string_lengths = []
            numeric_values = []
            
            for i, value in enumerate(first_row):
                col_name = columns[i][0]
                if isinstance(value, str):
                    string_lengths.append(len(value))
                    if len(value) > 50:  # Flag long strings
                        print(f"  Long string in {col_name}: {len(value)} chars")
                elif isinstance(value, (int, float)):
                    numeric_values.append(value)
                elif value is None:
                    pass  # NULL value
                else:
                    print(f"  Other type in {col_name}: {type(value)}")
            
            if string_lengths:
                print(f"String column stats: avg={sum(string_lengths)/len(string_lengths):.1f}, max={max(string_lengths)}")
            
            # Calculate approximate row size
            estimated_row_size = sum(
                len(str(val)) if val is not None else 0 
                for val in first_row
            )
            print(f"Estimated row size: {estimated_row_size} bytes")
            print(f"Estimated total data: {estimated_row_size * len(rows) / 1024 / 1024:.2f} MB")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error analyzing {name}: {e}")

def main():
    """Run analysis on all queries"""
    print("Analyzing benchmark queries for performance optimization...")
    print("=" * 70)
    
    for name, query in queries.items():
        analyze_query(name, query)
        print()

if __name__ == "__main__":
    main()