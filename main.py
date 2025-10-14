from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal
import time
import pyodbc
from typing import ClassVar
import string

# setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
# conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
conn_str= "DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:sqlsumitsardb.database.windows.net,1433;Database=AdventureWorks2022;Uid=sqladmin;Pwd=SoftMicro$123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"



# conn.autocommit = True
COMPLEX_JOIN_AGGREGATION = """
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
"""

#query 2
LARGE_DATASET = """
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
"""
# Query 3: Very Large Dataset with CROSS JOIN
VERY_LARGE_DATASET = """
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
    """
    # Query 4: CTE
SUBQUERY_WITH_CTE = """
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


print("Using pyodbc now")
start_time = time.perf_counter()
conn = pyodbc.connect(conn_str)

# conn.autocommit = True

cursor = conn.cursor()
cursor.execute(COMPLEX_JOIN_AGGREGATION)

rows = cursor.fetchall()

# for row in rows:
#     print(f"Database ID: {row[0]}, Name: {row[1]}")

end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in pyodbc for query 1: {elapsed_time:.4f} seconds")

start_time = time.perf_counter()
cursor.execute(LARGE_DATASET)
rows = cursor.fetchall()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in pyodbc for query 2: {elapsed_time:.4f} seconds")

start_time = time.perf_counter()
cursor.execute(VERY_LARGE_DATASET)
rows = cursor.fetchall()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in pyodbc for query 3: {elapsed_time:.4f} seconds")

start_time = time.perf_counter()
cursor.execute(SUBQUERY_WITH_CTE)
rows = cursor.fetchall()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in pyodbc for query 4: {elapsed_time:.4f} seconds")

cursor.close()
conn.close()

print("Using mssql-python now")
start_time = time.perf_counter()
conn = connect(conn_str)
cursor = conn.cursor()  # Performance optimizations are now enabled by default
cursor.execute(COMPLEX_JOIN_AGGREGATION)        
rows = cursor.fetchall()

end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in mssql-python (optimized) for query 1: {elapsed_time:.4f} seconds")

start_time = time.perf_counter()
cursor.execute(LARGE_DATASET)
rows = cursor.fetchall()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in mssql-python (optimized) for query 2: {elapsed_time:.4f} seconds")

start_time = time.perf_counter()
cursor.execute(VERY_LARGE_DATASET)
rows = cursor.fetchall()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in mssql-python (optimized) for query 3: {elapsed_time:.4f} seconds")

start_time = time.perf_counter()
cursor.execute(SUBQUERY_WITH_CTE)
rows = cursor.fetchall()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Elapsed time in mssql-python (optimized) for query 4: {elapsed_time:.4f} seconds")

cursor.close()
conn.close()