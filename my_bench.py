# Custom profiler script — conn and cursor are injected
# Usage: python -m profiler --script my_bench.py

cursor.execute("SELECT TOP 1000 * FROM sys.objects CROSS JOIN sys.columns")
rows = cursor.fetchall()
print(f"  Fetched {len(rows)} rows")
