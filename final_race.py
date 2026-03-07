import mssql_python
import threading
import time

# מחרוזת החיבור שלך (בלי MARS, כדי למנוע Overhead מיותר)
conn_str = "Server=jr-bidb19,1433;Database=BI_PCT;Trusted_Connection=Yes;TrustServerCertificate=Yes;"

def run_query(thread_id):
    try:
        start = time.time()
        # יצירת חיבור נפרד לכל ת'רד כדי לנצל את ה-GIL Release
        conn = mssql_python.connect(conn_str)
        cursor = conn.cursor()
        
        # פקודה שגורמת ל-DB להמתין 2 שניות
        cursor.execute("WAITFOR DELAY '00:00:02'")
        
        end = time.time()
        print(f"Thread {thread_id}: Took {end - start:.2f} seconds")
    except Exception as e:
        print(f"Thread {thread_id} Failed: {e}")

print("--- Starting The Final Race (4 Parallel Threads) ---")
total_start = time.time()

threads = []
for i in range(4):
    t = threading.Thread(target=run_query, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

total_end = time.time()
print("-" * 50)
print(f"TOTAL EXECUTION TIME: {total_end - total_start:.2f} seconds")
print("-" * 50)

if (total_end - total_start) < 3:
    print("STATUS: BULLETPROOF. Your driver is running in true parallel.")
else:
    print("STATUS: TRASH. Something is still locking the GIL.")