import mssql_python
import threading
import time

# המחרוזת המעודכנת שלך
conn_str = "Server=tcp:jr-bidb22-sec-,1433;Database=BI_PCT;Trusted_Connection=Yes;TrustServerCertificate=Yes;"

def run_test():
    try:
        # כאן הדרייבר ה-Native שקימפלת נכנס לפעולה
        conn = mssql_python.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION; WAITFOR DELAY '00:00:02'")
        print(f"Thread {threading.get_ident()} done.")
    except Exception as e:
        print(f"Connection failed: {e}")

# הרצה של 4 ת'רדים במקביל
start = time.time()
threads = [threading.Thread(target=run_test) for _ in range(4)]

for t in threads: t.start()
for t in threads: t.join()

print(f"--- TOTAL TIME: {time.time() - start:.2f} seconds ---")