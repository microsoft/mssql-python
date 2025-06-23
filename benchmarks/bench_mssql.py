import atexit
import pyodbc
import os
import sys
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mssql_python
print(mssql_python.__file__)
import time

CONNECTION_STRING = "Driver={ODBC Driver 18 for SQL Server};" + os.environ.get('DB_CONNECTION_STRING')
pyodbc.pooling = True
mssql_python.pooling(max_size=100, idle_timeout=600)

def setup_database():
    print("Setting up the database...")
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    try:
        # Drop permanent tables and stored procedure if they exist
        print("Dropping existing tables and stored procedure if they exist...")
        cursor.execute("""
            IF OBJECT_ID('perfbenchmark_child_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_child_table;
            IF OBJECT_ID('perfbenchmark_parent_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_parent_table;
            IF OBJECT_ID('perfbenchmark_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_table;
            IF OBJECT_ID('perfbenchmark_stored_procedure', 'P') IS NOT NULL DROP PROCEDURE perfbenchmark_stored_procedure;
        """)

        # Create permanent tables with new names
        print("Creating tables...")
        cursor.execute("""
            CREATE TABLE perfbenchmark_table (
                id INT,
                name NVARCHAR(50),
                age INT
            )
        """)

        cursor.execute("""
            CREATE TABLE perfbenchmark_parent_table (
                id INT PRIMARY KEY,
                name NVARCHAR(50)
            )
        """)

        cursor.execute("""
            CREATE TABLE perfbenchmark_child_table (
                id INT PRIMARY KEY,
                parent_id INT,
                description NVARCHAR(100),
                FOREIGN KEY (parent_id) REFERENCES perfbenchmark_parent_table(id)
            )
        """)

        # Create stored procedure
        print("Creating stored procedure...")
        cursor.execute("""
            CREATE PROCEDURE perfbenchmark_stored_procedure
            AS
            BEGIN
                SELECT * FROM perfbenchmark_table;
            END
        """)

        conn.commit()
        print("Database setup completed.")
    finally:
        cursor.close()
        conn.close()

# Call setup_database to ensure permanent tables and procedure are recreated
setup_database()

def cleanup_database():
    print("Cleaning up the database...")
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    try:
        # Drop tables and stored procedure after benchmarks
        print("Dropping tables and stored procedure...")
        cursor.execute("""
            IF OBJECT_ID('perfbenchmark_child_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_child_table;
            IF OBJECT_ID('perfbenchmark_parent_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_parent_table;
            IF OBJECT_ID('perfbenchmark_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_table;
            IF OBJECT_ID('perfbenchmark_stored_procedure', 'P') IS NOT NULL DROP PROCEDURE perfbenchmark_stored_procedure;
        """)
        conn.commit()
        print("Database cleanup completed.")
    finally:
        cursor.close()
        conn.close()

# Register cleanup function to run at exit
atexit.register(cleanup_database)

# Define benchmark functions for pyodbc
def bench_select_pyodbc():
    print("Running SELECT benchmark with pyodbc...")
    # start = time.perf_counter()
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM perfbenchmark_table")
    cursor.fetchall()
    cursor.close()
    conn.close()
    print("SELECT benchmark with pyodbc completed.")
    # duration = time.perf_counter() - start
    # print(f"pyodbc SELECT completed in {duration:.4f} seconds.")

def bench_insert_pyodbc():
    print("Running INSERT benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO perfbenchmark_table (id, name, age) VALUES (1, 'John Doe', 30)")
        conn.commit()
        cursor.close()
        conn.close()
        print("INSERT benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during INSERT benchmark: {e}")

def bench_update_pyodbc():
    print("Running UPDATE benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("UPDATE perfbenchmark_table SET age = 31 WHERE id = 1")
        conn.commit()
        cursor.close()
        conn.close()
        print("UPDATE benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during UPDATE benchmark: {e}")

def bench_delete_pyodbc():
    print("Running DELETE benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM perfbenchmark_table WHERE id = 1")
        conn.commit()
        cursor.close()
        conn.close()
        print("DELETE benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during DELETE benchmark: {e}")

def bench_complex_query_pyodbc():
    print("Running COMPLEX QUERY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT name, COUNT(*)
            FROM perfbenchmark_table
            GROUP BY name
            HAVING COUNT(*) > 1
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("COMPLEX QUERY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during COMPLEX QUERY benchmark: {e}")

def bench_multiple_connections_pyodbc():
    print("Running MULTIPLE CONNECTIONS benchmark with pyodbc...")
    try:
        connections = []
        for _ in range(10):
            conn = pyodbc.connect(CONNECTION_STRING)
            connections.append(conn)
        
        for conn in connections:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM perfbenchmark_table")
            cursor.fetchall()
            cursor.close()
        
        for conn in connections:
            conn.close()
        print("MULTIPLE CONNECTIONS benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during MULTIPLE CONNECTIONS benchmark: {e}")

def bench_1000_connections_pyodbc():
    print("Running 1000 CONNECTIONS benchmark with pyodbc...")
    try:
        threads = []
        for _ in range(1000):
            thread = threading.Thread(target=lambda: pyodbc.connect(CONNECTION_STRING).close())
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        print("1000 CONNECTIONS benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during 1000 CONNECTIONS benchmark: {e}")

def bench_1000_inserts_pyodbc():
    print("Running 1000 INSERTS benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        for i in range(1000):
            cursor.execute("INSERT INTO perfbenchmark_table (id, name, age) VALUES (?, 'John Doe', 30)", i)
        conn.commit()
        cursor.close()
        conn.close()
        print("1000 INSERTS benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during 1000 INSERTS benchmark: {e}")

def bench_fetchone_pyodbc():
    print("Running FETCHONE benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("FETCHONE benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during FETCHONE benchmark: {e}")

def bench_fetchmany_pyodbc():
    print("Running FETCHMANY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        cursor.fetchmany(10)
        cursor.close()
        conn.close()
        print("FETCHMANY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during FETCHMANY benchmark: {e}")

def bench_executemany_pyodbc():
    print("Running EXECUTEMANY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        data = [(i, 'John Doe', 30) for i in range(1000)]
        cursor.executemany("INSERT INTO perfbenchmark_table (id, name, age) VALUES (?, ?, ?)", data)
        conn.commit()
        cursor.close()
        conn.close()
        print("EXECUTEMANY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during EXECUTEMANY benchmark: {e}")

def bench_stored_procedure_pyodbc():
    print("Running STORED PROCEDURE benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("{CALL perfbenchmark_stored_procedure}")
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("STORED PROCEDURE benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during STORED PROCEDURE benchmark: {e}")

def bench_nested_query_pyodbc():
    print("Running NESTED QUERY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM (
                SELECT name, age FROM perfbenchmark_table
            ) AS subquery
            WHERE age > 25
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("NESTED QUERY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during NESTED QUERY benchmark: {e}")

def bench_join_query_pyodbc():
    print("Running JOIN QUERY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT a.name, b.age
            FROM perfbenchmark_table a
            JOIN perfbenchmark_table b ON a.id = b.id
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("JOIN QUERY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during JOIN QUERY benchmark: {e}")

def bench_transaction_pyodbc():
    print("Running TRANSACTION benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("INSERT INTO perfbenchmark_table (id, name, age) VALUES (1, 'John Doe', 30)")
            cursor.execute("UPDATE perfbenchmark_table SET age = 31 WHERE id = 1")
            cursor.execute("DELETE FROM perfbenchmark_table WHERE id = 1")
            cursor.execute("COMMIT")
        except:
            cursor.execute("ROLLBACK")
        cursor.close()
        conn.close()
        print("TRANSACTION benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during TRANSACTION benchmark: {e}")

def bench_large_data_set_pyodbc():
    print("Running LARGE DATA SET benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        while cursor.fetchone():
            pass
        cursor.close()
        conn.close()
        print("LARGE DATA SET benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during LARGE DATA SET benchmark: {e}")

def bench_insert_with_foreign_key_pyodbc():
    print("Running INSERT WITH FOREIGN KEY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO perfbenchmark_parent_table (id, name) VALUES (1, 'Parent 1')")
        cursor.execute("INSERT INTO perfbenchmark_child_table (id, parent_id, description) VALUES (1, 1, 'Child 1')")
        conn.commit()
        cursor.close()
        conn.close()
        print("INSERT WITH FOREIGN KEY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during INSERT WITH FOREIGN KEY benchmark: {e}")

def bench_join_with_foreign_key_pyodbc():
    print("Running JOIN WITH FOREIGN KEY benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT p.name, c.description
            FROM perfbenchmark_parent_table p
            JOIN perfbenchmark_child_table c ON p.id = c.parent_id
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("JOIN WITH FOREIGN KEY benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during JOIN WITH FOREIGN KEY benchmark: {e}")

def bench_update_with_join_pyodbc():
    print("Running UPDATE WITH JOIN benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""UPDATE perfbenchmark_child_table
            SET description = 'Updated Child 1'
            FROM perfbenchmark_child_table c
            JOIN perfbenchmark_parent_table p ON c.parent_id = p.id
            WHERE p.name = 'Parent 1'
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("UPDATE WITH JOIN benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during UPDATE WITH JOIN benchmark: {e}")

def bench_delete_with_join_pyodbc():
    print("Running DELETE WITH JOIN benchmark with pyodbc...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""DELETE c
            FROM perfbenchmark_child_table c
            JOIN perfbenchmark_parent_table p ON c.parent_id = p.id
            WHERE p.name = 'Parent 1'
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("DELETE WITH JOIN benchmark with pyodbc completed.")
    except Exception as e:
        print(f"Error during DELETE WITH JOIN benchmark: {e}")

# Define benchmark functions for mssql_python
def bench_select_mssql_python():
    print("Running SELECT benchmark with mssql_python...")
    try:
        # start = time.perf_counter()
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        cursor.fetchall()
        cursor.close()
        conn.close()
        # duration = time.perf_counter() - start
        # print(f"pyodbc SELECT completed in {duration:.4f} seconds.")
        print("SELECT benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during SELECT benchmark with mssql_python: {e}")

def bench_insert_mssql_python():
    print("Running INSERT benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO perfbenchmark_table (id, name, age) VALUES (1, 'John Doe', 30)")
        conn.commit()
        cursor.close()
        conn.close()
        print("INSERT benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during INSERT benchmark with mssql_python: {e}")

def bench_update_mssql_python():
    print("Running UPDATE benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("UPDATE perfbenchmark_table SET age = 31 WHERE id = 1")
        conn.commit()
        cursor.close()
        conn.close()
        print("UPDATE benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during UPDATE benchmark with mssql_python: {e}")

def bench_delete_mssql_python():
    print("Running DELETE benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM perfbenchmark_table WHERE id = 1")
        conn.commit()
        cursor.close()
        conn.close()
        print("DELETE benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during DELETE benchmark with mssql_python: {e}")

def bench_complex_query_mssql_python():
    print("Running COMPLEX QUERY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT name, COUNT(*)
            FROM perfbenchmark_table
            GROUP BY name
            HAVING COUNT(*) > 1
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("COMPLEX QUERY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during COMPLEX QUERY benchmark with mssql_python: {e}")

def bench_multiple_connections_mssql_python():
    print("Running MULTIPLE CONNECTIONS benchmark with mssql_python...")
    try:
        connections = []
        for _ in range(10):
            conn = mssql_python.connect(CONNECTION_STRING)
            connections.append(conn)
        
        for conn in connections:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM perfbenchmark_table")
            cursor.fetchall()
            cursor.close()
        
        for conn in connections:
            conn.close()
        print("MULTIPLE CONNECTIONS benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during MULTIPLE CONNECTIONS benchmark with mssql_python: {e}")

def bench_1000_connections_mssql_python():
    print("Running 1000 CONNECTIONS benchmark with mssql_python...")
    try:
        threads = []
        for _ in range(1000):
            thread = threading.Thread(target=lambda: mssql_python.connect(CONNECTION_STRING).close())
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        print("1000 CONNECTIONS benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during 1000 CONNECTIONS benchmark with mssql_python: {e}")

def bench_1000_inserts_mssql_python():
    print("Running 1000 INSERTS benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        for i in range(1000):
            cursor.execute("INSERT INTO perfbenchmark_table (id, name, age) VALUES (?, 'John Doe', 30)", i)
        conn.commit()
        cursor.close()
        conn.close()
        print("1000 INSERTS benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during 1000 INSERTS benchmark with mssql_python: {e}")

def bench_fetchone_mssql_python():
    print("Running FETCHONE benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("FETCHONE benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during FETCHONE benchmark with mssql_python: {e}")

def bench_fetchmany_mssql_python():
    print("Running FETCHMANY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        cursor.fetchmany(10)
        cursor.close()
        conn.close()
        print("FETCHMANY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during FETCHMANY benchmark with mssql_python: {e}")

def bench_executemany_mssql_python():
    print("Running EXECUTEMANY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        data = [(i, 'John Doe', 30) for i in range(1000)]
        cursor.executemany("INSERT INTO perfbenchmark_table (id, name, age) VALUES (?, ?, ?)", data)
        conn.commit()
        cursor.close()
        conn.close()
        print("EXECUTEMANY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during EXECUTEMANY benchmark with mssql_python: {e}")

def bench_stored_procedure_mssql_python():
    print("Running STORED PROCEDURE benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("{CALL perfbenchmark_stored_procedure}")
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("STORED PROCEDURE benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during STORED PROCEDURE benchmark with mssql_python: {e}")

def bench_nested_query_mssql_python():
    print("Running NESTED QUERY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM (
                SELECT name, age FROM perfbenchmark_table
            ) AS subquery
            WHERE age > 25
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("NESTED QUERY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during NESTED QUERY benchmark with mssql_python: {e}")

def bench_join_query_mssql_python():
    print("Running JOIN QUERY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT a.name, b.age
            FROM perfbenchmark_table a
            JOIN perfbenchmark_table b ON a.id = b.id
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("JOIN QUERY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during JOIN QUERY benchmark with mssql_python: {e}")

def bench_transaction_mssql_python():
    print("Running TRANSACTION benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("INSERT INTO perfbenchmark_table (id, name, age) VALUES (1, 'John Doe', 30)")
            cursor.execute("UPDATE perfbenchmark_table SET age = 31 WHERE id = 1")
            cursor.execute("DELETE FROM perfbenchmark_table WHERE id = 1")
            cursor.execute("COMMIT")
        except:
            cursor.execute("ROLLBACK")
        cursor.close()
        conn.close()
        print("TRANSACTION benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during TRANSACTION benchmark with mssql_python: {e}")

def bench_large_data_set_mssql_python():
    print("Running LARGE DATA SET benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM perfbenchmark_table")
        while cursor.fetchone():
            pass
        cursor.close()
        conn.close()
        print("LARGE DATA SET benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during LARGE DATA SET benchmark with mssql_python: {e}")

def bench_insert_with_foreign_key_mssql_python():
    print("Running INSERT WITH FOREIGN KEY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO perfbenchmark_parent_table (id, name) VALUES (1, 'Parent 1')")
        cursor.execute("INSERT INTO perfbenchmark_child_table (id, parent_id, description) VALUES (1, 1, 'Child 1')")
        conn.commit()
        cursor.close()
        conn.close()
        print("INSERT WITH FOREIGN KEY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during INSERT WITH FOREIGN KEY benchmark with mssql_python: {e}")

def bench_join_with_foreign_key_mssql_python():
    print("Running JOIN WITH FOREIGN KEY benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""SELECT p.name, c.description
            FROM perfbenchmark_parent_table p
            JOIN perfbenchmark_child_table c ON p.id = c.parent_id
        """)
        cursor.fetchall()
        cursor.close()
        conn.close()
        print("JOIN WITH FOREIGN KEY benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during JOIN WITH FOREIGN KEY benchmark with mssql_python: {e}")

def bench_update_with_join_mssql_python():
    print("Running UPDATE WITH JOIN benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""UPDATE perfbenchmark_child_table
            SET description = 'Updated Child 1'
            FROM perfbenchmark_child_table c
            JOIN perfbenchmark_parent_table p ON c.parent_id = p.id
            WHERE p.name = 'Parent 1'
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("UPDATE WITH JOIN benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during UPDATE WITH JOIN benchmark with mssql_python: {e}")

def bench_delete_with_join_mssql_python():
    print("Running DELETE WITH JOIN benchmark with mssql_python...")
    try:
        conn = mssql_python.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("""DELETE c
            FROM perfbenchmark_child_table c
            JOIN perfbenchmark_parent_table p ON c.parent_id = p.id
            WHERE p.name = 'Parent 1'
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("DELETE WITH JOIN benchmark with mssql_python completed.")
    except Exception as e:
        print(f"Error during DELETE WITH JOIN benchmark with mssql_python: {e}")

# Define benchmarks
__benchmarks__ = [
    (bench_select_pyodbc, bench_select_mssql_python, "SELECT operation"),
    (bench_insert_pyodbc, bench_insert_mssql_python, "INSERT operation"),
    (bench_update_pyodbc, bench_update_mssql_python, "UPDATE operation"),
    # (bench_delete_pyodbc, bench_delete_mssql_python, "DELETE operation"),
    # (bench_complex_query_pyodbc, bench_complex_query_mssql_python, "Complex query operation"),
    # (bench_multiple_connections_pyodbc, bench_multiple_connections_mssql_python, "Multiple connections operation"),
    # (bench_fetchone_pyodbc, bench_fetchone_mssql_python, "Fetch one operation"),
    # (bench_fetchmany_pyodbc, bench_fetchmany_mssql_python, "Fetch many operation"),
    # (bench_executemany_pyodbc, bench_executemany_mssql_python, "Execute many operation"),
    # (bench_stored_procedure_pyodbc, bench_stored_procedure_mssql_python, "Stored procedure operation"),
    # (bench_1000_connections_pyodbc, bench_1000_connections_mssql_python, "1000 connections operation"),
    # (bench_1000_inserts_pyodbc, bench_1000_inserts_mssql_python, "1000 inserts operation"),
    # (bench_nested_query_pyodbc, bench_nested_query_mssql_python, "Nested query operation"),
    # (bench_join_query_pyodbc, bench_join_query_mssql_python, "Join query operation"),
    # (bench_transaction_pyodbc, bench_transaction_mssql_python, "Transaction operation"),
    # (bench_large_data_set_pyodbc, bench_large_data_set_mssql_python, "Large data set operation"),
    # # (bench_insert_with_foreign_key_pyodbc, bench_insert_with_foreign_key_mssql_python, "Insert with foreign key operation"),
    # # (bench_join_with_foreign_key_pyodbc, bench_join_with_foreign_key_mssql_python, "Join with foreign key operation"),
    # (bench_update_with_join_pyodbc, bench_update_with_join_mssql_python, "Update with join operation"),
    # (bench_delete_with_join_pyodbc, bench_delete_with_join_mssql_python, "Delete with join operation"),
]