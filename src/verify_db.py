import duckdb
import os

DB_PATH = "../data/osm_analysis.db"

if not os.path.exists(DB_PATH):
    print(f"Database not found at {DB_PATH}")
    exit(1)

con = duckdb.connect(DB_PATH)
try:
    print("--- Existing Tables ---")
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    print(table_names)
    
    print("\n--- Row Counts ---")
    for table in table_names:
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count}")

except Exception as e:
    print(f"Error inspecting DB: {e}")
finally:
    con.close()
