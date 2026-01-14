import duckdb
import pandas as pd
import sys
import os

db_path = "data/chargers.db"

def inspect_chargers():
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return

    try:
        con = duckdb.connect(db_path)
        tables = con.execute("SHOW TABLES").df()
        
        print(f"\n--- Chargers Database: {db_path} ---")
        print(tables)

        for table in tables['name']:
            print(f"\n--- Table: {table} ---")
            count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"Total Rows: {count}")
            
            cols = con.execute(f"DESCRIBE {table}").df()
            print("Columns:")
            print(cols[['column_name', 'column_type']])
            
            print("\nTop 5 rows:")
            # Use col name string formatting to avoid issues with large column counts
            df = con.execute(f"SELECT * FROM {table} LIMIT 5").df()
            print(df)
            print("-" * 30)

        con.close()
    except Exception as e:
        print(f"Error inspecting database: {e}")

if __name__ == "__main__":
    inspect_chargers()
