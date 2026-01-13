import duckdb
import pandas as pd
import sys

db_path = "osm_analysis.db"

def inspect():
    try:
        con = duckdb.connect(db_path)
        con.execute("LOAD spatial;")
        
        tables = con.execute("SHOW TABLES").df()
        if tables.empty:
            print("The database is empty.")
            return

        print("--- Database Tables ---")
        print(tables)
        print("\n")

        for table in tables['name']:
            print(f"--- Table: {table} ---")
            # Get column info
            cols = con.execute(f"DESCRIBE {table}").df()
            print("Columns:")
            print(cols[['column_name', 'column_type']])
            
            # Show top 5 rows
            print("\nTop 5 rows:")
            df = con.execute(f"SELECT * FROM {table} LIMIT 5").df()
            print(df)
            print("-" * 30 + "\n")
            
        con.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect()
