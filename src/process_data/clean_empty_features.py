import duckdb
import pandas as pd
import os

# Configuration
PRE_ML_DB = "../../data/pre_ml.db"

def clean_empty_features():
    print(f"Cleaning empty features from {PRE_ML_DB}...")
    
    if not os.path.exists(PRE_ML_DB):
        print("Error: Database not found.")
        return

    con = duckdb.connect(PRE_ML_DB)
    
    # We focus specifically on station_distances
    table_name = "station_distances"
    
    tables = con.execute("SHOW TABLES").df()
    if table_name not in tables['name'].values:
        print(f"Table {table_name} not found. Ensure enrichment has run first.")
        con.close()
        return

    # Load data for check
    df = con.execute(f"SELECT * FROM {table_name}").df()
    
    # Logic: Remove only if the ENTIRE column is empty/null-marker
    cols_to_drop = []
    
    for col in df.columns:
        if col == 'station_id': continue
        
        raw_data = df[col]
        
        is_empty = False
        if "_count" in col:
            is_empty = (raw_data == 0).all()
        elif "_dist" in col:
            is_empty = (raw_data == -1).all()
        else:
            is_empty = raw_data.isna().all()
            
        if is_empty:
            cols_to_drop.append(col)

    if not cols_to_drop:
        print("No empty columns found to drop.")
    else:
        print(f"Found {len(cols_to_drop)} empty columns out of {len(df.columns)}. Pruning...")
        
        for col in cols_to_drop:
            try:
                # Use quotes for columns with special characters
                con.execute(f'ALTER TABLE {table_name} DROP COLUMN "{col}"')
            except Exception as e:
                print(f"  ! Error dropping {col}: {e}")

        remaining_count = len(df.columns) - len(cols_to_drop)
        print(f"Success. Table now has {remaining_count} columns.")

    con.close()

if __name__ == "__main__":
    clean_empty_features()
