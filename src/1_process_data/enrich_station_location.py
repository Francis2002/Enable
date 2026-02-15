import duckdb
import pandas as pd
import os

# Configuration
STATIONS_CSV = "../../data/MOBIe_Lista_de_postos_corrected.csv"
PRE_ML_DB = "../../data/pre_ml.db"

def add_coordinates_table():
    print(f"Adding 'coordinates' table to {PRE_ML_DB}...")
    
    if not os.path.exists(STATIONS_CSV):
        print(f"Error: {STATIONS_CSV} not found.")
        return

    # 1. Load Data
    df = pd.read_csv(STATIONS_CSV, sep=';', encoding='utf-8', converters={'ID': str})
    
    # Columns requested
    # Note: We rename 'ID' to 'station_id' to match our enrichment table for easy joining later.
    cols_map = {
        'ID': 'station_id',
        'CIDADE': 'CIDADE',
        'MORADA': 'MORADA',
        'LATITUDE': 'LATITUDE',
        'LONGITUDE': 'LONGITUDE'
    }
    
    # Check if all columns exist
    for col in cols_map.keys():
        if col not in df.columns:
            print(f"Error: Column {col} missing in CSV.")
            return

    # 2. Extract and Deduplicate by station location
    stations = df[list(cols_map.keys())].rename(columns=cols_map)
    stations_unique = stations.drop_duplicates(subset=['station_id', 'LATITUDE', 'LONGITUDE'])
    
    print(f"Unique stations to add: {len(stations_unique)}")

    # 3. Add to pre_ml.db (This will only replace the 'coordinates' table if it exists)
    con = duckdb.connect(PRE_ML_DB)
    con.execute("CREATE OR REPLACE TABLE coordinates AS SELECT * FROM stations_unique")
    
    # Verification
    tables = con.execute("SHOW TABLES").df()
    print("\nTables now in pre_ml.db:")
    print(tables)
    
    count = con.execute("SELECT count(*) FROM coordinates").fetchone()[0]
    print(f"\nSuccess. 'coordinates' table ready with {count} rows.")
    con.close()

if __name__ == "__main__":
    add_coordinates_table()
