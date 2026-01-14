import geopandas as gpd
import duckdb
import pandas as pd
import re
import os

gpkg_path = "../data/GRID1K21_CONT.gpkg"
db_path = "../data/osm_analysis.db"

def normalize_census_id(official_id):
    """
    Transforms official INE ID to our standard format.
    PT_CRS3035RES1000mN1729000E2730000 -> RES1kmN1729E2730
    """
    if not isinstance(official_id, str):
        return None
    match = re.search(r'N(\d+)E(\d+)', official_id)
    if match:
        n = int(match.group(1)) // 1000
        e = int(match.group(2)) // 1000
        return f"RES1kmN{n}E{e}"
    return None

def process_census():
    if not os.path.exists(gpkg_path):
        print(f"Error: {gpkg_path} not found.")
        return

    print(f"Loading {gpkg_path}...")
    # We don't need the geometry for this table, just the attributes
    df = gpd.read_file(gpkg_path, ignore_geometry=True)
    
    print(f"Normalizing IDs for {len(df)} rows...")
    df['cell_id'] = df['GRD_ID2021_OFICIAL'].apply(normalize_census_id)
    
    # Select and rename columns for readability
    rename_map = {
        'N_INDIVIDUOS': 'pop_total',
        'N_INDIVIDUOS_H': 'pop_male',
        'N_INDIVIDUOS_M': 'pop_female',
        'N_INDIVIDUOS_0_14': 'pop_0_14',
        'N_INDIVIDUOS_15_24': 'pop_15_24',
        'N_INDIVIDUOS_25_64': 'pop_25_64',
        'N_INDIVIDUOS_65_OU_MAIS': 'pop_65_plus',
        'N_EDIFICIOS_CLASSICOS': 'buildings_total',
        'N_ALOJAMENTOS_TOTAL': 'households_total',
        'N_AGREGADOS_DOMESTICOS_PRIVADO': 'private_households'
    }
    
    # Filter columns that exist
    cols_to_keep = ['cell_id'] + [c for c in rename_map.keys() if c in df.columns]
    df_clean = df[cols_to_keep].copy()
    df_clean = df_clean.rename(columns=rename_map)
    
    # Fill missing values with 0
    numeric_cols = df_clean.select_dtypes(include=['number']).columns
    df_clean[numeric_cols] = df_clean[numeric_cols].fillna(0)
    
    print("Connecting to DuckDB...")
    con = duckdb.connect(db_path)
    con.register("tmp_census", df_clean)
    
    print("Creating census_stats table...")
    con.execute("DROP TABLE IF EXISTS census_stats")
    con.execute("CREATE TABLE census_stats AS SELECT * FROM tmp_census")
    
    count = con.execute("SELECT count(*) FROM census_stats").fetchone()[0]
    print(f"Success: Stored {count} rows in census_stats.")
    
    con.close()

if __name__ == "__main__":
    process_census()
