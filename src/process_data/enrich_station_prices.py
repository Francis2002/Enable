import duckdb
import pandas as pd
import os

# Configuration
TARIFAS_CSV = "../../data/MOBIE_Tarifas.csv"
PRE_ML_DB = "../../data/pre_ml.db"

def enrich_prices():
    print("Starting Station Prices Enrichment...")
    
    if not os.path.exists(TARIFAS_CSV):
        print(f"Error: {TARIFAS_CSV} not found.")
        return

    # Load Tariffs CSV
    print(f"Loading {TARIFAS_CSV}...")
    df_tarifas = pd.read_csv(TARIFAS_CSV, sep=';', encoding='utf-8-sig')
    
    # Select and rename columns as requested
    # Request: station_ID (from ID), UID_TOMADA, TIPO_TARIFARIO, TIPO_TARIFA, TARIFA
    cols_to_keep = ['ID', 'UID_TOMADA', 'TIPO_TARIFARIO', 'TIPO_TARIFA', 'TARIFA']
    
    # Check if columns exist
    missing_cols = [c for c in cols_to_keep if c not in df_tarifas.columns]
    if missing_cols:
        print(f"Error: Missing columns in CSV: {missing_cols}")
        return

    df_prices = df_tarifas[cols_to_keep].rename(columns={'ID': 'station_ID'})
    
    print(f"Processing {len(df_prices)} price entries.")

    # Save to DuckDB
    print(f"Saving to {PRE_ML_DB}...")
    try:
        con = duckdb.connect(PRE_ML_DB)
        con.execute("CREATE OR REPLACE TABLE prices AS SELECT * FROM df_prices")
        con.close()
        print("Success: 'prices' table created in pre_ml.db.")
    except Exception as e:
        print(f"Error saving to database: {e}")

if __name__ == "__main__":
    # Ensure we are in the script's directory for relative paths to work
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    enrich_prices()
