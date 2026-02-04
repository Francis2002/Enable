import duckdb
import pandas as pd
import os

# Configuration
STATIONS_CSV = "../../data/MOBIe_Lista_de_postos_corrected.csv"
PRE_ML_DB = "../../data/pre_ml.db"

def enrich_configuration():
    print("Starting Station Configuration Enrichment...")
    
    if not os.path.exists(STATIONS_CSV):
        print(f"Error: {STATIONS_CSV} not found.")
        return

    # Load Stations CSV
    print(f"Loading {STATIONS_CSV}...")
    # Using sep=';' as confirmed from previous inspections
    df = pd.read_csv(STATIONS_CSV, sep=';', encoding='utf-8')
    
    # Required columns:
    # station_ID (from ID), UID DA TOMADA, TIPO DE CARREGAMENTO, NIVEL DE TENSÃO, 
    # TIPO DE TOMADA, FORMATO DE TOMADA, POTÊNCIA DA TOMADA
    
    col_mapping = {
        'ID': 'station_ID',
        'UID DA TOMADA': 'UID DA TOMADA',
        'TIPO DE CARREGAMENTO': 'TIPO DE CARREGAMENTO',
        'NÍVEL DE TENSÃO': 'NÍVEL DE TENSÃO',
        'TIPO DE TOMADA': 'TIPO DE TOMADA',
        'FORMATO DA TOMADA': 'FORMATO DA TOMADA',
        'POTÊNCIA DA TOMADA (kW)': 'POTÊNCIA DA TOMADA (kW)'
    }
    
    # Check if columns exist
    missing_cols = [c for c in col_mapping.keys() if c not in df.columns]
    if missing_cols:
        print(f"Error: Missing columns in CSV: {missing_cols}")
        return

    df_config = df[list(col_mapping.keys())].rename(columns=col_mapping)
    
    print(f"Processing {len(df_config)} configuration entries.")

    # Save to DuckDB
    print(f"Saving to {PRE_ML_DB}...")
    try:
        con = duckdb.connect(PRE_ML_DB)
        con.execute("CREATE OR REPLACE TABLE station_configuration AS SELECT * FROM df_config")
        con.close()
        print("Success: 'station_configuration' table created.")
    except Exception as e:
        print(f"Error saving to database: {e}")

if __name__ == "__main__":
    # Ensure we are in the script's directory for relative paths to work
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    enrich_configuration()
