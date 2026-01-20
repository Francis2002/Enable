import pandas as pd
import duckdb
import os
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE_DIR, "../data/MOBIe_Lista_de_postos.csv")
OUTPUT_DB = os.path.join(BASE_DIR, "../data/mobie_data.db")

def process_static():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found.")
        return

    print("--- 1. Loading Mobi.E Station List ---")
    df = pd.read_csv(INPUT_CSV, sep=';', encoding='utf-8-sig')
    
    # Strip column names
    df.columns = df.columns.str.strip()
    
    # Clean numeric columns
    def clean_power(val):
        try:
            return float(str(val).replace(',', '.'))
        except:
            return 0.0

    df['power_kw'] = df['POTÊNCIA DA TOMADA (kW)'].apply(clean_power)
    
    print(f"Loaded {len(df)} socket rows.")

    print("--- 2. Encoding features ---")
    
    # Voltage Encoding (Ordinal)
    voltage_map = {
        'Baixa Tensão Normal': 1,
        'Baixa Tensão Especial': 2,
        'Média Tensão': 3
    }
    df['voltage_lvl'] = df['NÍVEL DE TENSÃO'].map(voltage_map).fillna(1)

    # Connector Mapping
    def categorize_connector(val):
        val = str(val).upper()
        if 'CCS' in val: return 'ccs'
        if 'CHADEMO' in val: return 'chademo'
        if 'MENNEKES' in val or 'TYPE 2' in val or 'TYPE2' in val: return 'type2'
        return 'other'
    
    df['conn_type'] = df['TIPO DE TOMADA'].apply(categorize_connector)
    conn_dummies = pd.get_dummies(df['conn_type'], prefix='conn')
    df = pd.concat([df, conn_dummies], axis=1)

    print("--- 3. Aggregating to Station level ---")
    
    # Define aggregation logic
    agg_funcs = {
        'LATITUDE': 'first',
        'LONGITUDE': 'first',
        'OPERADOR': 'first',
        'CIDADE': 'first',
        'MORADA': 'first',
        'UID DA TOMADA': 'nunique', # Capacity (stalls)
        'power_kw': 'max',          # Max Power
        'voltage_lvl': 'max'        # Max Voltage level if mixed
    }
    
    # Add connector counts to agg
    for col in conn_dummies.columns:
        agg_funcs[col] = 'sum'

    stations = df.groupby('ID').agg(agg_funcs).reset_index()
    stations.rename(columns={'UID DA TOMADA': 'stalls', 'power_kw': 'max_power_kw'}, inplace=True)

    print("--- 4. CPO Portfolio Analysis ---")
    
    # Calculate global operator share
    op_counts = stations['OPERADOR'].value_counts()
    total_stations = len(stations)
    stations['operator_share'] = stations['OPERADOR'].map(op_counts) / total_stations
    
    # Top Operators Dummy Coding
    top_operators = op_counts.head(10).index.tolist()
    for op in top_operators:
        clean_name = ''.join(e for e in op if e.isalnum()).lower()
        stations[f'is_op_{clean_name}'] = (stations['OPERADOR'] == op).astype(int)

    print("--- 5. Saving to database ---")
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB) # Starting fresh as requested

    con = duckdb.connect(OUTPUT_DB)
    con.execute("CREATE TABLE stations AS SELECT * FROM stations")
    
    # Add spatial index? For now just raw table.
    row_count = con.execute("SELECT count(*) FROM stations").fetchone()[0]
    print(f"Saved {row_count} stations to {OUTPUT_DB}")
    con.close()

if __name__ == "__main__":
    process_static()
