import json
import pandas as pd
import os
import shutil

# Correct paths relative to where this script might be run or absolute paths
# Assuming script is in src/process_data/ and run from project root or src
# We will verify paths dynamically

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
CORRECTIONS_FILE = os.path.join(DATA_DIR, "station_corrections.json")
RAW_DATA_FILE = os.path.join(DATA_DIR, "MOBIe_Lista_de_postos.csv")
BACKUP_DATA_FILE = os.path.join(DATA_DIR, "MOBIe_Lista_de_postos_downloaded.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "MOBIe_Lista_de_postos_corrected.csv")

def load_corrections(json_path):
    if not os.path.exists(json_path):
        print(f"Warning: Corrections file not found at {json_path}")
        return []
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data.get("corrections", [])

def apply_station_corrections(df, corrections):
    print(f"Applying {len(corrections)} corrections...")
    count = 0
    
    # Ensure ID column is string for matching
    if 'ID' in df.columns:
        df['ID'] = df['ID'].astype(str)
        
    for correction in corrections:
        station_id = correction.get("ID")
        if not station_id:
            continue
            
        # Filter for the specific station
        mask = df['ID'] == station_id
        
        if not mask.any():
            print(f"Warning: Station ID {station_id} not found in dataset.")
            continue
            
        # Apply field updates
        for field, new_value in correction.items():
            if field == "ID": continue # Skip ID itself
            
            if field in df.columns:
                # Log modification if value is actually changing
                current_values = df.loc[mask, field].unique()
                if len(current_values) > 0 and current_values[0] != new_value:
                    print(f"Updating {station_id}: {field} '{current_values[0]}' -> '{new_value}'")
                    df.loc[mask, field] = new_value
                    count += 1
            else:
                print(f"Warning: Column '{field}' not found in dataframe.")
                
    print(f"Total corrections applied: {count}")
    return df

def main():
    print("--- Starting Data Correction Process ---")
    
    # 1. File Rotation Strategy
    if os.path.exists(RAW_DATA_FILE):
        print(f"Found new raw file: {RAW_DATA_FILE}")
        print(f"Renaming to: {BACKUP_DATA_FILE}")
        shutil.move(RAW_DATA_FILE, BACKUP_DATA_FILE)
    elif os.path.exists(BACKUP_DATA_FILE):
        print(f"No new raw file found. Using existing backup: {BACKUP_DATA_FILE}")
    else:
        print(f"Error: No data file found at {RAW_DATA_FILE} or {BACKUP_DATA_FILE}")
        return

    # 2. Load Data
    print(f"Loading data from {BACKUP_DATA_FILE}...")
    try:
        # Using separator ';' and encoding 'utf-8-sig' as per EDA notebook
        df = pd.read_csv(BACKUP_DATA_FILE, sep=';', encoding='utf-8-sig')
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # 3. Load and Apply Corrections
    corrections = load_corrections(CORRECTIONS_FILE)
    df_corrected = apply_station_corrections(df, corrections)

    # 4. Save Output
    print(f"Saving corrected data to {OUTPUT_FILE}...")
    # Saving with the same format: sep=';', encoding='utf-8-sig', quoting=1 (QUOTE_ALL usually for these CSVs if they have newlines, but default might be fine. Let's aim for minimal diff).
    # Pandas default to quotes only when needed unless specified. The input file seems to have quotes around all fields.
    # Let's try to match quoting behavior if possible, usually QUOTE_ALL (1) match headers.
    import csv
    df_corrected.to_csv(OUTPUT_FILE, sep=';', encoding='utf-8-sig', index=False, quoting=csv.QUOTE_ALL)
    print("Done.")

if __name__ == "__main__":
    main()
