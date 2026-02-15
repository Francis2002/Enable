import duckdb
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "../../data/pre_ml.db")
JSON_PATH = os.path.join(SCRIPT_DIR, "station_corrections.json")

def get_station_id_column(con, table_name):
    """Detects if table uses 'station_id' or 'station_ID'."""
    columns = [col[1] for col in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    for col in columns:
        if col.lower() == 'station_id':
            return col
    return None

def apply_correction(con, table_name, correction):
    station_id_val = correction.get("station_id")
    old_values = correction.get("old", {})
    new_values = correction.get("new", {})
    
    id_col = get_station_id_column(con, table_name)
    if not id_col:
        print(f"  [SKIP] Table '{table_name}' does not have a station ID column.")
        return

    # Check if entry exists and matches old values
    where_clause = f"{id_col} = '{station_id_val}'"
    for col, val in old_values.items():
        # Match column name case-insensitively
        table_cols = [c[1] for c in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
        matched_col = next((c for c in table_cols if c.lower() == col.lower()), None)
        if matched_col:
            where_clause += f" AND {matched_col} = '{val}'"
        else:
            print(f"  [WARN] Column '{col}' not found in table '{table_name}'. Skipping check for this column.")

    count = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}").fetchone()[0]
    
    if count == 0:
        print(f"  [INFO] No matching entries found in '{table_name}' for {station_id_val} with specified old values.")
        return

    # Build UPDATE statement
    set_clauses = []
    table_cols = [c[1] for c in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    
    for col, val in new_values.items():
        matched_col = next((c for c in table_cols if c.lower() == col.lower()), None)
        if matched_col:
            set_clauses.append(f"{matched_col} = '{val}'")
        else:
            print(f"  [ERROR] Cannot update column '{col}' because it doesn't exist in '{table_name}'.")

    if not set_clauses:
        return

    update_query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {where_clause}"
    con.execute(update_query)
    print(f"  [SUCCESS] Updated {count} row(s) in '{table_name}' for station {station_id_val}.")

def main():
    if not os.path.exists(JSON_PATH):
        print(f"Error: {JSON_PATH} not found.")
        return

    with open(JSON_PATH, 'r') as f:
        data = json.load(f)

    con = duckdb.connect(DB_PATH)
    tables_in_db = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

    print("--- Database Correction Tool ---")
    print(f"Available tables: {', '.join(tables_in_db)}")
    
    target_table = input("Enter table name to fix (or 'ALL' to process based on JSON): ").strip()

    corrections = data.get("corrections", [])
    
    for entry in corrections:
        json_table = entry.get("TABLE")
        
        # Decide which table(s) to process for this correction
        if target_table.upper() == "ALL" or target_table == json_table:
            tables_to_process = []
            if json_table == "ALL":
                tables_to_process = tables_in_db
            else:
                tables_to_process = [json_table]
            
            for t in tables_to_process:
                if t in tables_in_db:
                    print(f"Processing correction for station {entry.get('station_id')} in table '{t}'...")
                    apply_correction(con, t, entry)
                else:
                    print(f"  [ERROR] Table '{t}' listed in JSON not found in database.")

    con.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
