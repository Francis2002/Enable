import json
import duckdb
import pandas as pd
import os
import re

INPUT_FILE = "../data/ocm_portugal_raw.json"
DB_PATH = "../data/chargers.db"

def slugify(text):
    # Clean column names for SQL: lowercase, replace spaces/special chars with underscores
    text = text.lower()
    text = re.sub(r'[^a-z0-9]', '_', text)
    text = re.sub(r'_+', '_', text).strip('_')
    return f"cnt_{text}"

def process_chargers():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Run fetch_chargers.py first.")
        return

    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    processed_data = []
    all_connector_slugs = set()
    
    print("Pre-scanning for all unique connector types...")
    for entry in data:
        for conn in entry.get("Connections", []) or []:
            conn_type = (conn.get("ConnectionType") or {}).get("Title", "Unknown")
            all_connector_slugs.add(slugify(conn_type))

    print(f"Found {len(all_connector_slugs)} unique connector types.")

    for entry in data:
        station_id = entry.get("ID")
        addr = entry.get("AddressInfo", {}) or {}
        title = addr.get("Title")
        lat = addr.get("Latitude")
        lon = addr.get("Longitude")
        
        connections = entry.get("Connections", []) or []
        
        max_kw = 0.0
        total_stalls = 0
        
        # Initialize counts for this station
        station_counts = {slug: 0 for slug in all_connector_slugs}
        
        for conn in connections:
            kw = conn.get("PowerKW")
            if kw:
                try:
                    max_kw = max(max_kw, float(kw))
                except: pass
            
            qty = conn.get("Quantity") or 1
            total_stalls += int(qty)
            
            conn_type = (conn.get("ConnectionType") or {}).get("Title", "Unknown")
            slug = slugify(conn_type)
            station_counts[slug] += int(qty)

        usage_obj = entry.get("UsageType") or {}
        usage = usage_obj.get("Title", "Unknown")
        
        operator_obj = entry.get("OperatorInfo") or {}
        operator = operator_obj.get("Title", "Unknown")

        row = {
            "station_id": station_id,
            "title": title,
            "latitude": lat,
            "longitude": lon,
            "max_kw": max_kw,
            "stalls": total_stalls,
            "usage_type": usage,
            "operator": operator
        }
        row.update(station_counts)
        processed_data.append(row)

    df = pd.DataFrame(processed_data)
    # Ensure 0 instead of NaN for missing connectors
    df = df.fillna(0)
    
    print(f"Connecting to {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    
    # Create the table
    con.execute("DROP TABLE IF EXISTS chargers")
    con.execute("CREATE TABLE chargers AS SELECT * FROM df")
    
    count = con.execute("SELECT count(*) FROM chargers").fetchone()[0]
    print(f"Successfully processed {count} stations with {len(all_connector_slugs)} connector columns.")
    con.close()

if __name__ == "__main__":
    process_chargers()
