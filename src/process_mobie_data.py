"""
Mobi.E Native Session Processor (Step 20)

This script aggregates raw session data (detailed-20260116.csv) into 
Daily Station Labels and Features.

Labels (Targets):
- kwh_daily: Realized energy delivered.
- sessions_daily: Number of sessions that BEGAN on this day.

Features (Realized Demand Context):
- max_concurrent: Peak number of simultaneous users.
- saturation_ratio: Fraction of the day (0-1) where all stalls were occupied.
"""

import duckdb
import pandas as pd
import numpy as np
import os
from datetime import timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "../data/detailed-20260116.csv")
DB_PATH = os.path.join(BASE_DIR, "../data/mobie_data.db")

def process_sessions():
    if not os.path.exists(CSV_PATH) or not os.path.exists(DB_PATH):
        print("Missing input files.")
        return

    print("--- 1. Loading Native Station Metadata ---")
    con = duckdb.connect(DB_PATH)
    stations_meta = con.execute("SELECT ID, stalls FROM stations").df().set_index('ID')
    
    print("--- 2. Loading Session Data ---")
    df = pd.read_csv(CSV_PATH, sep=';', thousands='.', decimal=',', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()
    
    # Parse Timestamps
    df['start'] = pd.to_datetime(df['startTimestamp'].astype(str), format='%Y%m%d%H%M%S', errors='coerce')
    df['end'] = pd.to_datetime(df['stopTimestamp'].astype(str), format='%Y%m%d%H%M%S', errors='coerce')
    df['start_day'] = df['start'].dt.strftime('%Y%m%d')
    df = df.dropna(subset=['start', 'end'])
    
    # Numerical cleaning
    for col in ['energia_total_periodo', 'periodDuration']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    print(f"Loaded {len(df)} session fragments.")

    print("--- 3. Aggregating Labels & Saturation ---")
    results = []
    
    # Group by Station and fragment date (idDay)
    for (station_id, day), group in df.groupby(['idChargingStation', 'idDay']):
        if station_id not in stations_meta.index:
            continue # We only care about stations in our master list
            
        capacity = int(stations_meta.loc[station_id, 'stalls'])
        
        # A. Labels
        kwh_daily = group['energia_total_periodo'].sum()
        # count(distinct idCdr) WHERE session began today
        sessions_daily = group[group['start_day'].astype(str) == str(day)]['idCdr'].nunique()
        
        # B. Timeline Reconstruction (Minute-by-Minute)
        day_start = pd.to_datetime(str(day), format='%Y%m%d')
        day_end = day_start + timedelta(days=1)
        timeline = np.zeros(1440, dtype=int)
        
        # Map ALL sessions active in this station-day to the 1440m timeline
        # We need to look across the whole dataframe for the full start/end of these sessions
        sids = group['idCdr'].unique()
        for sid in sids:
            # Get the overall session boundaries (not just the fragment)
            # Efficiently: use the group's min start/max end if they are in the same day, 
            # but safer to query the whole df for that sid to handle multiday spans correctly.
            # For performance with large datasets, we'd pre-extract unique session bounds.
            
            s_rows = df[df['idCdr'] == sid]
            s_bound = s_rows['start'].min()
            e_bound = s_rows['end'].max()
            
            # Clip session to the current day
            s = max(s_bound, day_start)
            e = min(e_bound, day_end)
            
            if s >= e: continue
            
            start_min = int((s - day_start).total_seconds() // 60)
            end_min = int((e - day_start).total_seconds() // 60)
            
            # Boundary checks
            start_min = max(0, min(1439, start_min))
            end_min = max(0, min(1440, end_min))
            
            timeline[start_min:end_min] += 1
            
        max_concurrent = np.max(timeline) if len(timeline) > 0 else 0
        saturated_mins = np.sum(timeline >= capacity)
        saturation_ratio = saturated_mins / 1440.0
        
        results.append({
            'station_id': station_id,
            'date_str': str(day),
            'kwh_daily': round(float(kwh_daily), 2),
            'sessions_daily': int(sessions_daily),
            'max_concurrent': int(max_concurrent),
            'saturation_ratio': round(float(saturation_ratio), 4)
        })
        
    print(f"Generated {len(results)} station-day data points.")

    print("--- 4. Saving to mobie_data.db ---")
    if not results:
        print("No results to save.")
        con.close()
        return
        
    res_df = pd.DataFrame(results)
    con.execute("DROP TABLE IF EXISTS session_stats")
    con.execute("CREATE TABLE session_stats AS SELECT * FROM res_df")
    
    con.close()
    print("Done. Tables in mobie_data.db: stations, prices, session_stats")

if __name__ == "__main__":
    process_sessions()
