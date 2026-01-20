"""
inspect_ml_ready.py

This script demonstrates the FINAL data structure that would be fed into an ML model.
It joins static features (stations), pricing features (prices), and labels (session_stats).

Discretization: Daily per Station.
"""

import duckdb
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data/mobie_data.db")

def view_ml_rows():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    con = duckdb.connect(DB_PATH)
    
    # Configure pandas for wide display
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    print("=== ML-READY DATASET VIEW (Joined Mobi.E Data) ===")
    print("One row per Station-Day. Perfect alignment for training.")

    # SQL Join across all three components
    query = """
    SELECT 
        -- 1. Identifiers & Time
        s.date_str as date,
        st.ID as station_id,
        st.CIDADE as city,
        
        -- 2. Static Physical Features (Independent Variables)
        st.stalls,
        st.max_power_kw as power,
        st.voltage_lvl as volt,
        st.conn_ccs as ccs,
        st.conn_chademo as chademo,
        st.conn_type2 as type2,
        round(st.operator_share, 4) as op_share,
        
        -- 3. Pricing Features (Independent Variables)
        round(p.effective_kwh_ref, 3) as price_kwh,
        round(p.price_percentile, 2) as price_rank,
        round(p.adhoc_premium_pct, 1) as adhoc_prem,
        
        -- 4. Realized Demand Features (Dynamic Context)
        s.max_concurrent as peak_occ,
        round(s.saturation_ratio, 3) as saturation,
        
        -- 5. TARGET LABELS (The 'y' values)
        s.kwh_daily as LABEL_kwh,
        s.sessions_daily as LABEL_sessions
        
    FROM session_stats s
    JOIN stations st ON s.station_id = st.ID
    LEFT JOIN prices p ON st.ID = p.station_id_code
    ORDER BY s.date_str DESC, s.kwh_daily DESC
    LIMIT 20
    """
    
    df = con.execute(query).df()
    
    if df.empty:
        print("No matches found. Ensure all processing scripts (static, prices, sessions) have run.")
    else:
        print("\nSAMPLE ROWS:")
        print(df.to_string(index=False))
        
        print("\n--- Column Dictionary for your ML Model ---")
        print("1. [stalls, power, volt, ccs, chademo, type2, op_share]: Supply-side attractiveness.")
        print("2. [price_kwh, price_rank, adhoc_prem]: Cost barriers.")
        print("3. [peak_occ, saturation]: Historical context (can be shifted as lag features).")
        print("4. [LABEL_kwh, LABEL_sessions]: What you are trying to predict.")

    con.close()

if __name__ == "__main__":
    view_ml_rows()
