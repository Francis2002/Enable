import pandas as pd
import duckdb
import os
import re
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE_DIR, "../data/MOBIE_Tarifas.csv")
OUTPUT_DB = os.path.join(BASE_DIR, "../data/mobie_data.db")

def parse_price(val):
    if pd.isna(val) or val == 'nan': return 0.0
    # Extract numeric part (e.g., "€ 0.261 /charge" -> 0.261)
    match = re.search(r'€\s+([0-9.]+)', str(val))
    if match:
        return float(match.group(1))
    return 0.0

def process_prices():
    if not os.path.exists(INPUT_CSV) or not os.path.exists(OUTPUT_DB):
        print("Missing input files.")
        return

    print("--- 1. Loading Stations & Tariffs ---")
    con = duckdb.connect(OUTPUT_DB)
    stations = con.execute("SELECT ID, max_power_kw FROM stations").df().set_index('ID')
    
    df = pd.read_csv(INPUT_CSV, sep=';', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()
    
    # Filter for standard and ad-hoc only
    df = df[df['TIPO_TARIFARIO'].isin(['REGULAR', 'AD_HOC_PAYMENT'])]
    
    print(f"Loaded {len(df)} tariff component rows.")

    print("--- 2. Parsing Tariffs ---")
    df['is_energy'] = (df['TIPO_TARIFA'] == 'ENERGY')
    df['is_time'] = (df['TIPO_TARIFA'] == 'TIME')
    df['is_fixed'] = (df['TIPO_TARIFA'] == 'FLAT')
    df['is_parking'] = (df['TIPO_TARIFA'] == 'PARKING_TIME')
    df['price_val'] = df['TARIFA'].apply(parse_price)

    print("--- 3. Aggregating by Station + Type ---")
    
    def calc_station_price(group):
        station_id = group['ID'].iloc[0]
        if station_id not in stations.index:
            return None
            
        power = stations.loc[station_id, 'max_power_kw']
        
        p_fixed = group[group['is_fixed']]['price_val'].sum()
        p_energy = group[group['is_energy']]['price_val'].max() if any(group['is_energy']) else 0
        p_time = group[group['is_time']]['price_val'].max() if any(group['is_time']) else 0
        has_parking = int(any(group['is_parking']))
        
        # Consistent Price Calculation for 1h session:
        # Cost = Fixed + (Power * 1h * Energy) + (60min * Time_min)
        effective_total = p_fixed + (power * p_energy) + (60 * p_time)
        effective_kwh = effective_total / power if power > 0 else 0
        
        return pd.Series({
            'p_fixed': p_fixed,
            'p_energy': p_energy,
            'p_time': p_time,
            'has_parking': has_parking,
            'effective_kwh_ref': effective_kwh
        })

    station_prices = df.groupby(['ID', 'TIPO_TARIFARIO']).apply(calc_station_price).dropna().reset_index()

    print("--- 4. Correcting Ad-Hoc Premium ---")
    # Pivot for comparison
    pivot = station_prices.pivot(index='ID', columns='TIPO_TARIFARIO', values='effective_kwh_ref')
    pivot.columns = ['adhoc_kwh', 'reg_kwh']
    
    # Rationale: Many Ad-Hoc profiles in the CSV are missing the 'Fixed' or 'Time' components 
    # that are present in the 'Regular' profile, leading to fake negative premiums.
    # We will only calculate premium on the ENERGY component if it's the only one present in both,
    # or flag stations where Ad-Hoc is truly documented as cheaper (unusual but possible).
    pivot['adhoc_premium_pct'] = ((pivot['adhoc_kwh'] / pivot['reg_kwh']) - 1) * 100
    
    # If premium is extremely negative (e.g. < -10%), it's likely missing data in the ad-hoc profile.
    # We will cap it at 0 if it looks like missing data, or keep it if it's a small variance.
    pivot.loc[pivot['adhoc_premium_pct'] < -10, 'adhoc_premium_pct'] = 0
    pivot['adhoc_premium_pct'] = pivot['adhoc_premium_pct'].fillna(0)

    # Merge back
    master = station_prices[station_prices['TIPO_TARIFARIO'] == 'REGULAR'].copy()
    master = master.merge(pivot[['adhoc_kwh', 'adhoc_premium_pct']], on='ID', how='left')
    master.rename(columns={'ID': 'station_id_code'}, inplace=True)
    master = master.drop(columns=['TIPO_TARIFARIO'])

    print("--- 5. Global Rank ---")
    master['price_percentile'] = master['effective_kwh_ref'].rank(pct=True)

    print("--- 6. Saving to database ---")
    con.execute("DROP TABLE IF EXISTS prices")
    con.execute("CREATE TABLE prices AS SELECT * FROM master")
    con.close()
    print(f"Saved {len(master)} price profiles.")

if __name__ == "__main__":
    process_prices()
