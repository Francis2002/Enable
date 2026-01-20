import duckdb
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data/mobie_data.db")

def inspect():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    con = duckdb.connect(DB_PATH)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)

    print(f"=== DATABASE: {DB_PATH} ===")
    
    # 1. STATIONS TABLE
    print("\n[TABLE: stations]")
    cols_stations = con.execute("DESCRIBE stations").df()
    print("\nColumns and Types:")
    print(cols_stations[['column_name', 'column_type']].to_string(index=False))
    
    print("\nRaw Data Sample (5 rows):")
    # Using * to show everything
    df_stations = con.execute("SELECT * FROM stations LIMIT 5").df()
    print(df_stations.to_string(index=False))

    # 2. PRICES TABLE
    print("\n" + "="*50)
    print("\n[TABLE: prices]")
    cols_prices = con.execute("DESCRIBE prices").df()
    print("\nColumns and Types:")
    print(cols_prices[['column_name', 'column_type']].to_string(index=False))
    
    print("\nRaw Data Sample (5 rows):")
    df_prices = con.execute("SELECT * FROM prices LIMIT 5").df()
    print(df_prices.to_string(index=False))

    # 3. SESSIONS TABLE
    print("\n" + "="*50)
    print("\n[TABLE: session_stats]")
    cols_sessions = con.execute("DESCRIBE session_stats").df()
    print("\nColumns and Types:")
    print(cols_sessions[['column_name', 'column_type']].to_string(index=False))
    
    print("\nRaw Data Sample (5 rows, Joined with Title):")
    df_sessions = con.execute("""
        SELECT 
            s.date_str as date,
            st.ID,
            st.stalls,
            s.kwh_daily as kwh,
            s.sessions_daily as cnt,
            s.max_concurrent as peak,
            s.saturation_ratio as sat
        FROM session_stats s
        JOIN stations st ON s.station_id = st.ID
        ORDER BY s.kwh_daily DESC
        LIMIT 5
    """).df()
    print(df_sessions.to_string(index=False))

    # 4. OVERVIEW STATS
    print("\n" + "="*50)
    print("\n=== DATA COVERAGE SUMMARY ===")
    total_stations = con.execute("SELECT count(*) FROM stations").fetchone()[0]
    total_prices = con.execute("SELECT count(*) FROM prices").fetchone()[0]
    total_stats = con.execute("SELECT count(*) FROM session_stats").fetchone()[0]
    matches = con.execute("SELECT count(*) FROM stations s JOIN prices p ON s.ID = p.station_id_code").fetchone()[0]
    
    print(f"Total Unique Stations: {total_stations}")
    print(f"Total Price Profiles: {total_prices}")
    print(f"Total Station-Day Samples: {total_stats}")
    print(f"Stations with Price Data: {matches} ({round(matches/total_stations*100, 1)}%)")

    con.close()

if __name__ == "__main__":
    inspect()
