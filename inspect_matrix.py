import duckdb
import pandas as pd
import os

# Use paths relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MATRIX_DB = os.path.join(BASE_DIR, "data/travel_matrix.db")
CHARGERS_DB = os.path.join(BASE_DIR, "data/chargers.db")

def inspect():
    if not os.path.exists(MATRIX_DB):
        print(f"Error: {MATRIX_DB} not found.")
        return

    print(f"--- Inspecting {MATRIX_DB} ---")
    con = duckdb.connect(MATRIX_DB)
    
    # Basic Counts
    count = con.execute("SELECT count(*) FROM travel_times").fetchone()[0]
    print(f"\nTotal Sparse Edges (Routes): {count:,}")
    
    if count == 0:
        con.close()
        return

    # Coverage Stats
    print("\n--- Coverage Statistics (Unique Stations per Cell) ---")
    stats = con.execute("""
        SELECT 
            avg(station_count) as avg_stations, 
            min(station_count) as min_stations, 
            max(station_count) as max_stations
        FROM (
            SELECT cell_id, count(DISTINCT station_id) as station_count 
            FROM travel_times 
            GROUP BY cell_id
        )
    """).df()
    print(stats.to_string(index=False))

    # Detailed Sample (Best Time per Station)
    if os.path.exists(CHARGERS_DB):
        print("\n--- Sample Route Details (Joined with Chargers, Best Time) ---")
        try:
            con.execute(f"ATTACH '{CHARGERS_DB}' AS chg")
            
            # Get a sample cell
            sample_cell = con.execute("SELECT cell_id FROM travel_times LIMIT 1").fetchone()[0]
            print(f"Sample Cell: {sample_cell}")
            
            query = f"""
                SELECT 
                    m.station_id,
                    c.title,
                    c.max_kw, 
                    min(m.time_min) as best_time_min, 
                    min(m.distance_km) as best_dist_km
                FROM travel_times m 
                JOIN chg.chargers c ON m.station_id = c.station_id 
                WHERE m.cell_id = '{sample_cell}' 
                GROUP BY m.station_id, c.title, c.max_kw
                ORDER BY best_time_min ASC 
                LIMIT 10
            """
            print(con.execute(query).df().to_string(index=False))
        except Exception as e:
            print(f"Could not join with chargers.db: {e}")
            print("Showing raw matrix data instead:")
            print(con.execute("SELECT * FROM travel_times LIMIT 10").df())
    else:
        print("\n--- Raw Sample (No Charger Data Found) ---")
        print(con.execute("SELECT * FROM travel_times LIMIT 10").df())

    con.close()

if __name__ == "__main__":
    inspect()
