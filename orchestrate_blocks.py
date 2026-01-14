import duckdb
import pandas as pd
import geopandas as gpd
from pyrosm import OSM
from process_cell_logic import analyze_single_cell
import os
import numpy as np

db_path = "osm_analysis.db"
pbf_path = "portugal-latest.osm.pbf"

def orchestrate():
    con = duckdb.connect(db_path)
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # Get the grid spine extent
    spine_bounds = con.execute("SELECT min(x_3035), min(y_3035), max(x_3035), max(y_3035) FROM grid_spine").fetchone()
    xmin, ymin, xmax, ymax = spine_bounds
    
    # Process in 10x10km blocks
    block_size = 10000 
    
    xs = np.arange(xmin, xmax + block_size, block_size)
    ys = np.arange(ymin, ymax + block_size, block_size)
    
    # --- TEST OVERRIDE (Lisbon Area) ---
    # Marquês de Pombal is approx X=2664000, Y=1947000
    xs = [2660000] # Test one block column
    ys = [1940000, 1950000] # Test two block rows
    # -----------------------------------
    
    print(f"Test Blocks to process: {len(xs) * len(ys)}")
    
    for bx in xs:
        for by in ys:
            # 1. Get cells in this block
            block_cells = con.execute(f"""
                SELECT * FROM grid_spine 
                WHERE x_3035 >= {bx} AND x_3035 < {bx + block_size}
                AND y_3035 >= {by} AND y_3035 < {by + block_size}
            """).df()
            
            if block_cells.empty: continue
            
            print(f"Processing Block X={bx} Y={by} ({len(block_cells)} cells)...")
            
            # Get block bbox in WGS84 for extraction
            b_min_lon, b_min_lat = block_cells[['min_lon', 'min_lat']].min()
            b_max_lon, b_max_lat = block_cells[['max_lon', 'max_lat']].max()
            bbox = [b_min_lon, b_min_lat, b_max_lon, b_max_lat]
            
            # 2. Extract Data Once per Block
            try:
                osm = OSM(pbf_path, bounding_box=bbox)
                roads = osm.get_network(network_type="driving")
                if roads is not None: roads = roads.to_crs("EPSG:3035")
                
                pois = osm.get_pois()
                if pois is not None: pois = pois.to_crs("EPSG:3035")
                
                # Landuse and Natural
                lu = osm.get_landuse()
                if lu is not None: lu = lu.to_crs("EPSG:3035")
                
                nat = osm.get_natural()
                if nat is not None: nat = nat.to_crs("EPSG:3035")
                    
            except Exception as e:
                print(f"  Error extracting block data: {e}")
                continue

            # 3. Process Cell by Cell
            road_results = []
            poi_results = []
            poly_results = []
            origin_results = []
            
            for _, cell in block_cells.iterrows():
                res = analyze_single_cell(cell, roads, pois, lu, nat)
                if res:
                    road_results.append(res['road_stats'])
                    poi_results.append(res['poi_stats'])
                    poly_results.append(res['poly_stats'])
                    origin_results.extend(res['origins'])
            
            # 4. Save Block Results to DuckDB
            if road_results:
                save_to_db(con, "road_stats", pd.DataFrame(road_results))
            if poi_results:
                save_to_db(con, "poi_stats", pd.DataFrame(poi_results))
            if poly_results:
                save_to_db(con, "poly_stats", pd.DataFrame(poly_results))
            if origin_results:
                save_to_db(con, "cell_origins", pd.DataFrame(origin_results))

    con.close()
    print("Orchestration Complete.")

def save_to_db(con, table_name, df):
    # Flexible column handling
    con.register("tmp_block", df)
    if con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] == 0:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_block")
    else:
        con.execute(f"CREATE TABLE {table_name}_new AS SELECT * FROM {table_name} UNION BY NAME SELECT * FROM tmp_block")
        con.execute(f"DROP TABLE {table_name}")
        con.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
    con.unregister("tmp_block")

if __name__ == "__main__":
    orchestrate()
