import duckdb
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from process_cell_logic import analyze_single_cell
import numpy as np
import time
import gc

db_path = "../data/osm_analysis.db"

def analyze():
    con = duckdb.connect(db_path)
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # Get grid spine
    print("Loading Grid Spine...")
    try:
        spine_bounds = con.execute("SELECT min(x_3035), min(y_3035), max(x_3035), max(y_3035) FROM grid_spine").fetchone()
        xmin, ymin, xmax, ymax = spine_bounds
    except Exception as e:
        print(f"Critical Error: 'grid_spine' table not found or empty. Ingestion might have failed completely. {e}")
        con.close()
        return

    # Check for existence of data tables
    required_tables = ["roads_global", "pois_global", "lu_global", "nat_global"]
    existing_tables = set([t[0] for t in con.execute("SHOW TABLES").fetchall()])
    missing_tables = set(required_tables) - existing_tables
    if missing_tables:
        print(f"Warning: The following tables are missing from the DB: {missing_tables}")
        print("Analysis will proceed assuming empty data for these categories.")

    
    # ---------------------------------------------------------
    # PROCESSSING LOOP
    # ---------------------------------------------------------
    block_size = 10000 
    
    xs = np.arange(xmin, xmax + block_size, block_size)
    ys = np.arange(ymin, ymax + block_size, block_size)
    
    # xs = [2660000] # Test one block column
    # ys = [1940000, 1950000] # Test two block rows

    
    total_blocks = len(xs) * len(ys)
    print(f"Processing {total_blocks} blocks using Database-First approach...")
    
    current_block = 0
    for bx in xs:
        for by in ys:
            current_block += 1
            # Get cells in this block
            block_cells = con.execute(f"""
                SELECT * FROM grid_spine 
                WHERE x_3035 >= {bx} AND x_3035 < {bx + block_size}
                AND y_3035 >= {by} AND y_3035 < {by + block_size}
            """).df()
            
            if block_cells.empty: continue
            
            print(f"  > Block {current_block}/{total_blocks} at ({bx}, {by})...", end="\r")

            
            # Create WKT for block geometry to filter in SQL
            # DuckDB spatial uses ST_GeomFromText
            block_wkt = box(bx, by, bx + block_size, by + block_size).wkt
            
            # --- FETCH DATA PER BLOCK ---
            # Instead of slicing huge DataFrames, we ask DuckDB for just this square.
            # ST_Intersects(geom, ST_GeomFromText('...'))
            try:
                # 1. Roads
                roads_block = gpd.GeoDataFrame()
                if "roads_global" not in missing_tables:
                    try:
                        _df = con.execute(f"""
                            SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) AS geometry FROM roads_global 
                            WHERE ST_Intersects(geometry, ST_GeomFromText('{block_wkt}'))
                        """).df()
                        if not _df.empty:
                            _df['geometry'] = _df['geometry'].apply(bytes)
                            roads_block = gpd.GeoDataFrame(_df, geometry=gpd.GeoSeries.from_wkb(_df['geometry']), crs="EPSG:3035")
                    except Exception as e:
                        print(f"Error querying roads: {e}")
                
                # 2. POIs
                pois_block = gpd.GeoDataFrame()
                if "pois_global" not in missing_tables:
                    try:
                        _df = con.execute(f"""
                            SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) AS geometry FROM pois_global 
                            WHERE ST_Intersects(geometry, ST_GeomFromText('{block_wkt}'))
                        """).df()
                        if not _df.empty:
                            _df['geometry'] = _df['geometry'].apply(bytes)
                            pois_block = gpd.GeoDataFrame(_df, geometry=gpd.GeoSeries.from_wkb(_df['geometry']), crs="EPSG:3035")
                    except Exception as e:
                         print(f"Error querying pois: {e}")

                # 3. Landuse
                lu_block = gpd.GeoDataFrame()
                if "lu_global" not in missing_tables:
                    try:
                        _df = con.execute(f"""
                            SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) AS geometry FROM lu_global 
                            WHERE ST_Intersects(geometry, ST_GeomFromText('{block_wkt}'))
                        """).df()
                        if not _df.empty:
                            _df['geometry'] = _df['geometry'].apply(bytes)
                            lu_block = gpd.GeoDataFrame(_df, geometry=gpd.GeoSeries.from_wkb(_df['geometry']), crs="EPSG:3035")
                    except Exception as e:
                         print(f"Error querying landuse: {e}")

                # 4. Natural
                nat_block = gpd.GeoDataFrame()
                if "nat_global" not in missing_tables:
                    try:
                        _df = con.execute(f"""
                            SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) AS geometry FROM nat_global 
                            WHERE ST_Intersects(geometry, ST_GeomFromText('{block_wkt}'))
                        """).df()
                        if not _df.empty:
                            _df['geometry'] = _df['geometry'].apply(bytes)
                            nat_block = gpd.GeoDataFrame(_df, geometry=gpd.GeoSeries.from_wkb(_df['geometry']), crs="EPSG:3035")
                    except Exception as e:
                         print(f"Error querying natural: {e}")

            except Exception as e:
                print(f"Error fetching block {bx},{by}: {e}")
                continue

            # Process Cell by Cell
            t_start_process = time.time()
            road_results, poi_results, poly_results, origin_results = [], [], [], []
            
            for _, cell in block_cells.iterrows():
                res = analyze_single_cell(cell, roads_block, pois_block, lu_block, nat_block)
                if res:
                    road_results.append(res['road_stats'])
                    poi_results.append(res['poi_stats'])
                    poly_results.append(res['poly_stats'])
                    origin_results.extend(res['origins'])
            
            t_end_process = time.time()
            count = len(block_cells)
            if count > 0:
                print(f"Block {bx},{by} ({count} cells): {t_end_process - t_start_process:.2f}s")
            
            # Save Block Results to DuckDB (Append Only)
            if road_results: append_to_db(con, "road_stats", pd.DataFrame(road_results))
            if poi_results: append_to_db(con, "poi_stats", pd.DataFrame(poi_results))
            if poly_results: append_to_db(con, "poly_stats", pd.DataFrame(poly_results))
            if origin_results: append_to_db(con, "cell_origins", pd.DataFrame(origin_results))

            # GC
            del roads_block, pois_block, lu_block, nat_block
            gc.collect()

    con.close()
    print("Analysis Complete.")

def append_to_db(con, table_name, df):
    df = df.fillna(0.0)
    con.register("tmp_chunk", df)
    
    # Check table existence
    try:
        exists = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0
    except: exists = False
        
    if not exists:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_chunk")
    else:
        # Schema evolution check
        existing_cols = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        new_cols = set(df.columns)
        for col in new_cols - existing_cols:
            dtype = df[col].dtype
            sql_type = "BIGINT" if "int" in str(dtype) else "DOUBLE" if "float" in str(dtype) else "VARCHAR"
            try: 
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {sql_type}')
            except Exception as e:
                print(f"Warning: Could not add column '{col}': {e}")
            
        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM tmp_chunk")
        
    con.unregister("tmp_chunk")

if __name__ == "__main__":
    analyze()
