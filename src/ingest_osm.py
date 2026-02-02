import duckdb
import geopandas as gpd
from pyrosm import OSM
import time
import gc
import os
import numpy as np
from pyproj import Transformer
from shapely.geometry import box

# Configuration
DB_PATH = "../data/osm_analysis.db"
PBF_PATH = "../data/portugal-latest.osm.pbf"
CHUNKS_X = 2  # 2x5 = 10 chunks - Compromise between Speed and RAM
CHUNKS_Y = 5

def get_wgs84_bounds(con):
    """Get grid spine bounds and convert to WGS84 for Pyrosm filtering"""
    print("Fetching grid bounds...")
    try:
        # Get 3035 bounds from DB
        row = con.execute("SELECT min(x_3035), min(y_3035), max(x_3035), max(y_3035) FROM grid_spine").fetchone()
        xmin, ymin, xmax, ymax = row
        print(f"  - Grid Bounds (EPSG:3035): {xmin}, {ymin}, {xmax}, {ymax}")
        
        # Convert to WGS84
        transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
        min_lon, min_lat = transformer.transform(xmin, ymin)
        max_lon, max_lat = transformer.transform(xmax, ymax)
        
        # Add a small buffer to ensure overlap/coverage
        buff = 0.05
        return (min_lon - buff, min_lat - buff, max_lon + buff, max_lat + buff)
    except Exception as e:
        print(f"  ! Error reading grid_spine ({e}). Using default Portugal bounds.")
        return (-9.6, 36.9, -6.1, 42.2)

def generate_chunks(bounds, nx, ny):
    min_x, min_y, max_x, max_y = bounds
    width = (max_x - min_x) / nx
    height = (max_y - min_y) / ny
    
    chunks = []
    for i in range(nx):
        for j in range(ny):
            x0 = min_x + i * width
            y0 = min_y + j * height
            x1 = x0 + width
            y1 = y0 + height
            # Create a box list [minx, miny, maxx, maxy]
            # Add small overlap buffer to avoid missing edge features
            overlap = 0.005 # ~500m
            chunks.append([x0 - overlap, y0 - overlap, x1 + overlap, y1 + overlap])
    return chunks

def ingest():
    print(f"Starting Database-First Ingestion (Chunked Strategy {CHUNKS_X}x{CHUNKS_Y})...")
    start_all = time.time()
    
    con = duckdb.connect(DB_PATH)
    con.execute("INSTALL spatial; LOAD spatial;")
    
    bounds_wgs84 = get_wgs84_bounds(con)
    chunks = generate_chunks(bounds_wgs84, CHUNKS_X, CHUNKS_Y)
    print(f"  - Generated {len(chunks)} spatial chunks for processing.")

    # Define Categories
    categories = [
        ("roads_global", "network"),
        ("pois_global", "pois"),
        ("lu_global", "landuse"),
        ("nat_global", "natural")
    ]

    for table_name, cat_type in categories:
        print(f"\n--- Ingesting {cat_type.upper()} ---")
        
        # Reset table
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        table_created = False
        
        for i, chunk_bbox in enumerate(chunks):
            print(f"  > Processing Chunk {i+1}/{len(chunks)}...")
            t_chunk = time.time()
            
            try:
                # Init OSM restricted to this chunk
                osm = OSM(PBF_PATH, bounding_box=chunk_bbox)
                
                gdf = None
                if cat_type == "network":
                    gdf = osm.get_network(network_type="all")
                elif cat_type == "pois":
                    gdf = osm.get_pois()
                elif cat_type == "landuse":
                    gdf = osm.get_landuse()
                elif cat_type == "natural":
                    gdf = osm.get_natural()
                
                if gdf is not None and not gdf.empty:
                    # Reproject to EPSG:3035 immediately
                    gdf = gdf.to_crs("EPSG:3035")
                    
                    # FIX: Explicitly convert geometry to WKB (binary) for DuckDB
                    # This resolves "Data type 'geometry' not recognized"
                    gdf['geom_wkb'] = gdf['geometry'].apply(lambda x: x.wkb)
                    # Drop original shapely objects to avoid DuckDB confusion
                    gdf = gdf.drop(columns=['geometry'])
                    
                    # Store in DB
                    con.register("tmp_chunk", gdf)
                    if not table_created:
                        con.execute(f"CREATE TABLE {table_name} AS SELECT * EXCLUDE(geom_wkb), ST_GeomFromWKB(geom_wkb) AS geometry FROM tmp_chunk")
                        table_created = True
                    else:
                        # Append with schema alignment
                        # 1. Check for new columns
                        existing_cols = {r[1] for r in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
                        new_cols = set(gdf.columns) - {'geom_wkb'}
                        for col in new_cols - existing_cols:
                            dtype = gdf[col].dtype
                            sql_type = "BIGINT" if "int" in str(dtype) else "DOUBLE" if "float" in str(dtype) else "VARCHAR"
                            try: 
                                con.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {sql_type}')
                            except Exception as e: 
                                print(f"    ! Warning: Could not add column '{col}': {e}")
                            
                        # 2. Insert with explicit geometry reconstruction
                        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * EXCLUDE(geom_wkb), ST_GeomFromWKB(geom_wkb) AS geometry FROM tmp_chunk")
                    
                    con.unregister("tmp_chunk")
                    print(f"    - Loaded {len(gdf)} features in {time.time() - t_chunk:.2f}s")
                else:
                    print("    - Empty chunk.")
                
            except Exception as e:
                print(f"    ! Failed chunk {i+1}: {e}")
            
            # Critical GC
            del osm, gdf
            gc.collect()

    con.close()
    print(f"\nTotal Ingestion Time: {time.time() - start_all:.2f}s")

if __name__ == "__main__":
    ingest()
