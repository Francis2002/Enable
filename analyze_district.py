from pyrosm import OSM
import geopandas as gpd
import pandas as pd
from shapely.geometry import box
import numpy as np
import duckdb
import os
import argparse

def analyze_district(pbf_path, bbox, district_name, db_path):
    print(f"--- Analyzing {district_name} ---")
    
    # 1. Official EEA Grid Generation (Aligned to 1000m in EPSG:3035)
    extent_gdf = gpd.GeoDataFrame({"geometry": [box(*bbox)]}, crs="EPSG:4326").to_crs("EPSG:3035")
    b = extent_gdf.total_bounds
    xmin, ymin = np.floor(b[0] / 1000) * 1000, np.floor(b[1] / 1000) * 1000
    xmax, ymax = np.ceil(b[2] / 1000) * 1000, np.ceil(b[3] / 1000) * 1000
    
    xs = np.arange(xmin, xmax, 1000)
    ys = np.arange(ymin, ymax, 1000)
    grid_cells = []
    cell_ids = []
    for x in xs:
        for y in ys:
            grid_cells.append(box(x, y, x + 1000, y + 1000))
            cell_ids.append(f"1kmN{int(y)}E{int(x)}")
            
    grid = gpd.GeoDataFrame({"geometry": grid_cells, "cell_id": cell_ids}, crs="EPSG:3035")
    
    osm = OSM(pbf_path, bounding_box=bbox)
    
    # --- A. ROADS (Filter active cells) ---
    print("Extracting roads...")
    roads = osm.get_network(network_type="driving")
    if roads is None or roads.empty:
        print(f"No roads in {district_name}, skipping.")
        return

    roads_3035 = roads.to_crs("EPSG:3035")
    roads_subset = roads_3035[['highway', 'geometry']].copy()
    
    print("Calculating road intersections...")
    road_intersections = gpd.overlay(roads_subset, grid, how='intersection')
    road_intersections['length_m'] = road_intersections.geometry.length
    
    road_agg = road_intersections.groupby(['cell_id', 'highway'])['length_m'].sum().unstack(fill_value=0)
    road_stats = road_agg.copy()
    road_stats['total_road_len'] = road_stats.sum(axis=1)
    
    # CRITICAL: Filter out cells with no roads as requested
    active_cell_ids = road_stats.index.tolist()
    grid = grid[grid['cell_id'].isin(active_cell_ids)].copy()
    road_stats = road_stats.reset_index()
    print(f"Found {len(active_cell_ids)} active cells with roads.")

    # --- B. POIs ---
    print("Processing POIs...")
    pois = osm.get_pois()
    poi_stats = pd.DataFrame(columns=['cell_id'])
    if pois is not None and not pois.empty:
        pois_3035 = pois.to_crs("EPSG:3035")
        pois_in_cells = gpd.sjoin(pois_3035, grid, how="inner", predicate="within")
        
        poi_counts = []
        for key in ['amenity', 'shop', 'tourism']:
            if key in pois_in_cells.columns:
                p = pois_in_cells.dropna(subset=[key])
                if not p.empty:
                    c = p.groupby(['cell_id', key]).size().unstack(fill_value=0)
                    poi_counts.append(c)
        
        if poi_counts:
            poi_stats = pd.concat(poi_counts, axis=1).fillna(0).reset_index()

    # --- C. LAND USE & NATURAL ---
    print("Processing Land Use & Natural...")
    landuse = osm.get_landuse()
    natural = osm.get_natural()
    
    poly_stats = pd.DataFrame(columns=['cell_id'])
    for df, key in [(landuse, 'landuse'), (natural, 'natural')]:
        if df is not None and not df.empty:
            df_3035 = df.to_crs("EPSG:3035")
            # Overlay to cut polygons by grid
            poly_intersections = gpd.overlay(df_3035[[key, 'geometry']], grid, how='intersection')
            if not poly_intersections.empty:
                poly_intersections['area_m2'] = poly_intersections.geometry.area
                p_agg = poly_intersections.groupby(['cell_id', key])['area_m2'].sum().unstack(fill_value=0).reset_index()
                if poly_stats.empty:
                    poly_stats = p_agg
                else:
                    poly_stats = poly_stats.merge(p_agg, on='cell_id', how='outer').fillna(0)

    # --- D. ENTRY POINTS (ORIGINS) ---
    print("Calculating entry points...")
    grid_bounds = grid.copy()
    grid_bounds.geometry = grid.geometry.boundary
    entry_points = gpd.overlay(roads_3035[['highway', 'geometry']], grid_bounds, how='intersection', keep_geom_type=False)
    entry_points = entry_points[entry_points.geometry.type == 'Point']
    
    priority_map = {'motorway': 1, 'trunk': 2, 'primary': 3, 'secondary': 4, 'tertiary': 5, 'residential': 6}
    entry_points['priority'] = entry_points['highway'].map(priority_map).fillna(99)
    entry_points = entry_points.sort_values(['cell_id', 'priority'])
    
    top_3 = entry_points.drop_duplicates(subset=['cell_id', 'geometry']).groupby('cell_id').head(3)
    top_3_wgs84 = top_3.to_crs("EPSG:4326")
    top_3_wgs84['lon'] = top_3_wgs84.geometry.x
    top_3_wgs84['lat'] = top_3_wgs84.geometry.y
    origins = top_3_wgs84[['cell_id', 'lon', 'lat', 'highway', 'priority']]

    # --- STORAGE (DuckDB with UNION BY NAME) ---
    print("Saving to DuckDB...")
    con = duckdb.connect(db_path)
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # Function to append or create table with automatic schema expansion
    def store_table(con, name, df):
        if df.empty or 'cell_id' not in df.columns: return
        tmp_name = f"tmp_{name}"
        con.register(tmp_name, df)
        if con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{name}'").fetchone()[0] == 0:
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM {tmp_name}")
        else:
            # Union by name handles new columns automatically
            con.execute(f"CREATE TABLE {name}_new AS SELECT * FROM {name} UNION BY NAME SELECT * FROM {tmp_name}")
            con.execute(f"DROP TABLE {name}")
            con.execute(f"ALTER TABLE {name}_new RENAME TO {name}")
        con.unregister(tmp_name)

    store_table(con, "road_stats", road_stats)
    if not poi_stats.empty: store_table(con, "poi_stats", poi_stats)
    if not poly_stats.empty: store_table(con, "poly_stats", poly_stats)
    store_table(con, "cell_origins", origins)
    
    con.close()
    print(f"DONE: {district_name}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--district", required=True)
    parser.add_argument("--bbox", nargs=4, type=float, required=True)
    parser.add_argument("--pbf", default="portugal-latest.osm.pbf")
    parser.add_argument("--db", default="osm_analysis.db")
    args = parser.parse_args()
    analyze_district(args.pbf, args.bbox, args.district, args.db)
