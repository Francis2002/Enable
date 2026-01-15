import duckdb
import geopandas as gpd
import pandas as pd
from shapely.geometry import box, Point
from pyproj import Transformer
import pyrosm
import os
import warnings

warnings.filterwarnings('ignore')

OSM_FILE = "../portugal-latest.osm.pbf"
DB_PATH = "../data/osm_analysis.db"

def get_block_bounds(min_x, min_y):
    return (min_x, min_y, min_x + 10000, min_y + 10000)

def backfill():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return

    con = duckdb.connect(DB_PATH)
    
    # Get all processed cells
    print("Fetching existing cells...")
    cells_df = con.execute("SELECT * FROM cell_stats").df()
    
    # Identify unique 10km blocks they belong to
    # Assuming cell_id format RES1kmN{y}E{x} where x,y are in km
    # But easier to use the coordinate columns if available. 
    # If not, let's use the min/max lat/lon or just parse the ID if needed.
    # Fortunately cell_info usually has x_3035/y_3035 in generic generation, but here we might rely on re-fetching logic.
    # Wait, process_cell_logic receives cell_info dict with coords.
    
    # Let's iterate by blocks to be efficient
    # We need the grid spine or just derive blocks from cell coordinates in DB
    # Let's check columns
    cols = cells_df.columns
    if 'x_3035' not in cols:
        print("Cannot calculate geometry without EPSG:3035 coordinates in DB.")
        return

    cells_df['block_x'] = (cells_df['x_3035'] // 10000) * 10000
    cells_df['block_y'] = (cells_df['y_3035'] // 10000) * 10000
    
    blocks = cells_df.drop_duplicates(subset=['block_x', 'block_y'])
    print(f"Found {len(blocks)} blocks to process.")

    transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
    new_origins = []

    for _, block in blocks.iterrows():
        bx, by = block['block_x'], block['block_y']
        print(f"Processing Block {bx}, {by}...")
        
        # Load Roads for this block
        bounds = (bx, by, bx+10000, by+10000) # xmin, ymin, xmax, ymax
        try:
            osm = pyrosm.OSM(OSM_FILE, bounding_box=box(*bounds))
            roads = osm.get_network(network_type="driving")
            if roads is None: continue
            roads = roads.to_crs("EPSG:3035")
        except Exception as e:
            print(f"Error loading block {bounds}: {e}")
            continue

        # Process cells in this block
        block_cells = cells_df[(cells_df['block_x'] == bx) & (cells_df['block_y'] == by)]
        
        for idx, cell in block_cells.iterrows():
            # Define cell geometry
            cell_poly = box(cell['x_3035'], cell['y_3035'], cell['x_3035']+1000, cell['y_3035']+1000)
            
            # Clip roads
            try:
                # Spatial index query first for speed
                potential = roads.iloc[roads.sindex.query(cell_poly, predicate="intersects")]
                if potential.empty: continue
                
                cell_roads = gpd.clip(potential, cell_poly)
                if cell_roads.empty: continue
                
                # Logic: Internal Point
                centroid = cell_poly.centroid
                all_roads = cell_roads.geometry.unary_union
                
                if not all_roads.is_valid:
                    all_roads = all_roads.buffer(0)
                
                # Closest point on road network to centroid
                nearest = all_roads.interpolate(all_roads.project(centroid))
                lon, lat = transformer.transform(nearest.x, nearest.y)
                
                new_origins.append({
                    'cell_id': cell['cell_id'],
                    'lon': lon,
                    'lat': lat,
                    'highway': 'internal',
                    'priority': 0.0
                })
            except Exception as e:
                pass
                
    # Save to DB
    if new_origins:
        print(f"Adding {len(new_origins)} internal origins...")
        odf = pd.DataFrame(new_origins)
        # Append to cell_origins
        con.execute("INSERT INTO cell_origins SELECT * FROM odf")
        print("Done.")
    else:
        print("No new internal origins found.")
    
    con.close()

if __name__ == "__main__":
    backfill()
