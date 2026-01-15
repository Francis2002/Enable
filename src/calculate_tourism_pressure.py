import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import duckdb
import os

# Paths
CSV_PATH = "../data/Estabelecimentos_de_Alojamento_Local.csv"
GPKG_PATH = "../data/GRID1K21_CONT.gpkg"
DB_PATH = "../data/osm_analysis.db"

def calculate_tourism_pressure():
    print("Loading Alojamento Local data...")
    # Load CSV
    df_al = pd.read_csv(CSV_PATH)
    
    # Parse LatLong: "37,0657410006129 ; -7,82703800016578"
    def parse_latlong(s):
        try:
            lat_str, lon_str = s.split(";")
            lat = float(lat_str.replace(",", ".").strip())
            lon = float(lon_str.replace(",", ".").strip())
            return Point(lon, lat)
        except Exception as e:
            print(f"Error parsing LatLong '{s}': {e}")
            return None

    print("Parsing coordinates...")
    df_al['geometry'] = df_al['LatLong'].apply(parse_latlong)
    df_al = df_al.dropna(subset=['geometry'])
    
    # Create GeoDataFrame
    gdf_al = gpd.GeoDataFrame(df_al, geometry='geometry', crs="EPSG:4326")
    
    print("Loading population grid...")
    # Load Grid GPKG (contains population and geometry)
    gdf_grid = gpd.read_file(GPKG_PATH)
    
    # Reproject points to match grid CRS (EPSG:3035)
    print(f"Reprojecting points to {gdf_grid.crs}...")
    gdf_al_3035 = gdf_al.to_crs(gdf_grid.crs)
    
    print("Performing spatial join...")
    # Spatial Join: Points into Polygons
    # We want to know which cell each lodging establishment belongs to
    joined = gpd.sjoin(gdf_al_3035, gdf_grid[['GRD_ID2021_OFICIAL', 'N_INDIVIDUOS', 'geometry']], how='left', predicate='within')
    
    print("Aggregating capacity per cell...")
    # Aggregate NrUtentes per cell
    # Note: NrUtentes is capacity
    cell_lodging = joined.groupby('GRD_ID2021_OFICIAL')['NrUtentes'].sum().reset_index()
    cell_lodging.rename(columns={'NrUtentes': 'total_nr_utentes'}, inplace=True)
    
    print("Merging with population data...")
    # Merge back with the full grid to have population for all cells (even those without lodging)
    # Actually, the user wants a table with cell_id and tourism_pressure.
    # We should probably only include cells that have either population or lodging.
    
    # Get unique grid data
    grid_data = gdf_grid[['GRD_ID2021_OFICIAL', 'N_INDIVIDUOS']].copy()
    
    # Final merge
    tourism_df = grid_data.merge(cell_lodging, on='GRD_ID2021_OFICIAL', how='left')
    tourism_df['total_nr_utentes'] = tourism_df['total_nr_utentes'].fillna(0)
    
    print("Calculating tourism pressure...")
    # tourism_pressure = total_nr_utentes / N_INDIVIDUOS
    # Handing division by zero: if N_INDIVIDUOS is 0.
    # If pop is 0 but lodging > 0, pressure is technically infinite or very high.
    # If both are 0, pressure is 0.
    def compute_pressure(row):
        pop = row['N_INDIVIDUOS']
        utentes = row['total_nr_utentes']
        if pop > 0:
            return utentes / pop
        elif utentes > 0:
            return utentes  # Or some other indicator? For now, if pop=0 but lodging > 0, we just take the utentes number or set to null?
            # User said: "basically this metric is for each cell take both numbers and divide"
            # I will set it to utentes if pop is 0 but utentes exists, or handle if pop is 0.
            # Actually, let's keep it simple: if pop is 0, we can't really "divide" in a standard way.
            # I'll return the utentes count as a proxy or 0 if utentes is 0.
            return utentes 
        else:
            return 0.0

    tourism_df['tourism_pressure'] = tourism_df.apply(compute_pressure, axis=1)
    
    # Final Table: cell_id, tourism_pressure
    tourism_table = tourism_df[['GRD_ID2021_OFICIAL', 'tourism_pressure']].rename(columns={'GRD_ID2021_OFICIAL': 'cell_id'})
    
    # Only keep cells that have some activity (either pop > 0 or pressure > 0)
    # Actually, keep all cells that were in the original grid? 
    # The user said: "it will have only 2 variables, the cell_id and the tourism_pressure"
    
    print(f"Connecting to {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    
    print("Storing results in DuckDB...")
    con.execute("DROP TABLE IF EXISTS tourism")
    # Use the dataframe directly in DuckDB
    con.execute("CREATE TABLE tourism AS SELECT * FROM tourism_table")
    
    print(f"Successfully created 'tourism' table with {len(tourism_table)} rows.")
    con.close()

if __name__ == "__main__":
    calculate_tourism_pressure()
