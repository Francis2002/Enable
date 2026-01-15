import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import duckdb
import os

# Paths
AGREGADOS_CSV = "../data/Agregados_pub_2023.csv"
PASSIVOS_CSV = "../data/Sujeitos Passivos_pub_2023.csv"
CAOP_GPKG = "../data/Continente_CAOP2024_1.gpkg"
DB_PATH = "../data/osm_analysis.db"

def clean_value(val):
    if pd.isna(val) or val == 'nan':
        return None
    # Remove spaces and replace comma with dot
    s = str(val).replace(' ', '').replace(',', '.')
    try:
        return float(s)
    except:
        return None

def process_income_data():
    print("Loading Income CSVs...")
    # Header logic: skip first few rows as detected in research
    # Based on research, data rows start after metadata/headers. 
    # Usually around row 5-6. We can use the 'Município' in Col 1 to find relevant rows.
    
    def load_and_filter(path, col_mapping):
        df = pd.read_csv(path, sep=';', encoding='latin-1', header=None, dtype=str)
        
        valid_rows = []
        is_mainland = False
        for idx, row in df.iterrows():
            code = str(row[0]).strip()
            level = str(row[1]).strip()
            designation = str(row[2]).strip()
            
            # Use NUTS 1 Row to start/stop mainland detection
            if level == 'NUTS 1':
                if designation == 'Continente':
                    is_mainland = True
                else:
                    is_mainland = False
                    # Once we hit a non-mainland NUTS 1, we stop processing
                    break
            
            # Keep only Municipality level inside Mainland
            if is_mainland and level == 'Município':
                valid_rows.append(row)
        
        filtered_df = pd.DataFrame(valid_rows)
        # Apply mapping
        result = pd.DataFrame()
        result['dicc'] = filtered_df[0]
        result['municipio_name'] = filtered_df[2]
        for name, col_idx in col_mapping.items():
            result[name] = filtered_df[col_idx].apply(clean_value)
        return result

    # Col mapping based on Agregados_pub_2023.csv research
    # 3: Agregados Fiscais, 6: Rendimento Médio, 12: P50, 17: P90, 35: Gini
    agregados_mapping = {
        'num_households': 3,
        'avg_income': 6,
        'median_income': 12,
        'p90_income': 17,
        'gini_index': 35
    }
    
    # Col mapping based on Sujeitos Passivos_pub_2023.csv research
    # 3: Sujeitos Passivos
    passivos_mapping = {
        'num_taxpayers': 3
    }

    df_agr = load_and_filter(AGREGADOS_CSV, agregados_mapping)
    df_pas = load_and_filter(PASSIVOS_CSV, passivos_mapping)

    print(f"Extracted {len(df_agr)} mainland municipalities.")

    # Merge income datasets
    income_combined = df_agr.merge(df_pas[['dicc', 'num_taxpayers']], on='dicc', how='outer')
    
    # Imputation: Fill suppressed values with mainland minimums
    print("Performing conservative imputation for suppressed mainland values...")
    for col in ['median_income', 'p90_income', 'gini_index']:
        min_val = income_combined[col].min()
        null_mask = income_combined[col].isnull()
        if null_mask.any():
            count = null_mask.sum()
            print(f"  Imputing {count} values for {col} with Mainland Minimum: {min_val}")
            income_combined[col] = income_combined[col].fillna(min_val)

    print("Loading CAOP municipality polygons...")
    # Load CAOP municipalities
    gdf_muni = gpd.read_file(CAOP_GPKG, layer='cont_municipios')
    # Join with income data using 'dtmn' which is the DICC (first 4 digits of code)
    # Ensure types match
    gdf_muni['dicc'] = gdf_muni['dtmn'].astype(str)
    income_combined['dicc'] = income_combined['dicc'].astype(str)
    
    final_muni_gdf = gdf_muni.merge(income_combined, on='dicc', how='left')
    
    print("Connecting to DuckDB and loading grid spine...")
    con = duckdb.connect(DB_PATH)
    grid_df = con.execute("SELECT * FROM grid_spine").df()
    
    # Create GeoDataFrame for grid (taking centroids of the boxes)
    # The spine has min_lon, min_lat, max_lon, max_lat
    grid_df['lon'] = (grid_df['min_lon'] + grid_df['max_lon']) / 2
    grid_df['lat'] = (grid_df['min_lat'] + grid_df['max_lat']) / 2
    
    grid_gdf = gpd.GeoDataFrame(grid_df, geometry=gpd.points_from_xy(grid_df.lon, grid_df.lat), crs="EPSG:4326")
    
    # Reproject to match CAOP (EPSG:3763)
    grid_gdf_3763 = grid_gdf.to_crs(final_muni_gdf.crs)
    
    print("Assigning income data to grid cells...")
    # Spatial join: Grid centroids within Municipality polygons
    joined = gpd.sjoin(grid_gdf_3763, final_muni_gdf[['geometry', 'num_households', 'num_taxpayers', 'avg_income', 'median_income', 'p90_income', 'gini_index']], how='left', predicate='within')
    
    # Select final columns
    income_table = joined[['cell_id', 'num_households', 'num_taxpayers', 'avg_income', 'median_income', 'p90_income', 'gini_index']]
    
    print("Storing income table in DuckDB...")
    con.execute("DROP TABLE IF EXISTS income")
    con.execute("CREATE TABLE income AS SELECT * FROM income_table")
    
    print(f"Successfully created 'income' table with {len(income_table)} rows.")
    con.close()

if __name__ == "__main__":
    process_income_data()
