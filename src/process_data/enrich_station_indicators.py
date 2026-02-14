import duckdb
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os

# Configuration
PRE_ML_DB = "../../data/pre_ml.db"
GRID_GPKG = "../../data/GRID1K21_CONT.gpkg"
CAOP_GPKG = "../../data/Continente_CAOP2024_1.gpkg"
TOURISM_CSV = "../../data/Estabelecimentos_de_Alojamento_Local.csv"
AGREGADOS_CSV = "../../data/Agregados_pub_2023.csv"
PASSIVOS_CSV = "../../data/Sujeitos Passivos_pub_2023.csv"

def clean_value(val):
    if pd.isna(val) or val == 'nan':
        return None
    s = str(val).replace(' ', '').replace(',', '.')
    try:
        return float(s)
    except:
        return None

def parse_latlong(s):
    try:
        lat_str, lon_str = s.split(";")
        lat = float(lat_str.replace(",", ".").strip())
        lon = float(lon_str.replace(",", ".").strip())
        return Point(lon, lat)
    except Exception:
        return None

def load_and_filter_income(path, col_mapping):
    df = pd.read_csv(path, sep=';', encoding='latin-1', header=None, dtype=str)
    valid_rows = []
    is_mainland = False
    for idx, row in df.iterrows():
        code = str(row[0]).strip()
        level = str(row[1]).strip()
        designation = str(row[2]).strip()
        if level == 'NUTS 1':
            if designation == 'Continente':
                is_mainland = True
            else:
                is_mainland = False
                break
        if is_mainland and level == 'Município':
            valid_rows.append(row)
    filtered_df = pd.DataFrame(valid_rows)
    result = pd.DataFrame()
    result['dicc'] = filtered_df[0]
    result['municipio_name'] = filtered_df[2]
    for name, col_idx in col_mapping.items():
        result[name] = filtered_df[col_idx].apply(clean_value)
    return result

def enrich_indicators():
    print("Starting Consolidated Station Indicators Enrichment...")
    
    # 1. Load Stations from pre_ml.db
    print("Loading stations...")
    con = duckdb.connect(PRE_ML_DB)
    df_stations = con.execute("SELECT station_id, LATITUDE, LONGITUDE FROM coordinates").df()
    con.close()
    
    stations_gdf = gpd.GeoDataFrame(
        df_stations, 
        geometry=gpd.points_from_xy(df_stations.LONGITUDE, df_stations.LATITUDE),
        crs="EPSG:4326"
    )

    # 2. Get Cell Mapping and Population
    print("Mapping stations to grid cells and fetching population...")
    gdf_grid = gpd.read_file(GRID_GPKG)
    stations_3035 = stations_gdf.to_crs(gdf_grid.crs)
    stations_with_grid = gpd.sjoin(stations_3035, gdf_grid[['GRD_ID2021_OFICIAL', 'N_INDIVIDUOS', 'geometry']], how='left', predicate='within')
    stations_with_grid = stations_with_grid.rename(columns={'GRD_ID2021_OFICIAL': 'cell_id', 'N_INDIVIDUOS': 'population'})

    # 3. Calculate Tourism Pressure for each cell
    print("Calculating tourism pressure...")
    df_al = pd.read_csv(TOURISM_CSV)
    df_al['geometry'] = df_al['LatLong'].apply(parse_latlong)
    df_al = df_al.dropna(subset=['geometry'])
    gdf_al = gpd.GeoDataFrame(df_al, geometry='geometry', crs="EPSG:4326")
    gdf_al_3035 = gdf_al.to_crs(gdf_grid.crs)
    
    al_joined = gpd.sjoin(gdf_al_3035, gdf_grid[['GRD_ID2021_OFICIAL', 'geometry']], how='left', predicate='within')
    cell_lodging = al_joined.groupby('GRD_ID2021_OFICIAL')['NrUtentes'].sum().reset_index()
    cell_lodging.rename(columns={'NrUtentes': 'total_capacity', 'GRD_ID2021_OFICIAL': 'cell_id'}, inplace=True)
    
    # Merge tourism capacity with full grid to calculate pressure
    tourism_stats = gdf_grid[['GRD_ID2021_OFICIAL', 'N_INDIVIDUOS']].copy().rename(columns={'GRD_ID2021_OFICIAL': 'cell_id', 'N_INDIVIDUOS': 'pop'})
    tourism_stats = tourism_stats.merge(cell_lodging, on='cell_id', how='left').fillna(0)
    
    def compute_pressure(row):
        pop = row['pop']
        cap = row['total_capacity']
        if pop > 0: return cap / pop
        elif cap > 0: return cap
        return 0.0
    
    tourism_stats['tourism_pressure'] = tourism_stats.apply(compute_pressure, axis=1)

    # 4. Get Municipality Mapping and Income Stats
    print("Mapping stations to municipalities and fetching income stats...")
    gdf_muni = gpd.read_file(CAOP_GPKG, layer='cont_municipios')
    stations_3763 = stations_gdf.to_crs(gdf_muni.crs)
    stations_with_muni = gpd.sjoin(stations_3763, gdf_muni[['dtmn', 'geometry']], how='left', predicate='within')
    stations_with_muni = stations_with_muni.rename(columns={'dtmn': 'dicc'})
    
    agregados_mapping = {'num_households': 3, 'avg_income': 6, 'median_income': 12, 'p90_income': 17, 'gini_index': 35}
    passivos_mapping = {'num_taxpayers': 3}
    
    df_agr = load_and_filter_income(AGREGADOS_CSV, agregados_mapping)
    df_pas = load_and_filter_income(PASSIVOS_CSV, passivos_mapping)
    income_combined = df_agr.merge(df_pas[['dicc', 'num_taxpayers']], on='dicc', how='outer')
    
    # Imputation
    for col in ['median_income', 'p90_income', 'gini_index']:
        min_val = income_combined[col].min()
        income_combined[col] = income_combined[col].fillna(min_val)

    # 5. Combine Results
    print("Merging all indicators...")
    # Merge station-to-grid with tourism stats
    stations_final = stations_with_grid[['station_id', 'cell_id', 'population']].merge(tourism_stats[['cell_id', 'tourism_pressure']], on='cell_id', how='left')
    
    # Merge with station-to-muni and then with income stats
    stations_final = stations_final.merge(stations_with_muni[['station_id', 'dicc']], on='station_id', how='left')
    stations_final = stations_final.merge(income_combined.drop(columns=['municipio_name']), on='dicc', how='left')
    
    # Drop mapping IDs
    stations_final = stations_final.drop(columns=['cell_id', 'dicc'])
    
    # 6. Save to pre_ml.db (Wide Format)
    print("Saving wide-format indicators to pre_ml.db...")
    
    con = duckdb.connect(PRE_ML_DB)
    con.execute("CREATE OR REPLACE TABLE indicators AS SELECT * FROM stations_final")
    con.close()
    print(f"Success: Indicators saved to pre_ml.db with {len(stations_final)} stations and {len(stations_final.columns)-1} indicator columns.")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    enrich_indicators()
