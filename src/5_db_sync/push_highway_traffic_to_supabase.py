import os
import geopandas as gpd
import pyogrio
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GPKG_PATH = os.path.join(BASE_DIR, "../../data/03_interim/highway_traffic.gpkg")
GPKG_PATH = os.path.abspath(GPKG_PATH)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_POSTGRES_URL")

import sys
import pandas as pd

# Add src to path to import extra_fallbacks
sys.path.insert(0, os.path.abspath(os.path.join(BASE_DIR, '../1_highway_mapping')))

def push_fallbacks(engine):
    """Pushes the extra_fallbacks.py dictionary to a Supabase table."""
    print("Reading EXTRA_FALLBACKS for backup...")
    try:
        from extra_fallbacks import EXTRA_FALLBACKS
    except ImportError as e:
        print(f"Failed to import EXTRA_FALLBACKS: {e}")
        return

    records = []
    for k, v in EXTRA_FALLBACKS.items():
        if isinstance(k, tuple) and len(k) == 2:
            highway, node = k
        else:
            highway, node = "GLOBAL", str(k)
        
        records.append({
            "highway_ref": highway,
            "node_name": node,
            "longitude": v[0],
            "latitude": v[1]
        })
        
    if records:
        df = pd.DataFrame(records)
        table_name = "traffic_fallbacks"
        print(f"Pushing table '{table_name}' to Supabase with {len(df)} records...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully pushed fallbacks to '{table_name}'.")

def push_highway_traffic():
    """Reads all highway layers from the GPKG and pushes them to Supabase."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_POSTGRES_URL environment variable is not set. Check your .env file.")

    if not os.path.exists(GPKG_PATH):
        raise FileNotFoundError(f"Database not found: {GPKG_PATH}")

    print(f"Connecting to Supabase...")
    engine = create_engine(SUPABASE_URL, connect_args={"sslmode": "require"})
    
    # Ensure PostGIS extension is enabled
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.commit()

    # Push fallbacks
    push_fallbacks(engine)

    print(f"Reading layers from {GPKG_PATH}...")
    layers = pyogrio.list_layers(GPKG_PATH)
    
    if len(layers) == 0:
        print("No layers found in the GeoPackage.")
        return

    print(f"Found {len(layers)} layers. Beginning push to Supabase...")
    
    for layer_info in layers:
        layer_name = layer_info[0]
        
        # Postgres table names should generally be lowercase and use underscores
        table_name = f"traffic_{layer_name.lower().replace('-', '_')}"
        
        print(f"Reading layer '{layer_name}' from GPKG...")
        gdf = gpd.read_file(GPKG_PATH, layer=layer_name)
        
        # Lowercase column names to avoid Postgres quoting issues
        gdf.columns = [col.lower() for col in gdf.columns]
        
        print(f"Pushing table '{table_name}' to Supabase (this may take a moment)...")
        # Push to PostGIS
        gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully pushed {len(gdf)} rows to '{table_name}'.")

    print("Finished pushing all highway layers to Supabase!")

if __name__ == "__main__":
    push_highway_traffic()
