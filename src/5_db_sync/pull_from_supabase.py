import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DB_PATH = os.path.join(BASE_DIR, "../../data/04_processed/pre_ml.db")
LOCAL_DB_PATH = os.path.abspath(LOCAL_DB_PATH)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_POSTGRES_URL") # Format: postgresql://user:password@host:port/dbname

def pull_from_supabase():
    """Downloads all tables from Supabase and saves them to a local DuckDB database."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_POSTGRES_URL environment variable is not set. Check your .env file.")

    print("Connecting to Supabase...")
    engine = create_engine(SUPABASE_URL, connect_args={"sslmode": "require"})
    
    # Get all table names from Supabase
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print(f"Found {len(tables)} tables in Supabase: {tables}")
    
    print(f"Connecting to local DuckDB: {LOCAL_DB_PATH}")
    # Ensure directory exists
    os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)
    duck_conn = duckdb.connect(LOCAL_DB_PATH)

    for table_name in tables:
        print(f"Downloading table '{table_name}' from Supabase...")
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        
        print(f"Saving '{table_name}' locally to DuckDB...")
        # DuckDB can directly query pandas DataFrames registered in the local scope
        duck_conn.register("temp_df", df)
        duck_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        duck_conn.unregister("temp_df")
        
        print(f"Successfully downloaded {len(df)} rows to '{table_name}'.")

    duck_conn.close()
    print("Finished downloading all tables from Supabase!")

if __name__ == "__main__":
    pull_from_supabase()
