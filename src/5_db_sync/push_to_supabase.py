import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DB_PATH = os.path.join(BASE_DIR, "../../data/04_processed/pre_ml.db")
LOCAL_DB_PATH = os.path.abspath(LOCAL_DB_PATH)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_POSTGRES_URL") # Format: postgresql://user:password@host:port/dbname

def push_to_supabase():
    """Reads all tables from local DuckDB and pushes them to Supabase."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_POSTGRES_URL environment variable is not set. Check your .env file.")

    print(f"Connecting to local DuckDB: {LOCAL_DB_PATH}")
    duck_conn = duckdb.connect(LOCAL_DB_PATH)
    
    # Get all table names from DuckDB
    tables_df = duck_conn.execute("SHOW TABLES").df()
    if 'name' in tables_df.columns:
        tables = tables_df['name'].tolist()
    else:
        # Fallback for some duckdb versions
        tables = tables_df.iloc[:, 0].tolist()

    print(f"Found {len(tables)} tables: {tables}")
    
    # Connect to Supabase PostgreSQL using SQLAlchemy
    print("Connecting to Supabase...")
    engine = create_engine(SUPABASE_URL, connect_args={"sslmode": "require"})
    
    for table_name in tables:
        print(f"Reading table '{table_name}' from DuckDB...")
        df = duck_conn.execute(f"SELECT * FROM {table_name}").df()
        
        print(f"Pushing table '{table_name}' to Supabase (this may take a while)...")
        # if_exists='replace' will drop the table if it exists and recreate it.
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully pushed {len(df)} rows to '{table_name}'.")

    duck_conn.close()
    print("Finished pushing all tables to Supabase!")

if __name__ == "__main__":
    push_to_supabase()
