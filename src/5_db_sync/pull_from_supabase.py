import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
LOCAL_DB_PATH = "../../data/04_processed/pre_ml.db"
SUPABASE_URL = os.getenv("SUPABASE_POSTGRES_URL") # Format: postgresql://user:password@host:port/dbname

def pull_from_supabase():
    """Downloads all tables from Supabase and saves them to a local SQLite database."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_POSTGRES_URL environment variable is not set. Check your .env file.")

    print("Connecting to Supabase...")
    engine = create_engine(SUPABASE_URL)
    
    # Get all table names from Supabase
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print(f"Found {len(tables)} tables in Supabase: {tables}")
    
    print(f"Connecting to local DB: {LOCAL_DB_PATH}")
    # Ensure directory exists
    os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)
    sqlite_conn = sqlite3.connect(LOCAL_DB_PATH)

    for table_name in tables:
        print(f"Downloading table '{table_name}' from Supabase...")
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        
        print(f"Saving '{table_name}' locally to SQLite...")
        df.to_sql(table_name, sqlite_conn, if_exists='replace', index=False)
        print(f"Successfully downloaded {len(df)} rows to '{table_name}'.")

    print("Finished downloading all tables from Supabase!")

if __name__ == "__main__":
    pull_from_supabase()
