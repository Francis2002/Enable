import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
LOCAL_DB_PATH = "../../data/04_processed/pre_ml.db"
SUPABASE_URL = os.getenv("SUPABASE_POSTGRES_URL") # Format: postgresql://user:password@host:port/dbname

def push_to_supabase():
    """Reads all tables from local SQLite DB and pushes them to Supabase."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_POSTGRES_URL environment variable is not set. Check your .env file.")

    print(f"Connecting to local DB: {LOCAL_DB_PATH}")
    sqlite_conn = sqlite3.connect(LOCAL_DB_PATH)
    
    # Get all table names from SQLite
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql(query, sqlite_conn)['name'].tolist()

    print(f"Found {len(tables)} tables: {tables}")
    
    # Connect to Supabase PostgreSQL using SQLAlchemy
    print("Connecting to Supabase...")
    engine = create_engine(SUPABASE_URL)
    
    for table_name in tables:
        print(f"Reading table '{table_name}' from SQLite...")
        df = pd.read_sql(f"SELECT * FROM {table_name}", sqlite_conn)
        
        print(f"Pushing table '{table_name}' to Supabase (this may take a while)...")
        # if_exists='replace' will drop the table if it exists and recreate it. 
        # For production, consider 'append' or custom merge logic.
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully pushed {len(df)} rows to '{table_name}'.")

    print("Finished pushing all tables to Supabase!")

if __name__ == "__main__":
    push_to_supabase()
