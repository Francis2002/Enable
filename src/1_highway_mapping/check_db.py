
import sqlite3

def check_db_integrity(db_path):
    """Checks if the SQLite database file is valid."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print("Database is valid and readable.")
        conn.close()
        return True
    except sqlite3.DatabaseError as e:
        print(f"Database integrity check failed: {e}")
        return False

if __name__ == "__main__":
    db_file = "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/data/04_processed/pre_ml.db"
    check_db_integrity(db_file)
