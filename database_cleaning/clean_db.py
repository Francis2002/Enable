import duckdb

db_path = "../data/osm_analysis.db"
con = duckdb.connect(db_path)
tables = con.execute("SHOW TABLES").df()
for t in tables['name']:
    if t != 'grid_spine':
        print(f"Dropping table {t}...")
        con.execute(f"DROP TABLE {t}")
con.close()
print("Cleanup complete.")
