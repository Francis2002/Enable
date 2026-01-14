import duckdb

db_path = "../data/osm_analysis.db"
con = duckdb.connect(db_path)

tables = ['road_stats', 'poi_stats', 'poly_stats']

for table in tables:
    print(f"Sanitizing table: {table}")
    # Check if table exists
    exists = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'").fetchone()[0]
    if exists == 0:
        print(f"  Table {table} does not exist yet. Skipping.")
        continue
    
    # Get columns
    cols = con.execute(f"DESCRIBE {table}").df()
    
    # We only want to update numeric columns
    numeric_types = ['DOUBLE', 'FLOAT', 'BIGINT', 'INTEGER', 'HUGEINT']
    
    updates = []
    for _, row in cols.iterrows():
        col_name = row['column_name']
        col_type = row['column_type']
        
        if col_type in numeric_types and col_name != 'cell_id':
            updates.append(f"\"{col_name}\" = COALESCE(\"{col_name}\", 0.0)")
    
    if updates:
        update_query = f"UPDATE {table} SET {', '.join(updates)}"
        con.execute(update_query)
        print(f"  Updated {len(updates)} columns in {table}.")

con.close()
print("Database sanitation complete.")
