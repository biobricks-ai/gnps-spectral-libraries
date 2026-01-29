import pyarrow.parquet as pq
import os
import sys

def test_brick():
    print("Running brick tests...")
    brick_path = "brick/data.parquet"
    if not os.path.exists(brick_path):
        print(f"Error: {brick_path} not found")
        sys.exit(1)
        
    try:
        table = pq.read_table(brick_path)
        print(f"Table read successfully. Rows: {table.num_rows}, Columns: {table.num_columns}")
        
        if table.num_rows == 0:
            print("Error: Table is empty")
            sys.exit(1)
            
        if 'smiles' not in table.column_names:
            print("Error: SMILES column missing")
            sys.exit(1)
            
        print("Tests passed.")
        
    except Exception as e:
        print(f"Error reading parquet: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_brick()
