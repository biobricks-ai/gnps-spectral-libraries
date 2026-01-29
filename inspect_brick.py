import pyarrow.parquet as pq
import json

path = "brick/data.parquet"
table = pq.read_table(path)
print(f"Rows: {table.num_rows}")
print(f"Columns: {table.column_names}")

# Check for SMILES
if 'smiles' in table.column_names:
    print("SMILES column found.")
else:
    print("SMILES column NOT found.")

# Check for duplicates or issues
df = table.to_pandas()
print(df.head())
print(df['smiles'].head())
