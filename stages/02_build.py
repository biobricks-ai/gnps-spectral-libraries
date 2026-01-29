import pandas as pd
import json
import os
import re
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import traceback

input_path = "download/ALL_GNPS.json"
output_path = "brick/data.parquet"
CHUNK_SIZE = 10000

def normalize_column_name(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower().replace(" ", "_").replace(".", "")

def deduplicate_columns(columns):
    seen = {}
    new_columns = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_columns.append(col)
    return new_columns

def process_data():
    print(f"Reading {input_path} incrementally...")
    
    writer = None
    schema = None
    
    buffer = []
    count = 0
    
    try:
        with open(input_path, 'rb') as f:
            parser = ijson.items(f, 'item')
            
            for record in parser:
                buffer.append(record)
                count += 1
                
                if len(buffer) >= CHUNK_SIZE:
                    print(f"Writing chunk at {count}...")
                    schema, writer = write_chunk(buffer, schema, writer)
                    buffer = []
                    
            if buffer:
                print(f"Writing final chunk at {count}...")
                schema, writer = write_chunk(buffer, schema, writer)

        if writer:
            writer.close()
        
        print("Build complete.")
        
    except Exception as e:
        print(f"Error in process_data: {e}")
        traceback.print_exc()
        sys.exit(1)

def write_chunk(data, schema, writer):
    try:
        df = pd.DataFrame(data)
        
        # Normalize columns
        df.columns = [normalize_column_name(c) for c in df.columns]
        
        # Deduplicate columns
        df.columns = deduplicate_columns(df.columns)
        
        # Ensure 'smiles' column exists if possible
        possible_smiles = [c for c in df.columns if 'smiles' in c]
        if possible_smiles and 'smiles' not in df.columns:
             # Just take the first one
             df['smiles'] = df[possible_smiles[0]]
        
        # Convert all object columns to string
        for col in df.select_dtypes(['object']).columns:
            df[col] = df[col].astype(str)
            
        # Align with schema
        if schema:
            # 1. Add missing cols
            for name in schema.names:
                if name not in df.columns:
                    df[name] = None
            
            # 2. Reorder
            df = df[schema.names]
        
        table = pa.Table.from_pandas(df, schema=schema)
        
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(output_path, schema)
            
        writer.write_table(table)
        return schema, writer
        
    except Exception as e:
        print(f"Error in write_chunk: {e}")
        traceback.print_exc()
        raise e

if __name__ == "__main__":
    process_data()
