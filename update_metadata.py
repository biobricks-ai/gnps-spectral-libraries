import pyarrow.parquet as pq
import json

parquet_path = "brick/data.parquet"
json_path = ".bb/datapackage.json"

table = pq.read_table(parquet_path)
columns = []

# Map of known property URLs
prop_map = {
    'smiles': 'http://semanticscience.org/resource/CHEMINF_000018',
    'inchi': 'http://semanticscience.org/resource/CHEMINF_000113',
    'inchikey': 'http://semanticscience.org/resource/CHEMINF_000059',
    'cas_number': 'http://semanticscience.org/resource/CHEMINF_000446',
    'exact_mass': 'http://semanticscience.org/resource/CHEMINF_000216',
    'precursor_mz': 'http://purl.obolibrary.org/obo/MS_1000040'
}

for name in table.column_names:
    col_def = {
        "name": name,
        "titles": name.replace("_", " ").title(),
        "datatype": "string"
    }
    
    # refine types
    # We converted everything to string in build script, so datatype is string.
    # But ideally we describe what it should be.
    # But CSVW validation checks against actual data. Parquet has strings.
    # So "string" is correct.
    
    if name in prop_map:
        col_def["propertyUrl"] = prop_map[name]
        
    if name == 'spectrum_id':
        col_def["required"] = True
        
    columns.append(col_def)

with open(json_path, 'r') as f:
    pkg = json.load(f)

pkg['tables'][0]['tableSchema']['columns'] = columns

with open(json_path, 'w') as f:
    json.dump(pkg, f, indent=2)

print("Updated datapackage.json")
