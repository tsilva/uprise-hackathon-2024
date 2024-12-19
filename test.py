from pathlib import Path
import json

def process_schema(schema_path: Path) -> None:
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    
    if schema.get('record_count', 0) == 0:
        return

    # Ensure consistent field ordering
    ordered_schema = {
        "table_name": schema["table_name"],
        "record_count": schema["record_count"],
        "primary_key": f"{schema['table_name'].lower()}_id",  # Convention-based primary key
        "description": schema.get("description", ""),
        "fields": schema["fields"]
    }

    with open(schema_path, 'w') as f:
        json.dump(ordered_schema, f, indent=2)

# Process all schema files
schema_dir = Path("schema/tables")
if not schema_dir.exists():
    raise ValueError(f"Schema directory not found: {schema_dir}")

for schema_file in schema_dir.glob("*.json"):
    process_schema(schema_file)