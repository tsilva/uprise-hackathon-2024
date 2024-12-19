import json
import shutil
from pathlib import Path
from typing import Dict, Any

# [x] - calculates percentage of filled fields
# [x] - calculates percentage of correct primary key values
# [ ] - calculates percentage of valid foreign key integrity
# [ ] - calculates the number of fields that match their data type and regex
# [ ] - calculate final score
# would be nice: check if values are semantically correct

#!completeness: percentage of non-missing values
#consistency: percentage of field that match same data type, then regex
#uniqueness: duplicate rows
#redundancy: redundant data detection
#accuracy: are values faithful to ground truth

def load_table(table_name: str) -> List[Dict[str, Any]]:
    datasets_dir = "datasets/Synthea27Nj_5.4"
    table_file = Path(__file__).parent / datasets_dir / f'{table_name}.csv'
    with open(table_file, 'r') as f:
        return json.load(f)

def load_json_file(filepath: str) -> Dict[str, Any]:
    with open(filepath, 'r') as f:
        return json.load(f)

def calculate_column_quality(table_name, source_column_data, column_data: Dict[str, Any]):
    column_data["data_quality"] = {
        "non_empty_percentage" : source_column_data["non_empty_percentage"]
    } 
    if column_data.get("primary_key"):
        column_data["data_quality"]["primary_key_uniqueness_percentage"] = source_column_data["unique_values_percentage"]
        
    if column_data.get("foreign_key"):
        foreign_key = column_data["foreign_key"]
        foreign_table_name = foreign_key["table"]
        foreign_column_name = foreign_key["column"]
        table = load_table(table_name)
        foreign_table = load_table(foreign_table_name)
        foreign_key_integrity_percentage = sum([1 for row in table if row[column_name] in foreign_table[foreign_column_name]]) / len(table)
        column_data["data_quality"]["primary_key_uniqueness_percentage"] = foreign_key_integrity_percentage

def calculate_table_quality(table_name, table_data: Dict[str, Any]):
    table_data["data_quality"] = {
        "non_empty_percentage": sum([column["data_quality"]["non_empty_percentage"] for column in table_data["columns"].values()]) / len(table_data["columns"]),
        "primary_key_uniqueness_percentage": sum([column["data_quality"]["primary_key_uniqueness_percentage"] for column in table_data["columns"].values() if column.get("data_quality", {}).get("primary_key_uniqueness_percentage") is not None]) / len([column for column in table_data["columns"].values() if column.get("data_quality", {}).get("primary_key_uniqueness_percentage") is not None]),
        "foreign_key_integrity_percentage" : sum([column["data_quality"]["foreign_key_integrity_percentage"] for column in table_data["columns"].values() if column.get("data_quality", {}).get("foreign_key_integrity_percentage") is not None]) / len([column for column in table_data["columns"].values() if column.get("data_quality", {}).get("foreign_key_integrity_percentage") is not None])
    }

def generate_quality_schema():
    schema_dir = Path(__file__).parent / 'schema'
    schema_file = schema_dir / 'schema.json'
    output_file = schema_dir / 'schema_quality.json'
    shutil.copy(schema_file, output_file)
    
    # Load original schema
    schema_data = load_json_file(schema_file)
    output_data = load_json_file(output_file)

    for table_name, table_data in schema_data.items():
        table_file = schema_dir / 'tables' / f'{table_name}.json'
        table_data = load_json_file(table_file)
        for column, column_data in table_data.get('fields', {}).items():
            target_column_data = output_data[table_name]['columns'][column]
            calculate_column_quality(column_data, target_column_data)
        target_table = output_data[table_name]
        calculate_table_quality(target_table)
        
    # Save quality schema
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)

if __name__ == '__main__':
    generate_quality_schema()
