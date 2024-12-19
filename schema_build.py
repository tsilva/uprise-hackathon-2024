import os
import csv
import json
from pathlib import Path
from collections import Counter, defaultdict
from typing import Dict, Set

def analyze_csv_files(directory):
    # Track headers per file and overall counts
    header_counts = Counter()
    headers_by_file = {}
    
    # Process all CSV files
    csv_files = Path(directory).glob('*.csv')
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                file_name = csv_file.stem  # Get filename without extension
                headers_by_file[file_name] = set(headers)
                header_counts.update(headers)
                print(f"Processed: {csv_file.name}")
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
    
    # Print header statistics
    print("\nHeader occurrence across all files:")
    print("-" * 50)
    for header, count in sorted(header_counts.items()):
        print(f"{header}: {count} occurrences")
    print(f"\nTotal unique headers: {len(header_counts)}")
    
    # Analyze table connections
    print("\nTable Connections:")
    print("-" * 50)
    for table1, headers1 in headers_by_file.items():
        for table2, headers2 in headers_by_file.items():
            if table1 >= table2:  # Skip self-connections and duplicates
                continue
            shared_columns = headers1.intersection(headers2)
            if shared_columns:
                print(f"{table1} <-> {table2}")
                print(f"Shared columns: {', '.join(sorted(shared_columns))}\n")

def calculate_overlap_stats(table_values: dict) -> dict:
    """Calculate overlap statistics for values across tables."""
    if len(table_values) <= 1:
        return None
    
    # Convert all values to sets for efficient intersection/union operations
    table_sets = {table: set(values) for table, values in table_values.items()}
    
    # Calculate overlaps between each pair of tables
    overlap_stats = {
        "total_unique_values": len(set.union(*table_sets.values())),
        "shared_values": {},
        "overlap_percentages": {}
    }
    
    tables = list(table_sets.keys())
    for i, table1 in enumerate(tables):
        for table2 in tables[i+1:]:
            set1, set2 = table_sets[table1], table_sets[table2]
            intersection = set1.intersection(set2)
            
            if intersection:
                key = f"{table1}__{table2}"
                overlap_stats["shared_values"][key] = list(intersection)
                # Calculate overlap percentage based on smaller table
                smaller_size = min(len(set1), len(set2))
                overlap_percentage = (len(intersection) / smaller_size) * 100
                overlap_stats["overlap_percentages"][key] = round(overlap_percentage, 2)
    
    return overlap_stats

def calculate_field_stats(values: list) -> dict:
    """Calculate statistics for a field."""
    total_count = len(values)
    non_empty_count = sum(1 for v in values if v.strip() != '')
    return {
        "total_values": total_count,
        "non_empty_values": non_empty_count,
        "non_empty_percentage": round((non_empty_count / total_count * 100), 2) if total_count > 0 else 0,
        "unique_values": len(set(values)),
    }

def build_column_schema(directory: str):
    # Dictionary to store column information
    column_data = defaultdict(lambda: defaultdict(set))
    
    # Process all CSV files
    csv_files = Path(directory).glob('*.csv')
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                table_name = csv_file.stem
                
                # Create column index mapping
                column_indices = {header: idx for idx, header in enumerate(headers)}
                
                # Process each row
                for row in csv_reader:
                    for header, idx in column_indices.items():
                        if idx < len(row):  # Protect against malformed rows
                            column_data[header][table_name].add(row[idx])
                
                print(f"Processed: {csv_file.name}")
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
    
    # Create schema directory with columns subdirectory
    schema_dir = Path("schema/columns")
    schema_dir.mkdir(parents=True, exist_ok=True)
    
    # Create JSON files for each column
    for column_name, table_data in column_data.items():
        # Convert sets to lists for JSON serialization
        table_values = {
            table: list(values) 
            for table, values in table_data.items()
        }
        
        # Calculate overlap statistics if column appears in multiple tables
        overlap_stats = calculate_overlap_stats(table_values)
        
        json_data = {
            "column_name": column_name,
            "appears_in_tables": list(table_data.keys()),
            "values_by_table": table_values,
            "overlap_analysis": overlap_stats
        }
        
        # Create sanitized filename
        safe_filename = "".join(c if c.isalnum() else "_" for c in column_name)
        json_path = schema_dir / f"{safe_filename}.json"
        
        # Write JSON file
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2)
            
        print(f"Created schema file: {json_path}")

def build_table_schema(directory: str):
    """Build schema files for each table."""
    schema_dir = Path("schema/tables")
    schema_dir.mkdir(parents=True, exist_ok=True)
    
    csv_files = Path(directory).glob('*.csv')
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                
                # Initialize data collection
                table_data = {
                    "table_name": csv_file.stem,
                    "record_count": 0,
                    "fields": {header: [] for header in headers}
                }
                
                # Collect all values for each field
                for row in csv_reader:
                    table_data["record_count"] += 1
                    for i, value in enumerate(row):
                        if i < len(headers):
                            table_data["fields"][headers[i]].append(value)
                
                # Calculate statistics for each field
                field_stats = {}
                for field_name, values in table_data["fields"].items():
                    field_stats[field_name] = calculate_field_stats(values)
                
                # Prepare final schema
                schema = {
                    "table_name": table_data["table_name"],
                    "record_count": table_data["record_count"],
                    "fields": field_stats
                }
                
                # Write schema file
                json_path = schema_dir / f"{csv_file.stem}.json"
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(schema, f, indent=2)
                
                print(f"Created table schema: {json_path}")
                
        except Exception as e:
            print(f"Error processing table {csv_file}: {str(e)}")

def build_schema(directory: str):
    """Build both column and table schemas."""
    build_column_schema(directory)
    build_table_schema(directory)

if __name__ == "__main__":
    datasets_dir = "datasets/Synthea27Nj_5.4"
    build_schema(datasets_dir)


