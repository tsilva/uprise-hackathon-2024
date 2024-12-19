import os
import csv
import json
from pathlib import Path
from collections import Counter, defaultdict
from typing import Dict, Set
from datetime import datetime
from statistics import mean, median
from collections import Counter

def is_primary_key(column_name, table_name):
    return column_name.lower() == f"{table_name.lower()}_id"

def is_foreign_key(column_name, table_name, table_names):
    for _table_name in table_names:
        if _table_name.lower() == table_name.lower(): continue
        if column_name.lower() == f"{_table_name.lower()}_id": return True
    return False

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

def is_numeric(value: str) -> bool:
    """Check if a string value can be converted to a number."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def is_date(value: str) -> bool:
    """Try to parse a string as a date using common formats."""
    date_formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"
    ]
    for fmt in date_formats:
        try:
            datetime.strptime(value.strip(), fmt)
            return True
        except (ValueError, AttributeError):
            continue
    return False

def calculate_field_stats(values: list) -> dict:
    """Calculate enhanced statistics for a field."""
    total_count = len(values)
    if total_count == 0:
        return {"total_values": 0}

    # Basic counts
    non_empty_values = [v for v in values if v.strip() != '']
    non_empty_count = len(non_empty_values)
    unique_values = set(values)
    unique_count = len(unique_values)

    # Value frequencies
    value_counts = Counter(values)
    most_common = value_counts.most_common(5)
    
    # Initialize stats dictionary
    stats = {
        "total_values": total_count,
        "non_empty_values": non_empty_count,
        "non_empty_percentage": round((non_empty_count / total_count * 100), 2) if total_count > 0 else 0,
        "unique_values": unique_count,
        "unique_values_percentage": round((unique_count / total_count * 100), 2) if total_count > 0 else 0,
        "null_or_empty_count": sum(1 for v in values if v.lower() in ('null', 'none', '')),
        "whitespace_only_count": sum(1 for v in values if v.strip() == '' and v != ''),
        "most_common_values": {str(v): c for v, c in most_common}
    }

    # Length statistics - only if we have non-empty values
    if non_empty_values:
        lengths = [len(str(v)) for v in non_empty_values]
        stats["length_stats"] = {
            "min": min(lengths),
            "max": max(lengths),
            "average": round(mean(lengths), 2)
        }
    else:
        stats["length_stats"] = {
            "min": 0,
            "max": 0,
            "average": 0
        }

    # Check if values are numeric
    numeric_values = [float(v) for v in non_empty_values if is_numeric(v)]
    if numeric_values:  # Only add numeric stats if we have numeric values
        stats["numeric_stats"] = {
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": round(mean(numeric_values), 2),
            "median": round(median(numeric_values), 2)
        }

    # Check if values are dates
    if non_empty_values:  # Only check dates if we have non-empty values
        date_samples = [v for v in non_empty_values[:100] if is_date(v)]
        if date_samples and len(date_samples) >= 0.5 * min(len(non_empty_values[:100]), 100):
            dates = []
            for v in non_empty_values:
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        dates.append(datetime.strptime(v.strip(), fmt))
                        break
                    except (ValueError, AttributeError):
                        continue
            if dates:
                stats["date_stats"] = {
                    "min_date": min(dates).strftime("%Y-%m-%d"),
                    "max_date": max(dates).strftime("%Y-%m-%d"),
                    "distinct_years": len({d.year for d in dates})
                }

    return stats

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
    csv_files = [csv_file for csv_file in csv_files]
    table_names = [csv_file.stem for csv_file in csv_files]
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                
                # Initialize data collection
                table_name = csv_file.stem
                table_data = {
                    "table_name": table_name,
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
                    _field_stats = calculate_field_stats(values)
                    primary_key = is_primary_key(field_name, table_name)
                    foreign_key = is_foreign_key(field_name, table_name, table_names)
                    if primary_key: _field_stats['primary_key'] = primary_key
                    if foreign_key: _field_stats['foreign_key'] = foreign_key
                    field_stats[field_name] = _field_stats
                
                # Prepare final schema
                schema = {
                    "table_name": table_data["table_name"],
                    "record_count": table_data["record_count"],
                    "fields": field_stats
                }
                
                # Write schema file
                json_path = schema_dir / f"{table_name}.json"
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


