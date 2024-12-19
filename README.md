# Schema Analysis Tools

This repository contains tools for analyzing CSV datasets and generating schema documentation.

## Scripts Overview

### schema_build.py

Contains multiple utilities for analyzing CSV files and generating schema documentation:

- `analyze_csv_files(directory)`: Analyzes CSV files in a directory to find header patterns and table connections
- `build_column_schema(directory)`: Generates detailed JSON schema files for each column found across all tables
- `build_table_schema(directory)`: Generates JSON schema files for each table with field statistics
- `build_schema(directory)`: Combines both schema generation functions

## Usage

1. Place your CSV files in a directory (e.g., `datasets/Synthea27Nj_5.4/`)

2. Run the schema analysis:

```bash
python schema_build.py
```

This will:
- Create a `schema/` directory with two subdirectories:
  - `schema/columns/`: Contains JSON files for each unique column with cross-table analysis
  - `schema/tables/`: Contains JSON files for each table with detailed field statistics

## Schema Output

### Column Schema Files
Each column schema file includes:
- Which tables the column appears in
- Values found in each table
- Overlap analysis between tables (when a column appears in multiple tables)

### Table Schema Files
Each table schema file includes:
- Total record count
- Field statistics for each column:
  - Total values
  - Non-empty values count and percentage
  - Unique values count

## Requirements

- Python 3.6+
- Standard libraries (no additional dependencies required)
