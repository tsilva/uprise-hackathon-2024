# Schema Analysis Tools

Contains multiple utilities for analyzing CSV files and generating schema documentation:

- `analyze_csv_files(directory)`: Analyzes CSV files in a directory to find header patterns and table connections
- `build_column_schema(directory)`: Generates detailed JSON schema files for each column found across all tables
- `build_table_schema(directory)`: Generates JSON schema files for each table with field statistics
- `build_schema(directory)`: Combines both schema generation functions
