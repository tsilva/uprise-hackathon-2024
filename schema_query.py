import json
from pathlib import Path
import sys
from typing import Any, Dict, List

class SchemaQuerier:
    # Dictionary of available queries and their documentation
    AVAILABLE_QUERIES = {
        "1": {
            "description": "List tables with more than 10 records",
            "syntax": "python query_schema.py 1",
            "args": None
        },
        "values": {
            "description": "Get all values for a specific table column",
            "syntax": "python query_schema.py values <table.column>",
            "args": ["table.column"]
        },
        "tables_where_value": {
            "description": "Find all tables and columns containing a specific value",
            "syntax": "python query_schema.py tables_where_value <value>",
            "args": ["value"]
        },
        "columns": {
            "description": "List all columns in a table with their statistics",
            "syntax": "python query_schema.py columns <table_name>",
            "args": ["table_name"]
        },
        "tables": {
            "description": "List all tables alphabetically with record counts and descriptions",
            "syntax": "python query_schema.py tables",
            "args": None
        },
        "table_descriptions": {
            "description": "List all tables with their descriptions in a bullet list format",
            "syntax": "python query_schema.py table_descriptions",
            "args": None
        }
    }

    def __init__(self):
        self.tables_schema = {}
        self.columns_schema = {}
        self.tables_lookup = {}  # For case-insensitive table lookups
        self.columns_lookup = {}  # For case-insensitive column lookups
        self.table_descriptions = {
            "PERSON": "Demographics and identifying information about each person in the database",
            "OBSERVATION": "Clinical observations and measurements recorded for patients",
            "MEASUREMENT": "Measurements and lab results",
            "CONDITION_OCCURRENCE": "Records of diagnosed conditions and diseases",
            "DRUG_EXPOSURE": "Records of drug exposures and prescriptions",
            "PROCEDURE_OCCURRENCE": "Records of procedures performed",
            "VISIT_OCCURRENCE": "Records of visits/encounters with healthcare providers"
            # Add more descriptions as needed
        }
        self._load_schemas()
    
    def _load_schemas(self):
        """Load all schema files into memory"""
        schema_dir = Path("schema")
        
        # Load table schemas with case-insensitive lookups
        tables_dir = schema_dir / "tables"
        for schema_file in tables_dir.glob("*.json"):
            with open(schema_file) as f:
                data = json.load(f)
                self.tables_schema[schema_file.stem] = data
                self.tables_lookup[schema_file.stem.lower()] = schema_file.stem
        
        # Load column schemas with case-insensitive lookups
        columns_dir = schema_dir / "columns"
        for schema_file in columns_dir.glob("*.json"):
            with open(schema_file) as f:
                data = json.load(f)
                self.columns_schema[schema_file.stem] = data
                self.columns_lookup[data["column_name"].lower()] = schema_file.stem

    def _get_real_table_name(self, table_name: str) -> str:
        """Get the actual table name from case-insensitive input"""
        real_name = self.tables_lookup.get(table_name.lower())
        if not real_name:
            raise ValueError(f"Table '{table_name}' not found")
        return real_name

    def _get_real_column_name(self, column_name: str) -> str:
        """Get the actual column schema key from case-insensitive input"""
        real_name = self.columns_lookup.get(column_name.lower())
        if not real_name:
            raise ValueError(f"Column '{column_name}' not found")
        return real_name

    def query_1(self) -> List[Dict[str, Any]]:
        """Tables with more than 10 records"""
        result = []
        for table_name, schema in self.tables_schema.items():
            if schema["record_count"] > 10:
                result.append({
                    "table_name": table_name,
                    "record_count": schema["record_count"]
                })
        return sorted(result, key=lambda x: x["record_count"], reverse=True)

    def query_values(self, args: List[str]) -> Dict[str, Any]:
        """Query 2: Get all values for a specific table.column"""
        if not args or len(args) != 1:
            raise ValueError("Usage: values <table.column>")
        
        try:
            table_name, column_name = args[0].split('.')
        except ValueError:
            raise ValueError("Usage: values <table.column> (must include table and column names separated by dot)")
        
        real_table = self._get_real_table_name(table_name)
        real_column = self._get_real_column_name(column_name)
        
        column_data = self.columns_schema[real_column]
        if real_table not in column_data["values_by_table"]:
            raise ValueError(f"Column {column_name} not found in table {table_name}")
            
        return {
            "table": real_table,
            "column": column_data["column_name"],
            "values": sorted(column_data["values_by_table"][real_table])
        }

    def query_tables_where_value(self, args: List[str]) -> Dict[str, Any]:
        """Query 3: Find all tables and columns containing a specific value"""
        if not args or len(args) != 1:
            raise ValueError("Usage: tables_where_value <value>")
            
        search_value = args[0]
        results = []
        
        for column_name, column_data in self.columns_schema.items():
            for table_name, values in column_data["values_by_table"].items():
                if search_value in values:
                    results.append({
                        "table": table_name,
                        "column": column_data["column_name"],
                        "count": values.count(search_value)
                    })
        
        return {
            "search_value": search_value,
            "occurrences": sorted(results, key=lambda x: (x["count"], x["table"], x["column"]), reverse=True)
        }

    def query_columns(self, args: List[str]) -> Dict[str, Any]:
        """Query: List all columns in a table with their statistics"""
        if not args or len(args) != 1:
            raise ValueError("Usage: columns <table_name>")
        
        real_table = self._get_real_table_name(args[0])
        table_data = self.tables_schema[real_table]
        columns = []
        
        for field_name, stats in table_data["fields"].items():
            columns.append({
                "name": field_name,
                "non_empty_percentage": stats["non_empty_percentage"],
                "unique_values": stats["unique_values"],
                "total_values": stats["total_values"]
            })
        
        return {
            "table": real_table,
            "record_count": table_data["record_count"],
            "columns": sorted(columns, key=lambda x: (-x["non_empty_percentage"], x["name"]))
        }

    def query_tables(self, args: List[str] = None) -> Dict[str, Any]:
        """List all tables alphabetically with record counts and descriptions"""
        tables = []
        for table_name in sorted(self.tables_schema.keys()):
            table_info = {
                "name": table_name,
                "record_count": self.tables_schema[table_name]["record_count"],
                "description": self.table_descriptions.get(table_name, "No description available")
            }
            tables.append(table_info)
        
        return {
            "total_tables": len(tables),
            "tables": tables
        }

    def query_table_descriptions(self, args: List[str] = None) -> str:
        """List all tables with their descriptions in a bullet list format"""
        result = []
        for table_name, schema in sorted(self.tables_schema.items()):
            description = schema.get("description", "No description available")
            result.append(f"• {table_name}\n  {description}\n")
        
        return "\n".join(result)

    def execute_query(self, query_id: str, args: List[str] = None) -> Any:
        """Execute a specific query by its ID"""
        if query_id == "table_descriptions":
            # Special handling for text output
            return {
                "query_id": query_id,
                "result": self.query_table_descriptions(args),
                "format": "text"  # Add format hint
            }
        
        if query_id.isdigit():
            query_method = getattr(self, f"query_{query_id}", None)
            return {
                "query_id": query_id,
                "result": query_method()
            }
        else:
            query_method = getattr(self, f"query_{query_id}", None)
            if query_method is None:
                raise ValueError(f"Query {query_id} not implemented")
            
            result = query_method(args)
            return {
                "query_id": query_id,
                "result": result
            }

    @classmethod
    def print_help(cls):
        """Display help information about available queries"""
        print("Available Queries:")
        print("-" * 50)
        for query_id, info in sorted(cls.AVAILABLE_QUERIES.items()):
            print(f"\n{query_id}:")
            print(f"  Description: {info['description']}")
            print(f"  Usage: {info['syntax']}")
            if info['args']:
                print(f"  Arguments: {', '.join(info['args'])}")

def main():
    if len(sys.argv) < 2:
        SchemaQuerier.print_help()
        sys.exit(0)
    
    query_id = sys.argv[1]
    args = sys.argv[2:] if len(sys.argv) > 2 else None
    querier = SchemaQuerier()
    
    try:
        result = querier.execute_query(query_id, args)
        if result.get("format") == "text":
            # Print text output directly
            print(result["result"])
        else:
            # Print JSON output
            print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error executing query {query_id}: {str(e)}")
        print("\nFor help, run: python query_schema.py")
        sys.exit(1)

if __name__ == "__main__":
    main()