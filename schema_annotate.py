from dotenv import load_dotenv
load_dotenv()

import json
import anthropic
from pathlib import Path
from typing import Dict, List

# Define the tool schema for table documentation
TOOLS = [
    {
        "name": "document_schema",
        "description": "Document the purpose and structure of database tables and their columns, including primary and foreign key relationships",
        "input_schema": {
            "type": "object",
            "properties": {
                "tables": {
                    "type": "object",
                    "description": "Documentation for each table and its columns",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "description": {
                                "type": "string",
                                "description": "Clear description of the table's purpose and role in the system"
                            },
                            "columns": {
                                "type": "object",
                                "description": "Documentation for each column in the table",
                                "additionalProperties": {
                                    "type": "object",
                                    "properties": {
                                        "description": {
                                            "type": "string",
                                            "description": "Clear description of the column's purpose and contents"
                                        },
                                        "type": {
                                            "type": "string",
                                            "description": "Data type of the column"
                                        },
                                        "primary_key": {
                                            "type": "boolean",
                                            "description": "Whether this column is a primary key",
                                            "default": False
                                        },
                                        "foreign_key": {
                                            "type": ["object", "null"],
                                            "description": "Foreign key reference if this column references another table",
                                            "properties": {
                                                "table": {
                                                    "type": "string",
                                                    "description": "Name of the referenced table"
                                                },
                                                "column": {
                                                    "type": "string",
                                                    "description": "Name of the referenced column"
                                                }
                                            },
                                            "required": ["table", "column"],
                                            "default": None
                                        }
                                    },
                                    "required": ["description", "type", "primary_key"]
                                }
                            }
                        },
                        "required": ["description", "columns"]
                    }
                }
            },
            "required": ["tables"]
        }
    }
]

class MasterSchemaGenerator:
    def __init__(self):
        self.client = anthropic.Anthropic()
        
    def load_all_schemas(self, schema_dir: Path) -> List[Dict]:
        """Load all schema files from the specified directory"""
        schemas = []
        for schema_file in schema_dir.glob("*.json"):
            with open(schema_file, 'r') as f:
                schema = json.load(f)
                if schema.get('record_count', 0) > 0:  # Only include tables with records
                    schemas.append(schema)
        return schemas
    
    def generate_master_documentation(self, schemas: List[Dict]) -> Dict:
        """Generate documentation with table and column descriptions using Claude's tools"""
        
        # Create initial structure with table and column information
        initial_schema = {}
        for schema in schemas:
            table_name = schema["table_name"]
            initial_schema[table_name] = {
                "columns": {}
            }
            for field_name, field_info in schema["fields"].items():
                initial_schema[table_name]["columns"][field_name] = {
                    "description": field_info.get("description", ""),
                    "type": field_info.get("type_simple", "string"),
                    #"primary_key": False,  # Will be updated by Claude
                    #"foreign_key": None    # Will be updated by Claude
                }
        
        # Generate prompt for Claude
        prompt = f"""You are a database expert who thoroughly understands healthcare data models.
        
Analyze this database schema and document the purpose of each table and column, including all key relationships:

{json.dumps(initial_schema, indent=2)}

Create a comprehensive description that explains:
1. Each table's purpose and role in the healthcare system
2. Each column's specific function and importance
3. Primary keys for each table
4. Foreign key relationships between tables (look for column names ending in '_id' or matching a table name)
5. How the data models clinical workflows

IMPORTANT:
- Mark primary keys appropriately (primary_key: true)
- For foreign keys, specify both the referenced table and column (usually the primary key of that table)
- Common patterns to identify foreign keys:
  - Column names ending in '_id'
  - Column names matching another table's name
  - Column names that reference another entity (e.g., patient_id, provider_id)

Use the document_schema tool to provide structured documentation."""

        message = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=8192,
            temperature=0,
            messages=[{
                "role": "user",
                "content": prompt
            }],
            tools=TOOLS
        )
        
        # Extract the tool response
        for content in message.content:
            if hasattr(content, 'input'):
                return content.input["tables"]
        
        # Return original schema if no tool response
        return initial_schema

    def save_master_schema(self, documentation: Dict, output_path: Path):
        """Save the master schema documentation to a JSON file"""
        with open(output_path, 'w') as f:
            json.dump(documentation, f, indent=2)
        
        # Also save a relationships summary
        #relationships_path = output_path.parent / "relationships.json"
        #relationships = self.extract_relationships(documentation)
        #with open(relationships_path, 'w') as f:
        #    json.dump(relationships, f, indent=2)
    
    # def extract_relationships(self, documentation: Dict) -> Dict:
    #     """Extract and summarize all relationships from the schema"""
    #     relationships = {
    #         "primary_keys": {},
    #         "foreign_keys": []
    #     }
        
    #     for table_name, table_info in documentation.items():
    #         # Collect primary keys
    #         primary_keys = [
    #             col_name for col_name, col_info in table_info["columns"].items()
    #             if col_info.get("primary_key", False)
    #         ]
    #         if primary_keys:
    #             relationships["primary_keys"][table_name] = primary_keys
            
    #         # Collect foreign keys
    #         for col_name, col_info in table_info["columns"].items():
    #             if col_info.get("foreign_key"):
    #                 relationships["foreign_keys"].append({
    #                     "from_table": table_name,
    #                     "from_column": col_name,
    #                     "to_table": col_info["foreign_key"]["table"],
    #                     "to_column": col_info["foreign_key"]["column"]
    #                 })
        
    #     return relationships

def main():
    generator = MasterSchemaGenerator()
    
    # Setup paths
    schema_dir = Path("schema/tables")
    output_file = Path("schema/schema.json")
    
    print("Loading schema files...")
    schemas = generator.load_all_schemas(schema_dir)
    print(f"Loaded {len(schemas)} schema files")
    
    print("\nGenerating master documentation...")
    documentation = generator.generate_master_documentation(schemas)
    
    print("\nSaving master schema documentation...")
    generator.save_master_schema(documentation, output_file)
    
    print(f"\nMaster schema documentation saved to {output_file}")
    
    # Print a sample of the documentation
    sample_table = next(iter(documentation))
    print(f"\nSample documentation for table '{sample_table}':")
    print("-" * 50)
    print(f"Description: {documentation[sample_table]['description']}")
    print("\nSample columns:")
    for column_name, column_info in list(documentation[sample_table]['columns'].items())[:3]:
        print(f"\n{column_name}:")
        print(f"Description: {column_info['description']}")
        print(f"Type: {column_info['type']}")
        if column_info.get("primary_key"):
            print("Primary Key: Yes")
        if column_info.get("foreign_key"):
            fk = column_info["foreign_key"]
            print(f"Foreign Key -> {fk['table']}.{fk['column']}")

if __name__ == "__main__":
    main()