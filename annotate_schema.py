import json
import csv
from pathlib import Path
import sys
from typing import Dict
import anthropic
import os
from dotenv import load_dotenv

class SchemaAnnotator:
    def __init__(self):
        load_dotenv()
        self.client = anthropic.Anthropic(
            api_key=os.getenv('ANTHROPIC_API_KEY')
        )

    def read_csv_sample(self, table_name: str, max_rows: int = 5) -> Dict:
        """Read CSV file and return headers and sample rows"""
        csv_path = Path(f"datasets/Synthea27Nj_5.4/{table_name}.csv")
        if not csv_path.exists():
            raise ValueError(f"CSV file for table {table_name} not found")

        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            sample_rows = []
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                sample_rows.append(row)

        return {
            "headers": headers,
            "sample_rows": sample_rows
        }

    def generate_descriptions(self, table_name: str, csv_data: Dict) -> Dict:
        """Use Claude to generate table and column descriptions with regex patterns"""
        prompt = f"""Given this healthcare database table named {table_name}, analyze its structure and sample data to generate descriptions and regex patterns.

Headers:
{', '.join(csv_data['headers'])}

Sample rows:
{json.dumps(csv_data['sample_rows'], indent=2)}

Please provide:
1. TABLE DESCRIPTION: A concise 1-2 sentence description explaining what data this table stores, its purpose, and key fields.

2. COLUMN DETAILS: For each column, provide:
   - A brief description of what that field represents
   - A regex pattern that would match >90% of valid values for this column (based on the sample data)

Format your response exactly like this:
TABLE DESCRIPTION:
<table description here>

COLUMN DETAILS:
column_name1:
description: <description>
regex: <regex_pattern>

column_name2:
description: <description>
regex: <regex_pattern>
...and so on for each column"""

        message = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=8192,
            temperature=0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Parse the response
        response = message.content[0].text
        table_desc = ""
        column_details = {}
        
        sections = response.split("\n\n")
        current_column = None
        
        for section in sections:
            if section.startswith("TABLE DESCRIPTION:"):
                table_desc = section.replace("TABLE DESCRIPTION:", "").strip()
            elif section.startswith("COLUMN DETAILS:"):
                lines = section.replace("COLUMN DETAILS:", "").strip().split("\n")
                for line in lines:
                    line = line.strip()
                    if line and ":" in line:
                        if not line.startswith("description:") and not line.startswith("regex:"):
                            # This is a column name
                            current_column = line.rstrip(":")
                            column_details[current_column] = {}
                        elif current_column:
                            if line.startswith("description:"):
                                column_details[current_column]["description"] = line.replace("description:", "").strip()
                            elif line.startswith("regex:"):
                                column_details[current_column]["regex"] = line.replace("regex:", "").strip()
        
        return {
            "table_description": table_desc,
            "column_details": column_details
        }

    def update_schema(self, table_name: str, descriptions: Dict):
        """Update the table's schema file with descriptions and regex patterns"""
        schema_path = Path(f"schema/tables/{table_name}.json")
        if not schema_path.exists():
            raise ValueError(f"Schema file for table {table_name} not found")

        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        # Update field information and ensure consistent ordering
        fields = schema["fields"]
        for field_name, field_info in fields.items():
            if field_name in descriptions["column_details"]:
                # Create ordered field info
                ordered_field_info = {
                    "description": descriptions["column_details"][field_name].get("description", ""),
                    "regex": descriptions["column_details"][field_name].get("regex", ""),
                    **{k: v for k, v in field_info.items() if k not in ["description", "regex"]}
                }
                fields[field_name] = ordered_field_info

        # Ensure consistent schema ordering
        ordered_schema = {
            "table_name": schema["table_name"],
            "record_count": schema["record_count"],
            "primary_key": f"{schema['table_name'].lower()}_id",
            "description": descriptions["table_description"],
            "fields": fields
        }

        with open(schema_path, 'w') as f:
            json.dump(ordered_schema, f, indent=2)

def get_valid_tables(schema_dir: Path) -> list[str]:
    """Get all tables that have records from schema files"""
    valid_tables = []
    for schema_file in schema_dir.glob("*.json"):
        with open(schema_file, 'r') as f:
            schema = json.load(f)
            if schema.get('record_count', 0) > 0:
                valid_tables.append(schema['table_name'])
    return valid_tables

def main():
    annotator = SchemaAnnotator()
    schema_dir = Path("schema/tables")

    if len(sys.argv) == 1:
        # No table specified - process all valid tables
        tables = get_valid_tables(schema_dir)
        for table_name in tables:
            try:
                print(f"\nProcessing table {table_name}...")
                csv_data = annotator.read_csv_sample(table_name)
                descriptions = annotator.generate_descriptions(table_name, csv_data)
                annotator.update_schema(table_name, descriptions)
                print(f"Updated schema for {table_name}")
                print("-" * 50)
                print("Table description:")
                print(descriptions["table_description"])
                print("\nColumn descriptions added for", len(descriptions["column_details"]), "columns")
            except Exception as e:
                print(f"Error processing {table_name}: {str(e)}")
                continue
    elif len(sys.argv) == 2:
        # Single table specified
        table_name = sys.argv[1]
        try:
            print(f"Reading sample data from {table_name}...")
            csv_data = annotator.read_csv_sample(table_name)
            print("Generating descriptions using Claude...")
            descriptions = annotator.generate_descriptions(table_name, csv_data)
            print("Updating schema file...")
            annotator.update_schema(table_name, descriptions)
            print("\nTable Description:")
            print("-" * 50)
            print(descriptions["table_description"])
            print("\nColumn descriptions added for", len(descriptions["column_details"]), "columns")
            print("\nSchema updated successfully!")
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    else:
        print("Usage: python annotate_schema.py [table_name]")
        print("If table_name is not specified, processes all tables with records")
        sys.exit(1)

if __name__ == "__main__":
    main()
