import json
import csv
from pathlib import Path
import sys
from typing import Dict
import anthropic
import os
from dotenv import load_dotenv
import re
from datetime import datetime
from collections import defaultdict

class SchemaAnnotator:
    def __init__(self):
        load_dotenv()
        self.client = anthropic.Anthropic(
            api_key=os.getenv('ANTHROPIC_API_KEY')
        )

    def detect_value_types(self, values: list) -> tuple:
        """Detect simple and composite types of values"""
        # Remove empty values
        non_empty_values = [v for v in values if v.strip()]
        if not non_empty_values:
            return "string", "unknown"
        
        sample = non_empty_values[0]
        
        # Try to detect numbers
        try:
            int(sample)
            return "integer", "number"
        except ValueError:
            try:
                float(sample)
                return "float", "number"
            except ValueError:
                pass
        
        # Try to detect dates
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # ISO format
            r'\d{2}/\d{2}/\d{4}',  # US format
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'  # ISO timestamp
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, sample):
                return "string", "datetime"
        
        # Detect other common patterns
        if re.match(r'^[A-F0-9-]{36}$', sample):  # UUID
            return "string", "uuid"
        elif re.match(r'^\d+\.\d+\.\d+\.\d+$', sample):  # IP address
            return "string", "ip_address"
        elif all(word.istitle() for word in sample.split()):  # Name-like
            return "string", "name"
        elif '@' in sample and '.' in sample:  # Email-like
            return "string", "email"
        elif len(re.findall(r'[A-Z]', sample)) > len(sample) * 0.5:  # Code-like
            return "string", "code"
        
        return "string", "text"

    def test_regex_matches(self, values: list, regex_pattern: str) -> tuple:
        """Test how many values match the regex pattern"""
        try:
            compiled_regex = re.compile(regex_pattern)
            matches = [1 for value in values if compiled_regex.match(str(value))]
            match_count = sum(matches)
            match_percentage = (match_count / len(values)) * 100 if values else 0
            return match_count, round(match_percentage, 2)
        except re.error:
            return 0, 0

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

    def analyze_key_relationships(self, schema: dict) -> dict:
        """Use Claude to analyze and identify primary and foreign keys"""
        prompt = f"""Analyze this database table schema and identify:
1. Which fields are likely primary keys (unique identifiers for this table)
2. Which fields are likely foreign keys (references to other tables)

Schema:
{json.dumps(schema, indent=2)}

Provide your analysis in this exact format:
PRIMARY_KEYS:
field1: reason
field2: reason

FOREIGN_KEYS:
field1: referenced_table.referenced_field
field2: referenced_table.referenced_field

Base your analysis on:
- Field names (e.g., *_id fields)
- Uniqueness (100% unique values likely indicate primary keys)
- Value patterns and descriptions
- Common database naming conventions
"""

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
        key_info = {"primary_keys": {}, "foreign_keys": {}}
        
        current_section = None
        for line in response.split('\n'):
            line = line.strip()
            if line == "PRIMARY_KEYS:":
                current_section = "primary_keys"
                continue
            elif line == "FOREIGN_KEYS:":
                current_section = "foreign_keys"
                continue
            
            if current_section and line and ":" in line:
                field, info = line.split(':', 1)
                field = field.strip()
                info = info.strip()
                if field and info:
                    key_info[current_section][field] = info

        return key_info

    def update_schema(self, table_name: str, descriptions: Dict):
        """Update the table's schema file with descriptions and regex patterns"""
        schema_path = Path(f"schema/tables/{table_name}.json")
        if not schema_path.exists():
            raise ValueError(f"Schema file for table {table_name} not found")

        # Read all values from the CSV for regex testing and type detection
        csv_path = Path(f"datasets/Synthea27Nj_5.4/{table_name}.csv")
        field_values = defaultdict(list)
        
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                for i, value in enumerate(row):
                    if i < len(headers):
                        field_values[headers[i]].append(value)

        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        fields = schema["fields"]
        for field_name, field_info in fields.items():
            if field_name in descriptions["column_details"]:
                values = field_values[field_name]
                regex_pattern = descriptions["column_details"][field_name].get("regex", "")
                match_count, match_percentage = self.test_regex_matches(values, regex_pattern)
                simple_type, composite_type = self.detect_value_types(values)
                
                # Create ordered field info with renamed fields
                ordered_field_info = {
                    "description": descriptions["column_details"][field_name].get("description", ""),
                    "type_regex": regex_pattern,
                    "type_regex_match_count": match_count,
                    "type_regex_match_percentage": match_percentage,
                    "type_simple": simple_type,
                    "type_composite": composite_type,
                    **{k: v for k, v in field_info.items() if k not in [
                        "description", "regex", "regex_match_count", 
                        "regex_match_percentage", "simple_type", "composite_type",
                        "type_regex", "type_regex_match_count",
                        "type_regex_match_percentage", "type_simple", "type_composite"
                    ]}
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

        # After creating ordered_schema, analyze key relationships
        key_info = self.analyze_key_relationships(ordered_schema)
        
        # Add key relationship information
        ordered_schema["key_relationships"] = {
            "primary_keys": key_info["primary_keys"],
            "foreign_keys": key_info["foreign_keys"]
        }

        # Update field information with key relationship flags
        for field_name in ordered_schema["fields"]:
            if field_name in key_info["primary_keys"]:
                ordered_schema["fields"][field_name]["is_primary_key"] = True
                ordered_schema["fields"][field_name]["primary_key_reason"] = key_info["primary_keys"][field_name]
            if field_name in key_info["foreign_keys"]:
                ordered_schema["fields"][field_name]["is_foreign_key"] = True
                ordered_schema["fields"][field_name]["references"] = key_info["foreign_keys"][field_name]

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
