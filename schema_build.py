from dotenv import load_dotenv
load_dotenv()

import os
import random
import csv
import json
from pathlib import Path
from collections import Counter
from typing import Dict
from datetime import datetime
from statistics import mean, median
from collections import Counter
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

# TODO: merge with defaults from each column
class MasterSchemaGenerator:
    def __init__(self):
        self.client = anthropic.Anthropic()
        
    def load_all_schemas(self, schema_dir: Path) -> List[Dict]:
        """Load all schema files from the specified directory"""
        schemas = []
        for schema_file in schema_dir.glob("*.json"):
            with open(schema_file, 'r') as f:
                schema = json.load(f)
                schemas.append(schema)
        return schemas
    
    def generate_master_documentation(self, table_schemas: List[Dict]) -> Dict:
        """Generate documentation with table and column descriptions using Claude's tools"""
        
        # Create initial structure with table and column information
        all_tables_schema = {}
        for table_schema in table_schemas:
            table_name = table_schema["table_name"]
            if not table_schema.get('record_count', 0) > 0: continue
            all_tables_schema[table_name] = table_schema

        # Define system prompt with full schema context
        system_prompt = f"""You are a specialized database expert focusing on healthcare data models. Your task is to analyze database schemas and create comprehensive documentation for specified tables.

You will be provided with the full schema context for understanding relationships:

{json.dumps(all_tables_schema, indent=2)}

For each requested table, you will:
1. Document the table's purpose and clinical role in the healthcare system
2. Explain each column's specific function and importance
3. Identify and mark all primary keys
4. Document all foreign key relationships
5. Explain how the table fits into clinical workflows

When analyzing tables:
- Document each table's healthcare-specific purpose
- Explain each column's clinical significance
- Identify primary keys with certainty (mark as primary_key: true)
- Detect and document foreign key relationships, specifying both referenced table and column
- Describe how tables interconnect to model healthcare processes

You understand common database patterns where:
- Primary keys are typically id columns
- Foreign keys often end in '_id'
- Column names may match other table names
- Healthcare-specific relationships exist between patients, providers, encounters, etc.

Requirements for your analysis:
- Mark all primary keys (primary_key: true)
- Specify foreign key references (table and column)
- Consider healthcare-specific relationships
- Document all clinical workflows represented

Use the document_schema tool to provide your analysis."""

        all_table_names = list(set(all_tables_schema.keys()))
        missing_table_names = list(all_table_names)
        processed_tables = {}

        while missing_table_names:
            chunk_table_names = missing_table_names[:4]

            message = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=8192,
                temperature=0,
                system=[
                    {
                        "type": "text", 
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"}
                    }
                ],
                messages=[{
                    "role": "user",
                    "content": f"Please generate documentation for the following tables: {', '.join(chunk_table_names)}"
                }],
                tools=TOOLS
            )
            
            # Extract the tool response
            for content in message.content:
                if not hasattr(content, 'input'): continue
                _processed_tables = content.input["tables"]
                processed_tables = {**processed_tables, **_processed_tables}
                break
            
            missing_table_names = list(set(all_table_names) - set(processed_tables.keys()))
            #missing_table_names = [] # TODO: hack remove

            print(f"All tables: {all_table_names}")
            print(f"Requested tables: {chunk_table_names}")
            print(f"Processed tables: {list(_processed_tables.keys())}")
            print(f"Missing tables: {missing_table_names}")
            print("-" * 50)

        return processed_tables

    def save_master_schema(self, documentation: Dict, output_path: Path):
        """Save the master schema documentation to a JSON file"""
        with open(output_path, 'w') as f:
            json.dump(documentation, f, indent=2)

def is_primary_key(column_name, table_name):
    return column_name.lower() == f"{table_name.lower()}_id"

def is_foreign_key(column_name, table_name, table_names):
    for _table_name in table_names:
        if _table_name.lower() == table_name.lower(): continue
        if column_name.lower() == f"{_table_name.lower()}_id": return True
    return False

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


# Define the tool schema for type inference
TYPE_INFERENCE_TOOLS = [
    {
        "name": "infer_column_types",
        "description": "Analyze column values to infer data types, patterns, and semantic meaning",
        "input_schema": {
            "type": "object",
            "properties": {
                "columns": {
                    "type": "object",
                    "description": "Type inference for each analyzed column",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "regex": {
                                "type": "string",
                                "description": "A regex pattern that matches most values in this column"
                            },
                            "meaning": {
                                "type": "string",
                                "description": "The semantic meaning of the data (e.g., date, name, id, code)"
                            }
                        },
                        "required": ["regex", "meaning"]
                    }
                }
            },
            "required": ["columns"]
        }
    }
]

def infer_column_types(table_name, column_name: dict, unique_values, client: anthropic.Anthropic) -> dict:
    """
    Use Claude with the simplified tool to infer column patterns and meanings.
    
    Args:
        column_data: Dictionary mapping column names to lists of unique values
        client: Anthropic client instance
    
    Returns:
        Dictionary containing inferred patterns and meanings for each column
    """

    # TODO: speed hack    
    return {"regex" : ".*", "meaning": "string"}

    # System prompt for consistent context
    system_prompt = """You are a data pattern analysis expert specializing in healthcare data. Your role is to:

    1. Create precise regex patterns matching data formats
    2. Identify semantic meanings and purposes of data columns

    IMPORTANT REGEX GUIDELINES:
    - Always create general-purpose regex patterns that match the expected data format
    - Even when there's only one unique value, create a pattern that would match similar valid values
    - Never create regexes that only match specific string literals
    - Use appropriate character classes and quantifiers to match the general pattern

    Examples of good general patterns vs overly specific ones:
    ✓ Organization name: "^[A-Za-z0-9\\s.,-]+$" 
    ✗ Organization name: "^OHDSI$"

    ✓ URL pattern: "^https://github\\.com/[A-Za-z0-9-]+/[A-Za-z0-9-]+$"
    ✗ URL pattern: "^https://github\\.com/OHDSI/ETL-Synthea$"

    ✓ Description text: "^[A-Za-z0-9\\s.,()-]+$"
    ✗ Description text: "^SyntheaTM is a Synthetic Patient Population Simulator\\.$"

    Consider these common healthcare data patterns:
    - Patient identifiers and medical record numbers
    - Diagnostic codes (ICD-10, SNOMED, etc.)
    - Medication codes and names 
    - Date/time formats
    - Healthcare provider identifiers
    - Clinical measurements and vital signs
    - Billing codes and amounts
    - Medical procedure codes

    For regex patterns:
    - Start with broad character classes ([A-Za-z0-9]) instead of specific characters
    - Use appropriate quantifiers (*, +, ?) to handle varying lengths
    - Consider common pattern variations in healthcare data
    - Account for both required and optional components
    - Include common delimiters and special characters where appropriate

    For semantic meanings:
    - Use clear, specific categories (e.g., "patient_id", "diagnosis_code", "medication_name")
    - Consider the context of healthcare data
    - Note if a column appears to be a foreign key or reference"""

    # Prepare sample data for Claude
    sample_data = {}
    random.shuffle(unique_values)
    sample_data[column_name] = unique_values[:10]

    # Get Claude's analysis using the tool
    message = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=4096,
        temperature=0,
        system=[
            {
                "type": "text", 
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"}
            }
        ],
        messages=[{
            "role": "user",
            "content": f"""
Here is a sample of the unique values found in the column in table `{table_name}`, column `{column_name}`:
{json.dumps(sample_data, indent=2)}
"""
        }],
        tools=TYPE_INFERENCE_TOOLS
    )
    
    # Extract the tool response
    for content in message.content:
        if hasattr(content, 'input'):
            return next(iter(content.input["columns"].values()))

    # Return empty dict if no tool response
    return {}

def calculate_column_stats(table_name, values: list, column_name: str, client: anthropic.Anthropic) -> dict:
    """Calculate enhanced statistics for a column, including type inference."""
    total_count = len(values)
    if total_count == 0:
        return {"total_values": 0}

    # Basic counts
    non_empty_values = [v for v in values if v.strip() != '']
    non_empty_count = len(non_empty_values)
    unique_values = list(set(values))  # Convert to list for JSON serialization
    unique_count = len(unique_values)
    value_types = list(set([type(v).__name__ for v in values]))

    sample_values = list(values)
    sample_values = [x for x in sample_values if type(x) == str and x.strip() != '' or type(x) != str]
    random.shuffle(sample_values)
    sample_values = sample_values[:10]

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
        "most_common_values": {str(v): c for v, c in most_common},
        "sample_values": sample_values,
        "value_types": value_types
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
    if numeric_values:
        stats["numeric_stats"] = {
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": round(mean(numeric_values), 2),
            "median": round(median(numeric_values), 2)
        }

    # Check if values are dates
    if non_empty_values:
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

    
    # Add type inference information to column stats
    #stats["type"] =  infer_column_types(table_name, column_name, unique_values, client)
    #print(f"`{table_name}.{column_name}`: {json.dumps(stats, indent=2)}")

    return stats

def build_table_schema(directory: str):
    """Build schema files for each table with enhanced type inference."""
    schema_dir = Path("schema/tables")
    schema_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize Anthropic client
    client = anthropic.Anthropic()
    
    csv_files = Path(directory).glob('*.csv')
    csv_files = [csv_file for csv_file in csv_files]
    table_names = [csv_file.stem for csv_file in csv_files]
    
    for csv_file in csv_files:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            headers = next(csv_reader)
            
            # Initialize data collection
            table_name = csv_file.stem
            table_data = {
                "table_name": table_name,
                "record_count": 0,
                "columns": {header: [] for header in headers}
            }
            
            # Collect all values for each column
            for row in csv_reader:
                table_data["record_count"] += 1
                for i, value in enumerate(row):
                    if i < len(headers):
                        table_data["columns"][headers[i]].append(value)
            
            # Calculate statistics for each column with type inference
            column_stats = {}
            for column_name, values in table_data["columns"].items():
                _column_stats = calculate_column_stats(table_name, values, column_name, client)
                primary_key = is_primary_key(column_name, table_name)
                foreign_key = is_foreign_key(column_name, table_name, table_names)
                if primary_key: _column_stats['primary_key'] = primary_key
                if foreign_key: _column_stats['foreign_key'] = foreign_key
                column_stats[column_name] = _column_stats
            
            # Prepare final schema
            schema = {
                "table_name": table_data["table_name"],
                "record_count": table_data["record_count"],
                "columns": column_stats
            }
            
            # Write schema file
            json_path = schema_dir / f"{table_name}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(schema, f, indent=2)
            
            print(f"Created table schema: {json_path}")

def build_master_schema(datasets_dir):
    generator = MasterSchemaGenerator()
    
    # Setup paths
    schema_dir = Path("schema/tables")
    output_file = Path("schema/schema.json")
    
    print("Loading schema files...")
    schemas = generator.load_all_schemas(schema_dir)
    print(f"Loaded {len(schemas)} schema files")
    
    print("\nGenerating master schema...")
    master_schema = generator.generate_master_documentation(schemas)
    
    print("\nSaving master schema...")
    generator.save_master_schema(master_schema, output_file)
    print(f"\nMaster schema saved to {output_file}")

if __name__ == "__main__":
    datasets_dir = "datasets/Synthea27Nj_5.4"
    build_table_schema(datasets_dir)
    #build_master_schema(datasets_dir)
