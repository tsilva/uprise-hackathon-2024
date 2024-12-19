import os
import re
import anthropic
import json
import pandas as pd

MODEL_ID = "claude-3-5-sonnet-20241022"

DAMAGED_DATASET_DIR = "datasets/Synthea27Nj_5.4_damaged"
HEALED_DATASET_DIR = "datasets/Synthea27Nj_5.4_healed"

HEAL_TOOL = {
    "name": "heal_values",
    "description": "Heal damaged or incorrectly formatted values to match the expected schema while preserving meaning",
    "input_schema": {
        "type": "object",
        "properties": {
            "healed_values": {
                "type": "array",
                "description": "Array of healed values in the same order as provided",
                "items": {
                    "type": "object",
                    "properties": {
                        "original": {
                            "type": "string",
                            "description": "The original damaged value"
                        },
                        "healed": {
                            "type": "string",
                            "description": "The healed value that matches the regex pattern"
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score (0-1) that the healed value preserves original meaning",
                            "minimum": 0,
                            "maximum": 1
                        }
                    },
                    "required": ["original", "healed", "confidence"]
                }
            }
        },
        "required": ["healed_values"]
    }
}

SYSTEM_PROMPT = """You are a healthcare data quality restoration specialist. Your mission is to analyze and repair data values that have deviated from their expected formats while preserving their original semantic meaning. Your expertise lies in recognizing common data entry patterns and standardizing them back to their formal representations.

Core Restoration Principles:
1. Pattern Recognition
   - Identify the underlying meaning despite formatting issues
   - Recognize regional and cultural data entry variations
   - Detect common human input patterns and shortcuts

2. Semantic Preservation
   - Maintain the original meaning above all else
   - Consider domain context when resolving ambiguity
   - Preserve data integrity while reformatting

3. Format Standardization
   - Convert variations back to canonical forms
   - Remove unnecessary decorators and separators
   - Standardize case and representation

4. Confidence Assessment
   - High confidence (0.9-1.0): Clear meaning, obvious correction
   - Medium confidence (0.7-0.9): Multiple valid interpretations
   - Low confidence (0.5-0.7): Significant ambiguity
   - Below 0.5: Too uncertain to heal reliably

Healing Strategy:
1. Analyze deviation from target regex pattern
2. Identify core semantic value within the noise
3. Strip non-essential formatting elements
4. Apply standard format according to regex
5. Validate semantic preservation
6. Assign confidence score

Remember: Your goal is to restore data to its proper format while ensuring 
the original meaning remains intact. When multiple interpretations exist, 
choose the most probable one based on healthcare domain context.

Use the heal_values tool to provide your analysis and healed values."""

client = anthropic.Anthropic()

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

def load_table(table_name):
    table_path = f"{DAMAGED_DATASET_DIR}/{table_name}.csv"
    return pd.read_csv(table_path)

def heal_values(regex, values):
    message = client.messages.create(
        model=MODEL_ID,
        max_tokens=1000,
        temperature=0.0,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{
            "role": "user", 
            "content": f"Regex pattern: {regex}\n\nValues to heal:\n" + "\n".join(values)
        }],
        tools=[HEAL_TOOL]
    )
    
    for content in message.content:
        if not hasattr(content, 'input'): continue
        result = content.input["healed_values"]
        values = [item["healed"] for item in result]
    
    return values

def heal_dataset():
    schema = load_json("schema/schema.json")
    
    for table_name, table_data in schema.items():
        table_df = load_table(table_name)
        
        for column_name, column_data in table_data["columns"].items():
            regex = column_data.get("regex")
            if not regex: continue

            values = table_df[column_name].values
            damaged_indexes = [i for i, value in enumerate(values) if not re.match(regex, str(value))]
            damaged_indexes = damaged_indexes[:10] # HACK: currently capping, would need to paginate
            damaged_values = values[damaged_indexes]

            if len(damaged_values) == 0: continue

            damaged_values = list(map(str, damaged_values))
            healed_values = heal_values(regex, damaged_values)
            table_df.loc[damaged_indexes, column_name] = healed_values

            print("Table name:", table_name)
            print("Column name:", column_name)
            print("Regex:", regex)
            print("Damaged values:", damaged_values)
            print("Healed values:", healed_values)
            print("-" * 50)

        table_df.to_csv(f"{HEALED_DATASET_DIR}/{table_name}.csv", index=False)
    
if __name__ == "__main__":
    os.makedirs(HEALED_DATASET_DIR, exist_ok=True)
    os.system(f"cp -r {DAMAGED_DATASET_DIR}/* {HEALED_DATASET_DIR}")
    heal_dataset()