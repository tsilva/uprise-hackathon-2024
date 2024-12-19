import os
import anthropic
import json
import numpy as np
import pandas as pd

MODEL_ID = "claude-3-5-sonnet-20241022"

DATASET_DIR = "datasets/Synthea27Nj_5.4"
DAMAGED_DATASET_DIR = "datasets/Synthea27Nj_5.4_damaged"

SYSTEM_PROMPT = """You are a data quality degradation specialist. Your goal is to introduce realistic data entry variations that make values break their regex pattern while ensuring a human could easily recover the original intended value. The changes should preserve semantic meaning in a way that makes the correct value obvious to humans.

Common real-world examples:
- Years: "1984" → "0984", "'84", "1984y", "19 84", "1,984"
- Dates: "2024-01-01" → "Jan-01-2024", "01/01/24", "2024.01.01", "1st Jan 2024"
- Names: "John" → "john", "JOHN", "Jon", "Jhon", "J0hn"
- Phone: "1234567890" → "123-456-7890", "(123) 456 7890", "123.456.7890"
- IDs: "ABC123" → "ABC-123", "abc123", "ABC 123", "abc_123"
- Gender: "F" → "Female", "female", "f", "fem"
- Numbers: "123456" → "123,456", "123 456", "123.456", "123K"
- Email: "user@domain.com" → "user @ domain.com", "USER@domain.com"
- Boolean: "true" → "True", "YES", "y", "T"
- Currency: "1000" → "$1000", "1,000", "1000.00", "1k"
- Addresses: "123 Main St" → "123 main street", "123, Main St.", "123 Main Street"
- Countries: "USA" → "United States", "U.S.A.", "US", "U.S."
- Times: "13:45" → "1:45 PM", "13.45", "1345hrs"

You will receive a regex pattern followed by values. For each value:
1. Try to break the regex pattern while keeping meaning crystal clear
2. Ensure a human could confidently restore the original format
3. Output ONLY the disturbed values, one per line, in the same order
4. No explanations or additional text, just the disturbed values

Focus on introducing common real-world variations like:
- Adding spaces, dots, dashes or alternative separators
- Using different regional formats (US/UK/EU styles)
- Adding typical human typos and spelling variations
- Mixing letter cases
- Converting between full and abbreviated forms
- Using text instead of numbers or vice versa
- Adding common prefixes/suffixes
- Using alternative but equivalent representations"""

client = anthropic.Anthropic()

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

def load_table(table_name):
    table_path = f"{DATASET_DIR}/{table_name}.csv"
    return pd.read_csv(table_path)

def damage_values(regex, values):
    # Special handling for gender values
    if regex == "^[MF]$":
        # Randomly choose between keeping original or switching case
        return [v.lower() if np.random.random() > 0.5 else v for v in values]
    
    message = client.messages.create(
        model=MODEL_ID,
        max_tokens=1000,
        temperature=0.7,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content":  f"Regex pattern: {regex}\n\nValues to disturb:\n" + "\n".join(values)}],
    )
    disturbed_values = message.content[0].text.strip().split('\n')
    return disturbed_values

def damage_dataset():
    schema = load_json("schema/schema.json")
    for table_name, table_data in schema.items():
        table_df = load_table(table_name)
        for column_name, column_data in table_data["columns"].items():
            regex = column_data.get("regex")
            if not regex: continue

            values = table_df[column_name].values
            sampled_indexes = np.random.randint(0, len(values), 10)
            sampled_values = values[sampled_indexes]
            if all(pd.isnull(sampled_values)): continue
            sampled_values = list(map(str, sampled_values))
            damaged_values = damage_values(regex, sampled_values)
            table_df.loc[sampled_indexes, column_name] = damaged_values


            print("Table name:", table_name)
            print("Column name:", column_name)
            print("Regex:", regex)
            print("Sampled values:", sampled_values)
            print("Damaged values:", damaged_values)
            print("-" * 50)

        # save the table back to damaged
        table_df.to_csv(f"{DAMAGED_DATASET_DIR}/{table_name}.csv", index=False)

if __name__ == "__main__":
    os.makedirs(DAMAGED_DATASET_DIR, exist_ok=True)
    os.system(f"cp -r {DATASET_DIR}/* {DAMAGED_DATASET_DIR}")
    damage_dataset()