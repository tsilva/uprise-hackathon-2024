import os
import anthropic
import json
import numpy as np
import pandas as pd

MODEL_ID = "claude-3-5-sonnet-20241022"

DATASET_DIR = "datasets/Synthea27Nj_5.4"
DAMAGED_DATASET_DIR = "datasets/Synthea27Nj_5.4_damaged"

SYSTEM_PROMPT = """You are a value disturber. You will receive a regex pattern followed by values that match that pattern. Your task is to disturb each value while ensuring:
1. The disturbed value maintains compliance with the provided regex pattern
2. The disturbed value maintains the same semantic meaning as the original
3. You output ONLY the disturbed values, one per line, in the exact same order as received
4. No explanations or additional text, just the disturbed values

You can introduce variations like:
- Adding spaces or dots in strings
- Using different date formats for dates
- Adding typical human typos
- Changing letter cases
- Using alternative number formats
- Any other variations that preserve meaning while changing representation"""

client = anthropic.Anthropic()

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

def load_table(table_name):
    table_path = f"{DATASET_DIR}/{table_name}.csv"
    return pd.read_csv(table_path)

def damage_values(regex, values):
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