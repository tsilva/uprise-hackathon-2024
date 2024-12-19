import re
import json
import pandas as pd

DATASET_DIR = "datasets/Synthea27Nj_5.4"

def load_json(file_path):
    with open(file_path, 'r') as f: data = json.load(f)
    return data

def save_json(file_path, data):
    with open(file_path, 'w') as f: json.dump(data, f, indent=4)

def load_table(table_name):
    table_path = f"{DATASET_DIR}/{table_name}.csv"
    return pd.read_csv(table_path)

def set_column_eval(schema_quality, table_name, column_name, eval_name, eval_value):
    table = schema_quality.get(table_name, {})
    table_columns = table.get("columns", {})
    column = table_columns.get(column_name, {})
    column_evals = column.get("evals", {})
    column_evals[eval_name] = eval_value
    column["evals"] = column_evals
    table_columns[column_name] = column
    table["columns"] = table_columns
    schema_quality[table_name] = table

def eval_regex_accuracy(schema, schema_quality):
    for table_name, table_data in schema.items():
        df = load_table(table_name)

        for column_name, column_data in table_data["columns"].items():
            if not 'regex' in column_data: continue

            regex = column_data['regex']
            compiled_pattern = re.compile(regex)

            values = df[column_name].values
            mismatches = [value for value in values if not compiled_pattern.fullmatch(str(value))]

            regex_accuracy = 1.0 - len(mismatches) / len(values)
            set_column_eval(schema_quality, table_name, column_name, "regex_accuracy", regex_accuracy)

def calculate_table_evals(schema_quality):
    def _calculate_table_evals(table_data):
        """Calculate aggregate evaluations for a table from its column evaluations."""
        if not table_data.get("columns"):
            return {}
        
        # Collect all column evals
        all_metrics = {}
        for column_data in table_data["columns"].values():
            if "evals" in column_data:
                for metric, value in column_data["evals"].items():
                    if metric not in all_metrics:
                        all_metrics[metric] = []
                    all_metrics[metric].append(value)
        
        # Calculate averages for each metric
        aggregates = {}
        for metric, values in all_metrics.items():
            if values:  # Only calculate if we have values
                aggregates[metric] = sum(values) / len(values)
        
        score = sum(aggregates.values()) / len(aggregates) if aggregates else 1.0
        aggregates["score"] = score

        return aggregates
    
    for table_name, table_data in schema_quality.items():
        table_evals = _calculate_table_evals(table_data)
        schema_quality[table_name]["evals"] = table_evals

def calculate_final_score(schema_quality):
    final_score = 0
    for _, table_data in schema_quality.items(): final_score += table_data["evals"]["score"]
    final_score /= len(schema_quality)
    return final_score

schema = load_json('schema/schema.json')
schema_quality = {}
eval_regex_accuracy(schema, schema_quality)
calculate_table_evals(schema_quality)
save_json('schema/schema_quality.json', schema_quality)

final_score = calculate_final_score(schema_quality)
print(f"Eval score: {final_score * 100:.2f}%")