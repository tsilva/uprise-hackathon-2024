import json

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)
    
schema = load_json("schemas/schema.json")
