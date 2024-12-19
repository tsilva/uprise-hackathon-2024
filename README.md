# Pipeline

1. Build the schema from the data - `build_schema.py`
2. Generate the data quality report - `data_evaluate.py`
3. Damage dataset - `damage_dataset.py`
4. Generate the data quality report (should be worse) - `data_evaluate.py`
5. Heal dataset - `repair_dataset.py`
6. Generate the data quality report (should be better) - `data_evaluate.py`
