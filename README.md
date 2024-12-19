# Pipeline

1. Build the schema from the data - `schema_build.py`
2. Generate the data quality report - `data_eval.py`
3. Damage dataset - `data_damage.py`
4. Generate the data quality report (should be worse) - `data_eval.py`
5. Heal dataset - `data_heal.py`
6. Generate the data quality report (should be better) - `data_eval.py`
