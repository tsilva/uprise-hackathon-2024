# vaulthaus-hackathon-2024

This is a repository for the [VaultHaus](https://www.vaulthaus.health/) Hackathon 2024. 
The goal of this project was to build a data quality report for a dataset, damage the dataset, and then heal the dataset.

# Recipe

1. Build the schema from the data - `schema_build.py`
2. Generate the data quality report - `data_eval.py`
3. Created damaged dataset - `data_damage.py`
4. Eval damaged dataset - `data_eval.py damaged`
5. Heal dataset - `data_heal.py`
6. Eval healed dataset - `data_eval.py healed`
