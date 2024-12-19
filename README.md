# uprise-hackathon-2024

## Installation

To set up the conda environment using the `environment.yml` file, run the following command:

```sh
conda env create -f environment.yml
```

This will create a new conda environment with all the dependencies specified in the `environment.yml` file.

## Updating the Environment

If you need to update the conda environment with any changes made to the `environment.yml` file, run:

```sh
conda env update --file environment.yml --prune
```

The `--prune` flag will remove any dependencies that are no longer required.


# TODO

# Build Schema

- Create a schema for each column name
- In which tables is the column found
- Which values in each of these tables
- How much overlap of values is found (primary key?)

- Build schema (quality assessment)
- Quantify data quality by mapping against schema


- Create dictionary of column names
- Create util that dumps data values for each column name
- Create util that creates regex for each of those columsn
- Create util that evaluates which values are out of that regex

Part 1: Chaos Creation
Brainstorm common healthcare data errors: missing values, duplicates, inconsistent formats, logical inconsistencies.
Script a pipeline to introduce these errors subtly and ensure they mimic real-world scenarios.
Part 2: Chaos Detection
Develop metrics to assess:
Completeness: Detect missing values.
Consistency: Flag format mismatches and logical inconsistencies.
Uniqueness: Identify duplicates.
Validity: Verify adherence to schema rules.
Write scripts to calculate a data quality score pre- and post-chaos.
Part 3: Chaos Correction
Use cleaning techniques like imputation, outlier detection, and deduplication.
Develop a pipeline to process the dataset and measure improvement.