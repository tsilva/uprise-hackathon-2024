#!/bin/bash

echo "==============================================="
echo "           CLEANUP: PREVIOUS RUNS              "
echo "==============================================="
# Clean up previous runs
rm -rf schema/
rm -f datasets/*_damaged datasets/*_healed

echo "==============================================="
echo "           STEP 1: BUILD SCHEMA               "
echo "==============================================="
python schema_build.py

echo "==============================================="
echo "        STEP 2: BASELINE EVALUATION           "
echo "==============================================="
python data_eval.py

echo "==============================================="
echo "         STEP 3: INJECT DAMAGE                "
echo "==============================================="
python data_damage.py

echo "==============================================="
echo "         STEP 4: APPLY HEALING                "
echo "==============================================="
python data_heal.py

echo "==============================================="
echo "         STEP 5: FINAL EVALUATION             "
echo "==============================================="
python data_eval.py