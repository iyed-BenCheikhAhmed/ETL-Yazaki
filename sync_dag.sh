#!/bin/bash
cp "/mnt/c/Projet fin d'etude/ETL-Yazaki/dags/yazaki_etl_dag.py" ~/ETL-Yazaki/dags/yazaki_etl_dag.py
echo "DAG copied"

# Verify DAG parses correctly
export AIRFLOW_HOME=~/airflow
export PATH=~/airflow-env/bin:$PATH
echo "=== Checking DAG syntax ==="
python ~/ETL-Yazaki/dags/yazaki_etl_dag.py && echo "DAG syntax OK"
echo ""
echo "=== Checking Airflow DAG list ==="
airflow dags list 2>&1 | grep -E "etl_yazaki|error|Error|import"
