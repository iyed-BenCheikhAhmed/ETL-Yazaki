#!/bin/bash
export AIRFLOW_HOME=~/airflow
export PATH=~/airflow-env/bin:$PATH
nohup airflow standalone > ~/airflow/airflow-standalone.log 2>&1 &
echo $! > ~/airflow/airflow.pid
echo "Airflow started with PID: $(cat ~/airflow/airflow.pid)"
echo "Waiting for startup..."
sleep 12
echo ""
echo "=== Status ==="
grep -E "standalone|8080|password|error|Error" ~/airflow/airflow-standalone.log | tail -15
echo ""
echo "=== Admin Password ==="
cat ~/airflow/simple_auth_manager_passwords.json.generated
echo ""
echo "=== Open browser at: http://localhost:8080 ==="
