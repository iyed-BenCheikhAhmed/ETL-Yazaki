#!/bin/bash
LOG=$(find ~/airflow/logs/dag_id=etl_yazaki -name "*.log" | sort -r | head -1)
echo "=== Log file: $LOG ==="
echo ""
cat "$LOG" 2>&1 | tail -60
