# ETL Yazaki - Usage Guide

## Overview
The ETL Yazaki pipeline extracts data from the source database, transforms it, and loads it into the data warehouse. It runs **automatically every day at midnight UTC** through Apache Airflow.

## Stop Airflow with this command:
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow webserver --stop"
---

## Quick Start
wsl -e bash "/mnt/c/Projet fin d'etude/ETL-Yazaki/start_airflow.sh"


Start-Process "wsl.exe" -ArgumentList "-u", "iyed", "--", "bash", "/home/iyed/start_af.sh"

### 1. Check if Airflow is Running
```bash
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow webserver --version"
```

### 2. Access Airflow UI
Open your browser and go to: **http://localhost:8080**
- **Username:** `admin`
- **Password:** `n4tMvhsDUu7mVnt7`

---

## Running the ETL

### Option 1: Trigger from Airflow UI (Recommended)
1. Go to http://localhost:8080
2. Click on the `etl_yazaki` DAG
3. Click the play button (▶) in the top-right corner
4. Click "Trigger DAG"

### Option 2: Trigger from Terminal
```bash
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow dags trigger etl_yazaki"
```

### Option 3: Run ETL Script Directly (Bypass Airflow)
```bash
wsl -e bash /home/iyed/run_etl_wrapper
```

---

## Monitor Execution

### View Task Logs
```bash
# Find the latest log
wsl -e bash -c "find /home/iyed/airflow/logs/dag_id=etl_yazaki -name '*.log' -type f | sort -r | head -1 | xargs cat | tail -50"
```

### Check DAG Status
```bash
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow dags list-runs -d etl_yazaki --limit 10"
```

---

## Restart Airflow (if needed)

### Start Airflow
```bash
wsl -e bash "/mnt/c/Projet fin d'etude/ETL-Yazaki/start_airflow.sh"
```

### Stop Airflow
```bash
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow webserver --stop"
```

---

## Database Information

- **Source DB:** `Yazaki_Source` on `LAPTOP-65SMMFRO\MSSQLSERVER2`
- **DW DB:** `DW_Yazaki` on `LAPTOP-65SMMFRO\MSSQLSERVER2`
- **ODBC Driver:** 17 for SQL Server
- **SQL Credentials:** User `iyed_sql`, Password `IYED123`

---

## Expected Output

When the ETL runs successfully, you should see:
```
[ETL] Démarrage du pipeline ETL Yazaki...
[ETL] Extraction des données...
[ETL] ChargesTelephoniques: (6240, 10)
[ETL] ChargesImpression: (16007, 10)
[ETL] Transformation des données...
[ETL] Chargement dans le Data Warehouse...
[LOAD] 16 lignes chargees dans Dim_Departement
[LOAD] 130 lignes chargees dans Dim_Employee
[LOAD] 10 lignes chargees dans Dim_Role
[LOAD] 4 lignes chargees dans Dim_Impression
[LOAD] 6240 lignes chargees dans Fact_Telephone
[LOAD] 16007 lignes chargees dans Fact_Impression
[ETL] Pipeline terminé avec succès!
```

---

## Project Structure

```
C:\Projet fin d'etude\ETL-Yazaki/
├── dags/
│   └── yazaki_etl_dag.py          # Airflow DAG definition
├── etl/
│   ├── config.py                  # Database connection config
│   ├── extract.py                 # Extract from source DB
│   ├── transform.py               # Transform data
│   └── load.py                    # Load into DW DB
├── run_etl.py                     # Standalone ETL runner
├── run_etl_wrapper                # Shell wrapper for Airflow
├── start_airflow.sh               # Start Airflow server
├── sync_dag.sh                    # Sync DAG to Airflow
└── show_logs.sh                   # Display latest task logs
```

---

## Troubleshooting

See `FIX_HISTORY.md` for comprehensive error resolution guide.
