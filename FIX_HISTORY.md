# ETL Yazaki - Complete Fix History & Troubleshooting Guide

## Table of Contents
1. [Original Errors & Root Causes](#original-errors--root-causes)
2. [Phase 1: ODBC Driver Fix](#phase-1-odbc-driver-fix)
3. [Phase 2: Database Column Error Fix](#phase-2-database-column-error-fix)
4. [Phase 3: Airflow Setup in WSL](#phase-3-airflow-setup-in-wsl)
5. [Phase 4: ETL Path Issues in Airflow](#phase-4-etl-path-issues-in-airflow)
6. [Phase 5: Dependency Issues](#phase-5-dependency-issues)
7. [Phase 6: DAG Execution Errors](#phase-6-dag-execution-errors)
8. [Quick Reference Commands](#quick-reference-commands)

---

## Original Errors & Root Causes

### Environment
- **OS:** Windows 11
- **Project Path:** `C:\Projet fin d'etude\ETL-Yazaki`
- **SQL Server:** `LAPTOP-65SMMFRO\MSSQLSERVER2`
- **Databases:** `Yazaki_Source` (source), `DW_Yazaki` (data warehouse)
- **Python:** Windows Anaconda + WSL Ubuntu 24.04
- **Airflow:** Version 3.1.8 in WSL (not Windows)

---

## Phase 1: ODBC Driver Fix

### Error
```
InterfaceError: ('HYT00', '[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]...')
ODBC Driver 18 for SQL Server not found
```

### Root Cause
The `config.py` specified ODBC Driver 18, which was **not installed**. Only Driver 17 was available.

### Original Code (BROKEN)
```python
# etl/config.py
def get_source_connection():
    connection_string = (
        'DRIVER={ODBC Driver 18 for SQL Server};'  # ❌ Driver 18 not installed
        f'SERVER={SERVER};'
        f'DATABASE={SOURCE_DB};'
        f'UID={SQL_USER};'
        f'PWD={SQL_PASSWORD};'
    )
    return pyodbc.connect(connection_string)

def get_dw_engine():
    connection_string = (
        'DRIVER={ODBC Driver 18 for SQL Server};'  # ❌ Driver 18 not installed
        ...
    )
```

### Fix Applied
Changed both functions to use **ODBC Driver 17**:

```python
# etl/config.py - FIXED
def get_source_connection():
    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'  # ✅ Use Driver 17
        f'SERVER={SERVER};'
        f'DATABASE={SOURCE_DB};'
        f'UID={SQL_USER};'
        f'PWD={SQL_PASSWORD};'
    )
    return pyodbc.connect(connection_string)

def get_dw_engine():
    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'  # ✅ Use Driver 17
        ...
    )
```

### Result
✅ Connection to SQL Server now works correctly.

---

## Phase 2: Database Column Error Fix

### Error
```
ProgrammingError: ('42S22', "[42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Invalid column name 'NumeroTel'.")
```

### Root Cause
The code tried to load a column `NumeroTel` into `Dim_Employee` table, but the actual column name in the database is `NumeroEmployee`.

### Original Code (BROKEN)
```python
# etl/load.py - load_dim_employee()
def load_dim_employee(emps):
    # emps has columns: CodeEmployee, NumeroTel
    # but Dim_Employee table expects: CodeEmployee, NumeroEmployee
    
    cursor = DW_CONN.cursor()
    for idx, row in emps.iterrows():
        cursor.execute(
            """INSERT INTO Dim_Employee (CodeEmployee, NumeroEmployee)
               VALUES (?, ?)""",
            row['CodeEmployee'],
            row['NumeroTel']  # ❌ Column doesn't exist - should be renamed
        )
    DW_CONN.commit()
```

### Query to Check Database Schema
```sql
-- Run on SQL Server to verify column names
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'Dim_Employee'
```

**Result:** Actual columns are `CodeEmployee` and `NumeroEmployee` (not `NumeroTel`)

### Fix Applied
Rename the column before inserting:

```python
# etl/load.py - FIXED
def load_dim_employee(emps):
    # Rename NumeroTel to NumeroEmployee to match DB schema
    emps = emps[['CodeEmployee', 'NumeroTel']].rename(
        columns={'NumeroTel': 'NumeroEmployee'}
    )
    
    cursor = DW_CONN.cursor()
    for idx, row in emps.iterrows():
        cursor.execute(
            """INSERT INTO Dim_Employee (CodeEmployee, NumeroEmployee)
               VALUES (?, ?)""",
            row['CodeEmployee'],
            row['NumeroEmployee']  # ✅ Correct column name
        )
    DW_CONN.commit()
```

### Result
✅ Employee data now loads successfully into the database.

---

## Phase 3: Airflow Setup in WSL

### Why WSL?
Apache Airflow 3 requires Python 3.9+. Setting it up on Windows directly is problematic. WSL provides a Linux environment where we can run Airflow cleanly.

### Steps

#### 3.1 Install WSL Ubuntu
```powershell
# On Windows PowerShell
wsl --install Ubuntu
```

#### 3.2 Create Python Virtual Environment in WSL
```bash
# In WSL terminal
cd ~
python3 -m venv airflow-env
source airflow-env/bin/activate
```

#### 3.3 Install Apache Airflow & Dependencies
```bash
# In WSL terminal
pip install --upgrade pip
pip install apache-airflow==3.1.8 pandas pyodbc sqlalchemy
```

#### 3.4 Initialize Airflow Database
```bash
# In WSL terminal
source ~/airflow-env/bin/activate
export AIRFLOW_HOME=~/airflow
airflow db migrate
```

#### 3.5 Create Admin User (Airflow 3 Approach)
**Problem:** Airflow 3 removed the `airflow users create` command.

**Solution:** Use `standalone` mode which auto-creates admin user:
```bash
# In WSL terminal
source ~/airflow-env/bin/activate
export AIRFLOW_HOME=~/airflow
airflow standalone
```

This generates initial credentials. Password appears in the console (save it).

For our setup: **Admin user:** `admin`, **Password:** `n4tMvhsDUu7mVnt7`

### Result
✅ Airflow running at http://localhost:8080

---

## Phase 4: ETL Path Issues in Airflow

### Error
When Airflow tried to run the ETL, it failed with:
```
python.exe: can't open file "/mnt/c/Projet fin d'etude/ETL-Yazaki/run_etl.py": 
[Errno 13] Permission denied
```

**Log details:**
```
"Running command: ['/usr/bin/bash', '-c', '\"...\" \"/mnt/c/Projet fin d'etude/ETL-Yazaki/run_etl.py\"']"
"Failed path: \\wsl.localhost\Ubuntu\mnt\c\Projet fin d'etude\ETL-Yazaki\run_etl.py"
```

### Root Causes

**Cause 1 - Wrong Path Type:** 
- Windows Python (Anaconda) can't read `/mnt/c/...` WSL paths
- It converts them to UNC paths (`\\wsl.localhost\Ubuntu\...`) which it can't access
- Windows Python needs Windows paths like `C:/Projet fin d'etude/...`

**Cause 2 - Quoting Issues:**
- The apostrophe in `d'etude` breaks bash single-quoted strings
- `bash_command="'C:/Projet fin d'etude/..."'` becomes invalid

### Original DAG Code (BROKEN)
```python
# dags/yazaki_etl_dag.py - BROKEN
WINDOWS_PYTHON = "/mnt/c/Users/HP/anaconda3/python.exe"
RUN_ETL_SCRIPT = "C:/Projet fin d'etude/ETL-Yazaki/run_etl.py"

with DAG("etl_yazaki", ...) as dag:
    etl_task = BashOperator(
        task_id="run_etl",
        # ❌ Single-quoted string breaks on apostrophe in d'etude
        bash_command=f"'{WINDOWS_PYTHON}' '{RUN_ETL_SCRIPT}'",
    )
```

### Fix Applied

**Step 1: Ensure ETL Script Uses Windows Paths**

```python
# run_etl.py - FIXED
import sys
import os

# Use Windows-style raw path (R-string)
PROJECT_DIR = r"C:\Projet fin d'etude\ETL-Yazaki"
sys.path.insert(0, PROJECT_DIR)

from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_all

if __name__ == "__main__":
    print("[ETL] Démarrage du pipeline ETL Yazaki...")
    try:
        data = extract_all()
        df_transformed = transform_all(data)
        load_all(df_transformed)
        print("[ETL] Pipeline terminé avec succès!")
    except Exception as e:
        print(f"[ETL] Erreur: {e}")
        raise
```

**Step 2: Create Shell Wrapper to Avoid Quoting Issues**

Create `/home/iyed/run_etl_wrapper` (no `.sh` extension):

```bash
#!/bin/bash
exec "/mnt/c/Users/HP/anaconda3/python.exe" "C:/Projet fin d'etude/ETL-Yazaki/run_etl.py"
```

**How to create it:**
```powershell
# From Windows PowerShell
$content = @"
#!/bin/bash
exec "/mnt/c/Users/HP/anaconda3/python.exe" "C:/Projet fin d'etude/ETL-Yazaki/run_etl.py"
"@
[System.IO.File]::WriteAllText("\\wsl`$\Ubuntu\home\iyed\run_etl_wrapper", $content, [System.Text.Encoding]::UTF8)
wsl -e chmod +x /home/iyed/run_etl_wrapper
```

**Step 3: Update DAG to Call Wrapper**

```python
# dags/yazaki_etl_dag.py - FIXED
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
}

with DAG(
    "etl_yazaki",
    default_args=default_args,
    description="ETL Yazaki - Extract, Transform, Load",
    schedule="@daily",  # ✅ Runs automatically every day at midnight UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "yazaki"],
) as dag:

    etl_task = BashOperator(
        task_id="run_etl",
        bash_command="/home/iyed/run_etl_wrapper",  # ✅ No extension, no quoting issues
    )
```

### Why No Extension?
Airflow 3's Jinja2 engine automatically treats filenames ending in `.sh`, `.sql`, etc. as template files and tries to load them from the dags folder. Removing the extension prevents this.

### Result
✅ Airflow can now call the ETL script through the wrapper without path/quoting issues.

---

## Phase 5: Dependency Issues

### Issue 1: Missing `libodbc.so.2` in WSL
When running `pyodbc` in WSL Linux, it couldn't find the ODBC library.

**Fix:**
```bash
# In WSL terminal
sudo apt-get update
sudo apt-get install -y unixodbc unixodbc-dev
```

### Issue 2: `msodbcsql17` Package Failed
Attempted to install Microsoft ODBC Driver 17 in WSL but setup kept getting interrupted.

**Solution:** Abandoned this approach. Instead, use **Windows Anaconda Python** which already has ODBC Driver 17 installed. This is why we call Windows Python via `/mnt/c/Users/HP/anaconda3/python.exe` from the wrapper script.

### Result
✅ Dependencies resolved without complex Linux driver setup.

---

## Phase 6: DAG Execution Errors

### Error 1: Template Loading Error
```
TemplateNotFound: 'bash /home/iyed/run_etl_wrapper.sh' not found in search path: '/home/iyed/airflow/dags'
```

**Cause:** Airflow tried to load `.sh` files as Jinja2 templates.

**Fix:** Remove `.sh` extension (handled in Phase 4, Step 2).

### Error 2: Wrapper Script Shebang Issue
When the wrapper was first created with certain quoting methods, the shebang line was corrupted:
```
/home/iyed/run_etl_wrapper: line 1: #!/bin/bash: No such file or directory
```

**Fix:** Use PowerShell to write the file correctly:
```powershell
$content = @"
#!/bin/bash
exec "/mnt/c/Users/HP/anaconda3/python.exe" "C:/Projet fin d'etude/ETL-Yazaki/run_etl.py"
"@
[System.IO.File]::WriteAllText("\\wsl`$\Ubuntu\home\iyed\run_etl_wrapper", $content, [System.Text.Encoding]::UTF8)
```

### Result
✅ DAG runs successfully end-to-end.

---

## Quick Reference Commands

### Start/Stop Airflow
```bash
# Start
wsl -e bash "/mnt/c/Projet fin d'etude/ETL-Yazaki/start_airflow.sh"

# Stop
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow webserver --stop"
```

### Trigger DAG Manually
```bash
# From terminal
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow dags trigger etl_yazaki"

# Direct ETL run
wsl -e bash /home/iyed/run_etl_wrapper
```

### Sync DAG to Airflow
```bash
wsl -e bash "/mnt/c/Projet fin d'etude/ETL-Yazaki/sync_dag.sh"
```

### View Logs
```bash
# Find latest log file
wsl -e bash -c "find /home/iyed/airflow/logs/dag_id=etl_yazaki -name '*.log' -type f | sort -r | head -1 | xargs cat | tail -50"

# Or use helper script
wsl -e bash "/mnt/c/Projet fin d'etude/ETL-Yazaki/show_logs.sh"
```

### Check DAG Syntax
```bash
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && python -m py_compile /home/iyed/airflow/dags/yazaki_etl_dag.py"
```

### Unpause/Pause DAG
```bash
# Unpause (enable scheduling)
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow dags unpause etl_yazaki"

# Pause (disable scheduling)
wsl -e bash -c "source /home/iyed/airflow-env/bin/activate && airflow dags pause etl_yazaki"
```

---

## File Modifications Summary

| File | Change | Line(s) |
|------|--------|---------|
| `etl/config.py` | ODBC Driver 18 → 17 | config strings in both functions |
| `etl/load.py` | Rename `NumeroTel` → `NumeroEmployee` | `load_dim_employee()` function |
| `run_etl.py` | Use Windows raw path: `r"C:\Projet fin d'etude\ETL-Yazaki"` | PROJECT_DIR assignment |
| `dags/yazaki_etl_dag.py` | Simplify to call wrapper script | BashOperator bash_command |
| **NEW** | `/home/iyed/run_etl_wrapper` | Shell wrapper to call Windows Python with correct paths |

---

## Success Indicators

When the ETL runs successfully, you'll see:
- ✅ 6,240 rows loaded into `Fact_Telephone`
- ✅ 16,007 rows loaded into `Fact_Impression`
- ✅ All dimension tables populated
- ✅ `[ETL] Pipeline terminé avec succès!` message
- ✅ Airflow task shows GREEN status
- ✅ Exit code: 0

---

## Prevention Checklist for Similar Issues

- [ ] Always verify ODBC driver version is installed: `odbcinst -j`
- [ ] Validate database column names before querying: run `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS`
- [ ] Test connection string locally before deploying to Airflow
- [ ] Use raw path strings (r"...") for Windows paths with special characters
- [ ] Avoid file extensions in Airflow BashOperator to prevent Jinja2 template loading
- [ ] Split complex bash commands into wrapper scripts to avoid quoting issues
- [ ] Test wrapper scripts manually before deploying to Airflow
- [ ] Check logs early and often: `airflow tasks logs <dag_id> <task_id>`

---

## Directory Structure (Final)

```
C:\Projet fin d'etude\ETL-Yazaki/
├── dags/
│   └── yazaki_etl_dag.py              # Airflow DAG - calls wrapper
├── etl/
│   ├── __init__.py
│   ├── config.py                      # ✅ FIXED: ODBC Driver 17
│   ├── extract.py
│   ├── transform.py
│   └── load.py                        # ✅ FIXED: NumeroEmployee rename
├── run_etl.py                         # ✅ FIXED: Windows raw path
├── run_etl_wrapper                    # ✅ NEW: Wrapper script in WSL
├── start_airflow.sh                   # Helper: Start Airflow
├── sync_dag.sh                        # Helper: Deploy DAG
├── show_logs.sh                       # Helper: View logs
├── USAGE_GUIDE.md                     # Quick start guide
└── FIX_HISTORY.md                     # This file
```

WSL home:
```
/home/iyed/
├── airflow-env/                       # Airflow Python venv
├── airflow/                           # Airflow home directory
│   ├── dags/                          # Synced DAG files
│   ├── logs/                          # Task execution logs
│   └── airflow.db                     # Airflow metadata DB
└── run_etl_wrapper                    # ✅ Wrapper script
```

