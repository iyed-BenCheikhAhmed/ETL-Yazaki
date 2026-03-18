![Security Notice](https://img.shields.io/badge/Notice-No%20real%20or%20sensitive%20data%20is%20stored%20in%20this%20repository%20—even%20with%20advanced%20analysis%20or%20exploitation%20attempts%2C%20no%20valuable%20information%20can%20be%20extracted-blue)

![Security Notice](https://img.shields.io/badge/Notice-Feel%20free%20to%20explore%20—you%20won’t%20find%20any%20real%20or%20exploitable%20data%20here-green)


No real or sensitive data is stored in this repository — even with advanced analysis or exploitation attempts, no valuable information can be extracted.

Feel free to explore — you won’t find any real or exploitable data here.




# ETL-Yazaki — Data Warehouse Pipeline

> **⚠️ IMPORTANT NOTICE — Synthetic Data Only**
> All data used in this project is **entirely generated/synthetic** and does **not** represent any real employee, department, or operational data from Yazaki or any other company. This measure was deliberately taken to **protect the confidentiality and privacy** of all corporate and personal information. No real data is stored, committed, or shared anywhere in this repository.

---

## Overview

**ETL-Yazaki** is an end-to-end ETL (Extract, Transform, Load) pipeline built as a final-year engineering project (*Projet de Fin d'Études*). It simulates a realistic data integration scenario for a manufacturing company, processing two cost-related data sources — **telephone charges** and **printing charges** — and loading them into a structured **Data Warehouse** for reporting and analysis.

The pipeline is fully orchestrated with **Apache Airflow**, scheduled to run every **Monday at midnight**.

---

## Architecture

```
┌──────────────────────┐       ┌───────────────────────┐       ┌──────────────────────┐
│   Source Database    │──────▶│   ETL Pipeline (Python)│──────▶│  Data Warehouse (DW) │
│  (SQL Server OLTP)   │       │  Extract → Transform   │       │  (SQL Server OLAP)   │
│                      │       │        → Load          │       │                      │
│ • ChargesTelephone   │       │  pandas / SQLAlchemy   │       │ • Dim_Departement    │
│ • ChargesImpression  │       │  pyodbc / ODBC 17      │       │ • Dim_Employee       │
└──────────────────────┘       └───────────────────────┘       │ • Dim_Role           │
                                          │                     │ • Dim_Impression     │
                               ┌──────────▼──────────┐         │ • Fact_Telephone     │
                               │   Apache Airflow     │         │ • Fact_Impression    │
                               │  DAG: etl_yazaki     │         └──────────────────────┘
                               │  Schedule: Mon 00:00 │
                               └─────────────────────┘
```

---

## Project Structure

```
ETL-Yazaki/
├── etl/
│   ├── config.py         # DB connection settings (credentials via env vars)
│   ├── extract.py        # Extract data from source SQL Server
│   ├── transform.py      # Data cleaning & transformation logic
│   └── load.py           # Load dimensions and fact tables into DW
├── dags/
│   └── yazaki_etl_dag.py # Airflow DAG definition (weekly schedule)
├── run_etl.py            # Standalone ETL entry point (called by Airflow)
├── .gitignore
├── README.md
├── USAGE_GUIDE.md
└── AIRFLOW_GUIDE.md
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.x |
| Data Manipulation | pandas |
| Database Connectivity | pyodbc, SQLAlchemy |
| Source / DW Database | Microsoft SQL Server |
| ODBC Driver | ODBC Driver 17 for SQL Server |
| Orchestration | Apache Airflow |
| OS / Environment | Windows + WSL (Ubuntu) |

---

## ETL Steps

### 1. Extract
Reads raw data from the source SQL Server database (`Yazaki_Source`):
- `ChargesTelephoniques` — phone usage charges per employee/department
- `ChargesImpression` — printing charges per employee/department

> ⚠️ **Reminder:** All source data is **synthetic and randomly generated**. It does not reflect real Yazaki operations, employees, or costs.

### 2. Transform
Key transformations applied:
- Department name normalization (strip, casing fixes)
- Missing value imputation via column mapping
- Deduplication and referential integrity preparation
- Separation into dimension and fact datasets

### 3. Load
Loads the transformed data into the `DW_Yazaki` Data Warehouse:
- Clears existing fact and dimension tables (with IDENTITY reseed)
- Loads dimensions: `Dim_Departement`, `Dim_Employee`, `Dim_Role`, `Dim_Impression`
- Loads facts: `Fact_Telephone`, `Fact_Impression`

---

## Airflow Orchestration

The DAG `etl_yazaki` is defined in [`dags/yazaki_etl_dag.py`](dags/yazaki_etl_dag.py):

- **Schedule:** Every Monday at 00:00 (`0 0 * * 1`)
- **Catchup:** Disabled
- **Task:** Calls `run_etl.py` via `BashOperator`

See [AIRFLOW_GUIDE.md](AIRFLOW_GUIDE.md) for setup and [USAGE_GUIDE.md](USAGE_GUIDE.md) for running instructions.

---

## Setup & Usage

### Prerequisites
- Python 3.x
- Microsoft SQL Server (local instance)
- ODBC Driver 17 for SQL Server
- Apache Airflow (via WSL/Linux)

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/ETL-Yazaki.git
cd ETL-Yazaki
```

### 2. Configure credentials
Edit `etl/config.py` and set your SQL Server credentials, **or** use environment variables (recommended):

```python
SQL_USER = os.environ.get("SQL_USER")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")
```

> 🔐 **Never commit real credentials.** The `.gitignore` is configured to help prevent accidental exposure.

### 3. Run the ETL manually
```bash
python run_etl.py
```

### 4. Deploy with Airflow
Refer to [AIRFLOW_GUIDE.md](AIRFLOW_GUIDE.md) for full Airflow setup on WSL.

---

## Data Notice

> **⚠️ ALL DATA IN THIS PROJECT IS SYNTHETIC**
>
> The datasets used throughout this project — including employee names, department codes, phone usage records, and printing charge records — are **entirely fictitious and computer-generated**. They were created solely for the purpose of demonstrating the ETL pipeline's functionality.
>
> This approach was deliberately chosen to **ensure full compliance with data privacy regulations** and to **protect the confidentiality of Yazaki's corporate data**. Under no circumstances should real operational data be added to or committed in this repository.

---

## Authors

- **Iyed** — ETL development, Airflow orchestration, Data Warehouse design
- **[Hafaiedh-Habib](https://github.com/Hafaiedh-Habib)** — Collaborator

---

## License

This project is for academic purposes only (*Projet de Fin d'Études*). All company names are used for educational context only.
