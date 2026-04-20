from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import json
import urllib.parse
import urllib.request


PROJECT_ROOT = Path(os.getenv("YAZAKI_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(PROJECT_ROOT))

from etl.extract import extract_charges_telephoniques, extract_charges_impression
from etl.transform import transform_charges_telephoniques, transform_charges_impression
from etl.load import load_all
from etl.config import get_dw_engine
from etl.prevision import run_previsions_pipeline

default_args = {
    "owner": "yazaki",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "etl_yazaki_dw_previsions",
    default_args=default_args,
    description="ETL Yazaki + generation previsions dans DW",
    schedule="0 2 * * *",
    catchup=False,
)


def extract_task(**context):
    df_tel = extract_charges_telephoniques()
    df_imp = extract_charges_impression()
    tmp_dir = Path("/tmp")
    df_tel.to_csv(tmp_dir / "tel.csv", index=False)
    df_imp.to_csv(tmp_dir / "imp.csv", index=False)
    return {"tel_rows": len(df_tel), "imp_rows": len(df_imp)}


def transform_task(**context):
    import pandas as pd

    tmp_dir = Path("/tmp")
    df_tel = pd.read_csv(tmp_dir / "tel.csv")
    df_imp = pd.read_csv(tmp_dir / "imp.csv")
    df_tel_transformed = transform_charges_telephoniques(df_tel)
    df_imp_transformed = transform_charges_impression(df_imp)

    df_tel_transformed.to_csv(tmp_dir / "tel_transformed.csv", index=False)
    df_imp_transformed.to_csv(tmp_dir / "imp_transformed.csv", index=False)

    return {
        "tel_rows_transformed": len(df_tel_transformed),
        "imp_rows_transformed": len(df_imp_transformed),
    }


def load_task(**context):
    import pandas as pd

    tmp_dir = Path("/tmp")
    df_tel = pd.read_csv(tmp_dir / "tel_transformed.csv", parse_dates=["DateOperation"])
    df_imp = pd.read_csv(tmp_dir / "imp_transformed.csv", parse_dates=["DateImpression"])
    load_all(df_tel, df_imp)
    return {"status": "success"}


def forecast_task(**context):
    engine = get_dw_engine()
    try:
        result = run_previsions_pipeline(
            engine,
            horizon_months=12,
            use_notebook_ml=True,
            notebook_path="/opt/airflow/project/Prévision/Prevision.ipynb",
        )
        return result
    finally:
        engine.dispose()


def powerbi_refresh_task(**context):
    refresh_mode = os.getenv("POWERBI_REFRESH_MODE", "service_principal").strip().lower()

    if refresh_mode in {"off", "disabled", "none"}:
        print("[INFO] Power BI refresh desactive via POWERBI_REFRESH_MODE")
        return {"status": "skipped", "reason": "disabled"}

    if refresh_mode in {"gateway", "onprem_gateway", "on-premises-gateway"}:
        print(
            "[INFO] Power BI refresh gere par On-Premises Data Gateway / refresh planifie Power BI Service"
        )
        return {"status": "skipped", "reason": "gateway_managed_refresh"}

    tenant_id = os.getenv("POWERBI_TENANT_ID", "").strip()
    client_id = os.getenv("POWERBI_CLIENT_ID", "").strip()
    client_secret = os.getenv("POWERBI_CLIENT_SECRET", "").strip()
    workspace_id = os.getenv("POWERBI_WORKSPACE_ID", "").strip()
    dataset_id = os.getenv("POWERBI_DATASET_ID", "").strip()

    if not all([tenant_id, client_id, client_secret, workspace_id, dataset_id]):
        raise ValueError(
            "Mode service_principal actif mais variables Power BI manquantes: "
            "POWERBI_TENANT_ID, POWERBI_CLIENT_ID, POWERBI_CLIENT_SECRET, "
            "POWERBI_WORKSPACE_ID, POWERBI_DATASET_ID"
        )

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }
    ).encode("utf-8")

    token_request = urllib.request.Request(
        token_url,
        data=token_payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    with urllib.request.urlopen(token_request, timeout=20) as token_response:
        token_data = json.loads(token_response.read().decode("utf-8"))

    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError("Impossible d'obtenir le token Power BI")

    refresh_url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    )
    refresh_request = urllib.request.Request(
        refresh_url,
        data=json.dumps({}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(refresh_request, timeout=20) as refresh_response:
        status_code = refresh_response.getcode()

    print(f"[INFO] Power BI refresh declenche (HTTP {status_code})")
    return {"status": "triggered", "http_status": status_code}


task_extract = PythonOperator(task_id="extract", python_callable=extract_task, dag=dag)
task_transform = PythonOperator(task_id="transform", python_callable=transform_task, dag=dag)
task_load = PythonOperator(task_id="load_dw", python_callable=load_task, dag=dag)
task_forecast = PythonOperator(task_id="forecast_previsions", python_callable=forecast_task, dag=dag)
task_powerbi_refresh = PythonOperator(
    task_id="refresh_powerbi_dataset",
    python_callable=powerbi_refresh_task,
    dag=dag,
)

task_extract >> task_transform >> task_load >> task_forecast >> task_powerbi_refresh