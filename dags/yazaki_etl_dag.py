from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys

# Path() convertit une chaîne de caractères en objet Path, ce qui facilite la manipulation des chemins de fichiers
# resolve() retourne le chemin absolu du fichier C:\Users\habib\Desktop\Yazaki\ETL-Yazaki\dags\yazaki_etl_dag.py
# parents[1] remonte d'un niveau dans l'arborescence pour atteindre le dossier racine du projet
# ou se trouve le dossier etl avec les modules extract, transform, load et config

PROJECT_ROOT = Path(os.getenv("YAZAKI_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(PROJECT_ROOT))

from etl.extract import extract_charges_telephoniques, extract_charges_impression
from etl.transform import transform_charges_telephoniques, transform_charges_impression
from etl.load import load_all
from etl.config import get_dw_engine

default_args = {
    "owner": "yazaki",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "etl_yazaki",
    default_args=default_args,
    description="ETL Yazaki DAG",
    schedule=None,  # Pas de planification automatique, exécution manuelle
    catchup=False,  # N'exécute pas les anciennes dates manquées
)


def extract_task():
    df_tel = extract_charges_telephoniques()
    df_imp = extract_charges_impression()
    tmp_dir = Path("/tmp")
    df_tel.to_csv(tmp_dir / "tel.csv", index=False)
    df_imp.to_csv(tmp_dir / "imp.csv", index=False)
    return {"tel_rows": len(df_tel), "imp_rows": len(df_imp)}


def transform_task():
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

# context = dictionnaire avec des infos Airflow
def load_task():
    import pandas as pd

    tmp_dir = Path("/tmp")
    df_tel = pd.read_csv(tmp_dir / "tel_transformed.csv", parse_dates=["DateOperation"])
    df_imp = pd.read_csv(tmp_dir / "imp_transformed.csv", parse_dates=["DateImpression"])
    load_all(df_tel, df_imp)
    return {"status": "success"}


task_extract = PythonOperator(task_id="extract", python_callable=extract_task, dag=dag)
task_transform = PythonOperator(task_id="transform", python_callable=transform_task, dag=dag)
task_load = PythonOperator(task_id="load_dw", python_callable=load_task, dag=dag)

task_extract >> task_transform >> task_load