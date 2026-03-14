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
    schedule="0 0 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "yazaki"],
) as dag:

    etl_task = BashOperator(
        task_id="run_etl",
        bash_command="/home/iyed/run_etl_wrapper",
    )