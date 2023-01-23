import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    'LocalIngestionDag',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2023, 1, 1)
)

URL = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

with local_workflow:

    wget_task = BashOperator(
        task_id = 'wget',
        bash_command = f'curl -sSL {URL} > {AIRFLOW_HOME}/output.csv'
    )

    ingest_task = BashOperator(
        task_id = 'ingest',
        bash_command = f'ls {AIRFLOW_HOME}'
    )


    wget_task >> ingest_task