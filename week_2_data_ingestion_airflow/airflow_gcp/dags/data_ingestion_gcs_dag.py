import os
from datetime import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
# TARGET_DATE = '{{ execution_date.strftime(\'%Y-%m\') }}'
TARGET_DATE = '2021-{{ execution_date.strftime(\'%m\') }}'
TARGET_FILE = 'yellow_tripdata_' + TARGET_DATE + '.parquet'
URL_TEMPLATE = URL_PREFIX + '/' + TARGET_FILE
OUTPUT_FILE_TEMPLATE = os.path.join(AIRFLOW_HOME, TARGET_FILE)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
    storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


with DAG(
    dag_id='data_ingestion_gcs_dag',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2023, 1, 1)
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{TARGET_FILE}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{TARGET_FILE}"],
            },
        },
    )


    download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task
