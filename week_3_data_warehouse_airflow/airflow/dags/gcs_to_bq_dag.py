import os
from datetime import datetime
import logging

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASET = 'tripdata'
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2023, 1, 1)
) as dag:

    for colour, ds_col in COLOUR_RANGE.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{colour}_{DATASET}_files_task',
            source_bucket=BUCKET,
            source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{colour}/{colour}_{DATASET}',
            move_object=True
        )

        biquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'bq_{colour}_{DATASET}_external_table_task',
            table_resource={
                'tableReference': {
                    'projectId': PROJECT_ID,
                    'datasetId': BIGQUERY_DATASET,
                    'tableId': f'{colour}_{DATASET}_external_table',
                },
                'externalDataConfiguration': {
                    'autodetect': 'True',
                    'sourceFormat': f'{INPUT_FILETYPE.upper()}',
                    'sourceUris': [f'gs://{BUCKET}/{colour}/*'],
                }
            }
        )

        CREATE_BQ_TBL_QUERY = (
            f'CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;'
        )

        # create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f'bq_create_{colour}_{DATASET}_partitioned_table_task',
            configuration={
                'query': {
                    'query': CREATE_BQ_TBL_QUERY,
                    'useLegacySql': False,
                }
            }
        )


        move_files_gcs_task >> biquery_external_table_task >> bq_create_partitioned_table_job