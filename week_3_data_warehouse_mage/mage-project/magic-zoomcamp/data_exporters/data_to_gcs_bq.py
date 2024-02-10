from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def tableExists(tableID, client):
    """
    Check if a table already exists using the tableID.
    return : (Boolean)
    """
    try:
        table = client.get_table(tableID)
        return True
    except Exception as e:#NotFound:
        return False

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """

    color = kwargs['color']
    year = kwargs['year']

    project_id = os.environ['PROJECT_ID']


    ############################
    # Export to GCS
    ############################
    bucket_name = os.environ['BUCKET_NAME']
    object_key = f'{color}/{color}_tripdata_{year}'
    root_path = f"{bucket_name}/{object_key}"
    
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        filesystem=gcs,
        use_deprecated_int96_timestamps=True  # Write timestamps to INT96 Parquet format
    )

    print(f'Exported to GCS: {root_path}')

    ############################
    # Create External Table
    # reference: https://cloud.google.com/bigquery/docs/external-data-cloud-storage
    ############################
    dataset_name = os.environ['DATASET_NAME']
    table_name = 'green_taxi'
    # Set table_id to the ID of the table to create
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Construct a BigQuery client object
    client = bigquery.Client()

    # Set the external source format of your table
    external_source_format = "PARQUET"

    # Set the source_uris to point to your data in Google Cloud
    source_uris = [ f'gs://{bucket_name}/{object_key}/*']

    # Create ExternalConfig object with external source format
    external_config = bigquery.ExternalConfig(external_source_format)
    # Set source_uris that point to your data in Google Cloud
    external_config.source_uris = source_uris
    external_config.autodetect = True

    # Set table schema manually (if no autodetect)
    # schema = [
    #     bigquery.schema.SchemaField("vendor_id", "INT64"),
    #     bigquery.schema.SchemaField("lpep_pickup_datetime", "TIMESTAMP"),
    #     bigquery.schema.SchemaField("lpep_dropoff_datetime", "TIMESTAMP"),
    #     bigquery.schema.SchemaField("store_and_fwd_flag", "STRING"),
    #     bigquery.schema.SchemaField("ratecode_id", "INT64"),
    #     bigquery.schema.SchemaField("pulocation_id", "INT64"),
    #     bigquery.schema.SchemaField("dolocation_id", "INT64"),
    #     bigquery.schema.SchemaField("passenger_count", "INT64"),
    #     bigquery.schema.SchemaField("trip_distance", "FLOAT64"),
    #     bigquery.schema.SchemaField("fare_amount", "FLOAT64"),
    #     bigquery.schema.SchemaField("extra", "FLOAT64"),
    #     bigquery.schema.SchemaField("mta_tax", "FLOAT64"),
    #     bigquery.schema.SchemaField("tip_amount", "FLOAT64"),
    #     bigquery.schema.SchemaField("tolls_amount", "FLOAT64"),
    #     bigquery.schema.SchemaField("ehail_fee", "INT64"),
    #     bigquery.schema.SchemaField("improvement_surcharge", "FLOAT64"),
    #     bigquery.schema.SchemaField("total_amount", "FLOAT64"),
    #     bigquery.schema.SchemaField("payment_type", "INT64"),
    #     bigquery.schema.SchemaField("trip_type", "INT64"),
    #     bigquery.schema.SchemaField("congestion_surcharge", "FLOAT64"),
    # ]
    
    table = bigquery.Table(table_id)#, schema)
    # Set the external data configuration of the table
    table.external_data_configuration = external_config


    if tableExists(table, client):
        print("Table already exists, Deleting the table ... ")
        client.delete_table(table)

    table = client.create_table(table)  # Make an API request.

    print(f'Created table with external source: {table_id}')
    print(f'Format: {table.external_data_configuration.source_format}')