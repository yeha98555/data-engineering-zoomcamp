import os
from io import BytesIO
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from google.cloud import bigquery

@task()
def fetch_to_gcs(year: int, month: int, color: str) -> None:
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = pd.read_csv(dataset_url)

    if color == "yellow":
        """Fix dtype issues"""
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if color == "green":
        """Fix dtype issues"""
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype('Int64')

    if color == "yellow" or color == "green":
        df["VendorID"] = df["VendorID"].astype('Int64')
        df["RatecodeID"] = df["RatecodeID"].astype('Int64')
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')
        df["passenger_count"] = df["passenger_count"].astype('Int64')
        df["payment_type"] = df["payment_type"].astype('Int64')

    if color == "fhv":
        """Fix dtype issues"""
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])

        # See https://pandas.pydata.org/docs/user_guide/integer_na.html
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    # df.to_parquet(path, compression='gzip')
    gcs_block = GcsBucket.load("dtcde-gcs")
    gcs_block.upload_from_file_object(
        BytesIO(df.to_parquet()),
        f'{color}/{dataset_file}.parquet',
        timeout=9000
    )

@flow(log_prints=True)
def gcs_to_bq(color: str) -> None: 

    gcp_project_block = Secret.load("dtcde-gcp-project")
    GCLOUD_PROJECT = gcp_project_block.get()
    os.environ["GCLOUD_PROJECT"] = GCLOUD_PROJECT

    gcp_bucket_block = Secret.load("dtcde-bucket")
    BUCKET = gcp_bucket_block.get()

    ############################
    # method 1 - Create External Table (cannot count number of rows)
    # reference: https://prefecthq.github.io/prefect-gcp/bigquery/#prefect_gcp.bigquery.bigquery_create_table
    ############################
    gcp_credentials_block = GcpCredentials.load("dtcde-gcp-creds")

    external_config = bigquery.ExternalConfig('PARQUET')
    external_config.autodetect = True
    external_config.source_uris = [ f'gs://{BUCKET}/{color}/*']

    table_name = bigquery_create_table(
        dataset='trips_data_all',
        table=f'{color}_tripdata',
        external_config=external_config,
        gcp_credentials=gcp_credentials_block
    )
    print(table_name)


    # ############################
    # # method 2 - Load data to BigQuery
    # ############################

    # bq_client = bigquery.Client()
    # table_id = GCLOUD_PROJECT + f'.trips_data_all.{color}_tripdata_data'

    # job_config = bigquery.LoadJobConfig(
    #     source_format=bigquery.SourceFormat.PARQUET,
    # )
    
    # gcs_uri = f'gs://{BUCKET}/{color}/*'

    # load_job = bq_client.load_table_from_uri(
    #     gcs_uri, table_id, job_config=job_config
    # )
    # load_job.result()

    # destination_table = bq_client.get_table(table_id)

    # print('Loaded {} rows'.format(destination_table.num_rows))


@flow()
def etl_gcs_to_bq(
    months: list[int] = [2, 3], years: list[int] = [2019], color: str = "fhv"
):
    """Main ETL flow to load data into BigQuery"""
    for year in years:
        for month in months:
            fetch_to_gcs(year, month, color)
    gcs_to_bq(color)


if __name__ ==  '__main__':
    etl_gcs_to_bq()