from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    # # test retries
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issue"""
    # yellow
    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    if 'tpep_dropoff_datetime' in df.columns:
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # green
    if 'lpep_pickup_datetime' in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    if 'lpep_dropoff_datetime' in df.columns:
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    output_dir = Path(f'data/{color}')
    if not Path.exists(output_dir):
        output_dir.mkdir(parents=True, exist_ok=True)
    path = Path(f'{output_dir}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtcde-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path='/'.join(str(path).split('/')[-2:])
    )


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = 'green'
    year = 2020
    month = 11
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == '__main__':
    etl_web_to_gcs()