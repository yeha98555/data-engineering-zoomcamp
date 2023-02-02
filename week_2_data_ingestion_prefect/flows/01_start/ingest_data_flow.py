import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, tags=['extract'])
def extract_data(url: str):
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'
    
    os.system(f'wget {url} -O {csv_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(log_prints=True)
def transform_data(df: pd.DataFrame):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def load_data(table_name: str, df: pd.DataFrame):
    # user = "root"
    # password = "root"
    # host = "localhost"
    # port = "5432"
    # db = "ny_taxi"
    # postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    # engine = create_engine(postgres_url)
    connection_block = SqlAlchemyConnector.load('pgdatabase-nytaxi')
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging Subflow for: {table_name}')

@flow(name="Ingest Data")
def main_flow(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)

    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(table_name, data)

if __name__ == '__main__':
    main_flow(table_name='yellow_taxi_trips')