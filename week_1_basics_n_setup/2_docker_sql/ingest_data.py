import os
import argparse

from time import time
import gzip

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # download csv
    if url.endswith('.parquet'):
        parquet_name = 'output.parquet'
        os.system(f'wget {url} -O {parquet_name}')
        df = pd.read_parquet(parquet_name)
        df.to_csv(csv_name)
    elif url.endswith('.csv.gz'):
        csvgz_name = 'output.csv.gz'
        os.system(f'wget {url} -O {csvgz_name}')
        with gzip.open(csvgz_name, 'rt', newline='') as csvgz_file:
            data = csvgz_file.read()
            with open(csv_name, 'wt') as csv_file:
                csv_file.write(data)
    else:
        os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

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

    # create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # insert first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            
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
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            
            print('inserted another chunk..., took %.3f second' % (t_end - t_start))
        
        except StopIteration:
            print('Finished ingesting data into the postgres database')
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='database for postgres')
    parser.add_argument('--db', required=True, help='user name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the result to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)