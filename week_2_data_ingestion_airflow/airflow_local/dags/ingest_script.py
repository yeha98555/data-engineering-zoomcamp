import os
import argparse

from time import time
import pyarrow.parquet as pq

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    parquet_file = pq.ParquetFile(csv_file)
    iter = 0
    for chunk in parquet_file.iter_batches(batch_size=100000):
        df = chunk.to_pandas()
        if iter == 0:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        iter += 1

        t_start = time()

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

        df.drop(columns=df.columns[0], axis=1, inplace=True)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'inserted {iter} chunk..., took {(t_end - t_start):.3f} second')
