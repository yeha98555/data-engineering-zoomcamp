import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    color = kwargs['color']
    year = kwargs['year']
    print(f'loading {color} taxi data for the year {year}\n')

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    # native date parsing
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    df_list = []

    for i in range(12):
        month = f"{i+1:02d}"
        file_name = f"{color}_tripdata_{year}-{month}.parquet"
        request_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'
        print(f'request_url: {request_url}')

        df_list.append(pd.read_parquet(request_url))
    
    res = pd.concat(df_list, axis=0)

    # parse dtype
    res = res.astype(taxi_dtypes)
    res["lpep_pickup_datetime"] = pd.to_datetime(res["lpep_pickup_datetime"])#, format='%Y-%m-%d %H:%M:%S')
    res["lpep_dropoff_datetime"] = pd.to_datetime(res["lpep_dropoff_datetime"])#, format='%Y-%m-%d %H:%M:%S')

    return res


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
