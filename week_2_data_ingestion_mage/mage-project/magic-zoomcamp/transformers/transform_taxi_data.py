if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # zero passenger
    print('Preprocessing: rows with zero passenger:', df.passenger_count.isin([0]).sum())
    df = df[df['passenger_count'] > 0]

    # zero distance
    print('Preprocessing: rows with zero distance:', df.trip_distance.isin([0]).sum())
    df = df[df['trip_distance'] > 0]

    # datetime to date
    df["lpep_pickup_date"] = df.lpep_pickup_datetime.dt.date
    df['lpep_dropoff_date'] = df.lpep_dropoff_datetime.dt.date

    # camel case to snake case
    df.columns = (df.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower())

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert 'vendor_id' in output.columns, 'vendor_id not in the columns'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passenger'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero distance'
