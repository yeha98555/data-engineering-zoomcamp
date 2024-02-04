BQ_DATASET = [
  {
    id          = "trips_data_all"
    description = "BigQuery Dataset that raw data (from GCS) will be written to"
  },
  {
    id          = "dbt_dev"
    description = "BigQuery Dataset that developing dbt data (from GCS) will be written to"
  },
  {
    id          = "production"
    description = "BigQuery Dataset that be running the models dbt data (from GCS) will be written to"
  }
]