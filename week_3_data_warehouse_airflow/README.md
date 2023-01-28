## Data Warehouse with Airflow
[video](https://www.youtube.com/watch?v=lAxAhHNeGww&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=30)

- add [gcs_to_bq_dag.py](./airflow/dags/gcs_to_bq_dag.py)
- use the [GCP lightweight version](./airflow/docker-compose-nofrills.yml) from week_2_data_ingestion_airflow
- run
```sh
docker compose -f docker-compose-nofrills.yml up
```