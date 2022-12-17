


### Postgres
write Postgres and pgAdmin into `docker-compose.yml`.
use it.
```shell
# start
# -d: run in background
docker compose up -d

# shutdown
docker compose down

# check newtork
docker network ls
```
1. go to `localhost:8080`.
2. login
   use the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD`
3. add Postgres database
   1) right click `Servers`, select `Register`->`Server`
   2) input name into the `name` in `General`
   3) input `pgdatabase` into the `Host name`, `root` into the `Username` and the `Password` in `Connection`
4. SQL Operation
   `Servers` -> `Docker localhost` -> `Database` -> `ny_taxi` -> `Schemas` -> `Tables` -> `yellow_taxi_data`
   right click, select `Query Tool`

### Ingest Data
#### yellow_taxi_trips
```shell
# build docker image
docker build -t taxi_ingest:v001 .

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"

# run docker image
docker run -it \
    --network=2_docker_sql_pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```
#### zones
```shell
# comment below lines
# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
# df.drop(columns=df.columns[0], axis=1, inplace=True)
# rebuild docker image
docker build -t taxi_ingest:v001 .

URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

# run docker image
docker run -it \
    --network=2_docker_sql_pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zones \
    --url=${URL}
```