


### Postgres
```shell
docker network create pg-network

docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```

### pgAdmin
```shell
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgAdmin \
    dpage/pgadmin4
```
go to `localhost:8080`.

### Ingest Data
```shell
# build docker image
docker build -t taxi_ingest:v001 .

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"

# run docker image
docker run -it \
	--network=pg-network \
	taxi_ingest:v001 \
	--user=root \
	--password=root \
	--host=pg-database \
	--port=5432 \
	--db=ny_taxi \
	--table_name=yellow_taxi_trips \
	--url=${URL}
```