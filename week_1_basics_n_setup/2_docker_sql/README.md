## Ingesting NY Taxi Data to Postgres

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

#### pgcli
```sh
# install
pip install pgcli psycopg-binary

# connect postgres
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

#### pgadmin
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
```shell
# build docker image
docker build -t taxi_ingest:v001 .
```

#### yellow_taxi_trips
```shell
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-09.parquet"

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

#### green_taxi_trips
```shell
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

# run docker image
docker run -it \
    --network=2_docker_sql_pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}
```

#### zones
```shell
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

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

## Vanna.ai text-to-sql Bot
Reference: [Ref1](https://vanna.ai/docs/postgres-openai-vanna-vannadb.html), [Ref2](https://github.com/r0mymendez/text-to-sql/blob/main/vanna-streamlit-tutorial.ipynb)

### Installation
```
pip install 'vanna[postgres]'
```

### Run
You need to register at [https://vanna.ai/](https://vanna.ai/) first, and then replace the email in the following command with your registered email. Additionally, the model needs to be trained initially, so set the `is_training` parameter to `"True"`. Once the training is complete, set it to `"False"`. If there are any changes to the data, you can set it back to `"True"`.
```sh
python vanna_ai_bot.py \
    --email "xxxx@example.com" \
    --model "ny_taxi_model" \
    --is_training "False" \
    --host "localhost" \
    --port "5432" \
    --db "ny_taxi" \
    --user "root" \
    --password "root" \
    --question "Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown\nWhich were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?"
```
Upon execution, the following message will appear in the terminal. It's necessary to enter the code received in the registered email.
```
Check your email for the code and enter it here: 
```