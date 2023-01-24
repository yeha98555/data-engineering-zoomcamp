## Ingesting Data with Airflow
[video](https://www.youtube.com/watch?v=0yK7LXwYeD0&list=PL3MmuxUbc_hKVX8VnwWCPaWlIHf1qmg8s&index=4)

### Airflow Setup

#### Pre-Reqs
Docker Settings
- upgrade docker compose version to 2.x+
- set the memory for the Docker Engine to minimum 5GB (ideally 8GB)

#### Setup
1. create a new directory `airflow` in the project.
2. download `docker-compose.yml` from airflow official website
```sh
curl -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
```
3. create the airflow subdirectory
- `./dags` - DAG_FOLDER for DAG files
- `./logs` - contains logs from task execution and scheduler.
- `./plugins` - for custom plugins
```sh
mkdir ./dags ./logs/ ./plugins
```
4. set the airflow user
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
``` 
5. run
```sh
docker compose build
docker compose up airflow-init
docker compose up
```
6. go to [localhost:8080](localhost:8080)
- username: airflow
- password: airflow


### Ingesting Data
I use **Local Version** first, and change it to **GCP Version**. <br>

#### Local Version
Ingesting Data to Local Postgres with Airflow <br>
[Code](./airflow_local/)

##### Extra Setup
1. set not to load examples in `docker-compose.yml`
```yml
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'  # change `true` to `false`
```
2. [Dockfile](./airflow_local/Dockerfile) and [requirements.txt](./airflow_local/requirements.txt)<br>

3. Postgres environment variables
- set the environment variables in `.env` <br>
(you can just copy the `.env.example` to `.env`, and change the value)
```.env
PG_HOST=pgdatabase
PG_USER=root
PG_PASSWORD=root
PG_PORT=5432
PG_DATABASE=ny_taxi
```
- add the variables to `x-airflow-common` in `docker-compose.yml`
```yml
    PG_HOST: "${PG_HOST}"
    PG_USER: "${PG_USER}"
    PG_PASSWORD: "${PG_PASSWORD}"
    PG_PORT: "${PG_PORT}"
    PG_DATABASE: "${PG_DATABASE}"
```
- use the variables in the `./dags` files, e.g. `os.getenv('PG_HOST')`

4. connection with two docker network
- add external network to `2_docker_sql_pg-network` in `week_1_basics_n_setup/2_docker_sql/docker-compose.yml`
```yml
networks:
  pg-network:
    ## New Add ##
    external:
      name: airflow_default
    #############
```
- add external link `pgdatabase` to `x-airflow-common` in `week_2_data_ingestion_airflow/airflow_local/docker-compose.yml`
```yml
  extra_hosts:
    - "host.docker.internal:host-gateway"
  external_links:
    - pgdatabase
```
5. change the pgadmin port in `week_1_basics_n_setup/2_docker_sql/docker-compose.yml` for avoiding port conflict
```yml
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    depends_on:
      - pgdatabase
    networks:
      - pg-network
    ports:
      - "8081:80"  # change from `8080` to `8081`
```

##### Execution
```sh
docker compose build
docker compose up airflow-init
docker compose up
```
Then, go to
- Airflow: [localhost:8080](localhost:8080)
- Local Postgres: [localhost:8081](localhost:8081)

#### GCP Version
Ingesting Data to GCP with Airflow <br>
[Code](./airflow_gcp/)

##### Extra Setup
1. GCP Service Account Credentials
- put the `google_credentials.json` to the given location
```sh
cd ~ && mkdir -p ~/.gc/credentials/
mv <path/to/your/service-account-authkeys>.json ~/.gc/credentials/google_credentials.json
```
- add loading the credentials to `x-airflow-common` in `docker-compose.yml`
```yml
environment:
    &airflow-common-env
    ...
    ## New Add ##
    GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'
    ##############
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - ~/.gc/credentials/:/.google/credentials:ro  # new add
```

2. GCP environemt variables <br>
set GCP variables (don't forget to remove the Postgres variables)
- set the environment variables in `.env` <br>
(you can just copy the `.env.example` to `.env`, and change the value)
```.env
GCP_PROJECT_ID=xxxx
GCP_GCS_BUCKET=xxxx
BIGQUERY_DATASET=xxxx
```
- add the variables to `x-airflow-common` in `docker-compose.yml`
```yml
    GCP_PROJECT_ID: "${GCP_PROJECT_ID}"
    GCP_GCS_BUCKET: "${GCP_GCS_BUCKET}"
    BIGQUERY_DATASET: "${BIGQUERY_DATASET}"
```
- use the variables in the `./dags` files, e.g. `os.environ.get("GCP_PROJECT_ID")`

3. Prepare the files for GCP settings
- `script/`
- `Dockerfile` add GCP Configurations
```Dockerfile
# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts
```

##### Execution
```sh
docker compose build
docker compose up airflow-init
docker compose up
```
Then, go to
- Airflow: [localhost:8080](localhost:8080)

