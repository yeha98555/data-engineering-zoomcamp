FROM apache/airflow:2.5.1

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
USER $AIRFLOW_UID
RUN pip install --no-cache-dir -r requirements.txt

# WORKDIR $AIRFLOW_HOME

# USER $AIRFLOW_UID