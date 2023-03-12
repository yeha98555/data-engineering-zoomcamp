#!/bin/bash
# Usage: ./download_data.sh green 2021

set -e

TAXI_TYPE=$1 # "green"
YEAR=$2 # 2021

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PATH}
    
    hadoop fs -mkdir -p /user/yic/${LOCAL_PREFIX}
    hadoop fs -put ${LOCAL_PATH} /user/yic/${LOCAL_PREFIX}

done