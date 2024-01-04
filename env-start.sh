#!/usr/bin/env bash

set -euxo pipefail

export PYTHON_VERSION=3.10
export AIRFLOW_VERSION=2.8.0
export DOCKER_BUILDKIT=1
export AIRFLOW_UID=$(id -u)

export AIRFLOW_IMAGE_NAME="airflow-poc-airflow-image:${AIRFLOW_VERSION}-v1"
export AIRFLOW_PROJ_DIR=./airflow
docker build airflow \
    --pull \
    --build-arg PYTHON_VERSION="${PYTHON_VERSION}" \
    --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
    --tag $AIRFLOW_IMAGE_NAME

docker-compose \
    -f docker-compose.airflow.yaml \
    -f docker-compose.minio.yaml \
    -f docker-compose.spark.yaml \
    up \
    --force-recreate

#    --env-file poc.env \
