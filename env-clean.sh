#!/usr/bin/env bash

export AIRFLOW_UID=$(id -u)

docker-compose \
	-f docker-compose.airflow.yaml \
	-f docker-compose.minio.yaml \
	-f docker-compose.spark.yaml \
	rm

VOLUMES="$(docker volume ls --filter name=airflow-poc -q)"
if [ -n "$VOLUMES" ]; then
	echo "Removing volume $VOLUMES"
	docker volume rm $VOLUMES
fi

rm -rf ./airflow/logs/dag* ./airflow/logs/scheduler*