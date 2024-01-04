import datetime
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Callable

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset

from metadata.entities import EMPLOYEE, DEPARTMENT, dataset_bronze, dataset_silver

JOB_PATH = Path(__file__, '..', '..', 'spark-jobs').resolve()
DAG_START_DATE=datetime.datetime(2024, 1, 1)
SPARK_PACKAGES = ','.join([
    'org.apache.hadoop:hadoop-aws:3.3.2',
    'com.amazonaws:aws-java-sdk-bundle:1.11.1026',
    'org.apache.hadoop:hadoop-common:3.3.2',
])

with DAG(
    dag_id=Path(__file__).stem,
    start_date=DAG_START_DATE,
    schedule=[
        Dataset(dataset_silver(EMPLOYEE)),
        Dataset(dataset_bronze(DEPARTMENT)),
    ],
    default_args={
        "depends_on_past": True
    },
    max_active_runs=1,

):
    # When the full load of the data has landed in the bronze layer
    # calculate the diff and add it to the silver layer
    SparkSubmitOperator(
        task_id=f"generate_gold_report",
        conn_id="spark_local",
        application=str(Path(JOB_PATH, '003_silver_to_gold.py')),
        application_args=['--date={{data_interval_start | ds}}'],
        packages=SPARK_PACKAGES,
        py_files=str(Path(__file__, '..', 'metadata/entities.py').resolve()),
        outlets=[Dataset('s3a://gold/domain=HR/product=department_stats')],
    )
