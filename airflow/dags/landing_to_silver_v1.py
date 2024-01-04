import datetime
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Callable

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

from generators.employee import generate_employee_csv_file
from generators.department import generate_department_csv_file
from metadata.entities import Entity, EMPLOYEE, DEPARTMENT, path_landingzone, dataset_bronze, dataset_silver

S3_CONN_ID = 'minio_local'
BUCKET_NAME = 'landingzone'
JOB_PATH = Path(__file__, '..', '..', 'spark-jobs').resolve()
DAG_START_DATE=datetime.datetime(2024, 1, 1)
SPARK_PACKAGES = ','.join([
    'org.apache.hadoop:hadoop-aws:3.3.2',
    'com.amazonaws:aws-java-sdk-bundle:1.11.1026',
    'org.apache.hadoop:hadoop-common:3.3.2',
])

@dataclass
class Config:
    entity: Entity
    fake_file_generator: Callable

configs = [
    Config(entity=EMPLOYEE, fake_file_generator=generate_employee_csv_file),
    Config(entity=DEPARTMENT, fake_file_generator=generate_department_csv_file),
]


for config in configs:
    entity_name = config.entity.name.lower()
    with DAG(
        dag_id=f"{entity_name}_{Path(__file__).stem}",
        start_date=DAG_START_DATE,
        schedule='@daily',
        default_args={
            "depends_on_past": True
        },
        max_active_runs=1,
    ):

        # The sensor waits for the file to arrive, in this dag we create it in parallel
        bucket_key=path_landingzone(config.entity)
        t_wait = S3KeySensor(
            task_id=f'wait_for_{entity_name}_file',
            aws_conn_id=S3_CONN_ID,
            bucket_name=BUCKET_NAME,
            bucket_key=bucket_key,
            deferrable=True,
            outlets=[Dataset(bucket_key)],
        )

        with TaskGroup(group_id=f'generate_fake_{entity_name}_data') as tg_fake_file:
            fake_file_path = '/tmp/'+entity_name+'_data_{{data_interval_start | ds_nodash}}.csv'
            t_gen_fake_file = PythonOperator(
                task_id=f'generate_{entity_name}_data_local',
                python_callable=config.fake_file_generator,
                provide_context=True,
                op_args=[fake_file_path, DAG_START_DATE]
            )

            t_upload_fake_file = LocalFilesystemToS3Operator(
                task_id=f'upload_{entity_name}_data_minio',
                aws_conn_id=S3_CONN_ID,
                dest_bucket=BUCKET_NAME,
                dest_key=bucket_key,
                filename=fake_file_path,
                replace=True,
            )

            t_clean_fake_file = PythonOperator(
                task_id=f'remove_local_{entity_name}_data',
                python_callable=lambda filename: os.remove(filename),
                op_args=[fake_file_path],
            )

            t_gen_fake_file >> t_upload_fake_file >> t_clean_fake_file

        # When the file is there, convert it to parquet
        t_bronze = SparkSubmitOperator(
            task_id=f"{entity_name}_csv_to_bronze",
            conn_id="spark_local",
            application=str(Path(JOB_PATH, '001_csv_to_bronze.py')),
            application_args=['--date={{data_interval_start | ds}}', f'--entity={entity_name}'],
            packages=SPARK_PACKAGES,
            py_files=str(Path(__file__, '..', 'metadata/entities.py').resolve()),
            outlets=[Dataset(dataset_bronze(config.entity))],
        )

        # When the full load of the data has landed in the bronze layer
        # calculate the diff and add it to the silver layer
        t_silver = SparkSubmitOperator(
            task_id=f"{entity_name}_bronze_to_silver",
            conn_id="spark_local",
            application=str(Path(JOB_PATH, '002_bronze_to_silver.py')),
            application_args=['--date={{data_interval_start | ds}}', f'--entity={entity_name}'],
            packages=SPARK_PACKAGES,
            py_files=str(Path(__file__, '..', 'metadata/entities.py').resolve()),
            outlets=[Dataset(dataset_silver(config.entity))],
        )

        # Tidy up when the previous steps have been completed
        t_clean = S3DeleteObjectsOperator(
            task_id=f'remove_{entity_name}_landingzone',
            aws_conn_id=S3_CONN_ID,
            bucket=BUCKET_NAME,
            keys=bucket_key
        )


        [t_wait, tg_fake_file] >> t_bronze >> [t_silver, t_clean]