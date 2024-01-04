import datetime
from pathlib import Path

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.utils.task_group import TaskGroup

S3_CONN_ID = 'minio_local'

def add_conn(conn_id, **conn_dict):
    session = settings.Session()
    conn = Connection(conn_id=conn_id, **conn_dict)
    if session.query(Connection).filter(Connection.conn_id == conn_id).first():
        print(f"Connection {conn_id} already exists.")
    else:
        # Add the connection to the session and commit
        session.add(conn)
        session.commit()
        print(f"Connection {conn_id} added.")
    session.close()


def init_connections(**kwargs):
    add_conn('spark_local',
        conn_type='generic',
        host='spark://spark-master',
        port=7077
    )
    add_conn(S3_CONN_ID,
        conn_type='aws',
        login='minioadmin', # As defined in docker-compose.minio.yml
        password='minioadmin',
        extra={'endpoint_url': 'http://minio:9000'},
    )

with DAG(
    dag_id=Path(__file__).stem,
    start_date=datetime.datetime(2023, 12, 1),
    schedule=None,
):
    t0 = PythonOperator(
        task_id='init_connections',
        python_callable=init_connections
    )

    for bucket in ['landingzone', 'bronze', 'silver', 'gold']:
        with TaskGroup(group_id=f'configure_bucket_{bucket}') as tg_bucket:
            t1 = S3DeleteBucketOperator(
                task_id=f'delete_bucket_{bucket}',
                bucket_name=bucket,
                force_delete=True,
                aws_conn_id=S3_CONN_ID,
            )
            t2 = S3CreateBucketOperator(
                task_id=f'create_bucket_{bucket}',
                bucket_name=bucket,
                aws_conn_id=S3_CONN_ID,
            )

            t1 >> t2

        t0 >> tg_bucket
