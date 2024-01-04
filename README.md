# Airflow/Minio/Spark PoC

A PoC of an lakehouse architecture.

## Introduction

At $WORK the target architecture is Microsoft Synapse which is Microsoft's umbrella product of ADLS Gen2, Spark, Serverless SQL
and Dedicated SQL, integrated with version control.

There are a few weak spots of Azure Synapse:

- Its data pipelines which provides too little control over dependencies
- Pull requests result in large JSON blobs, which makes them hard to review
- The data engineering team wants to generate templated pipelines which is essentially programming (and hence better served using a programming language)

This PoC serves as an investigation of an alternative orchistration set-up with Apache Airflow.

The following components are used:

- Airflow 2.8.0 (on Python 3.10)
- Minio to emulate ADLS Gen2. Although Microsoft supports Azurite for storage emulation but it does not support hierarchies, which is essential for this PoC.
- Spark for processing PySpark jobs

Ideally this we'd leverage dbt as well, but this might be a bridge too far [at the moment](https://dbt-msft.github.io/dbt-msft-docs/docs/better_together_pitch).
The current version of the `dbt-synapse` connector (1.4.0) is somewhat maintained by Microsoft and [only has support for Dedicated SQL, not Serverless SQL](https://docs.getdbt.com/docs/core/connect-data-platform/azuresynapse-setup).

## Components

### Storage

A medallion data lake architecture with the following layers:

- **landingzone**: This is where the files land, partitioned by application and entity.
- **bronze**: The landed files are transformed to parquet format and stored in raw format. Partitioned by application and entity.
  - TODO: This is where quality checks should take place.
- **silver**: This is where the delta is calculated and the data is structured according to our data domain definition.
- **gold**: A curated set of data ready for consumption.

Minio is used for S3 compatible storage.

### Processing

Processing data is done using Apache Spark. The jobs are defined in the `airflow/spark-jobs` directory.
Each job receives the metadata entities defined in `airflow/dags/metadata/entities.py`.

### DAGs

- **000_setup_env_v1**: Used to initialize Airflow connections and the Minio storage bucket. Only needs to be executed once to complete set-up of the PoC.
- **landing_to_silver_v1**: Moves a CSV-file from the landingzone via a parquet file in the bronze layer to a parquet file in the silver layer.
  The current DAG also generates fake data and uploads it to the landing zone to emulate a filedrop event.
  The data generation takes into account the point in time the DAG is executed to be able to simulate changes to the data over time.
- **gold_report_v1**: Waits for department and employee data to be updated before generating a dummy report.

## Getting started

- To start execute `./env-start.sh` to boot up Airflow, Minio and Spark.
- Once all services have started, Airflow should be operational at http://localhost:8080. You can login with airflow/airflow.
- Trigger the `setup_env_v1` DAG by pressing the play (⏵) button
- Enable the other 3 dags (`department_landing_to_silver_v1`, `employee_landing_to_silver_v1`, `gold_report_v1`) and give them some time to catch-up (start date 2024-01-01)
- To cleanup services and storage execute `./env-cleanup.sh`

## Operations

### Rebuild specific container while docker is running

When python requirements change source images need to be rebuild, you can do so using

- `docker-compose -f docker-compose.[airflow|spark|minio].yaml up -d --force-recreate --build [service-name]`
- E.g. `docker-compose -f docker-compose.airflow.yaml up -d --force-recreate --build airflow-scheduler`

## Future work

- From heresay it seems Delta is not mature enough in Microsoft Synapse.
  - This needs to be investigated.
  - If it is mature delta parquet should be used from the silver layer onwards.
- Investigate Iceberg support
- Check data quality e.g. using [Amazon Deequ](https://github.com/awslabs/python-deequ)
