
"""002 Bronze to silver conversion"""

import argparse
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from entities import ENTITIES, prefix_path_silver, path_bronze, path_silver

def parse_date(date_str: str) -> datetime:
    try:
        #return datetime(2024,1,1)
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError as e:
        print(f"Invalid date: {args.date}")
        exit(1)

parser = argparse.ArgumentParser(description='Process a date.')
parser.add_argument('--date', type=str, help='The date in YYYY-MM-DD format')
parser.add_argument('--entity', type=str, help='The name of the entity')
args = parser.parse_args()
ds = parse_date(args.date)

if args.entity not in ENTITIES:
    print(f'ERROR: entity "{args.entity}" does not exist')
    exit(1)

ENTITY=ENTITIES[args.entity]

print(f'Execution date: {ds.year}-{ds.month}-{ds.day}')

conf = (
    SparkConf()
    .setAppName("Bronze to silver conversion")
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

context = SparkContext(conf=conf)
spark = (SparkSession(context)
         .builder
         .appName("DefaultSparkSession")
         .getOrCreate())

df_bronze = (spark.read
    .option("recursiveFileLookup", "true")
    .format('parquet')
    .load(path_bronze(ENTITY, ds))
)

def path_exists(path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3a://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

# Check if file exists in silver bucket
silver_prefix = prefix_path_silver(ENTITY)
silver_path = path_silver(ENTITY, ds)

if path_exists(silver_prefix):
    print("Silver exists")

    df_silver = (spark.read
        .option("recursiveFileLookup", "true")
        .format('parquet')
        .load(silver_prefix)
    )

    df_silver.createOrReplaceTempView("silver")
    df_bronze.createOrReplaceTempView("bronze")


    columns = list(map(lambda c: c.source_name, ENTITY.columns))
    primary_key = next(c for c in ENTITY.columns if c.primary_key).name
    date = ds.strftime('%Y-%m-%d')
    df_delta = spark.sql(f"""
        WITH last_rows AS (
            SELECT {','.join(columns)}, row_hash, ds_first, ds_deleted, ds FROM (
                SELECT *, RANK() OVER (PARTITION BY {primary_key} ORDER BY ds DESC) AS rnk FROM silver
            ) x WHERE rnk = 1 AND ds_deleted IS NULL
        ),
        deleted_rows AS (
            SELECT {','.join(columns)}, row_hash, ds_first, '{date}' AS ds_deleted, '{date}' AS ds FROM last_rows WHERE {primary_key} NOT IN (
                SELECT {primary_key} FROM bronze
            )
        ),
        added_rows AS (
            SELECT {','.join(columns)}, row_hash, ds_first, ds_deleted, ds FROM bronze WHERE {primary_key} NOT IN (
                SELECT {primary_key} FROM last_rows
            )
        ),
        modified_rows AS (
            SELECT {','.join([f'b.{c}' for c in columns])}, b.row_hash, lr.ds_first, b.ds_deleted, b.ds FROM last_rows lr
            JOIN bronze b
                ON b.{primary_key} = lr.{primary_key}
                AND b.row_hash != lr.row_hash
                AND b.ds >= lr.ds
        )
        SELECT * FROM deleted_rows
        UNION
        SELECT * FROM added_rows
        UNION
        SELECT * FROM modified_rows
    """)
    df_delta.write.parquet(silver_path, mode='overwrite')

else:
    print("Silver does not exist")

    df_bronze.write.parquet(silver_path, mode='overwrite')

