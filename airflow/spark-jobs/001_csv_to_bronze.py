
"""001 CSV to bronze conversion"""

import argparse
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import xxhash64, lit
from pyspark.sql.types import StringType

from entities import ENTITIES, path_landingzone, path_bronze

def parse_date(date_str: str) -> datetime:
    try:
        #return datetime(2024,1,1)
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError as e:
        print(f'ERROR: Invalid date "{args.date}"')
        exit(1)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--date', type=str, help='The date in YYYY-MM-DD format', required=True)
parser.add_argument('--entity', type=str, help='The name of the entity', required=True)
args = parser.parse_args()
ds = parse_date(args.date)

if args.entity not in ENTITIES:
    print(f'ERROR: entity "{args.entity}" does not exist')
    exit(1)

ENTITY=ENTITIES[args.entity]

print(f'Execution date: {ds.year}-{ds.month}-{ds.day}')

conf = (
    SparkConf()
    .setAppName(__doc__)
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

columns = list(map(lambda c: c.source_name, ENTITY.columns))

# TODO: Do not infer schema but error if the schema doesn't match
df = (spark.read
  .format('csv')
  .option('inferSchema', 'true')
  .option('header', 'true')
  .load(f's3a://landingzone/{path_landingzone(ENTITY)}')
  .withColumn('row_hash', xxhash64(*columns))
  .withColumn('ds_first', lit(ds.strftime('%Y-%m-%d')))
  .withColumn('ds_deleted', lit(None).cast(StringType()))
  .withColumn('ds', lit(ds.strftime('%Y-%m-%d')))
)

# For debugging
df.show()

output_path = path_bronze(ENTITY, ds)
df.write.parquet(output_path, mode='overwrite')
