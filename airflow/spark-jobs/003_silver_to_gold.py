
"""002 Bronze to silver conversion"""

import argparse
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from entities import DEPARTMENT, EMPLOYEE, prefix_path_silver

def parse_date(date_str: str) -> datetime:
    try:
        #return datetime(2024,1,1)
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError as e:
        print(f"Invalid date: {args.date}")
        exit(1)

parser = argparse.ArgumentParser(description='Process a date.')
parser.add_argument('--date', type=str, help='The date in YYYY-MM-DD format')
args = parser.parse_args()
ds = parse_date(args.date)

print(f'Execution date: {ds.year}-{ds.month}-{ds.day}')

conf = (
    SparkConf()
    .setAppName("Gold table creation")
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

# Load department and employee as tables for spark SQL
for entity in [EMPLOYEE, DEPARTMENT]:
    silver_prefix = prefix_path_silver(entity)
    df_silver = (spark.read
        .option("recursiveFileLookup", "true")
        .format('parquet')
        .load(silver_prefix)
    )
    df_silver.createOrReplaceTempView(entity.name.lower())


df_report = spark.sql(f"""
    WITH last_rows_department AS (
        SELECT * FROM (
            SELECT *, RANK() OVER (PARTITION BY department_id ORDER BY ds DESC) AS rnk FROM department
        ) x WHERE rnk = 1 AND ds_deleted IS NULL
    ),
    last_rows_employee AS (
        SELECT * FROM (
            SELECT *, RANK() OVER (PARTITION BY employee_id ORDER BY ds DESC) AS rnk FROM employee
        ) x WHERE rnk = 1 AND ds_deleted IS NULL
    ),
    department_total_salary AS (
        SELECT department, SUM(salary) AS total_salary FROM last_rows_employee GROUP BY department
    )
    SELECT
        d.name,
        dts.total_salary,
        d.budget,
        (dts.total_salary / d.budget) AS salary_pct_budget
    FROM last_rows_department d
    JOIN department_total_salary dts ON d.name = dts.department
""")

df_report.write.parquet('s3a://gold/domain=HR/product=department_stats', mode='overwrite')


