---
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import boto3
from urllib.parse import urlparse

currentdate = datetime.datetime.now().strftime("%Y-%m-%d")

spark = SparkSession.builder.appName("_GOLDto_Plat_REPORTING_Revnue").getOrCreate()

# -------------------------------
# Function to read JSON config from S3
# -------------------------------
def read_config_from_s3(s3_path):
    parsed_url = urlparse(s3_path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    config_data = json.loads(content)
    print(f"✅ Loaded config from {s3_path}")
    return config_data

# -------------------------------
# Read config from your S3 path
# -------------------------------
config_file_path = 's3://glue-project123/config-777/03.CommonConfig.json'
config_data = read_config_from_s3(config_file_path)

# -------------------------------
# Access config params
# -------------------------------
platinum_layer_path = config_data.get("platinum_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
revenue_tbl = config_data.get("revenue_tbl", "")

# -------------------------------
# Read parquet helper
# -------------------------------
def read_parquet(spark, path):
    return spark.read.format("parquet").load(path)

# Read gold layer data
df_sb = read_parquet(spark, gold_layer_path + 'subscriber_details')

df_sb.createOrReplaceTempView("subscriber")

# Revenue report SQL_
revenue_report = spark.sql("""
    SELECT 
        SD.country AS Country,
        COUNT(SD.subscriberid) AS total_subscriber,
        SUM(COALESCE(pre_amount, pos_amount, 0)) AS total_revenue
    FROM subscriber SD
    WHERE SD.active_flag = 'A'
    GROUP BY SD.country, SD.active_flag
""")

revenue_report.show()

# Write output to platinum layer
def write_data_parquet_fs(spark, df, path):
    df.write.format("parquet").mode("append").save(path)
    print(f"✅ Data successfully written to {path}")

write_data_parquet_fs(spark, revenue_report, platinum_layer_path + revenue_tbl)
