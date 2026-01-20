2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from urllib.parse import urlparse
import json

spark = SparkSession.builder.appName("_Bronze_To_Silver").getOrCreate()

def read_config_from_json(s3_path):
    parsed_url = urlparse(s3_path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    config_data = json.loads(content)

    print("✅ Loaded config from:", s3_path)
    return config_data

# Point to your actual config location in S3
config_file_path = 's3://glue-project123/config-777/03.CommonConfig.json'

config_data = read_config_from_json(config_file_path)

# Access parameters from config
table_list = config_data.get("tables", [])
bronze_layer_path = config_data.get("bronze_layer_path", "")
silver_layer_path = config_data.get("silver_layer_path", "")

print(table_list)
print(bronze_layer_path)
print(silver_layer_path)

def read_csv(spark, path, delimiter=',', inferschema='false', header='false'):
    df = spark.read.format("csv") \
        .option("header", header) \
        .option("inferSchema", inferschema) \
        .option("delimiter", delimiter) \
        .load(path)
    return df

def write_data_parquet_fs(df, path):
    df.write.mode("overwrite").format("parquet").save(path)
    print(f"✅ Data written in parquet format at {path}")

for table in table_list:
    print(f"🚀 Processing table: {table}")
    input_path = bronze_layer_path.rstrip('/') + f"/{table}"
    output_path = silver_layer_path.rstrip('/') + f"/{table}"
    
    df = read_csv(spark, input_path, delimiter=',', inferschema='true', header='true')
    df.show(5)
    write_data_parquet_fs(df, output_path)

print("🎉 ******** Job Successfully Completed **********")
