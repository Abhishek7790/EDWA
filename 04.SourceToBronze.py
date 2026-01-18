#coder
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import boto3
from urllib.parse import urlparse

# --------------------------------
# UDF: Read JSON config from S3
# --------------------------------
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

# -------------------------------
# UDF: Extract config val
# -------------------------------
def extract_config_vars(config_data):
    tables = config_data.get("tables", [])
    host = config_data.get("host", "")
    username = config_data.get("username", "")
    pwd = config_data.get("pwd", "")
    driver = config_data.get("driver", "")
    bronze_layer_path = config_data.get("bronze_layer_path", "")
    silver_layer_path = config_data.get("silver_layer_path", "")
    gold_layer_path = config_data.get("gold_layer_path", "")
    platinum_layer_path = config_data.get("platinum_layer_path", "")
    sub_dtl_tgt_tbl = config_data.get("sub_dtl_tgt_tbl", "")
    cmp_dtl_tgt_tbl = config_data.get("cmp_dtl_tgt_tbl", "")
    revenue_tbl = config_data.get("revenue_tbl", "")

    return (tables, host, username, pwd, driver,
            bronze_layer_path, silver_layer_path, gold_layer_path,
            platinum_layer_path, sub_dtl_tgt_tbl, cmp_dtl_tgt_tbl, revenue_tbl)

# -------------------------------
# Initialize Spark Session
# -------------------------------
spark = SparkSession.builder.appName("_Source_To_Bronze").getOrCreate()

# -------------------------------
# Read config from S3
# -------------------------------
config_file_path = 's3://glue-project123/config-777/03.CommonConfig.json'
config_data = read_config_from_json(config_file_path)

# -------------------------------
# Extract config variables
# ----------------------------------
(tables, host, username, pwd, driver,
 bronze_layer_path, silver_path, gold_path,
 platinum_path, sub_dtl_table, cmp_dtl_table, revenue_table) = extract_config_vars(config_data)

print("🔧 Config values loaded:")
print("Tables:", tables)
print("Bronze Path:", bronze_layer_path)

# -------------------------------
# UDF: Read table from RDBMS
# -------------------------------
def read_data_from_rdbms(spark, host, username, pwd, driver, table_name):
    df = spark.read.format("jdbc") \
        .option("url", host) \
        .option("user", username) \
        .option("password", pwd) \
        .option("driver", driver) \
        .option("dbtable", table_name) \
        .load()
    return df

# -------------------------------
# UDF: Write data to S3 as CSV
# -------------------------------
def write_data_fs(df, path, delim=',', header="true"):
    df.write.mode("append").format("csv") \
        .option("delimiter", delim) \
        .option("header", header) \
        .save(path)
    print(f"✅ Data written to {path}")

# -------------------------------
# Loop through tables and process
# -------------------------------
for table in tables:
    print(f"🚀 Starting load for table: {table}")
    df = read_data_from_rdbms(spark, host, username, pwd, driver, table)
    df.show(5)
    output_path = bronze_layer_path.rstrip('/') + f"/{table}"
    write_data_fs(df, output_path)

print("🎉 ******** Job Successfully Completed ********")
