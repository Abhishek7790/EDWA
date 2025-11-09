#ok then# this is project code....! now abc # this is project code....! now abc
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from urllib.parse import urlparse
import json

spark = SparkSession.builder.appName("__Silver_To_Gold_SubDtl").getOrCreate()

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

config_file_path = 's3://glue-project123/config-777/03.CommonConfig.json'  # your actual config S3 path

config_data = read_config_from_json(config_file_path)

# Extract config variables
table_list = config_data.get("tables", [])
silver_layer_path = config_data.get("silver_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
sub_dtl_tgt_tbl = config_data.get("sub_dtl_tgt_tbl", "")

print(table_list)
print(gold_layer_path)
print(silver_layer_path)
print(sub_dtl_tgt_tbl)

def read_parquet(spark, path):
    df = spark.read.format("parquet").load(path)
    return df

# Reading required silver tables
df_sb = read_parquet(spark, silver_layer_path.rstrip('/') + '/subscriber')
df_ad = read_parquet(spark, silver_layer_path.rstrip('/') + '/address')
df_ct = read_parquet(spark, silver_layer_path.rstrip('/') + '/city')
df_cn = read_parquet(spark, silver_layer_path.rstrip('/') + '/country')
df_ppr = read_parquet(spark, silver_layer_path.rstrip('/') + '/plan_postpaid')
df_ppo = read_parquet(spark, silver_layer_path.rstrip('/') + '/plan_prepaid')

# Applying join logic
test = df_sb.join(df_ad, "add_id", "left") \
            .join(df_ct, "ct_id", "left") \
            .join(df_cn, "cn_id", "left")

test1 = test.join(df_ppr, df_ppr.plan_id == test.prepaid_plan_id, "left") \
            .drop("add_id", "ct_id", "cn_id", "plan_id") \
            .withColumnRenamed("plan_desc", "pre_plan_desc") \
            .withColumnRenamed("amount", "pre_amount")

test2 = test1.join(df_ppo, df_ppo.plan_id == test1.postpaid_plan_id, "left") \
             .drop("plan_id") \
             .withColumnRenamed("plan_desc", "pos_plan_desc") \
             .withColumnRenamed("amount", "pos_amount")

res = test2.selectExpr(
    "sid as subscriberid",
    "name as subscribername",
    "mob as contactnumber",
    "email as emailid",
    "street as address",
    "ct_name as city",
    "cn_name as country",
    "sys_cre_date as create_date",
    "sys_upd_date as update_date",
    "active_flag as active_flag",
    "pre_plan_desc as prepaid_desc",
    "pre_amount as pre_amount",
    "pos_plan_desc as postpaid_desc",
    "pos_amount as pos_amount"
)

res.show(5)

def write_data_parquet_fs(df, path):
    df.write.mode("overwrite").format("parquet").save(path)
    print(f"✅ Data written successfully at {path}")

write_data_parquet_fs(res, gold_layer_path.rstrip('/') + '/' + sub_dtl_tgt_tbl)
