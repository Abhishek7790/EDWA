#for silver to gold for complaints  details..!
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from urllib.parse import urlparse
import json

spark = SparkSession.builder.appName("SilverToGold_CmpDtl").getOrCreate()

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

table_list = config_data.get("tables", [])
silver_layer_path = config_data.get("silver_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
cmp_dtl_tgt_tbl = config_data.get("cmp_dtl_tgt_tbl", "")

print(table_list)
print(gold_layer_path)
print(silver_layer_path)

def read_parquet(spark, path):
    df = spark.read.format("parquet").load(path)
    return df

df_sb = read_parquet(spark, silver_layer_path.rstrip('/') + '/subscriber')
df_cm = read_parquet(spark, silver_layer_path.rstrip('/') + '/complaint')

df_sbb = df_sb.selectExpr("sid", "name")
c_test = df_cm.join(df_sbb, "sid", how="inner")

c_test.show(5)

res_cmd = c_test.selectExpr(
    "sid as subscriberId",
    "name as subscribername",
    "cmp_id as complaintId",
    "regarding as complaintReg",
    "descr as description",
    "sys_cre_date as com_cre_date",
    "sys_upd_date as com_upd_date",
    "status as status"
)

res_cmd.show(5)

def write_data_parquet_fs(df, path):
    df.write.mode("overwrite").format("parquet").save(path)
    print(f"✅ Data successfully written at {path}")

write_data_parquet_fs(res_cmd, gold_layer_path.rstrip('/') + '/' + cmp_dtl_tgt_tbl)
