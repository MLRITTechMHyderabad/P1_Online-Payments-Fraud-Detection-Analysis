from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_timestamp

spark = SparkSession.builder.appName("DataCleaning").enableHiveSupport().getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("gs://online-payments-fraud/source/synthetic_fraud_dataset.csv")

df = df.withColumnRenamed("timestamp", "time_stamp")
df = df.toDF(*[c.lower() for c in df.columns])
df = df.dropDuplicates().dropna()

string_cols = ['card_type', 'transaction_type', 'merchant_category', 'device_type', 'location', 'authentication_method']
for c in string_cols:
    df = df.withColumn(c, lower(trim(col(c))))


df = df.withColumn("time_stamp", to_timestamp("time_stamp"))

for col_name in ['account_balance', 'transaction_amount', 'risk_score']:
    df = df.filter(col(col_name) >= 0)


df.coalesce(1).write.mode("overwrite").option("header", True).csv("hgs://online-payments-fraud/destination/cleaned_data")
