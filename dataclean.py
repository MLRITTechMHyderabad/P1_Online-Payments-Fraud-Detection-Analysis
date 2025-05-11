from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_timestamp

spark = SparkSession.builder \
    .appName("DataClean") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.csv("gs://online-payments-fraud/source/synthetic_fraud_dataset.csv", header=True, inferSchema=True)

df = df.withColumnRenamed("timestamp", "time_stamp")

df = df.toDF(*[c.lower() for c in df.columns])

df = df.dropDuplicates().dropna()

df = df.repartition(1)

string_cols = ['card_type', 'transaction_type', 'merchant_category', 'device_type', 'location', 'authentication_method']
for c in string_cols:
    df = df.withColumn(c, lower(trim(col(c))))

for col_name in ['account_balance', 'transaction_amount', 'risk_score']:
    df = df.filter(col(col_name) >= 0)

df.write.mode("overwrite").csv("gs://online-payments-fraud/destination/cleaned_data", header=True)
