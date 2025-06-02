from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

spark = SparkSession.builder \
    .appName("DataClean") \
    .enableHiveSupport() \
    .getOrCreate()


df = spark.read.csv("gs://online-payments1/source/synthetic_fraud_dataset.csv", header=True, inferSchema=False)

df = df.withColumnRenamed("timestamp", "time_stamp")

df = df.toDF(*[c.lower() for c in df.columns])

df = df.dropDuplicates().dropna()

string_cols = ['card_type', 'transaction_type', 'merchant_category', 'device_type', 'location', 'authentication_method']
for c in string_cols:
    df = df.withColumn(c, lower(trim(col(c))))

numeric_cols = ['account_balance', 'transaction_amount', 'risk_score']
for col_name in numeric_cols:
    df = df.filter(col(col_name).cast("float") >= 0)

df = df.repartition(1)
df.write.mode("overwrite").csv("gs://online-payments1/destination/", header=True)
