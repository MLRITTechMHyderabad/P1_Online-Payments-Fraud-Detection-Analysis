from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("NormalizedTables") \
    .enableHiveSupport() \
    .getOrCreate()

transactions = spark.sql("SELECT * FROM transactions_db.transactions")
users = spark.sql("SELECT * FROM transactions_db.users")
device_info = spark.sql("SELECT * FROM transactions_db.device_info")
fraud_labels = spark.sql("SELECT * FROM transactions_db.fraud_labels")

transactions.write \
    .format("bigquery") \
    .option("table", "mlrit2.online_payments.transactions") \
    .option("temporaryGcsBucket", "online-payments-fraud") \
    .mode("overwrite") \
    .save()

users.write \
    .format("bigquery") \
    .option("table", "mlrit2.online_payments.users") \
    .option("temporaryGcsBucket", "online-payments-fraud") \
    .mode("overwrite") \
    .save()

device_info.write \
    .format("bigquery") \
    .option("table", "mlrit2.online_payments.device_info") \
    .option("temporaryGcsBucket", "online-payments-fraud") \
    .mode("overwrite") \
    .save()

fraud_labels.write \
    .format("bigquery") \
    .option("table", "mlrit2.online_payments.fraud_labels") \
    .option("temporaryGcsBucket", "online-payments-fraud") \
    .mode("overwrite") \
    .save()