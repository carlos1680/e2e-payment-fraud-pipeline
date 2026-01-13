from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# This schema matches exactly our raw JSON generator
PAYMENTS_RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True), # Read as string first to handle ISO format
    StructField("payment_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("status", StringType(), True)
])