from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("UberTripsKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("extra", FloatType(), True),
    StructField("mta_tax", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("tolls_amount", FloatType(), True),
    StructField("improvement_surcharge", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("congestion_surcharge", FloatType(), True),
    StructField("Airport_fee", FloatType(), True),
    StructField("cbd_congestion_fee", FloatType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "uber_trips") \
    .option("startingOffsets", "earliest") \
    .load()

trips_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

trips_df = trips_df \
    .withColumn("pickup_ts", col("tpep_pickup_datetime").cast(TimestampType())) \
    .withColumn("dropoff_ts", col("tpep_dropoff_datetime").cast(TimestampType()))

query_parquet = trips_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "D:/Just Data/Uber Real-Time Analytics Pipeline/uber_trips") \
    .option("checkpointLocation", "D:/Just Data/Uber Real-Time Analytics Pipeline/checkpoint") \
    .start()

