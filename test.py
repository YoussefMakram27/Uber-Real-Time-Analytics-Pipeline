from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("UberTrips") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.warehouse.dir", "C:\\tmp\\spark-warehouse") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Trip Schema ---
trip_schema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', StringType(), True),
    StructField('tpep_dropoff_datetime', StringType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('Airport_fee', DoubleType(), True),
    StructField('cbd_congestion_fee', DoubleType(), True)
])

# --- Kafka Config ---
kafka_bootstrap = "localhost:9092"
kafka_topic = "uber_trips"

# --- Read stream ---
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# --- Parse JSON values ---
kafka_values_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")

parsed_df = kafka_values_df.select(
    from_json(col("value"), trip_schema).alias("data")
).select("data.*")

# --- Transform timestamps ---
parsed_df = parsed_df \
    .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("trip_duration_seconds", expr("CAST((unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) AS INT)"))

# --- Load Taxi Zones Lookup ---
zones_path = r"D:\Just Data\Uber Real-Time Analytics Pipeline\taxi_zone_lookup.csv"
zones_df = spark.read.csv(zones_path, inferSchema=True, header=True)

pu_zones = zones_df.select(
    col("LocationID").alias("PULocationID"),
    col("Borough").alias("pickup_borough"),
    col("Zone").alias("pickup_zone")
)

do_zones = zones_df.select(
    col("LocationID").alias("DOLocationID"),
    col("Borough").alias("dropoff_borough"),
    col("Zone").alias("dropoff_zone")
)

# --- Join Enrichment ---
enriched_df = parsed_df.join(pu_zones, on="PULocationID", how="left") \
                       .join(do_zones, on="DOLocationID", how="left")


sql_server_url = "jdbc:sqlserver://MYSTERYCHEETAH:1433;databaseName=UberDB"
sql_table = "UberTrips"
sql_user = "spark_user"
sql_password = "Spark123!"  

def write_to_sqlserver(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", sql_server_url) \
        .option("dbtable", sql_table) \
        .option("user", sql_user) \
        .option("password", sql_password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

query = enriched_df.writeStream \
    .foreachBatch(write_to_sqlserver) \
    .outputMode("append") \
    .option("checkpointLocation", "D:\\spark_checkpoints\\uber_trips_sql") \
    .start()

query.awaitTermination()

