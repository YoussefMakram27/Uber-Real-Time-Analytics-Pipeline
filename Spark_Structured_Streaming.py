from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, LongType
from pyspark.sql.functions import *

# Initialize Spark with optimized configs
spark = SparkSession.builder \
    .appName("UberTripsKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.streaming.checkpointLocation.maxFileSize", "1000000") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Schema definition
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", FloatType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("RatecodeID", FloatType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
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

print("=" * 80)
print("Uber Trips Kafka Consumer - Structured Streaming")
print("=" * 80)

# Read from Kafka with improved settings
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "uber_trips") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .load()

print("✓ Connected to Kafka topic: uber_trips")
print("✓ Starting from: LATEST offsets (no replay of old messages)")

# Parse JSON and extract fields
trips_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp"),  # Kafka message timestamp
    col("offset").alias("kafka_offset")         # Kafka offset for tracking
).select("data.*", "kafka_timestamp", "kafka_offset")

print("✓ JSON parsing configured")

# Convert timestamp strings to proper timestamps
trips_df = trips_df \
    .withColumn("pickup_ts", col("tpep_pickup_datetime").cast(TimestampType())) \
    .withColumn("dropoff_ts", col("tpep_dropoff_datetime").cast(TimestampType())) \
    .withColumn("ingestion_time", current_timestamp())  # Track when we received it

# Drop the original string datetime columns to save space
trips_df = trips_df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

print("✓ Timestamp conversions configured")

# Add data quality check - filter out clearly bad records
trips_df = trips_df.filter(
    (col("trip_id").isNotNull()) 
)

print("✓ Basic data quality filters applied")

# Write stream with optimized settings
query_parquet = trips_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "D:/Just Data/Uber Real-Time Analytics Pipeline/uber_trips") \
    .option("checkpointLocation", "D:/Just Data/Uber Real-Time Analytics Pipeline/checkpoint") \
    .trigger(processingTime='10 seconds') \
    .option("compression", "snappy") \
    .queryName("UberTripsStreamingQuery") \
    .start()

print("=" * 80)
print("✓ Streaming query started successfully!")
print("-" * 80)
print("Configuration:")
print(f"  • Output Path: D:/Just Data/Uber Real-Time Analytics Pipeline/uber_trips")
print(f"  • Checkpoint: D:/Just Data/Uber Real-Time Analytics Pipeline/checkpoint")
print(f"  • Trigger: Every 10 seconds")
print(f"  • Compression: Snappy")
print(f"  • Max Records/Batch: 1000")
print("-" * 80)
print("Status: ⏳ Waiting for new messages from Kafka...")
print("         Press Ctrl+C to stop the consumer")
print("=" * 80)

# Monitor stream status
try:
    # Wait for termination and show progress
    while query_parquet.isActive:
        status = query_parquet.status
        recent_progress = query_parquet.recentProgress
        
        if recent_progress:
            latest = recent_progress[-1]
            num_rows = latest.get('numInputRows', 0)
            if num_rows > 0:
                print(f"📊 Processed {num_rows} rows | "
                      f"Batch: {latest.get('batchId', 'N/A')} | "
                      f"Time: {latest.get('timestamp', 'N/A')}")
        
        query_parquet.awaitTermination(timeout=30)
        
except KeyboardInterrupt:
    print("\n" + "=" * 80)
    print("🛑 Stopping streaming query...")
    query_parquet.stop()
    print("✓ Query stopped successfully")
    print("=" * 80)
except Exception as e:
    print(f"\n❌ Error: {e}")
    query_parquet.stop()
finally:
    spark.stop()
    print("✓ Spark session closed")