from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("UberTripsKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Read the saved data
saved_df = spark.read.parquet("D:/Just Data/Uber Real-Time Analytics Pipeline/uber_trips")

print("=" * 80)
print("DIAGNOSTIC CHECK - Let's see what's in your data")
print("=" * 80)

# Check total records
print(f"\nTotal records: {saved_df.count()}")

# Check schema
print("\nSchema:")
saved_df.printSchema()

# Show sample data
print("\nSample of raw data:")
saved_df.show(5, truncate=False)

# Check for nulls in each column
print("\nNull counts per column:")
saved_df.select([count(when(col(c).isNull(), c)).alias(c) for c in saved_df.columns]).show(vertical=True)

# Check timestamp columns specifically
print("\nTimestamp columns check:")
saved_df.select("pickup_ts", "dropoff_ts").show(5, truncate=False)

# Check if timestamps are actually null
print(f"Null pickup_ts: {saved_df.filter(col('pickup_ts').isNull()).count()}")
print(f"Null dropoff_ts: {saved_df.filter(col('dropoff_ts').isNull()).count()}")

# Check location IDs range
print("\nLocation ID ranges:")
saved_df.select(
    min("PULocationID").alias("min_pickup_loc"),
    max("PULocationID").alias("max_pickup_loc"),
    min("DOLocationID").alias("min_dropoff_loc"),
    max("DOLocationID").alias("max_dropoff_loc")
).show()

# Check trip_distance
print("\nTrip distance stats:")
saved_df.select(
    min("trip_distance").alias("min_dist"),
    max("trip_distance").alias("max_dist"),
    count(when(col("trip_distance") <= 0, 1)).alias("zero_or_negative")
).show()

# Check fare_amount
print("\nFare amount stats:")
saved_df.select(
    min("fare_amount").alias("min_fare"),
    max("fare_amount").alias("max_fare"),
    count(when(col("fare_amount") <= 0, 1)).alias("zero_or_negative")
).show()

print("=" * 80)
print("Now let's clean step by step and see where records get dropped...")
print("=" * 80)

# Step-by-step cleaning with counts
cleaned_df = saved_df
print(f"Starting records: {cleaned_df.count()}")

# 1. Remove nulls in trip_id
cleaned_df = cleaned_df.filter(col("trip_id").isNotNull())
print(f"After removing null trip_id: {cleaned_df.count()}")

# 2. Drop duplicates
cleaned_df = cleaned_df.dropDuplicates(["trip_id"])
print(f"After dropping duplicates: {cleaned_df.count()}")

# 3. Sort by trip_id
cleaned_df = cleaned_df.orderBy("trip_id")

# 4. Remove original datetime string columns IF they exist
if "tpep_pickup_datetime" in cleaned_df.columns:
    cleaned_df = cleaned_df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

# 5. Fix passenger_count
cleaned_df = cleaned_df.withColumn(
    "passenger_count",
    when(col("passenger_count") < 0, abs(col("passenger_count")))
    .when(col("passenger_count") == 0, 1)
    .otherwise(col("passenger_count"))
)

# 6. Check if we have valid timestamps BEFORE filtering
print(f"\nTimestamps check before filtering:")
print(f"Null pickup_ts: {cleaned_df.filter(col('pickup_ts').isNull()).count()}")
print(f"Null dropoff_ts: {cleaned_df.filter(col('dropoff_ts').isNull()).count()}")

# If timestamps are null, skip timestamp-dependent operations
has_valid_timestamps = cleaned_df.filter(col("pickup_ts").isNotNull() & col("dropoff_ts").isNotNull()).count() > 0

if has_valid_timestamps:
    # Fix pickup/dropoff order
    cleaned_df = cleaned_df.withColumn(
        "pickup_ts_corrected",
        when(col("dropoff_ts") < col("pickup_ts"), col("dropoff_ts"))
        .otherwise(col("pickup_ts"))
    ).withColumn(
        "dropoff_ts_corrected",
        when(col("dropoff_ts") < col("pickup_ts"), col("pickup_ts"))
        .otherwise(col("dropoff_ts"))
    )
    
    cleaned_df = cleaned_df.drop("pickup_ts", "dropoff_ts") \
        .withColumnRenamed("pickup_ts_corrected", "pickup_ts") \
        .withColumnRenamed("dropoff_ts_corrected", "dropoff_ts")
    
    # Calculate trip duration
    cleaned_df = cleaned_df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60
    )
    print(f"After adding trip duration: {cleaned_df.count()}")
    
    # Remove invalid durations
    cleaned_df = cleaned_df.filter(col("trip_duration_minutes") > 0)
    print(f"After filtering trip_duration > 0: {cleaned_df.count()}")
else:
    print("⚠️  WARNING: Timestamps are NULL - skipping timestamp validations")
    # Add dummy trip duration
    cleaned_df = cleaned_df.withColumn("trip_duration_minutes", lit(10.0))

# 7. Ensure trip_distance > 0
print(f"Records with trip_distance <= 0: {cleaned_df.filter(col('trip_distance') <= 0).count()}")
cleaned_df = cleaned_df.filter(col("trip_distance") > 0)
print(f"After filtering trip_distance > 0: {cleaned_df.count()}")

# 8. Fix and filter fare_amount
cleaned_df = cleaned_df.withColumn("fare_amount", abs(col("fare_amount")))
print(f"Records with fare_amount <= 0: {cleaned_df.filter(col('fare_amount') <= 0).count()}")
cleaned_df = cleaned_df.filter(col("fare_amount") > 0)
print(f"After filtering fare_amount > 0: {cleaned_df.count()}")

# 9. Fix monetary fields
money_columns = ["extra", "mta_tax", "tip_amount", "tolls_amount", 
                 "improvement_surcharge", "Airport_fee"]

for col_name in money_columns:
    if col_name in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            col_name,
            when(col(col_name).isNotNull(), abs(col(col_name))).otherwise(0)
        )

# 10. Recalculate total_amount
cleaned_df = cleaned_df.withColumn(
    "calculated_total",
    col("fare_amount") + 
    coalesce(col("extra"), lit(0)) + 
    coalesce(col("mta_tax"), lit(0)) + 
    coalesce(col("tip_amount"), lit(0)) + 
    coalesce(col("tolls_amount"), lit(0)) + 
    coalesce(col("improvement_surcharge"), lit(0)) +
    coalesce(col("congestion_surcharge"), lit(0)) +
    coalesce(col("Airport_fee"), lit(0)) +
    coalesce(col("cbd_congestion_fee"), lit(0))
)

cleaned_df = cleaned_df.drop("total_amount").withColumnRenamed("calculated_total", "total_amount")

# 11. Fix congestion_surcharge
cleaned_df = cleaned_df.withColumn(
    "congestion_surcharge",
    when(col("congestion_surcharge").isNotNull() & (col("congestion_surcharge") < 0), 
         abs(col("congestion_surcharge")))
    .otherwise(coalesce(col("congestion_surcharge"), lit(0)))
)

# Calculate average speed if we have valid timestamps
if has_valid_timestamps:
    cleaned_df = cleaned_df.withColumn(
        "avg_speed_mph",
        (col("trip_distance") / col("trip_duration_minutes")) * 60
    )
    
    print(f"\nSpeed check:")
    print(f"Records with speed > 150 mph: {cleaned_df.filter(col('avg_speed_mph') > 150).count()}")
    cleaned_df = cleaned_df.filter(col("avg_speed_mph") <= 150)
    print(f"After filtering speed <= 150 mph: {cleaned_df.count()}")
    
    print(f"Records with duration > 1440 min: {cleaned_df.filter(col('trip_duration_minutes') > 1440).count()}")
    cleaned_df = cleaned_df.filter(col("trip_duration_minutes") <= 1440)
    print(f"After filtering duration <= 1440 min: {cleaned_df.count()}")
else:
    cleaned_df = cleaned_df.withColumn("avg_speed_mph", lit(15.0))  # Dummy value

# Check location IDs before filtering
print(f"\nLocation ID check:")
print(f"Records with invalid PULocationID: {cleaned_df.filter(~col('PULocationID').between(1, 263)).count()}")
print(f"Records with invalid DOLocationID: {cleaned_df.filter(~col('DOLocationID').between(1, 263)).count()}")

# RELAXED location validation - only filter out extreme outliers
cleaned_df = cleaned_df.filter(
    (col("PULocationID") > 0) & (col("PULocationID") < 1000) &
    (col("DOLocationID") > 0) & (col("DOLocationID") < 1000)
)
print(f"After location ID validation (relaxed): {cleaned_df.count()}")

# Validate payment_type (only if not all null)
if "payment_type" in cleaned_df.columns:
    non_null_payment = cleaned_df.filter(col("payment_type").isNotNull()).count()
    if non_null_payment > 0:
        print(f"Records with payment_type available: {non_null_payment}")
        # Only validate if we have non-null values
        cleaned_df = cleaned_df.filter(col("payment_type").isNotNull())
        print(f"After payment_type validation: {cleaned_df.count()}")
    else:
        print(f"⚠️  Skipping payment_type validation - all values are NULL")

# Skip RatecodeID validation (often NULL in real data)
print(f"⚠️  Skipping RatecodeID validation - not required for analysis")

# Skip passenger_count validation (often NULL in real data)
print(f"⚠️  Skipping passenger_count validation - not required for analysis")

# Add quality flag (more lenient)
cleaned_df = cleaned_df.withColumn(
    "is_high_quality",
    when(
        (col("trip_distance") >= 0.1) &
        (col("trip_distance") <= 200) &
        (col("fare_amount") >= 0.01),
        True
    ).otherwise(False)
)

# Add processing timestamp
cleaned_df = cleaned_df.withColumn("processed_at", current_timestamp())

# Cache
cleaned_df = cleaned_df.cache()

# Show final results
print("\n" + "=" * 80)
print("FINAL CLEANING SUMMARY")
print("=" * 80)
print(f"Original records: {saved_df.count():,}")
print(f"Cleaned records: {cleaned_df.count():,}")
print(f"Records removed: {saved_df.count() - cleaned_df.count():,}")

if cleaned_df.count() > 0:
    print(f"Removal rate: {((saved_df.count() - cleaned_df.count()) / saved_df.count() * 100):.2f}%")
    print("\nHigh Quality Trips:")
    cleaned_df.groupBy("is_high_quality").count().show()
    print("\nSample of cleaned data:")
    cleaned_df.show(5, truncate=False)
    print("\nData quality metrics:")
    cleaned_df.describe("trip_distance", "fare_amount", "total_amount").show()
    
    # Save cleaned data
    cleaned_df.write \
        .mode("overwrite") \
        .parquet("D:/Just Data/Uber Real-Time Analytics Pipeline/cleaned_uber_trips")
    
    print("✓ Cleaned data saved successfully!")
else:
    print("❌ ERROR: All records were filtered out!")
    print("\nPlease check the diagnostic output above to see where the issue is.")
    print("Most likely causes:")
    print("1. Timestamp conversion failed (pickup_ts/dropoff_ts are NULL)")
    print("2. trip_distance is 0 or negative for all records")
    print("3. fare_amount is 0 or negative for all records")
    print("4. Location IDs are outside expected range")