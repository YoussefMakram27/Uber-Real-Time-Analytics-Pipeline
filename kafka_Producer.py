import time
import json
import pandas as pd
from confluent_kafka import Producer

# --- Kafka Config ---
bootstrap_servers = 'localhost:9092'
topic = 'uber_trips'

producer = Producer({'bootstrap.servers': bootstrap_servers})

# --- Load data ---
df = pd.read_parquet(r'D:\Just Data\Uber Real-Time Analytics Pipeline\yellow_tripdata_2025-01.parquet')

# --- Generate trip_id starting from 1 ---
df = df.reset_index(drop=True)  # reset index just in case
df['trip_id'] = df.index + 1  # trip_id = 1, 2, 3, ...

# Replace NaN with None for JSON serialization
df = df.where(pd.notnull(df), None)

# Ensure datetime columns are string
datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
df[datetime_cols] = df[datetime_cols].astype(str)

# --- Delivery report ---
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} produced to {msg.topic()} [{msg.partition()}]")

# --- Produce to Kafka ---
print("Starting to stream trip events to Kafka...")

for i, row in df.iterrows():
    record = row.to_dict()
    producer.produce(
        topic=topic,
        key=str(row['trip_id']),  # use trip_id as key
        value=json.dumps(record),
        on_delivery=delivery_report
    )
    time.sleep(0.5)  # adjust speed

    if i % 10 == 0:
        producer.flush()

producer.flush()
print("Finished producing messages.")
