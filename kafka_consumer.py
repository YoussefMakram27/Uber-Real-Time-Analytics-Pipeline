from confluent_kafka import Consumer
import json
import pyodbc

# --- Kafka Consumer Config ---
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'uber-sql-writer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['uber_trips'])

# --- SQL Server Connection ---
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=MYSTERYCHEETAH;'
    'DATABASE=master;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()
cursor.fast_executemany = True  # ⚡ bulk insert

# --- Create table if not exists ---
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='uber_trips' AND xtype='U')
CREATE TABLE uber_trips (
    trip_id INT PRIMARY KEY,
    VendorID INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT,
    trip_distance FLOAT,
    RatecodeID INT,
    store_and_fwd_flag NVARCHAR(10),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    cbd_congestion_fee FLOAT
)
""")
conn.commit()

batch_size = 10
batch = []

print("Streaming Uber trips to SQL Server...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f'Consumer error: {msg.error()}')
        continue

    trip = json.loads(msg.value().decode('utf-8'))

    # Add trip_id to batch
    batch.append([
        trip.get('trip_id'),
        trip.get('VendorID'),
        trip.get('tpep_pickup_datetime'),
        trip.get('tpep_dropoff_datetime'),
        trip.get('passenger_count'),
        trip.get('trip_distance'),
        trip.get('RatecodeID'),
        trip.get('store_and_fwd_flag'),
        trip.get('PULocationID'),
        trip.get('DOLocationID'),
        trip.get('payment_type'),
        trip.get('fare_amount'),
        trip.get('extra'),
        trip.get('mta_tax'),
        trip.get('tip_amount'),
        trip.get('tolls_amount'),
        trip.get('improvement_surcharge'),
        trip.get('total_amount'),
        trip.get('congestion_surcharge'),
        trip.get('Airport_fee'),
        trip.get('cbd_congestion_fee')
    ])

    # Bulk insert when batch is ready
    if len(batch) >= batch_size:
        try:
            cursor.executemany("""
                INSERT INTO uber_trips (
                    trip_id, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                    trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
                    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge, Airport_fee, cbd_congestion_fee
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            print(f"Inserted batch of {len(batch)} trips")
            batch = []
        except Exception as e:
            print("DB error:", e)
            conn.rollback()

consumer.close()
