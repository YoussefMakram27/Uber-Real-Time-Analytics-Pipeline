import time
import json
import pandas as pd
import os
import signal
import sys
from confluent_kafka import Producer

# --- Kafka Config ---
bootstrap_servers = 'localhost:9092'
topic = 'uber_trips'

# --- State Tracking Config ---
STATE_FILE = 'producer_state.json'  # Tracks last sent trip_id
BATCH_SIZE = 50  # How many records to send per batch
DELAY_BETWEEN_BATCHES = 5  # Seconds to wait between batches

# --- Global flag for graceful shutdown ---
running = True

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global running
    print("\n" + "=" * 70)
    print("🛑 Shutdown signal received. Finishing current batch...")
    print("=" * 70)
    running = False

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# --- Functions for State Management ---
def load_state():
    """Load the last processed trip_id from state file"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                return state.get('last_trip_id', 0)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"⚠️  Warning: Corrupted state file. Starting from beginning.")
            print(f"   Error: {e}")
            # Delete corrupted file
            os.remove(STATE_FILE)
            return 0
    return 0

def save_state(last_trip_id):
    """Save the last processed trip_id to state file"""
    with open(STATE_FILE, 'w') as f:
        json.dump({
            'last_trip_id': int(last_trip_id),
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
        }, f, indent=2)

# --- Delivery report ---
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")

# --- Initialize Producer ---
producer = Producer({'bootstrap.servers': bootstrap_servers})

# --- Load data ---
print("=" * 70)
print("Uber Trips Kafka Producer (Continuous Mode)")
print("=" * 70)

df = pd.read_parquet(r'D:\Just Data\Uber Real-Time Analytics Pipeline\yellow_tripdata_2025-01.parquet')

# --- Generate trip_id starting from 1 ---
df = df.reset_index(drop=True)
df['trip_id'] = df.index + 1

print(f"📊 Total records in dataset: {len(df):,}")

# Ensure datetime columns are string
datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
df[datetime_cols] = df[datetime_cols].astype(str)

# Replace NaN with None for JSON serialization
df = df.where(pd.notnull(df), None)

print(f"🔄 Batch size: {BATCH_SIZE} records")
print(f"⏱️  Delay between batches: {DELAY_BETWEEN_BATCHES} seconds")
print("-" * 70)
print("⏳ Starting continuous streaming... (Press Ctrl+C to stop)")
print("=" * 70)

# --- Continuous streaming loop ---
total_sent = 0
batch_count = 0

# DEBUG: Check source data before sending
print("\n🔍 DEBUG - Checking source data quality:")
print(f"passenger_count NaN count: {df['passenger_count'].isna().sum()} / {len(df)}")
print(f"RatecodeID NaN count: {df['RatecodeID'].isna().sum()} / {len(df)}")
print("\nSample of source data:")
print(df[['trip_id', 'passenger_count', 'RatecodeID', 'payment_type']].head(10))
print("=" * 70)

try:
    while running:
        # Get last processed trip_id
        last_trip_id = load_state()
        
        # Filter only NEW records
        new_records = df[df['trip_id'] > last_trip_id].head(BATCH_SIZE)
        
        if new_records.empty:
            print("\n" + "=" * 70)
            print("🎉 All records have been processed!")
            print(f"   Total records sent: {total_sent:,}")
            print(f"   Total batches: {batch_count}")
            print(f"   To restart from beginning, delete: {STATE_FILE}")
            print("=" * 70)
            break
        
        batch_count += 1
        batch_start = int(new_records['trip_id'].min())
        batch_end = int(new_records['trip_id'].max())
        
        print(f"\n📦 Batch #{batch_count}: Sending records {batch_start} to {batch_end}")
        
        # DEBUG: Check first record in batch
        if batch_count == 1:
            first_record = new_records.iloc[0]
            print("\n🔍 DEBUG - First record before sending:")
            print(f"  trip_id: {first_record['trip_id']} (type: {type(first_record['trip_id'])})")
            print(f"  passenger_count: {first_record['passenger_count']} (type: {type(first_record['passenger_count'])})")
            print(f"  RatecodeID: {first_record['RatecodeID']} (type: {type(first_record['RatecodeID'])})")
            print(f"  payment_type: {first_record['payment_type']} (type: {type(first_record['payment_type'])})")
            print(f"  Is passenger_count NaN? {pd.isna(first_record['passenger_count'])}")
            print(f"  Is RatecodeID NaN? {pd.isna(first_record['RatecodeID'])}")
        
        # Send records in this batch
        sent_in_batch = 0
        batch_start_time = time.time()
        
        for i, row in new_records.iterrows():
            if not running:  # Check if shutdown signal received
                break
                
            record = row.to_dict()
            
            # DEBUG: Print JSON payload for first record
            if sent_in_batch == 0 and batch_count == 1:
                json_payload = json.dumps(record)
                print(f"\n🔍 DEBUG - JSON payload being sent:")
                print(f"  Full JSON: {json_payload[:500]}...")
                # Parse it back to check
                parsed = json.loads(json_payload)
                print(f"  Parsed back - passenger_count: {parsed.get('passenger_count')}")
                print(f"  Parsed back - RatecodeID: {parsed.get('RatecodeID')}")
                print(f"  Parsed back - payment_type: {parsed.get('payment_type')}\n")
            
            try:
                producer.produce(
                    topic=topic,
                    key=str(row['trip_id']),
                    value=json.dumps(record),
                    on_delivery=delivery_report
                )
                sent_in_batch += 1
                total_sent += 1
                
                # Progress indicator every 10 records
                if sent_in_batch % 10 == 0:
                    print(f"   ↳ Sent {sent_in_batch}/{len(new_records)} records...")
                    producer.poll(0)  # Serve delivery callbacks
                
                time.sleep(0.1)  # Small delay between messages (0.1s = 10 msg/sec)
                
            except Exception as e:
                print(f"❌ Error sending trip_id {row['trip_id']}: {e}")
        
        # Final flush for this batch
        producer.flush()
        
        # Save state after successful batch
        last_id = int(new_records['trip_id'].max())
        save_state(last_id)
        
        batch_elapsed = time.time() - batch_start_time
        remaining = len(df) - last_id
        
        print(f"   ✓ Batch completed in {batch_elapsed:.2f}s")
        print(f"   ✓ State saved: last_trip_id = {last_id}")
        print(f"   ✓ Total sent so far: {total_sent:,} records")
        print(f"   ✓ Remaining in dataset: {remaining:,}")
        
        if not running:  # Check again before waiting
            break
        
        # Wait before next batch (only if there are more records)
        if remaining > 0:
            print(f"   ⏸️  Waiting {DELAY_BETWEEN_BATCHES}s before next batch...")
            time.sleep(DELAY_BETWEEN_BATCHES)

except KeyboardInterrupt:
    print("\n🛑 Interrupted by user")
except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
finally:
    # Cleanup
    producer.flush()
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    print(f"✓ Total records sent: {total_sent:,}")
    print(f"✓ Total batches: {batch_count}")
    print(f"✓ Last trip_id: {load_state()}")
    print(f"✓ State saved to: {STATE_FILE}")
    print("=" * 70)
    print("💡 Run this script again to continue from where it stopped")
    print("=" * 70)