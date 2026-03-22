# 🚕 Uber Real-Time Trip Analytics Pipeline

An end-to-end real-time data engineering pipeline that streams NYC taxi trip events through **Apache Kafka**, processes and enriches them with **Apache Spark Structured Streaming**, and persists results to both **SQL Server** and **Parquet** storage.

---

## 📐 Architecture
```
NYC TLC Parquet File
        │
        ▼
┌─────────────────┐
│  Kafka Producer  │  ← kafka_producer.py
│  (Batch Stream)  │    Reads parquet, sends batches of 50 trips
└────────┬────────┘
         │  Kafka Topic: uber_trips
         ├─────────────────────────────────────┐
         ▼                                     ▼
┌──────────────────────┐          ┌────────────────────────┐
│   Kafka Consumer     │          │  Spark Structured       │
│   (SQL Server sink)  │          │  Streaming Consumer     │
│  kafka_consumer.py   │          │  spark_structured_      │
│                      │          │  streaming.py           │
│  → SQL Server table  │          │  → Parquet (raw)        │
└──────────────────────┘          └────────────┬───────────┘
                                               │
                                               ▼
                                  ┌────────────────────────┐
                                  │   Spark Batch Job       │
                                  │   spark_processing.py   │
                                  │                         │
                                  │   → Cleaned Parquet     │
                                  └────────────────────────┘
```

---

## 📁 Project Structure
```
uber-realtime-pipeline/
│
├── kafka_producer.py            # Reads parquet, streams trips to Kafka in batches
├── kafka_consumer.py            # Consumes from Kafka, writes to SQL Server
├── spark_structured_streaming.py # Spark Structured Streaming → raw Parquet sink
├── spark_processing.py          # Batch cleaning & enrichment of raw Parquet
│
├── producer_state.json          # Auto-generated; tracks last sent trip_id
├── yellow_tripdata_2025-01.parquet  # Source dataset (NYC TLC)
│
└── README.md
```

---

## ⚙️ Tech Stack

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Batch Processing | PySpark |
| SQL Storage | Microsoft SQL Server (via pyodbc) |
| File Storage | Parquet (Snappy compressed) |
| Data Source | NYC TLC Yellow Taxi Trip Data |

---

## 🔧 Setup & Requirements

### Prerequisites

- Python 3.8+
- Apache Kafka (running on `localhost:9092`)
- Apache Spark 3.5.x
- Microsoft SQL Server (with ODBC Driver 17)
- Java 8 or 11 (required by Spark)

### Install Python Dependencies
```bash
pip install confluent-kafka pyspark pandas pyarrow pyodbc
```

### Kafka Setup

Create the topic before running:
```bash
kafka-topics.sh --create \
  --topic uber_trips \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## 🚀 Running the Pipeline

> Run each script in a **separate terminal**, in this order:

### Step 1 — Start the Kafka Producer

Reads the NYC TLC parquet file and streams trips in batches of 50 every 5 seconds.  
Saves progress to `producer_state.json` so it resumes from the last sent record on restart.
```bash
python kafka_producer.py
```

**Key config (top of file):**
```python
BATCH_SIZE = 50            # Records per batch
DELAY_BETWEEN_BATCHES = 5  # Seconds between batches
```

---

### Step 2a — Start the SQL Server Consumer

Consumes from the `uber_trips` Kafka topic and inserts into SQL Server in batches of 10.  
Auto-creates the `uber_trips` table if it doesn't exist.
```bash
python kafka_consumer.py
```

> **Update the connection string** in the script to match your SQL Server instance:
> ```python
> 'SERVER=YOUR_SERVER_NAME;'
> 'DATABASE=master;'
> ```

---

### Step 2b — Start the Spark Structured Streaming Consumer

Consumes from Kafka, parses JSON, applies basic quality filters, and writes to Parquet.
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_structured_streaming.py
```

**Output:**
- Raw Parquet: `uber_trips/`
- Checkpoint: `checkpoint/`

---

### Step 3 — Run Spark Batch Processing (Cleaning & Enrichment)

Reads the raw Parquet output, applies comprehensive cleaning, and writes cleaned data.
```bash
spark-submit spark_processing.py
```

**Cleaning steps applied:**
- Remove nulls and duplicates on `trip_id`
- Fix swapped pickup/dropoff timestamps
- Filter invalid trip distances, fares, durations, and speeds
- Recalculate `total_amount` from components
- Validate NYC location IDs (1–263)
- Add `trip_duration_minutes`, `avg_speed_mph`, `is_high_quality` flags

**Output:** `cleaned_uber_trips/` (Parquet)

---

## 📊 Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `trip_id` | INT | Auto-generated unique trip identifier |
| `VendorID` | INT | TPEP provider (1 = Creative Mobile, 2 = VeriFone) |
| `tpep_pickup_datetime` | DATETIME | Trip start time |
| `tpep_dropoff_datetime` | DATETIME | Trip end time |
| `passenger_count` | FLOAT | Number of passengers |
| `trip_distance` | FLOAT | Distance in miles |
| `PULocationID` | INT | Pickup TLC zone |
| `DOLocationID` | INT | Dropoff TLC zone |
| `payment_type` | INT | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `fare_amount` | FLOAT | Base fare |
| `total_amount` | FLOAT | Total including all surcharges |
| `trip_duration_minutes` | FLOAT | Derived: duration in minutes |
| `avg_speed_mph` | FLOAT | Derived: average speed |
| `is_high_quality` | BOOL | Derived: quality flag |

---

## 🔄 Producer State & Fault Tolerance

The producer tracks progress using `producer_state.json`:
```json
{
  "last_trip_id": 1500,
  "last_updated": "2025-01-15 14:30:00"
}
```

- On restart, it resumes from the last confirmed `trip_id`
- Only saves state **after Kafka delivery confirmation**
- To reprocess from the beginning: delete `producer_state.json`

---

## 📝 Data Source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi Trip Records, January 2025.

---

## 📌 Notes

- The Spark streaming consumer uses `startingOffsets: earliest` — it will reprocess all Kafka messages on restart unless the checkpoint directory exists.
- SQL Server consumer uses `cursor.fast_executemany = True` for optimized batch inserts.
- Spark job is configured with `maxOffsetsPerTrigger: 1000` to control micro-batch size.
