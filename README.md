# ⚡ Real-Time Uber Trips Streaming Pipeline

### 🧠 Data Streaming Project | PySpark | Kafka | SQL Server

![Tech](https://img.shields.io/badge/Tech-Streaming%20Data%20Engineering-blue?style=flat-square)
![PySpark](https://img.shields.io/badge/Framework-PySpark-orange?style=flat-square)
![Kafka](https://img.shields.io/badge/Source-Kafka-red?style=flat-square)
![SQLServer](https://img.shields.io/badge/Sink-SQL%20Server-green?style=flat-square)
![Status](https://img.shields.io/badge/Status-In%20Progress-yellow?style=flat-square)

---

## 🚕 Problem Statement

Ride-sharing platforms like **Uber** generate **continuous real-time trip data** such as:
- pickup & drop-off timestamps  
- passenger count, fare, and distance  
- location-based metrics  

The challenge: how to **ingest, process, and store this data in real-time** for analytics and reporting.

---

## 🎯 Project Goal

Build an **end-to-end real-time data pipeline** that:
✅ Streams Uber trip events from **Kafka**  
✅ Processes and enriches data using **PySpark Structured Streaming**  
✅ Stores curated data into a **SQL Server database** for analytics  

---

## 🧩 Data Flow Overview

```
Kafka (Producer)
     ↓
PySpark Structured Streaming (Consumer)
     ↓
Data Enrichment (join with zone lookup)
     ↓
SQL Server (final storage)
```

---

## 🧱 Architecture

| Layer | Technology | Description |
|--------|-------------|--------------|
| **Source** | Apache Kafka | Ingest real-time Uber trip data as JSON |
| **Processing** | PySpark Structured Streaming | Parse, clean, and enrich streams |
| **Storage (Sink)** | Microsoft SQL Server | Persist processed trip data |
| **Lookup Data** | CSV (Taxi Zone Lookup) | Map location IDs to zones and boroughs |

---

## 📦 Schema (Sample)

| Column | Type | Description |
|---------|------|-------------|
| `VendorID` | Integer | Trip provider ID |
| `tpep_pickup_datetime` | Timestamp | Pickup timestamp |
| `tpep_dropoff_datetime` | Timestamp | Drop-off timestamp |
| `passenger_count` | Integer | Number of passengers |
| `trip_distance` | Double | Distance in miles |
| `fare_amount` | Double | Fare paid |
| `pickup_zone` | String | Zone of pickup |
| `dropoff_zone` | String | Zone of drop-off |
| `trip_duration_seconds` | Integer | Duration in seconds |

---

## ⚙️ Key Steps

### 1️⃣ Read from Kafka
```python
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "uber_trips") \
    .option("startingOffsets", "earliest") \
    .load()
```

### 2️⃣ Parse JSON Value
```python
kafka_values_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = kafka_values_df.select(from_json(col("value"), trip_schema).alias("data")).select("data.*")
```

### 3️⃣ Transform & Enrich
```python
parsed_df = parsed_df \
    .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("trip_duration_seconds", expr("cast((unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) as int)"))

enriched_df = parsed_df \
    .join(pu_zones, on="PULocationID", how="left") \
    .join(do_zones, on="DOLocationID", how="left")
```

### 4️⃣ Write Stream to SQL Server
```python
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=UberDB"
connection_properties = {
    "user": "sa",
    "password": "YourPassword123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def write_to_sql_server(batch_df, batch_id):
    batch_df.write.jdbc(
        url=jdbc_url,
        table="dbo.TripsStream",
        mode="append",
        properties=connection_properties
    )

query = enriched_df.writeStream \
    .foreachBatch(write_to_sql_server) \
    .outputMode("append") \
    .start()
```

---

## 🧰 Technologies Used

| Category | Tools |
|-----------|-------|
| **Language** | Python |
| **Framework** | PySpark (Structured Streaming) |
| **Message Broker** | Apache Kafka |
| **Database** | Microsoft SQL Server |
| **File Lookup** | CSV (Taxi Zone Lookup) |

---

## 📊 Expected Output

Once the stream is active:
- Data flows **continuously** from Kafka → PySpark → SQL Server  
- SQL Server table `TripsStream` receives **live trip rows**  
- You can query:
```sql
SELECT TOP 10 * FROM dbo.TripsStream ORDER BY pickup_ts DESC;
```

---

## 🧠 Key Learnings

- Integrating **Kafka** with **Spark Structured Streaming**  
- Implementing **foreachBatch** to write micro-batches to SQL Server  
- Handling **real-time schema enforcement** and **timestamp conversions**  
- Joining streaming data with **static reference datasets**  

---

## 🔮 Future Improvements

- 🧱 Store in **Delta Lake** for ACID and versioning  
- 🚀 Add **Airflow** for orchestration  
- 🧰 Use **dbt** for transformations  
- 📈 Connect **Power BI / Grafana** for live dashboards  

---

## 👨‍💻 Author

**Youssef M. Makram**  
_Data Engineer | Real-Time Data Pipelines Enthusiast_

📫 [LinkedIn](https://www.linkedin.com/in/youssef-m-makram-m-osman-659a56233/)  
💻 [GitHub](https://github.com/YoussefMakram27)  
📧 Email: youssefmakram2108@gmail.com  
📱 [WhatsApp](https://wa.me/201281446248)
