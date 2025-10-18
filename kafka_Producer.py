import time
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

trip_schema = '''{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "UberTripEvent",
    "type": "object",
    "properties": {
        "VendorID": {"type": ["integer", "null"]},
        "tpep_pickup_datetime": {"type": ["string", "null"]},
        "tpep_dropoff_datetime": {"type": ["string", "null"]},
        "passenger_count": {"type": ["integer", "null"]},
        "trip_distance": {"type": ["number", "null"]},
        "RatecodeID": {"type": ["integer", "null"]},
        "store_and_fwd_flag": {"type": ["string", "null"]},
        "PULocationID": {"type": ["integer", "null"]},
        "DOLocationID": {"type": ["integer", "null"]},
        "payment_type": {"type": ["integer", "null"]},
        "fare_amount": {"type": ["number", "null"]},
        "extra": {"type": ["number", "null"]},
        "mta_tax": {"type": ["number", "null"]},
        "tip_amount": {"type": ["number", "null"]},
        "tolls_amount": {"type": ["number", "null"]},
        "improvement_surcharge": {"type": ["number", "null"]},
        "total_amount": {"type": ["number", "null"]},
        "congestion_surcharge": {"type": ["number", "null"]},
        "Airport_fee": {"type": ["number", "null"]},
        "cbd_congestion_fee": {"type": ["number", "null"]}
    },
    "required": ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
}'''

bootstrap_servers = 'localhost:9092'
schema_registry_url = 'http://localhost:8081'

schema_registry_conf = {'url': schema_registry_url}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

json_serializer = JSONSerializer(trip_schema, schema_registry_client)

producer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'key.serializer': StringSerializer('utf-8'),
    'value.serializer': json_serializer
}

producer = SerializingProducer(producer_conf)

df = pd.read_parquet(r'D:\Just Data\Uber Real-Time Analytics Pipeline\yellow_tripdata_2025-01.parquet')

topic = 'uber_trips'

def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}]")


for i, row in df.iterrows():
    record = row.to_dict()

    producer.produce(topic = topic, key = str(i), on_delivery = delivery_report)

    time.sleep(1)

    if i%10 == 0:
        producer.flush()

producer.flush()    