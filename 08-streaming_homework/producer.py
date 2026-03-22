import pandas as pd
from kafka import KafkaProducer
import json
from time import time

# Homework configuration
INPUT_FILE = 'green_tripdata_2025-10.parquet'
TOPIC_NAME = 'green-trips'

# Connect to Redpanda on port 9093
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load and filter required columns
columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 
    'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount'
]
df = pd.read_parquet(INPUT_FILE)
df = df[columns]

print(f"Sending {len(df)} rows to Redpanda topic '{TOPIC_NAME}'...")
t0 = time()

for row in df.itertuples(index=False):
    row_dict = row._asdict()
    # Convert timestamps to strings for JSON
    row_dict['lpep_pickup_datetime'] = str(row_dict['lpep_pickup_datetime'])
    row_dict['lpep_dropoff_datetime'] = str(row_dict['lpep_dropoff_datetime'])
    producer.send(TOPIC_NAME, value=row_dict)

producer.flush()
t1 = time()
print(f'Question 2 Result: took {(t1 - t0):.2f} seconds')
