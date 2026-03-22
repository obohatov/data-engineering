import json
from kafka import KafkaConsumer

# Initialize Kafka Consumer
# Set auto_offset_reset='earliest' to read from the very beginning of the topic
consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9093'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    # Stop after 5 seconds of inactivity to get the final count
    consumer_timeout_ms=5000
)

trip_count = 0
print("Consuming messages and filtering by distance > 5.0...")

for message in consumer:
    # Homework requirement: count trips with trip_distance > 5.0
    if message.value.get('trip_distance', 0) > 5.0:
        trip_count += 1

print(f"Question 3 Result: {trip_count}")
