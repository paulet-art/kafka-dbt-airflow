from kafka import KafkaConsumer
import json

# consumer instance
consumer = KafkaConsumer(
    'movies',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# consume from topic
for message in consumer:
    data = message.value
    print("Received data:", data)