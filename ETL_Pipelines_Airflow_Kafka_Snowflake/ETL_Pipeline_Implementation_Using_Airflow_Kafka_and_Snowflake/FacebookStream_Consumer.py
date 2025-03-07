from kafka import KafkaConsumer
import json

# Configure Kafka Consumer
consumer = KafkaConsumer(
    'facebook-stream',  # Topic name
    bootstrap_servers='localhost:9081',  
    auto_offset_reset='earliest',  # Start reading at the earliest available message
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='facebook-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process the consumed data
for message in consumer:
    data = message.value
    print(f"Received data: {data}")

