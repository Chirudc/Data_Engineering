from kafka import KafkaProducer
import json
import time
import random

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9081',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate simulated Facebook data
def generate_facebook_data():
    users = [
        'Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Edward Elric',
        'Fiona Gallagher', 'George Orwell', 'Hannah Montana', 'Irene Adler', 'Jack Sparrow',
        'Karen Page', 'Luke Cage', 'Mia Wallace', 'Nancy Drew', 'Oscar Wilde',
        'Peter Parker', 'Quinn Fabray', 'Rachel Green', 'Steve Rogers', 'Tony Stark'
    ]
    posts = [
        'Just had a great lunch!',
        'Loving the new features on Facebook!',
        'Had a fantastic workout today!',
        'Feeling blessed and grateful!',
        'Excited about the upcoming trip!',
        'Reading an amazing book!',
        'Watching a fantastic movie!',
        'Learning new recipes!',
        'Exploring new places!',
        'Enjoying a beautiful day at the park!',
        'Caught up with old friends today!',
        'Started a new hobby!',
        'Had a productive day at work!',
        'Volunteered at the local shelter!',
        'Went hiking in the mountains!',
        'Visited a new cafe today!',
        'Working on a new project!',
        'Attended a great concert!',
        'Experimenting with photography!',
        'Just finished a marathon!'
    ]
    data = {
        'user_id': random.choice(users),
        'post': random.choice(posts),
        'timestamp': time.time()
    }
    return data

# Produce data to Kafka topic
topic_name = 'facebook-stream'
print(topic_name)

try:
    while True:
        data = generate_facebook_data()
        print(f'Sending: {data}')
        producer.send(topic_name, value=data)
        time.sleep(1)  # Simulate data generation interval
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    producer.close()

