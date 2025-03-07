'''
1. cd mini_program/kafka
2. start zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
3. start kafka in new terminal: bin/kafka-server-start.sh config/server.properties
(change default java version if necessary)
4. run producer.py in new terminal
5. run consumer.py in new terminal
'''

from kafka import KafkaConsumer
import json
from  snowflake import connector
import pandas as pd
from snowflake_config import user, password, account, database, schema, role

# Configure Snowflake Connection
con = connector.connect(
    user=user,
    password=password,
    account=account,
    database=database,
    schema='MINI_PROJECT',
    role=role
)
cursor = con.cursor()

cursor.execute(f'''
    CREATE OR REPLACE TABLE KAFKA (
        user_id CHAR(50),
        post TEXT,
        timestamp FLOAT
    );
''')

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
try:
    for message in consumer:
        data = message.value
        print(f"Received data: {data}")
        # {'user_id': 'Luke Cage', 'post': 'Feeling blessed and grateful!', 'timestamp': 1717209402.379224}
        cursor.execute(f'''
            INSERT INTO KAFKA (user_id, post, timestamp)
            VALUES ('{data['user_id']}', '{data['post']}', {data['timestamp']})
        ''')
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    con.close()

