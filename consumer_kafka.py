from confluent_kafka import Consumer
from pymongo import MongoClient
import json
#set config for consumer
config = {
    'bootstrap.servers': 'localhost:9094,localhost:9194,localhost:9294',  
    'group.id': 'my-consumer-group',        
    'auto.offset.reset': 'earliest',        
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',             
    'sasl.username': 'admin',               
    'sasl.password': 'Unigap@2024',        
}
topic = 'project_topic' #topic of kafka
consumer = Consumer(config)
consumer.subscribe([topic])
#set config for Mongodb
client = MongoClient('mongodb://localhost:27017/')
db = client['project_kafka']
collection = db['product_view']
#Consume message
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'COnsumer error: {msg.error()}')
        data = json.loads(msg.value().decode('utf-8'))
        collection.insert_one(data)
except KeyboardInterrupt:
    print('Consumer shut down')
finally:
    consumer.close()