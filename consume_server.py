from confluent_kafka import Consumer,Producer
import json
# config of orginal source
config_server = {
    'bootstrap.servers': '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294',
    'group.id': 'my_consumer_group',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
}
#config of kafka that will produces message
config_produce = {
    'bootstrap.servers':'localhost:9094,localhost:9194,localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024'
}

topic_server = 'product_view' #topic server
topic_produce = 'project_topic' #topic kafka
consumer_server = Consumer(config_server)
consumer_server.subscribe([topic_server])
producer = Producer(config_produce)

#Consume message from source
try:
    while True:
        msg = consumer_server.poll(1.0)  
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        print(f"Received message: {msg.value().decode('utf-8')}")
       
       #produce message from source to kafka
        producer.produce(
            topic_produce,
            value = msg.value()
        )
        
finally:
    consumer_server.close()
    producer.flush()