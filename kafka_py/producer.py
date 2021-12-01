# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from twitter import stream_API

bootstrap_server = "localhost:9092"

def create_producer(bootstrap_server):
    producer = KafkaProducer(bootstrap_servers=bootstrap_server)
    return producer

def send_twitter(producer):
    twitter_stream = stream_API.twitter_stream()



if __name__ == "__main__":
    producer = create_producer(bootstrap_server)
    producer.send('sample', b'Hello, World!')
    producer.send('sample', key=b'message-two', value=b'This is Kafka-Python')

    producer.send('quickstart-events', b'Hello, World!')
    producer.send('quickstart-events', key=b'message-two', value=b'This is Kafka-Python')

    producer.flush()
    print("End")
