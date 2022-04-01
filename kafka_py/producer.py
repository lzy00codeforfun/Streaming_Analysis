# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import time
import random

bootstrap_server = "localhost:9092"

def create_producer(bootstrap_server):
    producer = KafkaProducer(bootstrap_servers=bootstrap_server)
    return producer

def send_twitter(producer):
    twitter_stream = stream_API.twitter_stream()



if __name__ == "__main__":
    producer = create_producer(bootstrap_server)

    # producer.send('sample', b'Hello, World!')
    # producer.send('sample', key=b'message-two', value=b'This is Kafka-Python')

    text_list = ["Hello", "Apple", "Banana", "Pear", "World", "Peach", "Lakers", "Mac", "Water", "Rice", "Vancouver"]


    while True:
        for i in range(0, 10):
            random_text = " ".join(" ".join([item] * random.randint(0, 10)) for item in text_list)
            producer.send('quickstart-events', random_text.encode("utf8"))
    #        producer.send('quickstart-events', b'Hello, World!')
    #        producer.send('quickstart-events', key=b'message-two', value=b'This is Kafka-Python')
    #        producer.send('quickstart-events', b'Hello, World!')

        producer.flush()

        time.sleep(1)
