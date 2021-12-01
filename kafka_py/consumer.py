# -*- coding: utf-8 -*-
from kafka import KafkaConsumer

if __name__ == "__main__":
    consumer = KafkaConsumer('quickstart-events', bootstrap_servers= ['localhost:9092'])
    for message in consumer:
        print (message)
