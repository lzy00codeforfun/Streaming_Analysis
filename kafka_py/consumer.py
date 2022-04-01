# -*- coding: utf-8 -*-
from kafka import KafkaConsumer

if __name__ == "__main__":
    temp_output = open("res_consumer.txt", "w")
    consumer = KafkaConsumer('quickstart-events',
            group_id="A",
            bootstrap_servers= ['localhost:9092'],
            enable_auto_commit = True,
            auto_commit_interval_ms = 1000,
            auto_offset_reset="earliest",
            consumer_timeout_ms=3000)
    # t = consumer.partitions_for_topic("quickstart-events")
    # t = consumer.beginning_offsets([0])
    # t = consumer.position()
    # t = consumer.assignment()
    t = consumer.metrics()
    print(t)
    for message in consumer:
        # print(dir(message))
        temp_output.write(str(message) + "\n")
        #consumer.commit()
    temp_output.close()
