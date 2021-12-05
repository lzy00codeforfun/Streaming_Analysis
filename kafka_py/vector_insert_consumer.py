# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
import threading
import time
import datetime
import wordcloud
import json



    def update_word_frequency(self):
        for message in message_list:
            print(message)
            print(isinstance(message, ConsumerRecord))
            if isinstance(message, ConsumerRecord):
                message_value = message.value.decode("utf8")
                message_value = json.loads(message_value)

                message_edts = ("-").join(message_value["window"]["end"].split("-")[:3])
                message_edts = datetime.datetime.fromisoformat(message_edts).timestamp()

                if message_edts - 5 <= self.begin_timestamp \
                        and self.begin_timestamp <= message_edts:
                    self.word_frequency[message_value["word"]] = message_value["count"]


class word_consumer_thread(threading.Thread):
    def __init__(self, thread_id):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.consumer = None

    def run(self):
        print("1 Begin")
        self._generate_consumer()
        self.dump_message()

    def _generate_consumer(self):
        self.consumer = KafkaConsumer('vector_insert',
                group_id="A",
                bootstrap_servers= ['localhost:9092'],
                enable_auto_commit = True,
                auto_commit_interval_ms = 500,
                auto_offset_reset="latest",
                consumer_timeout_ms=3000)

    def dump_message(self):
        for message in self.consumer:
            message_list.append(message)



if __name__ == "__main__":
    message_list = []
    word_consumer = word_consumer_thread(0)
    word_consumer.start()
    print("Word Consumer Begin")


