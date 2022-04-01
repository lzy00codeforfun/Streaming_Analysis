# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
import threading
import time
import datetime
import wordcloud
import json
import imageio

TOPIC = "Twitter_WordCount"

class word_cloud_thread(threading.Thread):
    def __init__(self, thread_id, begin_timestamp):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.begin_timestamp = begin_timestamp
        self.word_frequency = {}

    def run(self):
        current_timestamp = int(time.time())

        self.update_word_frequency()
        print(self.word_frequency)
        if self.word_frequency:
            color_list = ["#9B433E", "#E1B25B", "#45687B", "#4C6A4F"]
            # mk = imageio.imread("DataCube.png")
            # wc = wordcloud.WordCloud(background_color="white", mask=mk)
            wc = wordcloud.WordCloud(background_color="white")
            # wc = wordcloud.WordCloud()
            wc.generate_from_frequencies(self.word_frequency)
            wc.to_file("../frontend/src/assets/WordCloud.png")
        print("I AM DONE.", self.thread_id)

    def update_word_frequency(self):
        for message in message_list:
            # print(message)
            # print(isinstance(message, ConsumerRecord))
            if isinstance(message, ConsumerRecord):
                message_value = message.value.decode("utf8")
                message_value = json.loads(message_value)

                message_edts = ("-").join(message_value["window"]["end"].split("-")[:3])
                message_edts = datetime.datetime.fromisoformat(message_edts).timestamp()

                if message_edts - 5 <= self.begin_timestamp \
                        and self.begin_timestamp <= message_edts:
                    self.word_frequency[message_value["hashtag"]] = message_value["count"]


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
        self.consumer = KafkaConsumer(TOPIC,
                group_id="A",
                bootstrap_servers= ['localhost:9092'],
                enable_auto_commit = True,
                auto_commit_interval_ms = 500,
                auto_offset_reset="latest",
                consumer_timeout_ms=3000)

    def dump_message(self):
        for message in self.consumer:
            message_list.append(message)

class thread_timer(threading.Thread):
    def __init__(self, thread_id, workload, interval):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.workload = workload
        self.interval = interval

    def run(self):
        thread_index = 2
        while(True):
            current_timestamp = int(time.time())
            wc_thread = word_cloud_thread(thread_index, current_timestamp)
            wc_thread.start()
            thread_index += 1

            print("wc thread start")
            message_list.append({thread_index: "Hello" + str(thread_index)})
            time.sleep(self.interval)

            # word_cloud = word_cloud_thread(thread_index, current_timestamp)
            # word_cloud.start()

            # threading.Timer(self.interval, self.run(thread_index + 1)).start()


if __name__ == "__main__":
    message_list = []
    word_consumer = word_consumer_thread(0)
    word_consumer.start()
    print("Word Consumer Begin")

    timer = thread_timer(1, None, 5)
    timer.start()
    print("Timer Begin")

    print("END")





    # temp_output = open("res_consumer.txt", "w")
    # consumer = KafkaConsumer('word_count',
    #         group_id="A",
    #         bootstrap_servers= ['localhost:9092'],
    #         enable_auto_commit = True,
    #         auto_commit_interval_ms = 1000,
    #         auto_offset_reset="earliest",
    #         consumer_timeout_ms=3000)
    # # t = consumer.partitions_for_topic("quickstart-events")
    # # t = consumer.beginning_offsets([0])
    # # t = consumer.position()
    # # t = consumer.assignment()
    # t = consumer.metrics()
    # print(t)

    # word_frequency = {}
    # message_list
    # for message in consumer:
    #     current_time = time.time()
    #     print(current_time)
    #     # print(dir(message))
    #     # temp_output.write(str(message) + "\n")
    #     print(str(message) + "\n")
    #     #consumer.commit()
    # temp_output.close()
