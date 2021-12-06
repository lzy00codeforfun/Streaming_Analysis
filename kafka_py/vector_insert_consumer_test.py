# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
import threading
import time
import datetime
import os
import json
from kafka_py.sql_model import SQLModel
import numpy as np
import pickle
from sql_model import SQLModel
import requests

TOPIC = "vector_insert"
sql_M = SQLModel(Mode=1)

class vector_insert_thread(threading.Thread):
    def __init__(self, thread_id, begin_timestamp):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.begin_timestamp = begin_timestamp
        self.word_frequency = {}
        self.cur_id = 0
        with open("vector_map.pkl", 'rb') as pkl_file:
            self.vector_map = pickle.load(pkl_file)
        

    def get_vector(self, text): 
        words = text.split(" ")
        vector_lines = []
        for i in words:
            if i in self.vector_map:
                vector_lines.append(self.vector_map[i])
        vector_lines = np.array(vector_lines).mean(axis=0).tolist()
        if len(vector_lines) == 0:
            return None
        return vector_lines

    def run(self):
        current_timestamp = int(time.time())
        url_request_map = self.process_messages()
        try:
            self.make_url_post(url_request_map)
        except:
            pass
        print('url_request_map shape', len(url_request_map["id"]))
        
        print("I AM DONE.", self.thread_id)

    def process_messages(self):
        print("message number", len(message_list))
        url_request_map = {}
        url_request_map['id'] = []
        url_request_map['vector'] = []
        for message in message_list:
            # print(message)
            # print(isinstance(message, ConsumerRecord))
            if isinstance(message, ConsumerRecord):
                message_value = message.value.decode("utf8")
                print("message", type(message_value), message_value)
                message_value = json.loads(message_value)
                tmp_vector = self.get_vector(message_value["preprocessed_text"])
                if tmp_vector is None:
                    continue
                
                # get id from sql Model
                # message_value['id'] = self.cur_id
                url_request_map['id'].append(self.log_file(message_value))
                url_request_map['vector'].append(tmp_vector)
                # self.cur_id += 1
        return url_request_map

    def make_url_post(self, data):
        url = "localhost:5000/insert"
        requests.post(url, data=data)

    def log_file(self, data):
        cur_id = sql_M.insert(data)
        return cur_id

class vector_insert_consumer_thread(threading.Thread):
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
                # group_id="A",
                bootstrap_servers= ['localhost:9092'],
                # enable_auto_commit = True,
                auto_commit_interval_ms = 500,
                # auto_offset_reset="latest",
                consumer_timeout_ms=3000)

    def dump_message(self):
        for message in self.consumer:
            print(message)
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
            vi_thread = vector_insert_thread(thread_index, current_timestamp)
            vi_thread.start()
            thread_index += 1

            print("vi thread start")
            time.sleep(self.interval)

if __name__ == "__main__":
    message_list = []
    word_consumer = vector_insert_consumer_thread(0)
    word_consumer.start()
    print("Vector Consumer Begin")

    timer = thread_timer(1, None, 5)
    timer.start()
    print("Timer Begin")

    print("END")

