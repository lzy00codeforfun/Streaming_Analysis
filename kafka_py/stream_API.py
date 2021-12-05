from kafka import KafkaProducer
import json
import math
import os
import requests
import time

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>ß'
bearer_token = os.environ.get("BEARER_TOKEN")
bootstrap_server = "localhost:9092"
topic = "Twitter_streaming"
# topic = "debug"
print(bearer_token)

class TwitterStream():
    def create_url(self):
        return "https://api.twitter.com/2/tweets/sample/stream?expansions=author_id"

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {bearer_token}"
        r.headers["User-Agent"] = "v2SampledStreamPython"
        return r

    def save_json_resp(self, json_str, f):
        f.write((json.dumps(json_str, indent=4, sort_keys=True))+'\n')
        return

    def connect_to_endpoint(self, f, kafka_producer=None, num_to_get=None, debug=True):
        url = self.create_url()
        response = requests.request("GET", url, auth=self.bearer_oauth, stream=True)
        print(response.status_code)
        msg_cnt = 0
        msg_all = 0
        time_log = TimeLog()

        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                if debug:
                    pass
                    # print(json.dumps(json_response, indent=4, sort_keys=True))
                if kafka_producer:
                    kafka_producer.send(topic, json_response)
                self.save_json_resp(json_response, f)
                msg_cnt += 1
                if num_to_get != None and msg_cnt >= num_to_get:
                    msg_all += msg_cnt
                    print("Current Batch:{}".format(msg_cnt))
                    print("From Start:{:.2f} Seconds".format(time_log.from_start()))
                    print("Since Last Time: {:.2f} Seconds".format(time_log.from_last()))
                    print("Current Twitter API Speed: {} Tweets/Second".format(
                            math.floor(msg_cnt / time_log.from_last())))
                    print("Average Twitter API Speed: {} Tweets/Second".format(
                            math.floor(msg_all / time_log.from_start())))
                    time_log.lap()
                    print("___________________Next Batch___________________")
                    kafka_producer.flush()
                    msg_cnt = 0
                    # time.sleep(5)
                    # break
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )

class TimeLog():
    def __init__(self):
        self.start_time = time.time()
        self.last_time = time.time()

    def from_start(self):
        current_time = time.time()
        # return diff time, unit seconds
        return current_time - self.start_time

    def from_last(self):
        current_time = time.time()
        res = current_time - self.last_time
        return res

    def lap(self):
        self.last_time = time.time()
        return



def main():
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            bootstrap_servers=bootstrap_server)

    stream_ins = TwitterStream()
    timeout = 0
    f = open('tmp_data_folder/sampled_data_save_t1.txt', 'w')
    while True:
        stream_ins.connect_to_endpoint(f, producer, 100)
        timeout += 1
        break


if __name__ == "__main__":
    main()
