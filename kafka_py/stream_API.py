import requests
import os
import json
from kafka import KafkaProducer

import time

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>ß'
bearer_token = os.environ.get("BEARER_TOKEN")
bootstrap_server = "localhost:9092"

print(bearer_token)

class twitter_stream():
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
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                if debug:
                    print(json.dumps(json_response, indent=4, sort_keys=True))
                if kafka_producer:
                    kafka_producer.send("quickstart-events", json_response)
                self.save_json_resp(json_response, f)
                msg_cnt += 1
                if num_to_get != None and msg_cnt >= num_to_get:
                    kafka_producer.flush()
                    time.sleep(5)
                    # break
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )


def main():
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            bootstrap_servers=bootstrap_server)

    stream_ins = twitter_stream()
    timeout = 0
    f = open('tmp_data_folder/sampled_data_save_t1.txt', 'w')
    while True:
        stream_ins.connect_to_endpoint(f, producer, 10)
        timeout += 1
        break


if __name__ == "__main__":
    main()
