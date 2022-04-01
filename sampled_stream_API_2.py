import requests
import os
import json

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>ß'
bearer_token = os.environ.get("BEARER_TOKEN")



def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream?expansions=author_id"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r

def save_json_resp(json_str, f):
    f.write((json.dumps(json_str, indent=4, sort_keys=True))+'\n')
    return

def connect_to_endpoint(url, f, num_to_get=None, debug=True):
    response = requests.request("GET", url, auth=bearer_oauth, stream=True)
    print(response.status_code)
    msg_cnt = 0
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            if debug:
                print(json.dumps(json_response, indent=4, sort_keys=True))
            save_json_resp(json_response, f)
            msg_cnt += 1
            if num_to_get != None and msg_cnt >= num_to_get:
                break
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )


def main():
    url = create_url()
    timeout = 0
    f = open('tmp_data_folder/sampled_data_save_t1.txt', 'w')
    while True:
        connect_to_endpoint(url, f)
        timeout += 1
        break


if __name__ == "__main__":
    main()