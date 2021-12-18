# Fetch tweets from twitter api
# Fetch tweets from twitter api
import requests
import json
import os
import socket

# To set your enviornment variables in your terminal run the following line or set inscript variable
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = "AAAAAAAAAAAAAAAAAAAAADcCWQEAAAAAKNxThRGIKMXDOhR%2F2j9ERjblh90%3DkiXE4n4BnArDVN3hdGHKs3k5t4yvgkx4tVipVQIpRWm5PC7FlE"#os.environ.get("BEARER_TOKEN")

# you can comment out the next 3 lines if you want to run locally. Also remove line 98
s = socket.socket()  # Create a socket object
port = 5595  # Reserve a port for your service every new transfer wants a new port or you must wait.
s.connect(('localhost', port))

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print('Filtering rules>', json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    # print('Reset Rules',json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "voting rights", "tag": "usa"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print('Applied Filtering Rules>', json.dumps(response.json()))


def get_stream(set, limit):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    # print("Auth Status",response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    local=1
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            # Live tweets streaming
            data_out = json_response.get('data').get('text')
            print(data_out)
            local+=1
            if local > limit:
                break
            # sending data to port
            byt = data_out.encode()
            s.send(byt) #remove this if you want to run locally



def main(limit):
    """
    Function kicks off the live tweet streaming
    """
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set, limit=limit)


if __name__ == "__main__":
    # change limit to set the number of tweets to stream
    main(limit = 5)

    # for this to run the listener.py has to be running first as it emulates the spark client that is going to be listening



