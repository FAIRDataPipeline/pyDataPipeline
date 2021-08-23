import urllib
import requests
import json
from datetime import datetime
import hashlib
import random

def get_entry(
    url: str,
    endpoint: str,
    query: dict
)-> list:

    url = url + endpoint + '/'

    response = requests.get(url, query)
    assert response.status_code == 200

    return response.json()['results']

def extract_id(url: str)-> str:

    parse = urllib.parse.urlsplit(url).path
    extract = list(filter(None, parse.split('/')))[-1]

    return extract

def post_entry(
    token: str,
    url: str,
    endpoint: str,
    data: dict
)-> dict:

    headers = {
    'Authorization': 'token ' + token,
    'Content-type': 'application/json'
    }

    url = url + endpoint + '/'
    data = json.dumps(data)

    response = requests.post(url, data, headers=headers)
    assert response.status_code == 201

    return response.json()

def patch_entry(
    token: str,
    url: str,
    data: dict
)-> dict:

    headers = {
    'Authorization': 'token ' + token,
    'Content-type': 'application/json'
    }

    data = json.dumps(data)

    response = requests.patch(url, data, headers=headers)
    assert response.status_code == 200

    return response.json()

def random_hash()-> str:

    seed = datetime.now().timestamp() * random.uniform(1, 1000000)
    seed = str(seed).encode('utf-8')
    hashed = hashlib.sha1(seed)

    return hashed.hexdigest()

def get_file_hash(path: str)-> str:

    with open(path, 'r') as data:
        data = data.read()
    data = data.encode('utf-8')
    hashed = hashlib.sha1(data)

    return hashed.hexdigest()
