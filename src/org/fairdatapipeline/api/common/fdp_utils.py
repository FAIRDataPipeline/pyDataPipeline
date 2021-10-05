import os
import urllib
import requests
import json
from datetime import datetime
import hashlib
import random
import yaml

def get_entry(
    url: str,
    endpoint: str,
    query: dict,
    token: str = None,
)-> list:

    headers = {}
    
    if token is not None:
        headers = {
        'Authorization': 'token ' + token
        }

    # Remove api address from query
    for key in query:
        if type(query[key]) == str:
            if url in query[key]:
                query[key] = extract_id(query[key])
        elif type(query[key]) == dict:
            for _key in query[key]:
                if url in query[key][_key]:
                    query[key][_key] = extract_id(query[key][_key])
        elif type(query[key]) == list:
            for i in range(len(query[key])):
                if url in query[key][i]:
                    query[key][i] = extract_id(query[key][i])


    if url[-1] != "/":
        url+="/"
    url += endpoint + '/?'
    _query =  [f"{k}={v}" for k, v in query.items()]
    url += "&".join(_query)
    response = requests.get(url, headers=headers)
    if (response.status_code != 200):
        raise ValueError("Server responded with: " + str(response.status_code) + " Query = " + url)

    return response.json()['results']

def get_entity(
    url: str,
    endpoint: str,
    id: int,
    token: str = None,
)-> list:

    headers = {}
    
    if token is not None:
        headers = {
        'Authorization': 'token ' + token
        }
    
    if url[-1] != "/":
        url+="/"
    url += endpoint + '/' + id
    response = requests.get(url, headers=headers)
    if (response.status_code != 200):
        raise ValueError("Server responded with: " + str(response.status_code) + " Query = " + url)

    return response.json()

def extract_id(url: str)-> str:

    parse = urllib.parse.urlsplit(url).path
    extract = list(filter(None, parse.split('/')))[-1]

    return extract

def post_entry(
    url: str,
    endpoint: str,
    data: dict,
    token: str,
)-> dict:

    headers = {
    'Authorization': 'token ' + token,
    'Content-type': 'application/json'
    }

    if url[-1] != "/":
        url+="/"
    _url = url + endpoint + '/'
    _data = json.dumps(data)

    response = requests.post(_url, _data, headers=headers)

    if (response.status_code == 409):
        return get_entry(url, endpoint, data)[0]

    if (response.status_code != 201):
        raise ValueError("Server responded with: " + str(response.status_code))

    return response.json()

def patch_entry(
    url: str,
    data: dict,
    token: str
)-> dict:

    headers = {
    'Authorization': 'token ' + token,
    'Content-type': 'application/json'
    }

    data = json.dumps(data)

    response = requests.patch(url, data, headers=headers)
    if (response.status_code != 200):
        raise ValueError("Server responded with: " + str(response.status_code))

    return response.json()

def random_hash()-> str:

    seed = datetime.now().timestamp() * random.uniform(1, 1000000)
    seed = str(seed).encode('utf-8')
    hashed = hashlib.sha1(seed)

    return hashed.hexdigest()

def get_file_hash(path: str)-> str:

    with open(path, 'rb') as data:
        data = data.read()
    #data = data.encode('utf-8')
    hashed = hashlib.sha1(data)

    return hashed.hexdigest()

def read_token(token_path: str):
    with open(token_path) as token:
        token = token.readline().strip()
    return token

def get_token(token_path: str):
    return read_token(token_path)

def is_file(filename: str):
    return os.path.isfile(filename)

def is_yaml(filename: str):
    try:
        with open(filename, 'r') as data:
            yaml.safe_load(data)
    except: return False
    return True

def is_valid_yaml(filename: str):
    return is_file(filename) & is_yaml(filename)
