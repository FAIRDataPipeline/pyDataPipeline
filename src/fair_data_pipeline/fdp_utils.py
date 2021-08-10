import urllib
import requests
import json

def get_entry(endpoint, query):

    url = (
        'http://localhost:8000/api/' + \
        endpoint + \
        '?' + query
    )

    response = requests.get(url)
    assert response.status_code == 200

    return response.json()['results']

def extract_id(url):

    parse = urllib.parse.urlsplit(url).path
    extract = list(filter(None, parse.split('/')))[-1]

    return extract

def post_entry(token, endpoint, data):

    headers = {
    'Authorization': 'token ' + token,
    'Content-type': 'application/json'
    }

    url = (
        'http://localhost:8000/api/' + \
        endpoint +'/'
    )

    response = requests.post(url, data, headers=headers)
    assert response.status_code == 201

    return response
