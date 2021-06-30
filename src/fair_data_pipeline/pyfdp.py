import urllib
import datetime
import yaml
import requests
import json

class PyFDP():

    token = None

    def initialise(self, config, script):

        if self.token == None:
            raise ValueError(
                'Registry token needs to be set before initialising'
            )

        # read config file
        with open(config, 'r') as data:
            config_yaml = yaml.safe_load(data)
        run_metadata = config_yaml['run_metadata']

        # get config url

        config_storage_loc = self.get_entry(
            'storage_location',
            'path=' + config
            )[0]['url']
        config_storage_id = self.extract_id(config_storage_loc)
        config_object_url = self.get_entry(
            'object',
            'storage_location=' + config_storage_id
            )[0]['url']

        # get script url

        script_storage_loc = self.get_entry(
            'storage_location',
            'path=' + script
            )[0]['url']
        script_storage_id = self.extract_id(script_storage_loc)
        script_object_url = self.get_entry(
            'object',
            'storage_location=' + \
            script_storage_id)[0]['url']

        # record run in data registry
        run_data = {
            'run_date': datetime.datetime.now().strftime('%Y-%m-%dT%XZ'),
            'description': run_metadata['description'],
            'model_config': config_object_url,
            'submission_script': script_object_url,
            'input_urls': [],
            'output_urls': []
        }

        code_run = self.post_entry('code_run', json.dumps(run_data)).json()

        # return handle

        handle = {
            'yaml': config_yaml,
            'config_object': config_object_url,
            'script_object': script_object_url,
            'code_run': code_run['url']
                 }
        return handle

    def get_entry(self,endpoint, query):

        url = (
            'http://localhost:8000/api/' + \
            endpoint + \
            '?' + query
        )

        response = requests.get(url)
        assert response.status_code == 200

        return response.json()['results']

    def extract_id(self, url):
        parse = urllib.parse.urlsplit(url).path
        extract = list(filter(None, parse.split('/')))[-1]

        return extract

    def post_entry(self, endpoint, data):
        headers = {
        'Authorization': 'token ' + self.token,
        'Content-type': 'application/json'
        }

        url = (
            'http://localhost:8000/api/' + \
            endpoint +'/'
        )

        response = requests.post(url, data, headers=headers)
        assert response.status_code == 201

        return response
