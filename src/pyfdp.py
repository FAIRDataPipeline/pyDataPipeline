import yaml
import os
import requests
import hashlib
import urllib
import datetime

class PyFDP:
    def initialise(config, script):
    # read config file
        with open(config, 'r') as data:
            config_yaml = yaml.safe_load(data)
        run_metadata = config_yaml['run_metadata']

        # get config url

        config_storage_loc = get_entry('storage_location','path=' + config)[0]['url']
        config_storage_id = extract_id(config_storage_loc)
        config_object_url = get_entry('object', 'storage_location=' + config_storage_id)[0]['url']

        # get script url

        script_storage_loc = get_entry('storage_location','path=' + script)[0]['url']
        script_storage_id = extract_id(script_storage_loc)
        script_object_url = get_entry('object', 'storage_location=' + script_storage_id)[0]['url']

        # record run in data registry
        run_data = {
            'run_date': datetime.datetime.now().strftime('%x %H:%M'),
            'description': run_metadata['description'],
            'model_config_url': config_object_url,
            'submission_script_url': script_object_url,
            'input_urls': [],
            'output_urls': []
        }

        # return handle

        handle = {
            'yaml': config_yaml,
            'config_object': config_object_url,
            'script_object':script_object_url
                 }
        return handle
