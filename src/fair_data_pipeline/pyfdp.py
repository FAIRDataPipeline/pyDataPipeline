import urllib
import datetime
import yaml
import requests
import json
import fair_data_pipeline.fdp_utils as utils
import os

class PyFDP():

    token = None
    handle = None

    def initialise(self, config, script):

        if self.token is None:
            raise ValueError(
                'Registry token needs to be set before initialising'
            )

        # read config file and extract run metadata

        with open(config, 'r') as data:
            config_yaml = yaml.safe_load(data)
        run_metadata = config_yaml['run_metadata']
        registry_url = run_metadata['local_data_registry_url']
        filename = os.path.basename(config)

        print(f"Reading {filename} from data store")

        # record config.yaml location in data registry

        datastore_root = {'root': run_metadata['write_data_store']}

        config_storageroot_response = utils.post_entry(
            token = self.token,
            url = registry_url,
            endpoint = 'storage_root',
            data = datastore_root
        )

        config_storageroot_url = config_storageroot_response['url']
        config_storageroot_id = utils.extract_id(config_storageroot_url)
        config_hash = utils.get_file_hash(config)

        config_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'storage_location',
            query = {
                'hash': config_hash,
                'public': True,
                'storage_root': config_storageroot_id
            }
        )

        if config_exists:
            assert len(config_exists) == 1
            config_location_url = config_exists[0]['url']

        else:
            config_storage_data = {
                'path': config,
                'hash': config_hash,
                'public': True,
                'storage_root': config_storageroot_url
            }

            config_location_response = utils.post_entry(
                token = self.token,
                url = registry_url,
                endpoint = 'storage_location',
                data = config_storage_data
            )

            config_location_url = config_location_response['url']

        config_filetype_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'file_type',
            query = {
                'extension': 'yaml'
            }
        )

        if config_filetype_exists:
            config_filetype_url = config_filetype_exists['url']

        else:
            config_filetype_response = utils.post_entry(
                token = self.token,
                url = registry_url,
                endpoint = 'file_type',
                data = {
                    'name': 'yaml',
                    'extension': 'yaml'
                }
            )
            config_filetype_url = config_filetype_response['url']

        user_url = utils.get_entry(
            url = registry_url,
            endpoint = 'users',
            query = {
                'username': 'admin'
            }
        )[0]['url']

        user_id = utils.extract_id(user_url)

        author_url = utils.get_entry(
            url = registry_url,
            endpoint = 'user_author',
            query = {
                'user': user_id
            }
        )[0]['author']

        config_object_url = utils.post_entry(
            token = self.token,
            url = registry_url,
            endpoint = 'object',
            data = {
                'description': 'Working config.yaml location in datastore',
                'storage_location': config_location_url,
                'authors': [author_url],
                'file_type': config_filetype_url
            }
        )

        print(f'Writing {filename} to local registry')

        script_storageroot_url = config_storageroot_url
        script_storageroot_id = config_storageroot_id
        script_hash = utils.get_file_hash(script)

        script_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'storage_location',
            query = {
                'hash': script_hash,
                'public': True,
                'storage_root': script_storageroot_id
            }
        )

        if script_exists:
            assert len(script_exists) == 1
            script_location_url = script_exists[0]['url']

        else:
            script_storage_data = {
                'path': script,
                'hash': script_hash,
                'public': True,
                'storage_root': script_storageroot_url
            }

            script_location_response = utils.post_entry(
                token = self.token,
                url = registry_url,
                endpoint = 'storage_location',
                data = script_storage_data
            )

            script_location_url = script_location_response['url']

        script_filetype_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'file_type',
            query = {
                'extension': 'py'
            }
        )

        if script_filetype_exists:
            script_filetype_url = script_filetype_exists['url']

        else:
            script_filetype_response = utils.post_entry(
                token = self.token,
                url = registry_url,
                endpoint = 'file_type',
                data = {
                    'name': 'py',
                    'extension': 'py'
                }
            )

            script_filetype_url = script_filetype_response['url']

        script_object_url = utils.post_entry(
            token = self.token,
            url = registry_url,
            endpoint = 'object',
            data = {
                'description': 'Working script location in datastore',
                'storage_location': script_location_url,
                'authors': [author_url],
                'file_type': script_filetype_url
            }
        )

        print(f"Writing {os.path.basename(script)} to local registry")
        # record run in data registry

        run_data = {
            'run_date': datetime.datetime.now().strftime('%Y-%m-%dT%XZ'),
            'description': run_metadata['description'],
            'model_config': config_object_url,
            'submission_script': script_object_url,
            'input_urls': [],
            'output_urls': []
        }

        code_run = utils.post_entry(
            self.token,
            'code_run',
            json.dumps(run_data)
        ).json()

        # return handle

        self.handle = {
            'yaml': config_yaml,
            'config_object': config_object_url,
            'script_object': script_object_url,
            'code_run': code_run['url']
        }

    def link_write(self, data_product):
        # TODO Write resolve_write functionality for multiple writes

        run_metadata = self.handle['yaml']['run_metadata']
        datastore = run_metadata['write_data_store']
        write = self.handle['yaml']['write'][0]
        write_data_product = write['data_product']
        write_version = write['use']['version']
        file_type = write['file_type']
        description = write['description']
        write_namespace = run_metadata['default_output_namespace']
        write_public = run_metadata['public']

        filename = "dat-" + utils.random_hash() + "." + file_type

        path = os.path.join(
            datastore,
            write_namespace,
            write_data_product,
            filename
        ).replace('\\', '/')

        directory = os.path.dirname(path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        output = {
            'data_product': data_product,
            'use_data_product': write_data_product,
            'use_component': None,
            'use_version': write_version,
            'use_namespace': write_namespace,
            'path': path,
            'data_product_description': description,
            'component_discription': None,
            'public': write_public
        }

        self.handle['output'] = output

        return path

    def finalise(self):

        datastore = self.handle['yaml']['run_metadata']['write_data_store']
        root_data = {
            'root': datastore,
            'local': True
        }

        utils.post_entry(
            self.token,
            'storage_root',
            json.dumps(root_data)
        )
