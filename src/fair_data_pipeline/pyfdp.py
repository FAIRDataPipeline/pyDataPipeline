"""
PyFDP, a python implementation of the SCRC fair data pipeline
"""

import datetime
import os
import re
import json
import yaml
import fair_data_pipeline.fdp_utils as utils

class PyFDP():
    """Python fair data pipeline

    Attributes:
        token (str): Registry access token
        handle (dict): Pipeline handle, used to record run data and passed to
            finalise
    """

    token = None
    handle = None

    def initialise(self, config: str, script: str):
        """Reads in config file and script, creates necessary registry items
        and creates new code run.

        Args:
            config: Path to config file
            script: Path to script file
        """

        # Token must be set by user for initialising, if installed from WSL,
        # token is not in Windows root

        if self.token is None:
            raise ValueError(
                'Registry token needs to be set before initialising'
            )

        # Read config file and extract run metadata

        with open(config, 'r') as data:
            config_yaml = yaml.safe_load(data)
        run_metadata = config_yaml['run_metadata']
        registry_url = run_metadata['local_data_registry_url']
        filename = os.path.basename(config)

        print(f"Reading {filename} from data store")

        # Configure storage root for config

        config_storageroot_response = utils.post_entry(
            token = self.token,
            url = registry_url,
            endpoint = 'storage_root',
            data = {
                'root': run_metadata['write_data_store'],
                'local': True
            }
        )

        config_storageroot_url = config_storageroot_response['url']
        config_storageroot_id = utils.extract_id(config_storageroot_url)
        config_hash = utils.get_file_hash(config)

        # Check if storage location for file exists

        config_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'storage_location',
            query = {
                'hash': config_hash,
                'public': True,
                'storage_root': config_storageroot_id
            }
        )

        # If entry exists, extract url, otherwise create entry

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

        # Check if yaml file type exists in registry

        config_filetype_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'file_type',
            query = {
                'extension': 'yaml'
            }
        )

        # If file type doesn't exist, create entry

        if config_filetype_exists:
            config_filetype_url = config_filetype_exists[0]['url']

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

        # Get user for registry admin account

        user_url = utils.get_entry(
            url = registry_url,
            endpoint = 'users',
            query = {
                'username': 'admin'
            }
        )[0]['url']

        user_id = utils.extract_id(user_url)

        # Get author(s)

        author_url = utils.get_entry(
            url = registry_url,
            endpoint = 'user_author',
            query = {
                'user': user_id
            }
        )[0]['author']

        # Create new object for config file

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

        # Check if script exists in storage_location

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

        # If entry doesn't exist, create it

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

        # Check for script file type in registry, create if it doesn't exist

        script_filetype_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'file_type',
            query = {
                'extension': 'py'
            }
        )

        if script_filetype_exists:
            script_filetype_url = script_filetype_exists[0]['url']

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

        # Create new registry object for script

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

        # Create new remote storage root

        repo_storageroot_url = utils.post_entry(
            token = self.token,
            url = registry_url,
            endpoint = 'storage_root',
            data = {
                'root': 'https://github.com',
                'local': False
            }
        )['url']

        repo_storageroot_id = utils.extract_id(repo_storageroot_url)

        sha = run_metadata['latest_commit']
        repo_name = run_metadata['remote_repo']

        # Check if code repo entry exists for given hash

        coderepo_exists = utils.get_entry(
            url = registry_url,
            endpoint = 'storage_location',
            query = {
                'hash': sha,
                'public': True,
                'storage_root': repo_storageroot_id
            }
        )

        # If repo exists, check if object exists for the repo

        if coderepo_exists:
            coderepo_location_url = coderepo_exists[0]['url']
            coderepo_location_id = utils.extract_id(coderepo_location_url)

            obj_exists = utils.get_entry(
                url = registry_url,
                endpoint = 'object',
                query = {
                    'storage_location': coderepo_location_id
                }
            )

            # If repo object doesn't exist, create it

            if obj_exists:
                coderepo_object_url = obj_exists[0]['url']
            else:
                coderepo_object_response = utils.post_entry(
                    token = self.token,
                    url = registry_url,
                    endpoint = 'object',
                    data = {
                        'description': 'Analysis / processing script location',
                        'storage_location': coderepo_location_url,
                        'authors': [author_url]
                    }
                )

                coderepo_object_url = coderepo_object_response['url']

        # If code repo doesn't exist, create repo and object

        else:
            coderepo_location_response = utils.post_entry(
                token = self.token,
                url = registry_url,
                endpoint = 'storage_location',
                data = {
                    'path': repo_name,
                    'hash': sha,
                    'public': True,
                    'storage_root': repo_storageroot_url
                }
            )

            coderepo_location_url = coderepo_location_response['url']

            coderepo_object_response = utils.post_entry(
                token = self.token,
                url = registry_url,
                endpoint = 'object',
                data = {
                    'description': 'Analysis / processing script location',
                    'storage_location': coderepo_location_url,
                    'authors': [author_url]
                }
            )

            coderepo_object_url = coderepo_object_response['url']

        print(f"Writing {repo_name} to local registry")

        # Register new code run

        coderun_response = utils.post_entry(
            token = self.token,
            url = registry_url,
            endpoint = 'code_run',
            data ={
                'run_date': datetime.datetime.now().strftime('%Y-%m-%dT%XZ'),
                'description': run_metadata['description'],
                'code_repo': coderepo_object_url,
                'model_config': config_object_url,
                'submission_script': script_object_url,
                'input_urls': [],
                'output_urls': []
            }
        )

        coderun_url = coderun_response['url']
        coderun_uuid = coderun_response['uuid']

        print("Writing new code_run to local registry")

        # Write code run and object info to handle

        self.handle = {
            'yaml': config_yaml,
            'fdp_config_dir': os.path.dirname(config),
            'model_config': config_object_url,
            'submission_script': script_object_url,
            'code_repo': coderepo_object_url,
            'code_run': coderun_url,
            'code_run_uuid': coderun_uuid,
            'author': author_url
        }

    def link_write(self, data_product: str)-> str:
        """Reads write information in config file, updates handle with relevant
        metadata and returns path to write data product to.

        Args:
            data_product: Specified name of data product in config.

        Returns:
            path: Path to write data product to.

        """

        # Get metadata from handle
        run_metadata = self.handle['yaml']['run_metadata']
        datastore = run_metadata['write_data_store']

        # If multiple write blocks exist, find corresponding index for given DP
        for i in enumerate(self.handle['yaml']['write']):
            if i[1]['data_product'] == data_product:
                index = i[0]

        # Get metadata from config
        write = self.handle['yaml']['write'][index]
        write_data_product = write['data_product']
        write_version = write['use']['version']
        file_type = write['file_type']
        description = write['description']
        write_namespace = run_metadata['default_output_namespace']
        write_public = run_metadata['public']

        # Create filename for path
        filename = "dat-" + utils.random_hash() + "." + file_type

        # Get path
        path = os.path.join(
            datastore,
            write_namespace,
            write_data_product,
            filename
        ).replace('\\', '/')

        # Create directory structure if it doesn't exist
        directory = os.path.dirname(path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        # Create metadata dict
        output = {
            'data_product': data_product,
            'use_data_product': write_data_product,
            'use_component': None,
            'use_version': write_version,
            'use_namespace': write_namespace,
            'path': path,
            'data_product_description': description,
            'component_description': None,
            'public': write_public
        }

        # If output exists in handle, append new metadata, otherwise create dict
        if 'output' in self.handle.keys():
            self.handle['output'].append(output)
        else:
            self.handle['output'] = [output]

        return path

    def link_read(self, data_product: str)-> str:
        """Reads 'read' information in config file, updates handle with relevant
        metadata and returns path to write data product to.

        Args:
            data_product: Specified name of data product in config.

        Returns:
            path: Path to write data product to.

        """

        registry_url = self.handle['yaml']['run_metadata']['local_data_registry_url']
        namespace = self.handle['yaml']['run_metadata']['default_input_namespace']

        # If data product is already in handle, return path
        if 'input' in self.handle.keys():
            for i in enumerate(self.handle['input']):
                if i[1]['data_product'] == data_product:
                    return i[1]['path']

        # Check if data product is in config yaml
        read_list = [
            i[1]['data_product']
            for i in enumerate(self.handle['yaml']['read'])
        ]

        if data_product not in read_list:
            print("Read information for data product not in config")

        # Get index for given data product
        for i in enumerate(self.handle['yaml']['read']):
            if i[1]['data_product'] == data_product:
                index = i[0]

        # Get read info from config
        read = self.handle['yaml']['read'][index]

        # Get namespace url and extract id
        namespace_url = utils.get_entry(
            url = registry_url,
            endpoint = 'namespace',
            query = {
                'name': namespace
            }
        )[0]['url']

        namespace_id = utils.extract_id(namespace_url)

        # Get data_product metadata and extract object id
        data_product_response = utils.get_entry(
            url = registry_url,
            endpoint = 'data_product',
            query = {
                'name': data_product,
                'version': read['use']['version'],
                'namespace': namespace_id
            }
        )

        object_response = utils.get_entry(
            url = data_product_response[0]['object'],
            endpoint = '',
            query = {}
        )

        object_id = utils.extract_id(object_response[0]['url'])

        # Get component url and storage metadata
        component_url = utils.get_entry(
            url = registry_url,
            endpoint = 'object_component',
            query = {
                'object': object_id
            }
        )[0]['url']

        storage_location_response = utils.get_entry(
            url = object_response[0]['storage_location'],
            endpoint = '',
            query = {}
        )

        storage_root = utils.get_entry(
            url = storage_location_response[0]['storage_root'],
            endpoint = '',
            query = {}
        )[0]['root']

        # Get path of data product
        path = os.path.join(
            storage_root,
            storage_location_response[0]['path']
        ).replace('\\', '/')

        # Write to handle and return path
        input_dict = {
            'data_product': data_product,
            'use_data_product': data_product,
            'use_component': None,
            'use_version': read['use']['version'],
            'use_namespace': namespace,
            'path': path,
            'component_url': component_url
        }

        if 'input' in self.handle.keys():
            self.handle['input'].append(input_dict)
        else:
            self.handle['input'] = [input_dict]

        return path


    def finalise(self):

        registry_url = self.handle['yaml']['run_metadata']['local_data_registry_url']
        datastore = self.handle['yaml']['run_metadata']['write_data_store']

        datastore_root_url = utils.get_entry(
            url = registry_url,
            endpoint = 'storage_root',
            query = {
                'root': datastore
            }
        )[0]['url']

        datastore_root_id = utils.extract_id(datastore_root_url)

        if 'output' in self.handle:
            for output in self.handle['output']:

                write_namespace_url = utils.post_entry(
                    token = self.token,
                    url = registry_url,
                    endpoint = 'namespace',
                    data = {
                        'name': output['use_namespace']
                    }
                )['url']

                write_namespace_id = utils.extract_id(write_namespace_url)

                hash = utils.get_file_hash(output['path'])

                storage_exists = utils.get_entry(
                    url = registry_url,
                    endpoint = 'storage_location',
                    query = {
                        'hash': hash,
                        'public': output['public'],
                        'storage_root': datastore_root_url
                    }
                )

                if storage_exists:
                    storage_location_url = storage_exists[0]['url']

                    os.remove(output['path'])

                    directory = os.path.dirname(output['path'])

                    while True:
                        os.rmdir(directory)
                        directory = os.path.split(directory)[0]
                        if directory == datastore:
                            break

                    existing_path = storage_exists[0]['path']

                    existing_root = utils.get_entry(
                        url = storage_exists[0]['storage_root'],
                        endpoint = '',
                        query = {}
                    )

                    new_path = os.path.join(existing_root, existing_path)

                else:
                    tmp_filename = os.path.basename(output['path'])
                    extension = tmp_filename.split(sep='.')[-1]
                    new_filename = '.'.join([hash, extension])
                    new_path = os.path.join(
                        os.path.dirname(output['path']),
                        new_filename
                    ).replace('\\', '/')
                    os.rename(output['path'], new_path)
                    new_storage_location = re.sub(
                        rf"\b{datastore}\b",
                        "",
                        new_path)

                    storage_location_url = utils.post_entry(
                        token = self.token,
                        url = registry_url,
                        endpoint = 'storage_location',
                        data = {
                            'path': new_storage_location,
                            'hash': hash,
                            'public': output['public'],
                            'storage_root': datastore_root_url
                        }
                    )['url']

                file_type = os.path.basename(new_path).split('.')[-1]

                file_type_exists = utils.get_entry(
                    url = registry_url,
                    endpoint = 'file_type',
                    query = {
                        'extension': file_type
                    }
                )

                if file_type_exists:
                    file_type_url = file_type_exists[0]['url']
                else:
                    file_type_url = utils.post_entry(
                        token = self.token,
                        url = registry_url,
                        endpoint = 'file_type',
                        data = {
                            'name': file_type,
                            'extension': file_type
                        }
                    )['url']

                object_url = utils.post_entry(
                    token = self.token,
                    url = registry_url,
                    endpoint = 'object',
                    data = {
                        'description': output['data_product_description'],
                        'storage_location_url': storage_location_url,
                        'authors': [self.handle['author']],
                        'file_type': file_type_url
                    }
                )['url']

                component_url = utils.post_entry(
                    token = self.token,
                    url = registry_url,
                    endpoint = 'object_component',
                    data = {
                        'object': object_url,
                        'name': utils.random_hash()
                    }
                )['url']

                data_product_url = utils.post_entry(
                    token = self.token,
                    url = registry_url,
                    endpoint = 'data_product',
                    data = {
                        'name': output['use_data_product'],
                        'version': output['use_version'],
                        'object': object_url,
                        'namespace': write_namespace_url
                    }
                )['url']

                output['component_url'] = component_url
                output['data_product_url'] = data_product_url

                print(f"Writing {output['use_data_product']} to local registry")

        output_components = []
        input_components = []

        if 'output' in self.handle.keys():
            for output in self.handle['output']:
                output_components.append(output['component_url'])

        if 'input' in self.handle.keys():
            for input in self.handle['input']:
                input_components.append(input['component_url'])

        utils.patch_entry(
            token = self.token,
            url = self.handle['code_run'],
            data = {
                'inputs': input_components,
                'outputs': output_components
            }
        )

        coderuns_path = os.path.join(
            self.handle['fdp_config_dir'],
            'coderuns.txt'
        ).replace('\\', '/')

        with open(coderuns_path, 'a+') as coderun_file:
            coderun_file.seek(0)
            data = coderun_file.read(100)
            if len(data) > 0:
                coderun_file.write('\n')
            coderun_file.write(self.handle['code_run_uuid'])
