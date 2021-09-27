import datetime
import os
import yaml
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils
    
def initialise(token: str, config: str, script: str):
    """Reads in token, config file and script, creates necessary registry items
    and creates new code run.

    Args:
        token: path to token file
        config: Path to config file
        script: Path to script file

    Returns:
        dict: a dictionary containing the following keys: 
            'yaml': config_yaml path,
            'fdp_config_dir': config dir path,
            'model_config': model config url,
            'submission_script': submission script object url,
            'code_repo': code repo object url,
            'code_run': coderun url,
            'code_run_uuid': coderun uuid,
            'author': author url
    """

    # Validate Yamls
    if not fdp_utils.is_valid_yaml(config):
        raise ValueError('Config is not a valid YAML file')
    if not fdp_utils.is_file(token):
        raise ValueError("Token is not a valid token")
    if not fdp_utils.is_file(script):
        raise ValueError("Script does not exist")
    
    # Read the token
    token = fdp_utils.read_token(token)

    # Read config file and extract run metadata
    with open(config, 'r') as data:
        config_yaml = yaml.safe_load(data)
    run_metadata = config_yaml['run_metadata']
    registry_url = run_metadata['local_data_registry_url']
    if registry_url[-1] != "/":
        registry_url += "/"
    filename = os.path.basename(config)

    print(f"Reading {filename} from data store")

    # Configure storage root for config

    # Check if storage root exists


    config_storageroot_response = fdp_utils.post_entry(
        token = token,
        url = registry_url,
        endpoint = 'storage_root',
        data = {
            'root': run_metadata['write_data_store'],
            'local': True
        }
    )

    config_storageroot_url = config_storageroot_response['url']
    config_storageroot_id = fdp_utils.extract_id(config_storageroot_url)
    config_hash = fdp_utils.get_file_hash(config)

    # Check if storage location for file exists

    config_exists = fdp_utils.get_entry(
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

        config_location_response = fdp_utils.post_entry(
            token = token,
            url = registry_url,
            endpoint = 'storage_location',
            data = config_storage_data
        )

        config_location_url = config_location_response['url']

    # Check if yaml file type exists in registry

    config_filetype_exists = fdp_utils.get_entry(
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
        config_filetype_response = fdp_utils.post_entry(
            token = token,
            url = registry_url,
            endpoint = 'file_type',
            data = {
                'name': 'yaml',
                'extension': 'yaml'
            }
        )
        config_filetype_url = config_filetype_response['url']

    # Get user for registry admin account

    user_url = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'users',
        query = {
            'username': 'admin'
        },
        token = token
    )[0]['url']

    user_id = fdp_utils.extract_id(user_url)

    # Get author(s)

    author_url = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'user_author',
        query = {
            'user': user_id
        }
    )[0]['author']

    # Create new object for config file

    config_object = fdp_utils.post_entry(
        token = token,
        url = registry_url,
        endpoint = 'object',
        data = {
            'description': 'Working config.yaml location in datastore',
            'storage_location': config_location_url,
            'authors': [author_url],
            'file_type': config_filetype_url
        }
    )

    config_object_url = config_object['url']

    print(f'Writing {filename} to local registry')

    # Check if script exists in storage_location

    script_storageroot_url = config_storageroot_url
    script_storageroot_id = config_storageroot_id
    script_hash = fdp_utils.get_file_hash(script)

    script_exists = fdp_utils.get_entry(
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

        script_location_response = fdp_utils.post_entry(
            token = token,
            url = registry_url,
            endpoint = 'storage_location',
            data = script_storage_data
        )

        script_location_url = script_location_response['url']

    # Check for script file type in registry, create if it doesn't exist

    script_filetype_exists = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'file_type',
        query = {
            'extension': 'py'
        }
    )

    if script_filetype_exists:
        script_filetype_url = script_filetype_exists[0]['url']

    else:
        script_filetype_response = fdp_utils.post_entry(
            token = token,
            url = registry_url,
            endpoint = 'file_type',
            data = {
                'name': 'py',
                'extension': 'py'
            }
        )

        script_filetype_url = script_filetype_response['url']

    # Create new registry object for script

    script_object = fdp_utils.post_entry(
        token = token,
        url = registry_url,
        endpoint = 'object',
        data = {
            'description': 'Working script location in datastore',
            'storage_location': script_location_url,
            'authors': [author_url],
            'file_type': script_filetype_url
        }
    )

    script_object_url = script_object['url']

    print(f"Writing {os.path.basename(script)} to local registry")

    # Create new remote storage root

    repo_storageroot_url = fdp_utils.post_entry(
        token = token,
        url = registry_url,
        endpoint = 'storage_root',
        data = {
            'root': 'https://github.com',
            'local': False
        }
    )['url']

    repo_storageroot_id = fdp_utils.extract_id(repo_storageroot_url)

    sha = run_metadata['latest_commit']
    repo_name = run_metadata['remote_repo']

    # Check if code repo entry exists for given hash

    coderepo_exists = fdp_utils.get_entry(
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
        coderepo_location_id = fdp_utils.extract_id(coderepo_location_url)

        obj_exists = fdp_utils.get_entry(
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
            coderepo_object_response = fdp_utils.post_entry(
                token = token,
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
        coderepo_location_response = fdp_utils.post_entry(
            token = token,
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

        coderepo_object_response = fdp_utils.post_entry(
            token = token,
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

    coderun_response = fdp_utils.post_entry(
        token = token,
        url = registry_url,
        endpoint = 'code_run',
        data ={
            'run_date': str(datetime.datetime.now()),
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

    return {
        'yaml': config_yaml,
        'fdp_config_dir': os.path.dirname(config),
        'model_config': config_object_url,
        'submission_script': script_object_url,
        'code_repo': coderepo_object_url,
        'code_run': coderun_url,
        'code_run_uuid': coderun_uuid,
        'author': author_url
    }