import datetime
import os
import yaml
import re
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils
import shutil

def finalise(token: str, handle: dict):
    """
    Renames files with their hash, updates data_product names and records
    metadata in the registry

    Args:
        |   token: registry token
        |   config: Path to config file
        |   script: Path to script file

    Returns:
        |   dict: a dictionary containing the following keys:
        |       'yaml': config_yaml path
        |       'fdp_config_dir': config dir path
        |       'model_config': model config url
        |       'submission_script': submission script object url
        |       'code_repo': code repo object url
        |       'code_run': coderun url
        |       'code_run_uuid': coderun uuid
        |       'author': author url
        |       'inputs':
        |           'data_product': data product
        |           'use_data_product': data product
        |           'use_component': component
        |           'use_version': version
        |           'use_namespace': namespace
        |           'path': path
        |           'component_url': component url
        |       'outputs':
        |           component_url: component url
        |           data_product_url: data product url
    """
    #token = fdp_utils.read_token(token)
    registry_url = handle['yaml']['run_metadata']['local_data_registry_url']
    datastore = handle['yaml']['run_metadata']['write_data_store']

    datastore_root = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'storage_root',
        query = {
            'root': datastore
        }
    )
       
    datastore_root_url = None

    # Check datastore is in registry
    if datastore_root:
        datastore_root_url = datastore_root[0]['url']
    else:
        datastore_root_url = fdp_utils.post_entry(
            token = token,
            url = registry_url,
            endpoint = 'storage_root',
            data = {
                'root': datastore,
                'local' : True
            }
        )['url']

    datastore_root_id = fdp_utils.extract_id(datastore_root_url)

    if 'output' in handle:
        for output in handle['output']:

            if bool(re.search('\\$\\{\\{RUN_ID\\}\\}', output['use_data_product'])):
                output['use_data_product'] = re.sub(
                    '\\$\\{\\{RUN_ID\\}\\}',
                    handle['code_run_uuid'],
                    output['use_data_product']
                )

            write_namespace = fdp_utils.get_entry(
                url = registry_url,
                endpoint = 'namespace',
                query = {
                    'name': output['use_namespace']
                })
            
            write_namespace_url = None
            
            if write_namespace:
                write_namespace_url = write_namespace[0]['url']
            else:
                write_namespace_url = fdp_utils.post_entry(
                   token = token,
                    url = registry_url,
                    endpoint = 'namespace',
                    data = {
                        'name': output['use_namespace']
                    }
                )['url']

            hash = fdp_utils.get_file_hash(output['path'])

            storage_exists = fdp_utils.get_entry(
                url = registry_url,
                endpoint = 'storage_location',
                query = {
                    'hash': hash,
                    'public': output['public'],
                    'storage_root': datastore_root_id
                }
            )

            storage_location_url = None

            if storage_exists:
                storage_location_url = storage_exists[0]['url']

                os.remove(output['path'])

                directory = os.path.dirname(output['path'])

                #print("\ndirectory: " + os.path.normpath(directory))
                #print("\nDatastore Path: " + os.path.normpath(datastore))

                i = 0
                while os.path.normpath(directory) != os.path.normpath(datastore):
                        try:
                            os.rmdir(directory)
                        except Exception:
                            print("\nIgnoring Directory: " + directory + " as it is not empty\n")
                            pass
                        directory = os.path.split(directory)[0]
                        i += 1
                        if i > 4:
                            break

                existing_path = storage_exists[0]['path']

                existing_root = fdp_utils.get_entity(
                    url = registry_url,
                    endpoint = 'storage_root',
                    id = fdp_utils.extract_id(storage_exists[0]['storage_root'])
                )['root']

                new_path = os.path.join(existing_root, existing_path)

            else:
                tmp_filename = os.path.basename(output['path'])
                extension = tmp_filename.split(sep='.')[-1]
                new_filename = '.'.join([hash, extension])
                data_product = output['data_product']
                namespace = output['use_namespace']
                new_path = os.path.join(
                    datastore,
                    namespace,
                    data_product,
                    new_filename
                ).replace('\\', '/')
                
                os.rename(output['path'], new_path)

                new_storage_location = os.path.join(
                    namespace,
                    data_product,
                    new_filename
                ).replace('\\', '/')

                storage_location_url = fdp_utils.post_entry(
                    token = token,
                    url = registry_url,
                    endpoint = 'storage_location',
                    data ={
                        'path': new_storage_location,
                        'hash': hash,
                        'public': output['public'],
                        'storage_root': datastore_root_url
                    }
                )['url']

            file_type = os.path.basename(new_path).split('.')[-1]

            file_type_exists = fdp_utils.get_entry(
                url = registry_url,
                endpoint = 'file_type',
                query = {
                    'extension': file_type
                }
            )

            if file_type_exists:
                file_type_url = file_type_exists[0]['url']
            else:
                file_type_url = fdp_utils.post_entry(
                    token = token,
                    url = registry_url,
                    endpoint = 'file_type',
                    data = {
                        'name': file_type,
                        'extension': file_type
                    }
                )['url']
            
            object_url = fdp_utils.post_entry(
                token = token,
                url = registry_url,
                endpoint = 'object',
                data = {
                    'description': output['data_product_description'],
                    'storage_location': storage_location_url,
                    'authors': [handle['author']],
                    'file_type': file_type_url
                }
            )['url']

            component_url = None

            if output['use_component']:
                component_url = fdp_utils.post_entry(
                    token = token,
                    url = registry_url,
                    endpoint = 'object_component',
                    data = {
                        'object': object_url,
                        'name': output['use_component']
                    }
                )['url']
            else:              
                component_url = fdp_utils.get_entry(
                    url= registry_url,
                    endpoint= 'object_component',
                    query= {
                        'object': fdp_utils.extract_id(object_url),
                    }
                )[0]['url']

            data_product_url = fdp_utils.post_entry(
                token = token,
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

    if 'output' in handle.keys():
        for output in handle['output']:
            output_components.append(output['component_url'])

    if 'input' in handle.keys():
        for input in handle['input']:
            input_components.append(input['component_url'])

    fdp_utils.patch_entry(
        token = token,
        url = handle['code_run'],
        data = {
            'inputs': input_components,
            'outputs': output_components
        }
    )

    coderuns_path = os.path.join(
        handle['fdp_config_dir'],
        'coderuns.txt'
    ).replace('\\', '/')

    with open(coderuns_path, 'a+') as coderun_file:
        coderun_file.seek(0)
        data = coderun_file.read(100)
        if len(data) > 0:
            coderun_file.write('\n')
        coderun_file.write(handle['code_run_uuid'])