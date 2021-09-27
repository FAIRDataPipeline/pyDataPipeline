import datetime
import os
import yaml
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils

def link_read(handle: dict, data_product: str)-> str:
    """Reads 'read' information in config file, updates handle with relevant
    metadata and returns path to write data product to.

    Args:
        data_product: Specified name of data product in config.

    Returns:
        path: Path to write data product to.
    """

    # If data product is already in handle, return path
    if 'input' in handle.keys():
        for i in enumerate(handle['input']):
            if i[1]['data_product'] == data_product:
                return i[1]['path']

    # Check if data product is in config yaml
    read_list = [
        i[1]['data_product']
        for i in enumerate(handle['yaml']['read'])
    ]

    if data_product not in read_list:
        print("Read information for data product not in config")

    index = 0
    # Get index for given data product
    for i in enumerate(handle['yaml']['read']):
        if i[1]['data_product'] == data_product:
            index = i[0]

    # Get read info from config
    read = handle['yaml']['read'][index]

    use = None

    if 'use' in read:
        use = read['use']

    registry_url = handle['yaml']['run_metadata']['local_data_registry_url']
    namespace = handle['yaml']['run_metadata']['default_input_namespace']

    if 'namespace' in use:
        namespace = use['namespace']

    # Get namespace url and extract id
    namespace_url = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'namespace',
        query = {
            'name': namespace
        }
    )[0]['url']

    namespace_id = fdp_utils.extract_id(namespace_url)

    if 'data_product' in use:
        data_product = use['data_product']

    version = '0.0.1'
    if 'version' in use:
        version = use['version']

    # Get data_product metadata and extract object id
    data_product_response = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'data_product',
        query = {
            'name': data_product,
            'version': version,
            'namespace': namespace_id
        }
    )

    object_response = fdp_utils.get_entity(
        url = registry_url,
        endpoint = 'object',
        id = fdp_utils.extract_id(data_product_response[0]['object'])
    )

    object_id = fdp_utils.extract_id(object_response['url'])

    # Get component url and storage metadata
    component_url = fdp_utils.get_entry(
        url = registry_url,
        endpoint = 'object_component',
        query = {
            'object': object_id
        }
    )[0]['url']

    storage_location_response = fdp_utils.get_entity(
        url = registry_url,
        endpoint = 'storage_location',
        id = fdp_utils.extract_id(object_response['storage_location'])
    )

    storage_root = fdp_utils.get_entity(
        url = registry_url,
        endpoint = 'storage_root',
        id = fdp_utils.extract_id(storage_location_response['storage_root'])
    )['root']

    sl = storage_location_response['path']
    # remove leading character from path if it is eithe / or \
    if ("\\" in sl[0]) or "/" in sl[0]:
        sl = sl[1:]

    # remove file:// from storage root
    if "file://" in storage_root:
        storage_root = storage_root.replace("file://", "")

    # Get path of data product
    path = os.path.normpath(os.path.join(storage_root, sl))

    component = None
    if 'component' in use:
        component = use['component']

    # Write to handle and return path
    input_dict = {
        'data_product': data_product,
        'use_data_product': data_product,
        'use_component': component,
        'use_version': version,
        'use_namespace': namespace,
        'path': path,
        'component_url': component_url
    }

    if 'input' in handle.keys():
        handle['input'].append(input_dict)
    else:
        handle['input'] = [input_dict]

    return path
