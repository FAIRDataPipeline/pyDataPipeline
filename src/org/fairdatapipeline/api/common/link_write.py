import datetime
import os
import yaml
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils

def link_write(handle, data_product: str)-> str:
    """Reads write information in config file, updates handle with relevant
    metadata and returns path to write data product to.

    Args:
        data_product: Specified name of data product in config.

    Returns:
        path: Path to write data product to.

    """

    # Get metadata from handle
    run_metadata = handle['yaml']['run_metadata']
    datastore = run_metadata['write_data_store']

    index = 0

    # If multiple write blocks exist, find corresponding index for given DP
    for i in enumerate(handle['yaml']['write']):
        if i[1]['data_product'] == data_product:
            index = i[0]

    # Get metadata from config
    write = handle['yaml']['write'][index]
    write_data_product = write['data_product']
    write_version = write['use']['version']
    file_type = write['file_type']
    description = write['description']
    write_namespace = run_metadata['default_output_namespace']
    write_public = run_metadata['public']

    # Create filename for path
    filename = "dat-" + fdp_utils.random_hash() + "." + file_type

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
    if 'output' in handle.keys():
        handle['output'].append(output)
    else:
        handle['output'] = [output]

    return path