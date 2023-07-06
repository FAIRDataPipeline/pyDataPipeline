import logging
import os

from data_pipeline_api import fdp_utils


def link_write(handle: dict, data_product: str) -> str:
    """Reads write information in config file, updates handle with relevant
    metadata and returns path to write data product to.

    Args:
        |   data_product: Specified name of data product in config.

    Returns:
        |   path: Path to write data product to.
    """

    # Get metadata from handle
    run_metadata = handle["yaml"]["run_metadata"]
    datastore = run_metadata["write_data_store"]

    index = 0

    if "write" not in handle["yaml"].keys():
        raise ValueError(
            "Error: Write has not been specified in the given config file"
        )

    # If multiple write blocks exist, find corresponding index for given DP
    for i in enumerate(handle["yaml"]["write"]):
        if i[1]["data_product"] == data_product:
            index = i[0]

    # Get metadata from config
    write = handle["yaml"]["write"][index]
    write_data_product = write["data_product"]
    write_version = write["use"]["version"]
    file_type = write["file_type"]
    description = write["description"]
    write_namespace = run_metadata["default_output_namespace"]
    write_public = run_metadata["public"]

    # Create filename for path
    filename = "dat-" + fdp_utils.random_hash() + "." + file_type

    # Get path
    path = os.path.join(
        datastore, write_namespace, write_data_product, filename
    ).replace("\\", "/")

    # Create directory structure if it doesn't exist
    directory = os.path.dirname(path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    # Create metadata dict
    output_dict = {
        "data_product": data_product,
        "use_data_product": write_data_product,
        "use_component": None,
        "use_version": write_version,
        "use_namespace": write_namespace,
        "path": path,
        "data_product_description": description,
        "component_description": None,
        "public": write_public,
    }

    # If output exists in handle, append new metadata, otherwise create dict
    if "output" in handle:
        key = "output_" + str(len(handle["output"]))
        handle["output"][key] = output_dict
    else:
        handle["output"] = {}
        handle["output"]["output_0"] = output_dict

    return path


def link_read(handle: dict, data_product: str) -> str:
    """Reads 'read' information in config file, updates handle with relevant
    metadata and returns path to write data product to.

    Args:
        |   data_product: Specified name of data product in config.

    Returns:
        |   path: Path to write data product to.
    """

    # If data product is already in handle, return path
    if "input" in handle:
        for index in handle["input"].keys():
            if handle["input"][index]["data_product"] == data_product:
                return handle["input"][index]["path"]
    if "read" not in handle["yaml"].keys():
        raise ValueError(
            "Error: Read has not been specified in the given config file"
        )

    # Check if data product is in config yaml
    read_list = [
        i[1]["data_product"] for i in enumerate(handle["yaml"]["read"])
    ]

    if data_product not in read_list:
        logging.info("Read information for data product not in config")

    index = 0
    # Get index for given data product
    for i in enumerate(handle["yaml"]["read"]):
        if i[1]["data_product"] == data_product:
            index = i[0]

    # Get read info from config
    read = handle["yaml"]["read"][index]

    use = read["use"] if "use" in read else None

    registry_url = handle["yaml"]["run_metadata"]["local_data_registry_url"]
    namespace = handle["yaml"]["run_metadata"]["default_input_namespace"]
    api_version = handle["yaml"]["run_metadata"]["api_version"]

    if "namespace" in use:
        if use["namespace"]:
            namespace = use["namespace"]

    # Get namespace url and extract id
    namespace_url = fdp_utils.get_entry(
        url=registry_url,
        endpoint="namespace",
        query={"name": namespace},
        api_version=api_version,
    )[0]["url"]

    namespace_id = fdp_utils.extract_id(namespace_url)

    if "data_product" in use:
        if use["data_product"]:
            data_product = use["data_product"]

    version = use["version"] if "version" in use else "0.0.1"

    # Get data_product metadata and extract object id
    data_product_response = fdp_utils.get_entry(
        url=registry_url,
        endpoint="data_product",
        query={
            "name": data_product,
            "version": version,
            "namespace": namespace_id,
        },
        api_version=api_version,
    )

    object_response = fdp_utils.get_entity(
        url=registry_url,
        endpoint="object",
        id=int(fdp_utils.extract_id(data_product_response[0]["object"])),
    )

    object_id = fdp_utils.extract_id(object_response["url"])

    # Get component url and storage metadata
    component_url = fdp_utils.get_entry(
        url=registry_url,
        endpoint="object_component",
        query={"object": object_id},
        api_version=api_version,
    )[0]["url"]

    storage_location_response = fdp_utils.get_entity(
        url=registry_url,
        endpoint="storage_location",
        id=int(fdp_utils.extract_id(object_response["storage_location"])),
        api_version=api_version,
    )

    storage_root = fdp_utils.get_entity(
        url=registry_url,
        endpoint="storage_root",
        id=int(
            fdp_utils.extract_id(storage_location_response["storage_root"])
        ),
        api_version=api_version,
    )["root"]
    tmp_sl = storage_location_response["path"]
    # remove leading character from path if it is eithe / or \
    if ("\\" in tmp_sl[0]) or "/" in tmp_sl[0]:
        tmp_sl = tmp_sl[1:]

    # remove file:// from storage root
    storage_root = fdp_utils.remove_local_from_root(storage_root)

    # Get path of data product
    path = os.path.normpath(os.path.join(storage_root, tmp_sl))
    component = use["component"] if "component" in use else None

    # Write to handle and return path
    input_dict = {
        "data_product": data_product,
        "use_data_product": data_product,
        "use_component": component,
        "use_version": version,
        "use_namespace": namespace,
        "path": path,
        "component_url": component_url,
    }

    if "input" in handle:
        index = "input_" + str(len(handle["input"]))
        handle["input"][index] = input_dict
    else:
        handle["input"] = {}
        handle["input"]["input_0"] = input_dict

    return path
