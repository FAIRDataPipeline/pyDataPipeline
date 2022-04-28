import logging
import os
from typing import Any, Tuple

import netCDF4  # noqa: F401
import numpy as np

from data_pipeline_api import fdp_utils


def resolve_write(
    handle: dict, data_product: str, file_type: str = None
) -> Tuple[str, dict]:
    # If multiple write blocks exist, find corresponding index for given DP
    # Get metadata from handle
    run_metadata = handle["yaml"]["run_metadata"]
    datastore = run_metadata["write_data_store"]
    index = 0
    for i in enumerate(handle["yaml"]["write"]):
        if i[1]["data_product"] == data_product:
            index = i[0]

    # Get metadata from config
    write = handle["yaml"]["write"][index]
    write_data_product = write["data_product"]
    write_version = write["use"]["version"]
    description = write["description"]
    write_namespace = run_metadata["default_output_namespace"]
    write_public = run_metadata["public"]
    if write["file_type"]:
        filetype = write["file_type"]
    if file_type:
        filetype = file_type
    # Create filename for path

    filename = f"dat-{fdp_utils.random_hash()}.{filetype}"

    # Get path
    path = os.path.join(
        datastore, write_namespace, write_data_product, filename
    ).replace("\\", "/")

    # Create directory structure if it doesn't exist
    directory = os.path.dirname(path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    # Create metadata dict
    return path, {
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


def link_write(handle: dict, data_product: str) -> str:
    """Reads write information in config file, updates handle with relevant
    metadata and returns path to write data product to.

    Args:
        |   data_product: Specified name of data product in config.

    Returns:
        |   path: Path to write data product to.
    """

    if "write" not in handle["yaml"].keys():
        raise ValueError(
            "Error: Write has not been specified in the given config file"
        )

    path, output_dict = resolve_write(handle, data_product)

    # If output exists in handle, append new metadata, otherwise create dict
    if "output" in handle:
        key = "output_" + str(len(handle["output"]))
        handle["output"][key] = output_dict
    else:
        handle["output"] = {}
        handle["output"]["output_0"] = output_dict

    return path


def resolve_read(handle: dict, data_product: str) -> Tuple[str, dict]:
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
    return path, {
        "data_product": data_product,
        "use_data_product": data_product,
        "use_component": component,
        "use_version": version,
        "use_namespace": namespace,
        "path": path,
        "component_url": component_url,
    }


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

    path, input_dict = resolve_read(handle, data_product)

    if "input" in handle:
        index = "input_" + str(len(handle["input"]))
        handle["input"][index] = input_dict
    else:
        handle["input"] = {}
        handle["input"]["input_0"] = input_dict

    return path


def read_array(handle: dict, data_product: str, component: str) -> Any:
    """
    read_array  Function to read array type data from hdf5 file.

    Parameters
    ----------
    handle : _type_
        an object containing metadata required by the Data Pipeline API
    data_product : str
        a string specifying a data product
    component : str
        component a string specifying a data product component

    Returns
    -------
    Any
        Returns an array with title, units, values and units attributes, if available
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

    path, read_metadata = resolve_read(handle, data_product)
    # Get metadata ------------------------------------------------------------

    write_data_product = read_metadata["data_product"]  # noqa: F841
    write_version = read_metadata["use_version"]  # noqa: F841
    write_namespace = read_metadata["use_namespace"]  # noqa: F841
    # write_public = read_metadata["public"]  # noqa: F841
    # data_product_decription = read_metadata["description"]  # noqa: F841
    path = read_metadata["path"]  # noqa: F841

    if not os.path.exists(path):
        raise FileNotFoundError("File missing from data store")

    _ = netCDF4.Dataset("test.nc", "w", format="NETCDF4")

    return path
    # read netcdf file


# Extract data object


# Extract dimension names and make sure they're in the right order


# Attach dimension names to the object


# Attach remaining list elements as attributes


# Write to handle ---------------------------------------------------------

# If data product is already recorded in handle return index


def write_array(
    array: Any,
    handle: dict,
    data_product: str,
    component: str,
    description: str,
    dimension_names: list,
    dimension_values: list,
    dimension_units: list,
    # array: Any,
    # handle: dict,
    # data_product: str,
    # component: str='',
    # description: str='status',
    # dimension_names: list = ['ambulance exploded','ambulance out of gas', 'ambulance ok'],
    # # dimension_values: list,
    # dimension_units: ['n','n','n'],
) -> Any:
    """
    write_array  Function to populate netcdf file with array type data.

    Parameters
    ----------
    array : _type_
        an array containing the data
    handle : _type_
        an object containing metadata required by the data pipeline api
    data_product : str
        a string specifying  the name of the data product
    component : str
        string specifying a location within the netcdf file
    description : str
        string describing the data product component
    dimension_names : list
        where each element is a vector containing the labels associated with a particular dimension (e.g. element 1 corresponds to dimension 1, which corresponds to row names) and the name of each element describes the contents of each dimension (e.g. age classes).
    dimension_values : list
         (optional) a list of values corresponding to each dimension (e.g. list element 2 corresponds to columns)
    dimension_units : list
        (optional) a list of units corresponding to each dimension (e.g. list element 2 corresponds to columns)
    units : str
        (optional) a string specifying the units of the data as a whole

    Returns
    -------
    Any
         Returns a handle index associated with the just written component,
    which can be used to raise an issue if necessary
    """

    if "write" not in handle["yaml"].keys():
        raise ValueError(
            "Error: Write has not been specified in the given config file"
        )

    path, write_metadata = resolve_write(
        handle, data_product, file_type="netcdf"
    )
    # Get metadata ------------------------------------------------------------
    # return False
    write_data_product = write_metadata["data_product"]  # noqa: F841
    write_version = write_metadata["use_version"]  # noqa: F841
    write_namespace = write_metadata["use_namespace"]  # noqa: F841
    write_public = write_metadata["public"]  # noqa: F841
    data_product_decription = write_metadata[
        "data_product_description"
    ]  # noqa: F841
    path = write_metadata["path"]  # noqa: F841

    if not isinstance(array, np.ndarray):
        raise TypeError(f"{array} must be an array")
        # Check dimensions class
    if dimension_names:
        if not all(list(map(lambda x: isinstance(x, str), dimension_names))):
            raise TypeError("Elements of dimension_names must be strings")
            # Check number of dimensions
        # if (length(dim(array)) != length(dimension_names))
        if len(array) != len(dimension_names):
            raise ValueError(
                "Length of dimension_names does not equal number of dimensions in array"
            )
    parentdir = os.path.dirname(os.path.abspath(path))
    import pdb

    pdb.set_trace()
    # Write hdf5 file ---------------------------------------------------------

    # Generate directory structure
    if not os.path.dirname(parentdir):
        os.makedirs(parentdir, exist_ok=True)

    # if directory:= ox.exist(path):
    #     pass
    # return False

    # Write hdf5 file
    _ = netCDF4.Dataset(path, "w", format="NETCDF4")

    # Generate internal structure
    if component[-1] == "/":
        parentdir = path.join(parentdir, component.split("/")[0])
    else:
        parentdir = path.join(parentdir, component)

        # if group does not exist in Dataset:
        # create_group(root_group)


# This structure needs to be added

# If the structure doesn't exist make it

# Update current structure

# Attach data

# Dimension names and titles ----------------------------------------------


# Attach dimension titles


# Attach dimension names


# Dimension values and units ----------------------------------------------

# Attach dimension values


# Attach dimension units


# Attach units


# Write to handle ---------------------------------------------------------
# handle = {
#     "data_product": data_product,
#     "use_data_product": data_product,
#     "use_component": component,
#     "use_version": version,
#     "use_namespace": namespace,
#     "path": path,
#     "component_url": component_url,
#     "data_product_description": data_product_description,
#     "component_description": component_description,
#     "public": public,
# }


# Return handle index -----------------------------------------------------
