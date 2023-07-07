import hashlib
import json
import logging
import os
import random
import uuid
from datetime import datetime
from typing import Any, Optional

from urllib.parse import urlsplit

import requests
import yaml

FILE_PREFIX = "file://"
SERVER_RESPONSE_STR = "Server responded with: "

def get_first_entry(entries: list) -> dict:
    """
    get_first_entry helper function for get_entry that return first element

    exception handling is done in the main code

    Parameters
    ----------
    entries : list
        [response list from api]

    Returns
    -------
    dict
        [dictionary output from api]
    """
    return entries[0]


def get_entry(
    url: str,
    endpoint: str,
    query: dict,
    token: str = None,
    api_version: str = "1.0.0",
) -> list:
    """
    Internal function to retreive and item from the registry using a query
    Args:
        |   url: str of the registry url
        |   endpoint: endpoint (table)
        |   query: dict forming a query
        |   token: (optional) str of the registry token
    Returns:
        |   dict: responce from registry
    """
    headers = get_headers(token=token, api_version=api_version)

    # Remove api address from query
    for key in query:
        if isinstance(query[key], str):
            if url in query[key]:
                query[key] = extract_id(query[key])
        elif isinstance(query[key], dict):
            for _key in query[key]:
                if url in query[key][_key]:
                    query[key][_key] = extract_id(query[key][_key])
        elif isinstance(query[key], list):
            for i in range(len(query[key])):
                if url in query[key][i]:
                    query[key][i] = extract_id(query[key][i])

    if url[-1] != "/":
        url += "/"
    url += endpoint + "/?"
    _query = [f"{k}={v}" for k, v in query.items()]
    url += "&".join(_query)
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(
            SERVER_RESPONSE_STR
            + str(response.status_code)
            + " Query = "
            + url
        )
    return response.json()["results"]


def get_entity(
    url: str,
    endpoint: str,
    id: int,
    token: str = None,
    api_version: str = "1.0.0",
) -> dict:
    """
    Internal function to get an item from the registry using it's id
    Args:
        |   url: str of the registry url
        |   enpoint: endpoint (table)
        |   id: id of the item
        |   token: (optional) str of the registry token
    Returns:
        |   dict: responce from registry
    """
    headers = get_headers(token=token, api_version=api_version)

    if url[-1] != "/":
        url += "/"
    url += endpoint + "/" + str(id)
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(
            SERVER_RESPONSE_STR
            + str(response.status_code)
            + " Query = "
            + url
        )
    return response.json()


def extract_id(url: str) -> str:
    """
    Internal function to return the id from an api url
    Args:
        |   url: str of the api url
    Returns:
        |   str: id derrived from the url
    """

    split_url_path = urlsplit(url).path.split("/")
    if not split_url_path:
        raise IndexError(f"Unable to extract ID from registry URL: {url}")
    return [s for s in split_url_path if s != ""][-1]


def post_entry(
    url: str, endpoint: str, data: dict, token: str, api_version: str = "1.0.0"
) -> dict:
    """
    Internal function to post and entry on the registry
    Args:
        |   url: str of the registry url
        |   enpoint: str of the endpoint (table)
        |   data: a dictionary containing the data to be posted
        |   token: str of the registry token
    Returns:
        |   dict: responce from registry
    """
    headers = get_headers(
        request_type="post", token=token, api_version=api_version
    )

    if url[-1] != "/":
        url += "/"
    _url = url + endpoint + "/"
    _data = json.dumps(data)

    response = requests.post(_url, _data, headers=headers)

    if response.status_code == 409:
        logging.info("Entry Exists: Attempting to return Existing Entry")
        existing_entry = get_entry(url, endpoint, data)
        if not existing_entry:
            raise ValueError("Could not return existing Entry")
        return existing_entry[0]

    if response.status_code != 201:
        raise ValueError(SERVER_RESPONSE_STR + str(response.status_code))

    return response.json()


def patch_entry(
    url: str, data: dict, token: str, api_version: str = "1.0.0"
) -> dict:
    """
    Internal function to patch and entry on the registry
    Args:
        |   url: str of the url of what to be patched
        |   data: a dictionary containing the data to be patched
        |   token: str of the registry token
    Returns:
        |   dict: responce from registry
    """
    headers = get_headers(
        request_type="post", token=token, api_version=api_version
    )

    data_json = json.dumps(data)

    response = requests.patch(url, data_json, headers=headers)
    if response.status_code != 200:
        raise ValueError(SERVER_RESPONSE_STR + str(response.status_code))

    return response.json()


def get_headers(
    request_type: str = "get", token: str = None, api_version: str = "1.0.0"
) -> dict:
    """
    Internal function to return headers to be added to a request
    Args:
        |   request_type: (optional) type of request e.g. 'post' or 'get' defaults to 'get'
        |   token: (optional) token, if a token is supplied this will be added to the headers
        |   api_version: (optional) the version of the data registy to interact with, defaults to '1.0.0'
    Returns:
        |   dict: a dictionary of appropriate headers to be added to a request
    """
    headers = {"Accept": "application/json; version=" + api_version}
    if token:
        headers["Authorization"] = "token " + token
    if request_type == "post":
        headers["Content-Type"] = "application/json"
    return headers


def post_storage_root(
    url: str, data: dict, token: str, api_version: str = "1.0.0"
) -> dict:
    """
    Internal function to post a storage root to the registry
    the function first adds file:// if the root is local
    Args:
        |   url: str the url for the storage root e.g. https://github.com/
    Returns:
        |   dict: repsonse from the local registy
    """
    if "local" in data and data["local"]:
        data["root"] = FILE_PREFIX + data["root"]
        if data["root"][-1] != os.sep:
            data["root"] = data["root"] + os.sep
    elif data["root"][-1] != "/":
        data["root"] = data["root"] + "/"
    return post_entry(url, "storage_root", data, token, api_version)

def post_file_type(
    url: str, data: dict, token: str, api_version: str = "1.0.0"
) -> dict:
    """
    Internal wrapper function to return check if a file_type already exists and return it.
    """
    if "extension" not in data:
            if not data["extension"]:
                raise ValueError("error file_type name not specified")
    file_type_exists = get_entry(url= url, 
        endpoint= "file_type", 
        query= {"extension": data["extension"]}, 
        api_version= api_version)
    if file_type_exists : return file_type_exists[0]
    return post_entry(url= url,
        endpoint= "file_type",
        data= data,
        token= token,
        api_version= api_version)

def remove_local_from_root(root: str) -> str:
    """
    Internal function to remove prepending file:// from a given root
    Args:
        |   root: the root
    Returns:
        |   str: the root without file://
    """
    if FILE_PREFIX in root:
        root = root.replace(FILE_PREFIX, "")

    return root


def random_hash() -> str:
    """
    Internal function to generate a random unique hash

    Returns:
        |   str: 40 character randomly generated hash.
    """
    seed = datetime.now().timestamp() * random.uniform(1, 1000000)
    seed_encoded = str(seed).encode("utf-8")
    hashed = hashlib.sha1(seed_encoded)

    return hashed.hexdigest()


def get_file_hash(
    path: str,
) -> str:
    """
    Internal function to return a files sha1 hash
    Args:
        |   path: str file path
    Returns:
        |   str: sha1 hash
    """
    with open(path, "rb") as data:
        _data = data.read()
    hashed = hashlib.sha1(_data)

    return hashed.hexdigest()


def read_token(token_path: str) -> str:
    """
    Internal function read a token from a given file
    Args:
        |   token_path: path to token
    Returns:
        |   str: token
    """
    with open(token_path) as token:
        _token = token.readline().strip()
    return _token


def get_token(token_path: str) -> str:
    """
    Internal function alias for read_token()
    Args:
        |   token_path: path to token
    Returns:
        |   str: token
    """
    return read_token(token_path)


def is_file(filename: str) -> bool:
    """
    Internal function to check whether a file exists
    Args:
        |   filename: file to check
    Returns:
        |   boolean: whether the file exists
    """
    return os.path.isfile(filename)


def is_yaml(filename: str) -> bool:
    """
    Internal function to check whether a file can be opened as a YAML file
    ! warning returns True if the file can be coerced into yaml format
    Args:
        |   filename: path to the yaml file
    Returns:
        |   boolean: can the file be coerced into a yaml format?
    """
    try:
        with open(filename, "r") as data:
            yaml.safe_load(data)
    except Exception as err:
        print(f"{type(err).__name__} was raised: {err}")
        return False
    return True


def is_valid_yaml(filename: str) -> bool:
    """
    Internal function validate whether a file exists and can be coerced into a yaml format
    Args:
        |   filename: path to the yaml file
    Returns:
        |   boolean: does the file exist and can it be coerced into a yaml format
    """
    return is_file(filename) & is_yaml(filename)


def generate_uuid() -> str:
    """
    Internal function similar to random hash
    Returns:
        |   str: a random unique identifier
    """
    return datetime.now().strftime("%Y%m-%d%H-%M%S-") + str(uuid.uuid4())


def get_handle_index_from_path(handle: dict, path: str) -> Optional[Any]:
    """
    Get an input or output handle index from a path
    usually generated by link_read or link_write

    Args:
        |   handle: the handle containing the index
        |   path: path as generated by link_read or link_write
    """
    tmp = None
    if "output" in handle:
        for output in handle["output"]:
            if handle["output"][output]["path"] == path:
                tmp = output
    if "input" in handle:
        for input in handle["input"]:
            if handle["input"][input]["path"] == path:
                tmp = input
    return tmp


# flake8: noqa C901
def register_issues(token: str, handle: dict) -> dict:  # sourcery no-metrics
    """
    Internal function, should only be called from finalise.
    """

    api_url = handle["yaml"]["run_metadata"]["local_data_registry_url"]
    issues = handle["issues"]
    groups = {handle["issues"][i]["group"] for i in handle["issues"]}
    api_version = handle["yaml"]["run_metadata"]["api_version"]

    for group in groups:
        component_list = []
        issue = None
        severity = None
        for i in issues:
            if issues[i]["group"] == group:
                issue_type = issues[i]["type"]
                issue = issues[i]["issue"]
                severity = issues[i]["severity"]
                index = issues[i]["index"]
                data_product = issues[i]["use_data_product"]
                component = issues[i]["use_component"]
                version = issues[i]["version"]
                namespace = issues[i]["use_namespace"]

                component_url = None
                object_id = None
                if issue_type == "config":
                    object_id = handle["model_config"]
                elif issue_type == "github_repo":
                    object_id = handle["code_repo"]

                elif issue_type == "submission_script":
                    object_id = handle["submission_script"]
                if object_id:
                    component_url = get_entry(
                        url=api_url,
                        endpoint="object_component",
                        query={
                            "object": extract_id(object_id),
                            "whole_object": True,
                        },
                        api_version=api_version,
                    )[0]["url"]

                if index:
                    if "output" in handle:
                        for ii in handle["output"]:
                            if handle["output"][ii] == index:
                                if (
                                    "component_url"
                                    in handle["output"][ii].keys()
                                ):
                                    component_url = handle["output"][ii][
                                        "component_url"
                                    ]
                                else:
                                    logging.warning("No Component Found")
                    if "input" in handle:
                        for ii in handle["input"]:
                            if handle["input"][ii] == index:
                                if (
                                    "component_url"
                                    in handle["input"][ii].keys()
                                ):
                                    component_url = handle["input"][ii][
                                        "component_url"
                                    ]
                                else:
                                    logging.warning("No Component Found")

                if data_product:
                    current_namespace = get_entry(
                        url=api_url,
                        endpoint="namespace",
                        query={"name": namespace},
                        api_version=api_version,
                    )[0]["url"]

                    object_entry = get_entry(
                        url=api_url,
                        endpoint="data_product",
                        query={
                            "name": data_product,
                            "version": version,
                            "namespace": extract_id(current_namespace),
                        },
                        api_version=api_version,
                    )[0]["object"]
                    object_id = extract_id(object_entry)
                    if component:
                        component_url = get_entry(
                            url=api_url,
                            endpoint="object_component",
                            query={"name": component, "object": object_id},
                            api_version=api_version,
                        )
                    else:
                        component_obj = get_entry(
                            url=api_url,
                            endpoint="object_component",
                            query={"object": object_id, "whole_object": True},
                            api_version=api_version,
                        )
                        component_url = component_obj[0]["url"]

                if component_url:
                    component_list.append(component_url)

        # Register the issue:
        logging.info("Registering issue: {}".format(group))
        current_issue = post_entry(
            url=api_url,
            endpoint="issue",
            data={
                "severity": severity,
                "description": issue,
                "component_issues": component_list,
            },
            token=token,
            api_version=api_version,
        )
    return current_issue
