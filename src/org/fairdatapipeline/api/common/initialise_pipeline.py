import datetime
import logging
import os

import yaml

from org.fairdatapipeline.api.common import fdp_utils


def initialise(token: str, config: str, script: str):
    """Reads in token, config file and script, creates necessary registry items
    and creates new code run.

    Args:
        |   token: registry token
        |   config: Path to config file
        |   script: Path to script file

    Returns:
        |   dict: a dictionary containing the following keys:
        |       'yaml': config_yaml path,
        |       'fdp_config_dir': config dir path,
        |       'model_config': model config url,
        |       'submission_script': submission script object url,
        |       'code_repo': code repo object url,
        |       'code_run': coderun url,
        |       'code_run_uuid': coderun uuid,
        |       'author': author url
    """

    # Validate Yamls
    if not fdp_utils.is_valid_yaml(config):
        raise ValueError("Config is not a valid YAML file")
    if not fdp_utils.is_file(script):
        raise ValueError("Script does not exist")

    # Read config file and extract run metadata
    with open(config, "r") as data:
        config_yaml = yaml.safe_load(data)
    run_metadata = config_yaml["run_metadata"]
    registry_url = run_metadata["local_data_registry_url"]
    if registry_url[-1] != "/":
        registry_url += "/"
    filename = os.path.basename(config)

    # @todo to be set from config
    if "api_version" not in config_yaml["run_metadata"].keys():
        config_yaml["run_metadata"]["api_version"] = "1.0.0"

    api_version = config_yaml["run_metadata"]["api_version"]

    logging.info("Reading {} from local filestore".format(filename))

    # Configure storage root for config
    config_storageroot_response = fdp_utils.post_storage_root(
        token=token,
        url=registry_url,
        data={"root": run_metadata["write_data_store"], "local": True},
        api_version=api_version,
    )

    config_storageroot_url = config_storageroot_response["url"]
    config_storageroot_id = fdp_utils.extract_id(config_storageroot_url)
    config_hash = fdp_utils.get_file_hash(config)

    # Configure Storage Location for config
    config_storage_data = {
        "path": config.replace(run_metadata["write_data_store"], ""),
        "hash": config_hash,
        "public": True,
        "storage_root": config_storageroot_url,
    }

    config_location_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="storage_location",
        data=config_storage_data,
        api_version=api_version,
    )

    config_location_url = config_location_response["url"]

    # Configure Yaml File Type
    config_filetype_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="file_type",
        data={"name": "yaml", "extension": "yaml"},
        api_version=api_version,
    )
    config_filetype_url = config_filetype_response["url"]

    # Get user for registry admin account
    user = fdp_utils.get_entry(
        url=registry_url,
        endpoint="users",
        query={"username": "admin"},
        token=token,
        api_version=api_version,
    )[0]

    # Check users exists
    if not user:
        raise ValueError(
            "Error: Admin user not found\
        \nDid you run fair init?"
        )

    user_url = user["url"]
    user_id = fdp_utils.extract_id(user_url)

    # Get author(s)
    author = fdp_utils.get_entry(
        url=registry_url,
        endpoint="user_author",
        query={"user": user_id},
        api_version=api_version,
    )[0]

    # Check user author exists
    if not author:
        raise ValueError(
            "Error: user_author not found\
            \nDid you run fair init?"
        )

    author_url = author["author"]

    # Create new object for config file

    config_object = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="object",
        data={
            "description": "Working config.yaml location in datastore",
            "storage_location": config_location_url,
            "authors": [author_url],
            "file_type": config_filetype_url,
        },
        api_version=api_version,
    )

    config_object_url = config_object["url"]

    logging.info("Writing {} to local registry".format(filename))

    # Check if script exists in storage_location
    script_storageroot_url = config_storageroot_url
    _ = config_storageroot_id
    script_hash = fdp_utils.get_file_hash(script)

    # Create Script Storage Location
    script_storage_data = {
        "path": script.replace(run_metadata["write_data_store"], ""),
        "hash": script_hash,
        "public": True,
        "storage_root": script_storageroot_url,
    }

    script_location_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="storage_location",
        data=script_storage_data,
        api_version=api_version,
    )

    script_location_url = script_location_response["url"]

    # TODO: Change to Batch?
    # Create Script File Type
    script_filetype_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="file_type",
        data={"name": "py", "extension": "py"},
        api_version=api_version,
    )

    script_filetype_url = script_filetype_response["url"]

    # Create new registry object for script
    script_object = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="object",
        data={
            "description": "Working script location in datastore",
            "storage_location": script_location_url,
            "authors": [author_url],
            "file_type": script_filetype_url,
        },
        api_version=api_version,
    )

    script_object_url = script_object["url"]

    logging.info("Writing {} to local registry".format(script))

    # Create new remote storage root
    repo_storageroot_url = fdp_utils.post_storage_root(
        token=token,
        url=registry_url,
        data={"root": "https://github.com", "local": False},
        api_version=api_version,
    )["url"]

    repo_storageroot_id = fdp_utils.extract_id(repo_storageroot_url)

    sha = run_metadata["latest_commit"]
    repo_name = run_metadata["remote_repo"]

    # Check if code repo entry exists for given hash

    _ = fdp_utils.get_entry(
        url=registry_url,
        endpoint="storage_location",
        query={
            "hash": sha,
            "public": True,
            "storage_root": repo_storageroot_id,
        },
        api_version=api_version,
    )

    # Configure Code Repo Location
    coderepo_location_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="storage_location",
        data={
            "path": repo_name,
            "hash": sha,
            "public": True,
            "storage_root": repo_storageroot_url,
        },
        api_version=api_version,
    )

    coderepo_location_url = coderepo_location_response["url"]

    # Configure Code Repo Object
    coderepo_object_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="object",
        data={
            "description": "Analysis / processing script location",
            "storage_location": coderepo_location_url,
            "authors": [author_url],
        },
        api_version=api_version,
    )

    coderepo_object_url = coderepo_object_response["url"]

    logging.info("Writing {} to local registry".format(repo_name))

    # Register new code run

    coderun_response = fdp_utils.post_entry(
        token=token,
        url=registry_url,
        endpoint="code_run",
        data={
            "run_date": str(datetime.datetime.now()),
            "description": run_metadata["description"],
            "code_repo": coderepo_object_url,
            "model_config": config_object_url,
            "submission_script": script_object_url,
            "input_urls": [],
            "output_urls": [],
        },
        api_version=api_version,
    )

    coderun_url = coderun_response["url"]
    coderun_uuid = coderun_response["uuid"]

    logging.info("Writing new code_run to local registry")

    # Write code run and object info to handle

    return {
        "yaml": config_yaml,
        "fdp_config_dir": os.path.dirname(config),
        "model_config": config_object_url,
        "submission_script": script_object_url,
        "code_repo": coderepo_object_url,
        "code_run": coderun_url,
        "code_run_uuid": coderun_uuid,
        "author": author_url,
    }
