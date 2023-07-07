import datetime
import logging
import os

import yaml

from data_pipeline_api import fdp_utils

WRITING_STR = "Writing {} to local registry"

def initialise(token: str, config: str, script: str) -> dict:
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
    config_filetype_response = fdp_utils.post_file_type(
        token=token,
        url=registry_url,
        data={"name": "YAML Document", "extension": "yaml"},
        api_version=api_version,
    )
    config_filetype_url = config_filetype_response["url"]

    # Get user for registry admin account
    results = fdp_utils.get_entry(
        url=registry_url,
        endpoint="users",
        query={"username": "admin"},
        token=token,
        api_version=api_version,
    )

    if not results:
        raise IndexError(f"list {results} empty")
    else:
        user = fdp_utils.get_first_entry(results)
    # Check users exists
    if not user:
        raise ValueError(
            "Error: Admin user not found\
        \nDid you run fair init?"
        )

    user_url = user["url"]
    user_id = fdp_utils.extract_id(user_url)
    # Get author(s)
    results = fdp_utils.get_entry(
        url=registry_url,
        endpoint="user_author",
        query={"user": user_id},
        api_version=api_version,
    )
    if not results:
        raise IndexError(f"list {results} empty")
    else:
        author = fdp_utils.get_first_entry(results)
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

    logging.info(WRITING_STR.format(filename))

    # Check if script exists in storage_location
    script_storageroot_url = config_storageroot_url
    _ = config_storageroot_id
    script_hash = fdp_utils.get_file_hash(script)

    script_location_exists = fdp_utils.get_entry(
        registry_url,
        "storage_location",
        query={
            "hash": script_hash
        }
    )
    if script_location_exists:
        script_location_url = script_location_exists[0]["url"]
    else:
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
    script_file_type = os.path.basename(script).split(".")[-1]

    # Create Script File Type
    script_filetype_response = fdp_utils.post_file_type(
        token=token,
        url=registry_url,
        data={"name": "python submission script", "extension": script_file_type},
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

    logging.info(WRITING_STR.format(script))

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

    logging.info(WRITING_STR.format(repo_name))

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


# flake8: noqa C901
def finalise(token: str, handle: dict) -> None:
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
    registry_url = handle["yaml"]["run_metadata"]["local_data_registry_url"]
    datastore = handle["yaml"]["run_metadata"]["write_data_store"]
    api_version = handle["yaml"]["run_metadata"]["api_version"]

    datastore = fdp_utils.remove_local_from_root(datastore)
    datastore_root = fdp_utils.get_entry(
        url=registry_url,
        endpoint="storage_root",
        query={"root": datastore},
        api_version=api_version,
    )
    datastore_root_url = None

    # Check datastore is in registry
    if datastore_root:
        datastore_root_dict = fdp_utils.get_first_entry(datastore_root)
        datastore_root_url = datastore_root_dict["url"]
    else:
        datastore_root_url = fdp_utils.post_storage_root(
            token=token,
            url=registry_url,
            data={"root": datastore, "local": True},
            api_version=api_version,
        )["url"]

    datastore_root_id = fdp_utils.extract_id(datastore_root_url)

    if "output" in handle:
        for output in handle["output"]:

            if "${RUN_ID}" in handle["output"][output]["use_data_product"]:
                handle["output"][output]["use_data_product"] = handle[
                    "output"
                ][output]["use_data_product"].replace(
                    "${RUN_ID}", handle["code_run_uuid"]
                )
            write_namespace = fdp_utils.get_entry(
                url=registry_url,
                endpoint="namespace",
                query={"name": handle["output"][output]["use_namespace"]},
                api_version=api_version,
            )
            write_namespace_url = None
            if write_namespace:
                entry = fdp_utils.get_first_entry(write_namespace)
                write_namespace_url = entry["url"]
            else:
                write_namespace_url = fdp_utils.post_entry(
                    token=token,
                    url=registry_url,
                    endpoint="namespace",
                    data={"name": handle["output"][output]["use_namespace"]},
                    api_version=api_version,
                )["url"]

            file_hash = fdp_utils.get_file_hash(handle["output"][output]["path"])

            storage_exists = fdp_utils.get_entry(
                url=registry_url,
                endpoint="storage_location",
                query={
                    "hash": file_hash,
                    "public": str(handle["output"][output]["public"]).lower(),
                    "storage_root": datastore_root_id,
                },
                api_version=api_version,
            )

            storage_location_url = None

            if storage_exists:
                storage_exists_dict = fdp_utils.get_first_entry(storage_exists)
                storage_location_url = storage_exists_dict["url"]

                os.remove(handle["output"][output]["path"])

                directory = os.path.dirname(handle["output"][output]["path"])
                i = 0
                while os.path.normpath(directory) != os.path.normpath(
                    datastore
                ):
                    try:
                        os.rmdir(directory)
                    except Exception:
                        logging.warning(
                            "Ignoring Directory: {} as it is not empty".format(
                                directory
                            )
                        )
                    directory = os.path.split(directory)[0]
                    i += 1
                    if i > 4:
                        break

                existing_path = storage_exists_dict["path"]

                existing_root = fdp_utils.get_entity(
                    url=registry_url,
                    endpoint="storage_root",
                    id=int(
                        fdp_utils.extract_id(
                            storage_exists_dict["storage_root"]
                        )
                    ),
                    api_version=api_version,
                )["root"]

                existing_root = fdp_utils.remove_local_from_root(existing_root)

                new_path = os.path.join(existing_root, existing_path)

            else:
                tmp_filename = os.path.basename(
                    handle["output"][output]["path"]
                )
                extension = tmp_filename.split(sep=".")[-1]
                new_filename = ".".join([file_hash, extension])
                data_product = handle["output"][output]["data_product"]
                namespace = handle["output"][output]["use_namespace"]
                new_path = os.path.join(
                    datastore, namespace, data_product, new_filename
                ).replace("\\", "/")
                os.rename(handle["output"][output]["path"], new_path)
                new_storage_location = os.path.join(
                    namespace, data_product, new_filename
                ).replace("\\", "/")

                storage_location_url = fdp_utils.post_entry(
                    token=token,
                    url=registry_url,
                    endpoint="storage_location",
                    data={
                        "path": new_storage_location,
                        "hash": file_hash,
                        "public": str(handle["output"][output]["public"]).lower(),
                        "storage_root": datastore_root_url,
                    },
                    api_version=api_version,
                )["url"]

            file_type = os.path.basename(new_path).split(".")[-1]

            file_type_url = fdp_utils.post_file_type(
                token=token,
                url=registry_url,
                data={"name": file_type, "extension": file_type},
                api_version=api_version,
                )["url"]

            data_product_exists = fdp_utils.get_entry(
                url=registry_url,
                endpoint="data_product",
                query={
                    "name": handle["output"][output]["use_data_product"],
                    "version": handle["output"][output]["use_version"],
                    "namespace": write_namespace_url,
                },
                api_version=api_version,
            )

            if data_product_exists:
                data_product_exists_dict = fdp_utils.get_first_entry(
                    data_product_exists
                )
                data_product_url = data_product_exists_dict["url"]
                object_url = data_product_exists_dict["object"]
                obj = fdp_utils.get_entity(
                    url=registry_url,
                    endpoint="object",
                    id=int(fdp_utils.extract_id(object_url)),
                    api_version=api_version,
                )
                component_url = obj["components"][0]

            else:
                object_url = fdp_utils.post_entry(
                    token=token,
                    url=registry_url,
                    endpoint="object",
                    data={
                        "description": handle["output"][output][
                            "data_product_description"
                        ],
                        "storage_location": storage_location_url,
                        "authors": [handle["author"]],
                        "file_type": file_type_url,
                    },
                    api_version=api_version,
                )["url"]

                component_url = None

                if handle["output"][output]["use_component"]:
                    component_url = fdp_utils.post_entry(
                        token=token,
                        url=registry_url,
                        endpoint="object_component",
                        data={
                            "object": object_url,
                            "name": handle["output"][output]["use_component"],
                        },
                        api_version=api_version,
                    )["url"]
                else:
                    component_url = fdp_utils.get_entry(
                        url=registry_url,
                        endpoint="object_component",
                        query={
                            "object": fdp_utils.extract_id(object_url),
                        },
                        api_version=api_version,
                    )[0]["url"]

                data_product_url = fdp_utils.post_entry(
                    token=token,
                    url=registry_url,
                    endpoint="data_product",
                    data={
                        "name": handle["output"][output]["use_data_product"],
                        "version": handle["output"][output]["use_version"],
                        "object": object_url,
                        "namespace": write_namespace_url,
                    },
                    api_version=api_version,
                )["url"]

            handle["output"][output]["component_url"] = component_url
            handle["output"][output]["data_product_url"] = data_product_url

            logging.info(
                WRITING_STR.format(
                    handle["output"][output]["use_data_product"]
                )
            )

    output_components = []
    input_components = []

    if "output" in handle.keys():
        for output in handle["output"]:
            output_components.append(handle["output"][output]["component_url"])

    if "input" in handle.keys():
        for input in handle["input"]:
            input_components.append(handle["input"][input]["component_url"])

    if "issues" in handle.keys():
        fdp_utils.register_issues(token, handle)

    fdp_utils.patch_entry(
        token=token,
        url=handle["code_run"],
        data={"inputs": input_components, "outputs": output_components},
        api_version=api_version,
    )

    coderuns_path = os.path.join(
        handle["fdp_config_dir"], "coderuns.txt"
    ).replace("\\", "/")

    with open(coderuns_path, "a+") as coderun_file:
        coderun_file.seek(0)
        data = coderun_file.read(100)
        if len(data) > 0:
            coderun_file.write("\n")
        coderun_file.write(handle["code_run_uuid"])
