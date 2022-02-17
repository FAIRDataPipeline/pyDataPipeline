import logging
from typing import Optional


def raise_issue_by_index(
    handle: dict,
    index: Optional[bool],
    issue: str,
    severity: int,
    group: bool = True,
) -> None:
    """
    Raises an issue for a given input or output index and writes it to the handle

    Args:
        |   handle: the handle containing the index
        |   index: a unique reference to an input or output in
        |       the handle see get_handle_index_from_path()
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue(
        handle, "index", issue, severity, index=index, group=group
    )


def raise_issue_by_existing_data_product(
    handle: dict,
    data_product: str,
    version: str,
    namespace: str,
    issue: str,
    severity: int,
    group: bool = True,
) -> None:
    """
    Raises an issue for a given data_product and writes it to the handle

    Args:
        |   data_product: the data_product name as a string
        |   version: version of the data_product as a string
        |   namespace: namespace of the date_product as a string
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue(
        handle,
        "existing_data_product",
        issue,
        severity,
        data_product=data_product,
        namespace=namespace,
        version=version,
        group=group,
    )


def raise_issue_by_data_product(
    handle: dict,
    data_product: str,
    version: str,
    namespace: str,
    issue: str,
    severity: int,
    group: bool = True,
) -> None:
    """
    Raises an issue for a given data_product in the run_metadata and writes it to the handle

    Args:
        |   data_product: the data_product name as a string
        |   version: version of the data_product as a string
        |   namespace: namespace of the date_product as a string
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue(
        handle,
        "data_product",
        issue,
        severity,
        data_product=data_product,
        namespace=namespace,
        version=version,
        group=group,
    )


def raise_issue_with_config(
    handle: dict, issue: str, severity: int, group: bool = True
) -> None:
    """
    Raise an issue with config: add an issue for the config to the handle.

    Args:
        |   handle
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue_by_type(handle, "config", issue, severity, group)


def raise_issue_with_submission_script(
    handle: dict, issue: str, severity: int, group: bool = True
) -> None:
    """
    Raise an issue with submission script: add an issue for the submission_script to the handle.

    Args:
        |   handle
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue_by_type(
        handle, "submission_script", issue, severity, group
    )


def raise_issue_with_github_repo(
    handle: dict, issue: str, severity: int, group: bool = True
) -> None:
    """
    Raise an issue with config add an issue for the github_repo to the handle.

    Args:
        |   handle
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue_by_type(handle, "github_repo", issue, severity, group)


def raise_issue_by_type(
    handle: dict, type: str, issue: str, severity: int, group: bool = True
) -> None:
    """
    Raise an issue by type of issue (with config, with submission_script, with github_repo)
    """
    accepted_types = ["config", "submission_script", "github_repo"]
    if type not in accepted_types:
        raise ValueError(
            "Please supply a valid type. \
        valid types: config, submission_script, github_repo"
        )
    return raise_issue(handle, type, issue, severity, index=False, group=group)


# flake8: noqa C901
def raise_issue(
    handle: dict,
    issue_type: str,
    issue: str,
    severity: int,
    index: bool = None,
    data_product: str = None,
    component: str = None,
    version: str = None,
    namespace: str = None,
    group: bool = True,
) -> None:
    current_group = issue + ":" + str(severity)
    if issue_type in {
        "config",
        "submission_script",
        "github_repo",
        "existing_data_product",
    }:
        logging.info("Adding issue {} for {} to handle".format(issue, type))
    elif index is None:
        reads = (
            handle["yaml"]["read"] if "read" in handle["yaml"].keys() else None
        )
        writes = (
            handle["yaml"]["write"]
            if "write" in handle["yaml"].keys()
            else None
        )
        data_product_in_config = False
        if reads:
            for i in reads:
                if i["data_product"] == data_product:
                    data_product_in_config = True
                    if (
                        "use" in i.keys()
                        and "use_version" in i["use"].keys()
                        and i["use"]["version"] != version
                    ):
                        data_product_in_config = False
                    data_product = i["data_product"]
                    if not group:
                        current_group = i["data_product"]
        if writes:
            for i in writes:
                if i["data_product"] == data_product:
                    data_product_in_config = (
                        "use" not in i.keys()
                        or "use_version" not in i["use"].keys()
                        or i["use"]["version"] == version
                    )

                    data_product = i["data_product"]
                    if not group:
                        current_group = i["data_product"]

        if not data_product_in_config:
            raise ValueError("Data product not in config file")

        logging.info(
            "Raising issue {} for {}@{} to handle".format(
                issue, data_product, version
            )
        )

    else:
        tmp = None
        if "output" in handle:
            for output in handle["output"]:
                if output == index:
                    tmp = handle["output"][output]
                    if not group:
                        current_group = handle["output"][output]
        if "input" in handle:
            for input in handle["input"]:
                if input == index:
                    tmp = handle["input"][input]
                    if not group:
                        current_group = handle["input"][input]
        if tmp is None:
            raise ValueError("Error: index not found in handle")

        data_product = tmp["data_product"]
        component = tmp["use_component"]
        version = tmp["use_version"]
        namespace = tmp["use_namespace"]

        logging.info("Adding issue {} for {} to handle".format(issue, index))
    # Write to handle and return path
    issues_dict = {
        "index": index,
        "type": issue_type,
        "use_data_product": data_product,
        "use_component": component,
        "version": version,
        "use_namespace": namespace,
        "issue": issue,
        "severity": severity,
        "group": current_group,
    }

    if "issues" in handle:
        this_issue = "issue_" + str(len(handle["issues"]))
        handle["issues"][this_issue] = issues_dict
    else:
        handle["issues"] = {}
        handle["issues"]["issue_0"] = issues_dict
