import os
import shutil

import pytest

import org.fairdatapipeline.api as pipeline
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils

# from org.fairdatapipeline.api.common.link_read import link_read


@pytest.fixture
def test_dir():
    return os.path.join(os.path.dirname(__file__), "ext")


@pytest.fixture
def token():
    return fdp_utils.read_token(
        os.path.join(os.path.expanduser("~"), ".fair/registry/token")
    )


@pytest.fixture
def script(test_dir):
    return os.path.join(test_dir, "test_script.sh")


@pytest.fixture
def config(test_dir):
    return os.path.join(test_dir, "write_csv.yaml")


def test_initialise(token, config, script):
    handle = pipeline.initialise(token, config, script)
    assert type(handle) == dict


def test_link_write(token, config, script):
    handle = pipeline.initialise(token, config, script)
    pipeline.link_write(handle, "test/csv")
    assert handle["output"]["output_0"]["data_product"] == "test/csv"


def test_raise_issue_by_index(token, config, script):
    handle = pipeline.initialise(token, config, script)
    link_write = pipeline.link_write(handle, "test/csv")
    index = pipeline.get_handle_index_from_path(handle, link_write)
    pipeline.raise_issue_by_index(handle, index, "Test Issue", 7)
    assert handle["issues"]["issue_0"]["use_data_product"] == "test/csv"


def test_raise_issue_with_config(token, config, script):
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_with_config(handle, "Test Issue with config", 4)
    assert handle["issues"]["issue_0"]["type"] == "config"


def test_raise_issue_with_github_repo(token, config, script):
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_with_github_repo(
        handle, "Test Issue with github_repo", 4
    )
    assert handle["issues"]["issue_0"]["type"] == "github_repo"


def test_raise_issue_with_script(token, config, script):
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_with_submission_script(
        handle, "Test Issue with submission_script", 4
    )
    assert handle["issues"]["issue_0"]["type"] == "submission_script"


def test_link_read(token, config, script, test_dir):
    handle = pipeline.initialise(token, config, script)
    tmp_csv = os.path.join(test_dir, "test.csv")
    link_write = pipeline.link_write(handle, "test/csv")
    shutil.copy(tmp_csv, link_write)
    pipeline.finalise(token, handle)

    config = os.path.join(test_dir, "read_csv.yaml")
    handle = pipeline.initialise(token, config, script)
    link_read_1 = pipeline.link_read(handle, "test/csv")
    link_read_2 = pipeline.link_read(handle, "test/csv")
    assert type(link_read_1) == str and type(link_read_2) == str


def test_raise_issue_existing_data_product(token, config, script):
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_by_existing_data_product(
        handle,
        "test/csv",
        "0.0.1",
        "testing",
        "Problem with csv File : Test Issue # 2",
        5,
    )
    pipeline.finalise(token, handle)


def test_raise_issue_data_product_from_reads(token, script, test_dir):
    config = os.path.join(test_dir, "read_csv.yaml")
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_by_data_product(
        handle,
        "test/csv",
        "0.0.1",
        "testing",
        "Problem with csv File : Test Issue # 3",
        5,
    )
    pipeline.finalise(token, handle)
