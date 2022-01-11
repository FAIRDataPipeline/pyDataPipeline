import os
import shutil

import pytest

import fairdatapipeline as pipeline
import fairdatapipeline.fdp_utils as fdp_utils

# from org.fairdatapipeline.api.common.link_read import link_read


@pytest.fixture
def test_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "ext")


@pytest.fixture
def token() -> str:
    return fdp_utils.read_token(
        os.path.join(os.path.expanduser("~"), ".fair/registry/token")
    )


@pytest.fixture
def script(test_dir: str) -> str:
    return os.path.join(test_dir, "test_script.sh")


@pytest.fixture
def config(test_dir: str) -> str:
    return os.path.join(test_dir, "write_csv.yaml")


@pytest.mark.pipeline
def test_initialise(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    assert type(handle) == dict


@pytest.mark.pipeline
def test_link_write(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    pipeline.link_write(handle, "test/csv")
    assert handle["output"]["output_0"]["data_product"] == "test/csv"


@pytest.mark.issue
def test_raise_issue_by_index(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    link_write = pipeline.link_write(handle, "test/csv")
    index = pipeline.get_handle_index_from_path(handle, link_write)
    pipeline.raise_issue_by_index(handle, index, "Test Issue", 7)
    assert handle["issues"]["issue_0"]["use_data_product"] == "test/csv"


@pytest.mark.issue
def test_raise_issue_with_config(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_with_config(handle, "Test Issue with config", 4)
    assert handle["issues"]["issue_0"]["type"] == "config"


@pytest.mark.issue
def test_raise_issue_with_github_repo(
    token: str, config: str, script: str
) -> None:
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_with_github_repo(
        handle, "Test Issue with github_repo", 4
    )
    assert handle["issues"]["issue_0"]["type"] == "github_repo"


@pytest.mark.issue
def test_raise_issue_with_script(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_with_submission_script(
        handle, "Test Issue with submission_script", 4
    )
    assert handle["issues"]["issue_0"]["type"] == "submission_script"


@pytest.mark.pipeline
def test_link_read(
    token: str, config: str, script: str, test_dir: str
) -> None:
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


@pytest.mark.pipeline
def test_link_read_data_product_exists(
    token: str, config: str, script: str, test_dir: str
) -> None:
    handle = pipeline.initialise(token, config, script)
    tmp_csv = os.path.join(test_dir, "test.csv")
    link_write = pipeline.link_write(handle, "test/csv")
    shutil.copy(tmp_csv, link_write)
    pipeline.finalise(token, handle)


@pytest.mark.issue
def test_raise_issue_existing_data_product(
    token: str, config: str, script: str
) -> None:
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


@pytest.mark.issue
def test_raise_issue_data_product_from_reads(
    token: str, script: str, test_dir: str
) -> None:
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
