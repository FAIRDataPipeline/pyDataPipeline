import json
import os
import shutil

import pytest

import fairdatapipeline as pipeline
import fairdatapipeline.fdp_utils as fdp_utils


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


@pytest.fixture
def fconfig(test_dir: str) -> str:
    return os.path.join(test_dir, "find_csv.yaml")


@pytest.mark.pipeline
def test_initialise(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    assert type(handle) == dict
    assert handle["yaml"]["run_metadata"]["script"] == "python3 py.test"


@pytest.mark.pipeline
def test_initialise_noconfig(
    token: str, script: str, config: str = "file_that_does_not_exist"
) -> None:
    with pytest.raises(ValueError):
        _ = pipeline.initialise(token, config, script)


@pytest.mark.pipeline
def test_initialise_noscript(
    token: str, config: str, script: str = "file_that_does_not_exist"
) -> None:
    with pytest.raises(ValueError):
        _ = pipeline.initialise(token, config, script)


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
def test_raise_issue_by_type(token: str, config: str, script: str) -> None:
    handle = pipeline.initialise(token, config, script)
    with pytest.raises(ValueError):
        pipeline.raise_issue_by_type(
            handle, "testing_type", "Test Issue by type", 1, group=True
        )


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
def test_find_data_product(
    token: str,
    fconfig: str,
    script: str,
    test_dir: str,
    query: str = "find",
    key: str = "name",
) -> None:

    handle = pipeline.initialise(token, fconfig, script)
    tmp_csv = os.path.join(test_dir, "test.csv")
    link_write = pipeline.link_write(handle, "find/csv")
    shutil.copy(tmp_csv, link_write)
    pipeline.finalise(token, handle)

    config = os.path.join(test_dir, "read_csv.yaml")
    results = pipeline.search(
        token, config, script, query, "data_product", "name"
    )
    res = json.loads(results)
    assert len(res) == 1
    result = fdp_utils.get_first_entry(res)
    assert query in result[key]


@pytest.mark.pipeline
def test_link_read_data_product_exists(
    token: str, config: str, script: str, test_dir: str
) -> None:
    handle = pipeline.initialise(token, config, script)
    tmp_csv = os.path.join(test_dir, "test.csv")
    link_write = pipeline.link_write(handle, "test/csv")
    shutil.copy(tmp_csv, link_write)
    pipeline.finalise(token, handle)
    assert len(handle["yaml"]["write"]) == 1
    assert handle["yaml"]["write"][0] == {
        "data_product": "test/csv",
        "description": "test csv file with simple data",
        "file_type": "csv",
        "use": {"version": "0.0.1"},
    }


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
    assert handle["issues"]["issue_0"] == {
        "index": None,
        "type": "existing_data_product",
        "use_data_product": "test/csv",
        "use_component": None,
        "version": "0.0.1",
        "use_namespace": "testing",
        "issue": "Problem with csv File : Test Issue # 2",
        "severity": 5,
        "group": "Problem with csv File : Test Issue # 2:5",
    }


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
        "Problem with reading csv File : Test Issue # 3",
        5,
    )
    pipeline.finalise(token, handle)
    assert handle["issues"]["issue_0"] == {
        "index": None,
        "type": "data_product",
        "use_data_product": "test/csv",
        "use_component": None,
        "version": "0.0.1",
        "use_namespace": "testing",
        "issue": "Problem with reading csv File : Test Issue # 3",
        "severity": 5,
        "group": "Problem with reading csv File : Test Issue # 3:5",
    }


@pytest.mark.issue
def test_raise_issue_data_product_from_writes(
    token: str, script: str, test_dir: str
) -> None:
    config = os.path.join(test_dir, "write_csv.yaml")
    handle = pipeline.initialise(token, config, script)
    pipeline.raise_issue_by_data_product(
        handle,
        "test/csv",
        "0.0.1",
        "testing",
        "Problem with writing csv File : Test Issue # 4",
        5,
    )
    pipeline.finalise(token, handle)
    assert handle["issues"]["issue_0"] == {
        "index": None,
        "type": "data_product",
        "use_data_product": "test/csv",
        "use_component": None,
        "version": "0.0.1",
        "use_namespace": "testing",
        "issue": "Problem with writing csv File : Test Issue # 4",
        "severity": 5,
        "group": "Problem with writing csv File : Test Issue # 4:5",
    }
