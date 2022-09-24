# Test fdp_utils

import datetime
import os
import platform

import pytest
from _pytest.fixtures import FixtureRequest

import data_pipeline_api.fdp_utils as fdp_utils


@pytest.fixture
def test_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "ext")


@pytest.fixture
def read_csv_path(test_dir: str) -> str:
    return os.path.join(test_dir, "read_csv.yaml")


@pytest.fixture
def write_csv_path(test_dir: str) -> str:
    return os.path.join(test_dir, "write_csv.yaml")


# Test is_file()
@pytest.mark.utilities
def test_is_file_exists(test_dir: str) -> None:
    test_file = os.path.join(test_dir, "test.csv")
    assert fdp_utils.is_file(test_file)


@pytest.mark.utilities
@pytest.mark.parametrize(
    "file_path",
    [
        "file_not_found",
        "",
        # None
    ],
)
def test_is_file_not_exists(file_path: str) -> None:
    assert not fdp_utils.is_file(file_path)


@pytest.mark.utilities
@pytest.mark.parametrize("file_path", ["read_csv_path", "write_csv_path"])
def test_is_yaml(file_path: str, request: FixtureRequest) -> None:
    file_path = request.getfixturevalue(file_path)
    assert fdp_utils.is_yaml(file_path)


@pytest.mark.utilities
@pytest.mark.parametrize(
    "file_path",
    [
        "file_not_found",
        "",
        # None,
        # os.path.join(test_dir, 'test.csv')
    ],
)
def test_is_yaml_not(file_path: str) -> None:
    assert not fdp_utils.is_yaml(file_path)


@pytest.mark.utilities
@pytest.mark.parametrize("file_path", ["read_csv_path", "write_csv_path"])
def test_is_valid_yaml(file_path: str, request: FixtureRequest) -> None:
    file_path = request.getfixturevalue(file_path)
    assert fdp_utils.is_yaml(file_path)


@pytest.mark.utilities
@pytest.mark.parametrize(
    "file_path",
    [
        "file_not_found",
        "",
        # None,
        # os.path.join(test_dir, 'test.csv')
    ],
)
def test_is_valid_yaml_not(file_path: str) -> None:
    assert not fdp_utils.is_valid_yaml(file_path)


@pytest.mark.utilities
def test_read_token(test_dir: str) -> None:
    token = os.path.join(test_dir, "test_token")
    assert (
        fdp_utils.read_token(token)
        == "1a2b3c4d5e6d7f8a9b8c7d6e5f1a2b3c4d5e6d7f"
    )


@pytest.mark.utilities
def test_get_token(test_dir: str) -> None:
    token = os.path.join(test_dir, "test_token")
    assert (
        fdp_utils.get_token(token)
        == "1a2b3c4d5e6d7f8a9b8c7d6e5f1a2b3c4d5e6d7f"
    )


@pytest.mark.utilities
def test_read_token_get_token(test_dir: str) -> None:
    token = os.path.join(test_dir, "test_token")
    assert fdp_utils.read_token(token) == fdp_utils.get_token(token)


@pytest.fixture
def token() -> str:
    return fdp_utils.read_token(
        os.path.join(os.path.expanduser("~"), ".fair/registry/token")
    )


@pytest.mark.utilities
def test_get_file_hash(test_dir: str) -> None:
    file_path = os.path.join(test_dir, "test.csv")
    if platform.system() == "Windows":
        assert (
            fdp_utils.get_file_hash(file_path)
            == "1f71db9d999ded6d15c82a49b3ad7472cdcb19aa"
        )
    else:
        assert (
            fdp_utils.get_file_hash(file_path)
            == "51345410c236d375ccf47149196746bc7f4db29d"
        )


@pytest.mark.utilities
def test_random_hash_is_string() -> None:
    assert type(fdp_utils.random_hash()) == str


@pytest.mark.utilities
def test_random_hash_length() -> None:
    assert len(fdp_utils.random_hash()) == 40


@pytest.mark.utilities
def test_extract_id() -> None:
    assert fdp_utils.extract_id("http://localhost:8000/api/object/85") == "85"


@pytest.mark.utilities
def test_extract_id_should_fail() -> None:
    with pytest.raises(IndexError):
        fdp_utils.extract_id("")


@pytest.mark.utilities
def test_get_headers() -> None:
    assert type(fdp_utils.get_headers()) == dict
    headers = {"Accept": "application/json; version=" + "1.0.0"}
    assert headers == fdp_utils.get_headers()


@pytest.mark.utilities
def test_get_headers_with_token(token: str) -> None:
    headers = fdp_utils.get_headers(token=token)
    assert headers["Authorization"] == "token " + token


@pytest.mark.utilities
def test_get_headers_post() -> None:
    headers = fdp_utils.get_headers(request_type="post")
    assert headers["Content-Type"] == "application/json"


@pytest.mark.utilities
def test_get_headers_api_version() -> None:
    headers = fdp_utils.get_headers(api_version="0.0.1")
    assert headers["Accept"] == "application/json; version=0.0.1"


@pytest.fixture
def url() -> str:
    if platform.system() == "Windows":
        return "http://127.0.0.1:8000/api"
    return "http://localhost:8000/api"


@pytest.fixture
def storage_root_test(token: str, url: str, scope: str = "module") -> dict:
    return fdp_utils.post_entry(
        token=token,
        url=url,
        data={"root": "https://storage-root-test.com"},
        endpoint="storage_root",
    )


@pytest.mark.utilities
def test_post_entry(token: str, url: str) -> None:
    storage_root = fdp_utils.post_entry(
        token=token,
        url=url,
        data={"root": "https://test.com"},
        endpoint="storage_root",
    )
    assert type(storage_root) == dict


@pytest.mark.utilities
def test_post_entry_409(token: str, url: str) -> None:
    storage_root = fdp_utils.post_entry(
        token=token,
        url=url,
        data={"root": "https://test.com"},
        endpoint="storage_root",
    )
    assert type(storage_root) == dict


@pytest.mark.utilities
def test_post_entry_equal(token: str, url: str) -> None:
    storage_root = fdp_utils.post_entry(
        token=token,
        url=url,
        data={"root": "https://test_2.com"},
        endpoint="storage_root",
    )
    storage_root_2 = fdp_utils.post_entry(
        token=token,
        url=url,
        data={"root": "https://test_2.com"},
        endpoint="storage_root",
    )
    assert storage_root == storage_root_2


@pytest.mark.utilities
def test_post_entry_500(token: str, url: str) -> None:
    with pytest.raises(Exception):
        fdp_utils.post_entry(
            token=token,
            url=url,
            data={"root": "https://test.com"},
            endpoint="non_existant",
        )


@pytest.mark.utilities
def test_get_entry(url: str, token: str, storage_root_test: dict) -> None:
    entry = fdp_utils.get_entry(
        url=url,
        query={"root": "https://storage-root-test.com"},
        token=token,
        endpoint="storage_root",
    )
    assert entry[0] == storage_root_test


@pytest.mark.utilities
def test_get_entry_author(url: str, token: str) -> None:

    results = fdp_utils.get_entry(
        url=url,
        query={"user": 2},
        token=token,
        endpoint="user_author",
    )
    with pytest.raises(IndexError):
        _ = results[0]


@pytest.mark.utilities
def test_get_entry_users(url: str, token: str) -> None:

    results = fdp_utils.get_entry(
        url=url,
        query={"username": "admin1"},
        token=token,
        endpoint="users",
    )
    with pytest.raises(IndexError):
        _ = results[0]


@pytest.mark.utilities
def test_get_entity(url: str, storage_root_test: dict) -> None:
    entity = fdp_utils.get_entity(
        url=url,
        endpoint="storage_root",
        id=int(fdp_utils.extract_id(storage_root_test["url"])),
    )
    assert entity == storage_root_test


@pytest.mark.apiversion
def test_wrong_api_version(token: str, url: str) -> None:
    with pytest.raises(Exception):
        fdp_utils.post_entry(
            token=token,
            url=url,
            data={"root": "https://test.com"},
            endpoint="storage_root",
            api_version="2.2.2",
        )


@pytest.mark.apiversion
def test_wrong_api_version_get(token: str, url: str) -> None:
    with pytest.raises(Exception):
        fdp_utils.get_entry(
            token=token,
            url=url,
            query={"root": "https://test.com"},
            endpoint="storage_root",
            api_version="3.0.0",
        )


@pytest.mark.utilities
def test_get_entity_with_token(
    url: str, storage_root_test: dict, token: str
) -> None:
    entity = fdp_utils.get_entity(
        url=url,
        endpoint="storage_root",
        id=int(fdp_utils.extract_id(storage_root_test["url"])),
        token=token,
    )
    assert entity == storage_root_test


@pytest.mark.utilities
def test_get_entity_non_200(url: str, storage_root_test: dict) -> None:
    with pytest.raises(Exception):
        fdp_utils.get_entity(
            url=url,
            endpoint="non_existant",
            id=int(fdp_utils.extract_id(storage_root_test["url"])),
        )


@pytest.fixture
def model_config(url: str, token: str, scope: str = "module") -> dict:
    return fdp_utils.post_entry(
        url=url,
        endpoint="object",
        data={"description": "Model Config Test"},
        token=token,
    )


@pytest.fixture
def submission_script(url: str, token: str, scope: str = "module") -> dict:
    return fdp_utils.post_entry(
        url=url,
        endpoint="object",
        data={"description": "Submission Script Test"},
        token=token,
    )


@pytest.fixture
def input_1(url: str, token: str, scope: str = "module") -> dict:
    return fdp_utils.post_entry(
        url=url,
        endpoint="object",
        data={"description": "Input 1"},
        token=token,
    )


@pytest.fixture
def input_1_component(input_1: dict) -> str:
    return input_1["components"][0]


@pytest.fixture
def code_run(
    url: str,
    model_config: dict,
    submission_script: dict,
    token: str,
    scope: str = "module",
) -> dict:
    return fdp_utils.post_entry(
        url=url,
        endpoint="code_run",
        data={
            "description": "Test Code Run",
            "run_date": str(datetime.datetime.now()),
            "model_config": model_config["url"],
            "submission_script": submission_script["url"],
            "input_urls": [],
            "output_urls": [],
        },
        token=token,
    )


def test_patch_entry(
    code_run: dict, input_1_component: str, token: str, url: str
) -> None:
    fdp_utils.patch_entry(
        url=code_run["url"],
        data={"inputs": [input_1_component]},
        token=token,
    )
    code_run_updated = fdp_utils.get_entity(
        url=url,
        endpoint="code_run",
        id=int(fdp_utils.extract_id(code_run["url"])),
    )
    assert input_1_component in code_run_updated["inputs"]


def test_patch_entry_non_200(url: str, token: str) -> None:
    with pytest.raises(Exception):
        fdp_utils.patch_entry(
            url=url + "/api/users/1",
            data={"name": "New Name"},
            token=token,
        )


def test_post_storage_root_with_local(url: str, token: str) -> None:
    storage_root = fdp_utils.post_storage_root(
        token=token, url=url, data={"root": f'{os.sep}test{os.sep}test', "local": True}
    )
    assert storage_root["root"] == f'file://{os.sep}test{os.sep}test{os.sep}'
