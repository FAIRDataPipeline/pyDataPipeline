import os

import netCDF4
import pytest

import data_pipeline_api.fdp_utils as fdp_utils


@pytest.fixture
def test_dataset() -> netCDF4.Dataset:
    path = os.curdir
    if os.path.exists(f"{path}test.nc"):
        os.remove(f"{path}test.nc")
    return netCDF4.Dataset(f"{path}test.nc", "w", format="NETCDF4")


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
