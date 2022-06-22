import itertools
import math
import os
from typing import Tuple

import netCDF4
import numpy as np
import pytest

import data_pipeline_api.fdp_utils as fdp_utils


@pytest.fixture
def dataset_variable() -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    nx = 10
    xs = np.array(list(range(nx)))
    sins = np.array([math.sin(math.radians(x)) for x in xs])
    coss = np.array([math.cos(math.radians(x)) for x in xs])
    return xs, np.array(sins), np.array(coss)


@pytest.fixture
def dataset_variable_3d() -> Tuple[
    np.ndarray, np.ndarray, np.ndarray, np.ndarray
]:
    # # 3d    #data
    nx = 3
    ny = 4
    nz = 5

    xs = np.array(list(range(nx)))
    ys = np.array(list(range(ny)))
    zs = np.array(list(range(nz)))

    data = np.zeros((nx, ny, nz))

    for i, j, k in itertools.product(range(nx), range(ny), range(nz)):
        data[i][j][k] = math.sin(math.radians(i + j + k))

    return xs, ys, zs, np.array(data)


@pytest.fixture
def dataset_variable_2d() -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # # 3d    #data
    nx = 10
    ny = 5

    xs = np.array(list(range(nx)))
    ys = np.array(list(range(ny)))

    data = np.zeros((nx, ny))

    for i, j in itertools.product(range(nx), range(ny)):
        data[i][j] = math.sin(math.radians(i + j))
    return xs, ys, np.array(data)


@pytest.fixture
def test_dataset() -> netCDF4.Dataset:

    if not os.path.exists("tmp"):
        os.makedirs("tmp")
    path = os.path.join(os.curdir, "tmp")
    filename = f"test-{fdp_utils.random_hash()}.nc"
    return netCDF4.Dataset(f"{path}{os.sep}{filename}", "w", format="NETCDF4")


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
def netcdf_config(test_dir: str) -> str:
    return os.path.join(test_dir, "write_netcdf.yaml")


@pytest.fixture
def read_config(test_dir: str) -> str:
    return os.path.join(test_dir, "read_csv.yaml")
