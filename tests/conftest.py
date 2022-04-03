import math
import os
from typing import List, Tuple

import netCDF4
import numpy as np
import pytest

import data_pipeline_api.fdp_utils as fdp_utils


@pytest.fixture
def dataset_variable() -> Tuple[List, List, List]:
    nx = 10
    xs = [x for x in range(nx)]
    sins = [math.sin(math.radians(x)) for x in xs]
    coss = [math.cos(math.radians(x)) for x in xs]
    return xs, sins, coss


@pytest.fixture
def dataset_variable_3d() -> Tuple[List, List, List, List]:
    # # 3d    #data
    nx = 3
    ny = 4
    nz = 5

    xs = [x for x in range(nx)]
    ys = [y for y in range(ny)]
    zs = [z for z in range(nz)]

    data = np.zeros((nx, ny, nz))

    for i in range(nx):
        for j in range(ny):
            for k in range(nz):
                data[i][j][k] = math.sin(math.radians(i + j + k))

    return xs, ys, zs, data


@pytest.fixture
def dataset_variable_2d() -> Tuple[List, List, List]:
    # # 3d    #data
    nx = 10
    ny = 5

    xs = [x for x in range(nx)]
    ys = [y for y in range(ny)]

    data = np.zeros((nx, ny))

    for i in range(nx):
        for j in range(ny):
            data[i][j] = math.sin(math.radians(i + j))
    return xs, ys, data


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


@pytest.fixture
def read_config(test_dir: str) -> str:
    return os.path.join(test_dir, "read_csv.yaml")
