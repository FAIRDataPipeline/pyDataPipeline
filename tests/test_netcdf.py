import math
import os
import shutil
from typing import List

import netCDF4
import numpy as np
import pytest

import data_pipeline_api as pipeline
from data_pipeline_api.fdp_utils import (
    create_1d_variables_in_group,
    create_2d_variables_in_group,
    create_3d_variables_in_group,
    create_group,
    create_nested_groups,
    create_variable_in_group,
)


@pytest.mark.netcdf
def test_netcdf_write_wrappers(
    test_dataset: netCDF4.Dataset, group_name: str = "root_group"
) -> None:
    create_group(test_dataset, group_name)

    assert group_name in test_dataset.groups
    nx = 10
    xs = [x for x in range(nx)]
    sins = [math.sin(math.radians(x)) for x in xs]
    coss = [math.cos(math.radians(x)) for x in xs]
    create_variable_in_group(test_dataset[group_name], "first_var", xs, "f")

    assert all(test_dataset[group_name]["first_var"][:]) == all(xs)
    assert len(test_dataset[group_name]["first_var"][:]) == len(xs)

    create_1d_variables_in_group(
        test_dataset[group_name],
        ["sin1", "cos1"],
        xs,
        [sins, coss],
        ["f", "f"],
        variable_xname="xx",
    )
    assert all(test_dataset[group_name]["sin1"][:]) == all(sins)
    assert len(test_dataset[group_name]["sin1"][:]) == len(xs)
    assert all(test_dataset[group_name]["cos1"][:]) == all(coss)
    assert len(test_dataset[group_name]["cos1"][:]) == len(xs)

    # 3d    #data
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
    create_3d_variables_in_group(
        test_dataset[group_name],
        ["3cd"],
        xs,
        ys,
        zs,
        [data],
        ["f"],
        variable_xname="x3",
        variable_yname="y3",
        variable_zname="z3",
    )
    for var in [("x3", "xs"), ("y3", "ys"), ("z3", "zs")]:
        assert var[0] in test_dataset[group_name].variables.keys()
        assert all(test_dataset[group_name][var[0]][:]) == all(vars()[var[1]])
        assert test_dataset[group_name]["3cd"][:].shape == (nx, ny, nz)

    # 2d data
    # data
    nx = 10
    ny = 5

    xs = [x for x in range(nx)]
    ys = [y for y in range(ny)]

    data = np.zeros((nx, ny))

    for i in range(nx):
        for j in range(ny):
            data[i][j] = math.sin(math.radians(i + j))
    create_2d_variables_in_group(
        test_dataset[group_name],
        ["2cd"],
        xs,
        ys,
        [data],
        ["f", "f"],
        variable_xname="x2",
        variable_yname="y2",
    )
    for var in [("x2", "xs"), ("y2", "ys")]:
        assert var[0] in test_dataset[group_name].variables.keys()
        assert all(test_dataset[group_name][var[0]][:]) == all(vars()[var[1]])
        assert test_dataset[group_name]["2cd"][:].shape == (nx, ny)

    # test creation of nest groups
    path = "/1/2/3/"
    create_nested_groups(test_dataset, path)
    groups = [grp for grp in path.split("/") if grp != ""]
    new_dts = test_dataset
    for grp in groups:
        assert grp in new_dts.groups.keys()
        new_dts = new_dts[grp]


@pytest.mark.pipeline
def test_read_array(
    token: str, config: str, script: str, test_dir: str
) -> None:
    handle = pipeline.initialise(token, config, script)
    tmp_csv = os.path.join(test_dir, "test.csv")
    link_write = pipeline.link_write(handle, "test/csv")
    shutil.copy(tmp_csv, link_write)
    pipeline.finalise(token, handle)

    config = os.path.join(test_dir, "read_csv.yaml")
    handle = pipeline.initialise(token, config, script)

    path = pipeline.read_array(handle, "test/csv", "component")
    assert path


@pytest.mark.pipeline
def test_write_array(
    config: str,
    script: str,
    token: str,
    data_product: str = "",
    component: str = "",
    description: str = "",
    dimension_names: List = [],
    dimension_values: List = [],
    dimension_units: List = [],
    units: str = "",
) -> None:
    array: netCDF4.Dataset = []
    handle = pipeline.initialise(token, config, script)
    output = pipeline.write_array(
        array,
        handle,
        data_product,
        component,
        description,
        dimension_names,
        dimension_values,
        dimension_units,
        units,
    )
    assert output is False
