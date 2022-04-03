import os
import shutil
from typing import List, Tuple

import netCDF4
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
def test_create_group(
    test_dataset: netCDF4.Dataset, group_name: str = "root_group"
) -> None:
    create_group(test_dataset, group_name)

    assert group_name in test_dataset.groups


@pytest.mark.netcdf
def test_create_variable(
    dataset_variable: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, _, _ = dataset_variable
    create_variable_in_group(test_dataset[group_name], "first_var", xs, "f")

    assert all(test_dataset[group_name]["first_var"][:]) == all(xs)
    assert len(test_dataset[group_name]["first_var"][:]) == len(xs)


@pytest.mark.netcdf
def test_creat_variable_already_exists_should_fail(
    dataset_variable: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, _, _ = dataset_variable
    create_variable_in_group(test_dataset[group_name], "first_var", xs, "f")
    with pytest.raises(ValueError):
        create_variable_in_group(
            test_dataset[group_name], "first_var", xs, "f"
        )


@pytest.mark.netcdf
def test_create_1d_variables(
    dataset_variable: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, sins, coss = dataset_variable
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


@pytest.mark.skip
@pytest.mark.parametrize(
    ("args"),
    [
        (
            ("sin1", "sin1"),
            ("xx", "xxbis"),
            ("x3", "x3"),
            ("y3", "y3"),
            ("z3", "z3"),
            ("data3d", "data3d"),
            ("x2", "x2"),
            ("y2", "y2"),
            ("data2d", "data2d"),
        ),
    ],
)
@pytest.mark.netcdf
def test_create_1d_variables_already_exists_should_fail(
    args: str,
    dataset_variable: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, sins, coss = dataset_variable
    create_1d_variables_in_group(
        test_dataset[group_name],
        ["sin", "cos"],
        xs,
        [sins, coss],
        ["f", "f"],
        variable_xname=args[0],
    )
    with pytest.raises(ValueError):
        create_1d_variables_in_group(
            test_dataset[group_name],
            ["sin", "cos"],
            xs,
            [sins, coss],
            ["f", "f"],
            variable_xname=args[0],
        )


def test_create_3d_variable(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, ys, zs, data = dataset_variable_3d

    create_3d_variables_in_group(
        test_dataset[group_name],
        ["data"],
        xs,
        ys,
        zs,
        [data],
        ["f"],
        variable_xname="x",
        variable_yname="y",
        variable_zname="z",
    )
    nx = len(xs)
    ny = len(ys)
    nz = len(zs)
    for var in [("x", "xs"), ("y", "ys"), ("z", "zs")]:
        assert var[0] in test_dataset[group_name].variables.keys()
        assert all(test_dataset[group_name][var[0]][:]) == all(vars()[var[1]])
        assert test_dataset[group_name]["data"][:].shape == (nx, ny, nz)


@pytest.mark.netcdf
@pytest.mark.skip
@pytest.mark.parametrize(
    ("args"),
    [
        (
            ("sin1", "sin1"),
            ("xx", "xxbis"),
            ("x3", "x3"),
            ("y3", "y3"),
            ("z3", "z3"),
            ("data3d", "data3d"),
            ("x2", "x2"),
            ("y2", "y2"),
            ("data2d", "data2d"),
        ),
    ],
)
def test_create_3d_variable_already_exist_should_fail(
    args: Tuple[str, str, str, str],
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, ys, zs, data = dataset_variable_3d

    create_3d_variables_in_group(
        test_dataset[group_name],
        ["data"],
        xs,
        ys,
        zs,
        [data],
        ["f"],
        variable_xname="x",
        variable_yname="y",
        variable_zname="z",
    )

    with pytest.raises(ValueError):
        create_3d_variables_in_group(
            test_dataset[group_name],
            [args[0]],
            xs,
            ys,
            zs,
            [data],
            ["f"],
            variable_xname=args[1],
            variable_yname=args[2],
            variable_zname=args[3],
        )


def test_create_2d_variables(
    dataset_variable_2d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, ys, data = dataset_variable_2d

    create_2d_variables_in_group(
        test_dataset[group_name],
        ["data"],
        xs,
        ys,
        [data],
        ["f", "f"],
        variable_xname="x",
        variable_yname="y",
    )
    nx = len(xs)
    ny = len(ys)

    for var in [("x", "xs"), ("y", "ys")]:
        assert var[0] in test_dataset[group_name].variables.keys()
        assert all(test_dataset[group_name][var[0]][:]) == all(vars()[var[1]])
        assert test_dataset[group_name]["data"][:].shape == (nx, ny)


@pytest.mark.netcdf
@pytest.mark.skip
@pytest.mark.parametrize(
    ("args"),
    [
        (
            ("sin1", "sin1"),
            ("xx", "xxbis"),
            ("x3", "x3"),
            ("y3", "y3"),
            ("z3", "z3"),
            ("data3d", "data3d"),
            ("x2", "x2"),
            ("y2", "y2"),
            ("data2d", "data2d"),
        ),
    ],
)
def test_create_2d_variables_already_exist_should_fail(
    args: Tuple[str, str, str],
    dataset_variable_2d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    create_group(test_dataset, group_name)
    xs, ys, data = dataset_variable_2d

    create_2d_variables_in_group(
        test_dataset[group_name],
        ["data"],
        xs,
        ys,
        [data],
        ["f", "f"],
        variable_xname="x",
        variable_yname="y",
    )
    with pytest.raises(ValueError):
        create_2d_variables_in_group(
            test_dataset[group_name],
            [args[0]],
            xs,
            ys,
            [data],
            ["f", "f"],
            variable_xname=args[1],
            variable_yname=args[2],
        )


@pytest.mark.netcdf
def test_create_nested_groups(
    dataset_variable: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:
    # test creation of nest groups
    path = "/1/2/3/"
    create_nested_groups(test_dataset, path)
    groups = [grp for grp in path.split("/") if grp != ""]
    new_dts = test_dataset
    for grp in groups:
        assert grp in new_dts.groups.keys()
        new_dts = new_dts[grp]


@pytest.mark.xfail(reason="read_array function not yet implemented")
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


@pytest.mark.xfail(reason="write_array function not yet implemented")
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
