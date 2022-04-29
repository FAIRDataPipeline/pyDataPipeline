import os
import shutil
from typing import Tuple

import netCDF4
import numpy as np
import pytest

import data_pipeline_api as pipeline
from data_pipeline_api.exceptions import AttributeSizeError, DataSizeError
from data_pipeline_api.fdp_utils import (
    create_1d_variables_in_group,
    create_2d_variables_in_group,
    create_3d_variables_in_group,
    create_group,
    create_nd_variables_in_group_w_attribute,
    create_nested_groups,
    create_variable_in_group,
    set_or_create_attr,
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


@pytest.mark.parametrize(
    ("args"),
    [("x", "sin"), ("xx", "sin")],
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
        variable_xname="x",
    )
    with pytest.raises(ValueError):
        create_1d_variables_in_group(
            test_dataset[group_name],
            [args[1], "cos"],
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
@pytest.mark.parametrize(
    ("args"),
    [
        ("data", "x", "y", "z"),
        ("data", "xx", "y", "z"),
        ("data", "xx", "yy", "z"),
        ("data", "xx", "yy", "zz"),
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
@pytest.mark.parametrize(
    ("args"),
    [
        ("data", "x", "y"),
        ("data", "xx", "y"),
        ("data", "xx", "yy"),
        ("data", "xx", "yy"),
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


# @pytest.mark.skip
@pytest.mark.pipeline
@pytest.mark.parametrize(
    ("args"),
    [
        ("1/2/3", "test1"),
        ("first/second/third", "test2"),
        ("test_group", "test3"),
    ],
)
def test_write_array(
    args: Tuple[str, str],
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    netcdf_config: str,  # data product
    script: str,
    token: str,
    array_name: str = "array",
) -> None:
    xs, ys, zs, data = dataset_variable_3d

    handle = pipeline.initialise(token, netcdf_config, script)
    netcdf_handle = pipeline.write_array(
        data,
        "f",
        handle,
        "test/netCDF",
        args[0],
        args[1],
        dimension_names=["x3d", "y3d", "z3d"],
        dimension_values=[xs, ys, zs],
        data_units_name=["m"],
        dimension_types=["f", "f", "f"],
        array_name=array_name,
    )

    netCDF_file = netCDF4.Dataset(netcdf_handle["path"], "r", format="NETCDF4")
    groups = [grp for grp in args[0].split("/") if grp != ""]
    for group in groups:
        current = group
        netCDF_file = netCDF_file[current]
    assert netCDF_file[array_name].name == array_name
    assert netCDF_file[array_name].title == args[1]
    assert netCDF_file[array_name].units == "m"

    assert len(netCDF_file[array_name].ncattrs()) == 5

    assert isinstance(handle, dict)
    # following code is to test what happens when you call write again with same parameters, execpt one (kg instead of m). array is updated.
    netcdf_handle = pipeline.write_array(
        3 * data,
        "f",
        handle,
        "test/netCDF",
        args[0],
        args[1],
        dimension_names=["x3d", "y3d", "z3d"],
        dimension_values=[xs, ys, zs],
        data_units_name=["kg"],
        dimension_types=["f", "f", "f"],
        array_name=array_name,
    )
    netCDF_file = netCDF4.Dataset(netcdf_handle["path"], "r", format="NETCDF4")
    groups = [grp for grp in args[0].split("/") if grp != ""]
    for group in groups:
        current = group
        netCDF_file = netCDF_file[current]
    assert netCDF_file[array_name].name == array_name
    assert netCDF_file[array_name].title == args[1]
    assert netCDF_file[array_name].units == "kg"
    assert netCDF_file[array_name][:] / netCDF_file[array_name][:] == 3
    assert len(netCDF_file[array_name].ncattrs()) == 5

    assert isinstance(handle, dict)


@pytest.mark.netcdf
@pytest.mark.parametrize(
    ("args"),
    [
        ("data", "x", "y"),
        ("data", "xx", "y"),
        ("data", "xx", "yy"),
        ("data", "xx", "yy"),
    ],
)
def test_set_attribute(
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
    set_or_create_attr(
        test_dataset[group_name]["data"], "test_attribute", range(10)
    )
    assert "test_attribute" in test_dataset[group_name]["data"].ncattrs()
    assert len(test_dataset[group_name]["data"].test_attribute) == 10


@pytest.mark.netcdf
@pytest.mark.parametrize(
    ("args"),
    [
        ("data", "x", "y"),
        ("data", "xx", "y"),
        ("data", "xx", "yy"),
        ("data", "xx", "yy"),
    ],
)
def test_set_attribute_already_exists(
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
    set_or_create_attr(
        test_dataset[group_name]["data"], "test_attribute", range(10)
    )
    assert "test_attribute" in test_dataset[group_name]["data"].ncattrs()
    assert len(test_dataset[group_name]["data"].test_attribute) == 10
    set_or_create_attr(
        test_dataset[group_name]["data"], "test_attribute", range(5)
    )
    assert "test_attribute" in test_dataset[group_name]["data"].ncattrs()
    assert len(test_dataset[group_name]["data"].test_attribute) == 5


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    create_nd_variables_in_group_w_attribute(
        group=test_dataset[group_name],
        data_names=["data3d", "data3d_3"],
        attribute_data=[xs, ys, zs],
        data=[data, data1],
        attribute_type=["f", "f", "f"],
        attribute_var_name=["x3d", "y3d", "z3d"],
        title_names=[None],
        data_types=["f", "f"],
        dimension_names=[None],
        other_attribute_names=["a novel way to be me"],
        other_attribute_data=["lallero"],
    )

    assert len(test_dataset[group_name].variables.keys()) == 2
    assert len(test_dataset[group_name].dimensions.keys()) == 3
    assert len(test_dataset[group_name]["data3d"].ncattrs()) == 6
    assert test_dataset[group_name]["data3d"].units == "Unknown"
    assert test_dataset[group_name]["data3d"].title == "Unknown"
    assert len(test_dataset[group_name]["data3d_3"].ncattrs()) == 6


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail1(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(AttributeSizeError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail2(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(DataSizeError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail3(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(AttributeSizeError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=["data1"],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail4(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(AttributeSizeError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=["dim1"],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail5(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    xs = np.delete(xs, 0)
    data1 = 3 * data
    with pytest.raises(ValueError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail6(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(ValueError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail7(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(ValueError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d1", "y3d1", "z3d1"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero"],
        )


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail8(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(DataSizeError):
        create_nd_variables_in_group_w_attribute(
            group=test_dataset[group_name],
            data_names=["data3d", "data3d_3"],
            attribute_data=[xs, ys, zs],
            data=[data, data1],
            attribute_type=["f", "f", "f"],
            attribute_var_name=["x3d", "y3d", "z3d"],
            title_names=[None],
            data_types=["f", "f"],
            dimension_names=[None],
            other_attribute_names=["a novel way to be me"],
            other_attribute_data=["lallero", "seciao"],
        )
