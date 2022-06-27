import os
import shutil
from typing import Tuple

import netCDF4
import numpy as np
import pytest

import data_pipeline_api as pipeline
from data_pipeline_api.exceptions import AttributeSizeError, DataSizeError
from data_pipeline_api.fdp_utils import (
    create_2d_variables_in_group,
    create_group,
    create_nested_groups,
    prepare_headers,
    set_or_create_attr,
    write_2group,
)


@pytest.mark.netcdf
def test_create_group(
    test_dataset: netCDF4.Dataset, group_name: str = "root_group"
) -> None:
    create_group(test_dataset, group_name)

    assert group_name in test_dataset.groups


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
    test_dataset.close()


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


@pytest.mark.netcdf
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
    new_netCDF_file = netCDF_file
    groups = [grp for grp in args[0].split("/") if grp != ""]
    for group in groups:
        current = group
        new_netCDF_file = new_netCDF_file[current]
    assert new_netCDF_file[array_name].name == array_name
    assert new_netCDF_file[array_name].title == args[1]
    assert new_netCDF_file[array_name].units == "m"

    assert len(new_netCDF_file[array_name].ncattrs()) == 5

    assert isinstance(handle, dict)
    netCDF_file.close()
    # following code is to test what happens when you call write again with same parameters, execpt one (kg instead of m). array is updated.
    netcdf_handle = pipeline.write_array(
        data ** 2,
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
    new_netCDF_file = netCDF_file
    groups = [grp for grp in args[0].split("/") if grp != ""]
    for group in groups:
        current = group
        new_netCDF_file = new_netCDF_file[current]
    assert new_netCDF_file[array_name].name == array_name
    assert new_netCDF_file[array_name].title == args[1]
    assert new_netCDF_file[array_name].units == "kg"
    assert np.isclose(data, np.sqrt(new_netCDF_file[array_name][:]).data).all()
    assert len(new_netCDF_file[array_name].ncattrs()) == 5

    assert isinstance(handle, dict)
    netCDF_file.close()


@pytest.mark.netcdf
@pytest.mark.pipeline
@pytest.mark.parametrize(
    ("args"),
    [
        ("1/2/3", "test1"),
        ("first/second/third", "test2"),
        ("test_group", "test3"),
    ],
)
def test_write_array_append_data(
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
    new_netCDF_file = netCDF_file
    groups = [grp for grp in args[0].split("/") if grp != ""]
    for group in groups:
        current = group
        new_netCDF_file = new_netCDF_file[current]

    assert new_netCDF_file[array_name].name == array_name
    assert new_netCDF_file[array_name].title == args[1]
    assert new_netCDF_file[array_name].units == "m"

    assert len(new_netCDF_file[array_name].ncattrs()) == 5

    assert isinstance(handle, dict)
    netCDF_file.close()

    array_name = "array2"
    netcdf_handle = pipeline.write_array(
        data ** 2,
        "f",
        netcdf_handle,
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
    new_netCDF_file = netCDF_file
    groups = [grp for grp in args[0].split("/") if grp != ""]
    for group in groups:
        current = group
        new_netCDF_file = new_netCDF_file[current]
    assert new_netCDF_file[array_name].name == array_name
    assert new_netCDF_file[array_name].title == args[1]
    assert new_netCDF_file[array_name].units == "kg"
    assert np.isclose(data, np.sqrt(new_netCDF_file[array_name][:]).data).all()
    assert len(new_netCDF_file[array_name].ncattrs()) == 5

    assert isinstance(handle, dict)
    netCDF_file.close()


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
    test_dataset.close()


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
    test_dataset.close()


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    prepare_headers(
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

    write_2group(
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
    test_dataset.close()


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
        prepare_headers(
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

        write_2group(
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

    test_dataset.close()


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
        prepare_headers(
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

        write_2group(
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

    test_dataset.close()


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
        prepare_headers(
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

        write_2group(
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

    test_dataset.close()


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
        prepare_headers(
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

        write_2group(
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

    test_dataset.close()


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
        prepare_headers(
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

        write_2group(
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

    test_dataset.close()


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_write_again(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data

    prepare_headers(
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

    write_2group(
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
    write_2group(
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

    test_dataset.close()


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
        prepare_headers(
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

        write_2group(
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
        prepare_headers(
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
        write_2group(
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
    test_dataset.close()


@pytest.mark.netcdf
def test_create_nd_variables_in_group_w_attribute_shouldfail7(
    dataset_variable_3d: Tuple,
    test_dataset: netCDF4.Dataset,
    group_name: str = "root_group",
) -> None:

    create_group(test_dataset, group_name)

    xs, ys, zs, data = dataset_variable_3d
    data1 = 3 * data
    with pytest.raises(DataSizeError):
        prepare_headers(
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

        write_2group(
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
        # create_nd_variables_in_group_w_attribute(
        #     group=test_dataset[group_name],
        #     data_names=["data3d", "data3d_3"],
        #     attribute_data=[xs, ys, zs],
        #     data=[data, data1],
        #     attribute_type=["f", "f", "f"],
        #     attribute_var_name=["x3d", "y3d", "z3d"],
        #     title_names=[None],
        #     data_types=["f", "f"],
        #     dimension_names=[None],
        #     other_attribute_names=["a novel way to be me"],
        #     other_attribute_data=["lallero", "seciao"],
        # )
    test_dataset.close()
