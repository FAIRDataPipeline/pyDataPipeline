import os
import shutil
from typing import List

import pytest
from netCDF4 import Dataset

import data_pipeline_api as pipeline


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
    array: Dataset = []
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
