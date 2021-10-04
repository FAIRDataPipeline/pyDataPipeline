# Test fdp_utils

import pytest
import os
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils

test_dir = os.path.join(os.path.dirname(__file__), "ext")

# Test is_file()
def test_is_file_exists():
    test_file = os.path.join(test_dir, 'test.csv')
    #print(test_dir)
    assert fdp_utils.is_file(test_file)

@pytest.mark.parametrize('file_path', [
    'file_not_found',
    '',
    #None
])
def test_is_file_not_exists(file_path):
    assert not fdp_utils.is_file(file_path)

@pytest.mark.parametrize('file_path', [
    os.path.join(test_dir, 'read_csv.yaml'),
    os.path.join(test_dir, 'write_csv.yaml')
])
def test_is_yaml(file_path):
    assert fdp_utils.is_yaml(file_path)

@pytest.mark.parametrize('file_path', [
    'file_not_found',
    '',
    #None,
    #os.path.join(test_dir, 'test.csv')
])
def test_is_yaml_not(file_path):
    assert not fdp_utils.is_yaml(file_path)

@pytest.mark.parametrize('file_path', [
    os.path.join(test_dir, 'read_csv.yaml'),
    os.path.join(test_dir, 'write_csv.yaml')
])
def test_is_valid_yaml(file_path):
    assert fdp_utils.is_yaml(file_path)

@pytest.mark.parametrize('file_path', [
    'file_not_found',
    '',
    #None,
    #os.path.join(test_dir, 'test.csv')
])
def test_is_valid_yaml_not(file_path):
    assert not fdp_utils.is_valid_yaml(file_path)