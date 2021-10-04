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

def test_read_token():
    token = os.path.join(test_dir, 'test_token')
    assert fdp_utils.read_token(token) == "1a2b3c4d5e6d7f8a9b8c7d6e5f1a2b3c4d5e6d7f"

def test_get_token():
    token = os.path.join(test_dir, 'test_token')
    assert fdp_utils.get_token(token) == "1a2b3c4d5e6d7f8a9b8c7d6e5f1a2b3c4d5e6d7f"

def test_read_token_get_token():
    token = os.path.join(test_dir, 'test_token')
    assert fdp_utils.read_token(token) == fdp_utils.get_token(token)

token = fdp_utils.read_token(os.path.join(os.path.expanduser('~'), '.fair/registry/token'))

def test_get_file_hash():
    file_path = os.path.join(test_dir, 'test.csv')
    assert fdp_utils.get_file_hash(file_path) == '51345410c236d375ccf47149196746bc7f4db29d'

def test_random_hash_is_string():
    assert type(fdp_utils.random_hash()) == str

def test_random_hash_length():
    assert len(fdp_utils.random_hash()) == 40

def test_extract_id():
    assert fdp_utils.extract_id('http://localhost:8000/api/object/85') == '85'

storage_root = None
storage_root2 = None
url = 'http://localhost:8000/api'

def test_post_entry():
    global storage_root
    storage_root = fdp_utils.post_entry(
        token = token,
        url= url,
        endpoint= 'storage_root',
        data={
            'root': 'https://test.com'
        }
    )
    assert type(storage_root) == dict

def test_post_entry_409():
    global storage_root2
    storage_root2 = fdp_utils.post_entry(
        token = token,
        url= url,
        endpoint= 'storage_root',
        data={
            'root': 'https://test.com'
        }
    )
    assert type(storage_root2) == dict

def test_post_entry_equal():
    assert storage_root == storage_root2

def test_get_entry():
    entry = fdp_utils.get_entry(
        url = url,
        endpoint= 'storage_root',
        query= {
            'root': 'https://test.com'
        },
        token = token
    )
    assert entry[0] == storage_root