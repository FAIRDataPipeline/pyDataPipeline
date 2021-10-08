# Test fdp_utils

import pytest
import os
import datetime
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
        data={
            'root': 'https://test.com'
        },
        endpoint= 'storage_root'
    )
    assert type(storage_root) == dict

def test_post_entry_409():
    global storage_root2
    storage_root2 = fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://test.com'
        },
        endpoint= 'storage_root'
    )
    assert type(storage_root2) == dict

def test_post_entry_equal():
    assert storage_root == storage_root2

def test_post_entry_500():
    with pytest.raises(Exception):
        fdp_utils.post_entry(
            token = token,
            url= url,
            data={
                'root': 'https://test.com'
            },
            endpoint= 'non_existant'
        )

def test_get_entry():
    entry = fdp_utils.get_entry(
        url = url,
        query= {
            'root': 'https://test.com'
        },
        token = token,
        endpoint= 'storage_root'
    )
    assert entry[0] == storage_root

def test_get_entity():
    entity = fdp_utils.get_entity(
        url = url,
        endpoint='storage_root',
        id = fdp_utils.extract_id(storage_root['url']))
    assert entity == storage_root

def test_get_entity_with_token():
    entity = fdp_utils.get_entity(
        url = url,
        endpoint= 'storage_root',
        id = fdp_utils.extract_id(storage_root['url']),
        token = token)
    assert entity == storage_root

def test_get_entity_non_200():
    with pytest.raises(Exception):
        fdp_utils.get_entity(
            url = url,
            endpoint='non_existant',
            id = fdp_utils.extract_id(storage_root['url']))

model_config = fdp_utils.post_entry(
    url = url,
    endpoint= 'object',
    data={
        'description': 'Model Config Test'
    },
    token = token
)

submission_script = fdp_utils.post_entry(
    url = url,
    endpoint= 'object',
    data={
        'description': 'Submission Script Test'
    },
    token = token
)

input_1 = fdp_utils.post_entry(
    url = url,
    endpoint= 'object',
    data={
        'description': 'Input 1'
    },
    token = token
)

input_1_component = input_1['components'][0]

code_run = fdp_utils.post_entry(
    url = url,
    endpoint= 'code_run',
    data={
        'description': 'Test Code Run',
        'run_date': str(datetime.datetime.now()),
        'model_config': model_config['url'],
        'submission_script': submission_script['url'],
        'input_urls': [],
        'output_urls': []
    },
    token = token
)

def test_patch_entry():
    fdp_utils.patch_entry(
        url = code_run['url'],
        data = {
            'inputs': [input_1_component]
        },
        token = token,
    )
    code_run_updated = fdp_utils.get_entity(
        url= url,
        endpoint= 'code_run',
        id = fdp_utils.extract_id(code_run['url'])
    )
    assert input_1_component in code_run_updated['inputs']

def test_patch_entry_non_200():
    with pytest.raises(Exception):
        fdp_utils.patch_entry(
            url = url + '/api/users/1',
            data = {
                'name': 'New Name'
            },
            token = token,
        )

def test_post_storage_root_with_local():
    storage_root = fdp_utils.post_storage_root(
        token = token,
        url= url,
        data={
            'root': '/test/test',
            'local': True
        }
    )
    assert storage_root['root'] == 'file:///test/test'