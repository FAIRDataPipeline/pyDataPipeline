# Test fdp_utils

import pytest
import os
import datetime
import platform
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils

@pytest.fixture
def test_dir():
    return os.path.join(os.path.dirname(__file__), "ext")

@pytest.fixture
def read_csv_path(test_dir):
    return os.path.join(test_dir, 'read_csv.yaml')
    
@pytest.fixture
def write_csv_path(test_dir):
    return os.path.join(test_dir, 'write_csv.yaml')

# Test is_file()
def test_is_file_exists(test_dir):
    test_file = os.path.join(test_dir, 'test.csv')
    assert fdp_utils.is_file(test_file)

@pytest.mark.parametrize('file_path', [
    'file_not_found',
    '',
    #None
])
def test_is_file_not_exists(file_path):
    assert not fdp_utils.is_file(file_path)

@pytest.mark.parametrize('file_path', ["read_csv_path", "write_csv_path"])
def test_is_yaml(file_path, request):
    file_path = request.getfixturevalue(file_path)
    assert fdp_utils.is_yaml(file_path)

@pytest.mark.parametrize('file_path', [
    'file_not_found',
    '',
    #None,
    #os.path.join(test_dir, 'test.csv')
])
def test_is_yaml_not(file_path):
    assert not fdp_utils.is_yaml(file_path)

@pytest.mark.parametrize('file_path', ["read_csv_path", "write_csv_path"])
def test_is_valid_yaml(file_path, request):
    file_path = request.getfixturevalue(file_path)
    assert fdp_utils.is_yaml(file_path)

@pytest.mark.parametrize('file_path', [
    'file_not_found',
    '',
    #None,
    #os.path.join(test_dir, 'test.csv')
])
def test_is_valid_yaml_not(file_path):
    assert not fdp_utils.is_valid_yaml(file_path)

def test_read_token(test_dir):
    token = os.path.join(test_dir, 'test_token')
    assert fdp_utils.read_token(token) == "1a2b3c4d5e6d7f8a9b8c7d6e5f1a2b3c4d5e6d7f"

def test_get_token(test_dir):
    token = os.path.join(test_dir, 'test_token')
    assert fdp_utils.get_token(token) == "1a2b3c4d5e6d7f8a9b8c7d6e5f1a2b3c4d5e6d7f"

def test_read_token_get_token(test_dir):
    token = os.path.join(test_dir, 'test_token')
    assert fdp_utils.read_token(token) == fdp_utils.get_token(token)

@pytest.fixture
def token():
    return fdp_utils.read_token(os.path.join(os.path.expanduser('~'), '.fair/registry/token'))

def test_get_file_hash(test_dir):
    file_path = os.path.join(test_dir, 'test.csv')
    if platform.system() == 'Windows':
        assert fdp_utils.get_file_hash(file_path) == '1f71db9d999ded6d15c82a49b3ad7472cdcb19aa'
    else:
        assert fdp_utils.get_file_hash(file_path) == '51345410c236d375ccf47149196746bc7f4db29d'

def test_random_hash_is_string():
    assert type(fdp_utils.random_hash()) == str

def test_random_hash_length():
    assert len(fdp_utils.random_hash()) == 40

def test_extract_id():
    assert fdp_utils.extract_id('http://localhost:8000/api/object/85') == '85'

def test_get_headers():
    assert type(fdp_utils.get_headers()) == dict

def test_get_headers_with_token(token):
    headers = fdp_utils.get_headers(token = token)
    assert headers['Authorization'] == 'token ' + token

def test_get_headers_post():
    headers = fdp_utils.get_headers(request_type= 'post')
    assert headers['Content-Type'] == 'application/json'

def test_get_headers_api_version():
    headers = fdp_utils.get_headers(api_version= '0.0.1')
    assert headers['Accept'] == 'application/json; version=0.0.1'

@pytest.fixture
def url():
    if platform.system() == 'Windows':
        return 'http://127.0.0.1:8000/api'
    return 'http://localhost:8000/api'

@pytest.fixture
def storage_root_test(token, url, scope="module"):
    return fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://storage-root-test.com'
        },
        endpoint= 'storage_root'
    )

def test_post_entry(token, url):
    storage_root = fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://test.com'
        },
        endpoint= 'storage_root'
    )
    assert type(storage_root) == dict

def test_post_entry_409(token, url):
    storage_root = fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://test.com'
        },
        endpoint= 'storage_root'
    )
    assert type(storage_root) == dict

def test_post_entry_equal(token, url):
    storage_root = fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://test_2.com'
        },
        endpoint= 'storage_root'
    )
    storage_root_2 = fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://test_2.com'
        },
        endpoint= 'storage_root'
    )
    assert storage_root == storage_root_2

def test_post_entry_500(token, url):
    with pytest.raises(Exception):
        fdp_utils.post_entry(
            token = token,
            url= url,
            data={
                'root': 'https://test.com'
            },
            endpoint= 'non_existant'
        )

def test_get_entry(url, token, storage_root_test):
    entry = fdp_utils.get_entry(
        url = url,
        query= {
            'root': 'https://storage-root-test.com'
        },
        token = token,
        endpoint= 'storage_root'
    )
    assert entry[0] == storage_root_test

def test_get_entity(url, storage_root_test):
    entity = fdp_utils.get_entity(
        url = url,
        endpoint='storage_root',
        id = fdp_utils.extract_id(storage_root_test['url']))
    assert entity == storage_root_test

def test_wrong_api_version(token, url):
    with pytest.raises(Exception):
        fdp_utils.post_entry(
        token = token,
        url= url,
        data={
            'root': 'https://test.com'
        },
        endpoint= 'storage_root',
        api_version= '2.2.2'
        )

def test_wrong_api_version_get(token, url):
    with pytest.raises(Exception):
        fdp_utils.get_entry(
        token = token,
        url= url,
        query={
            'root': 'https://test.com'
        },
        endpoint= 'storage_root',
        api_version= '3.0.0'
        )

def test_get_entity_with_token(url, storage_root_test, token):
    entity = fdp_utils.get_entity(
        url = url,
        endpoint= 'storage_root',
        id = fdp_utils.extract_id(storage_root_test['url']),
        token = token)
    assert entity == storage_root_test

def test_get_entity_non_200(url, storage_root_test):
    with pytest.raises(Exception):
        fdp_utils.get_entity(
            url = url,
            endpoint='non_existant',
            id = fdp_utils.extract_id(storage_root_test['url']))

@pytest.fixture
def model_config(url, token, scope="module"):
    return fdp_utils.post_entry(
        url = url,
        endpoint= 'object',
        data={
            'description': 'Model Config Test'
        },
        token = token
    )

@pytest.fixture
def submission_script(url, token, scope="module"):
    return fdp_utils.post_entry(
        url = url,
        endpoint= 'object',
        data={
            'description': 'Submission Script Test'
        },
        token = token
    )

@pytest.fixture
def input_1(url, token, scope="module"):
    return fdp_utils.post_entry(
        url = url,
        endpoint= 'object',
        data={
            'description': 'Input 1'
        },
        token = token
    )

@pytest.fixture
def input_1_component(input_1):
    return input_1['components'][0]

@pytest.fixture
def code_run(url, model_config, submission_script, token, scope="module"):
    return fdp_utils.post_entry(
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

def test_patch_entry(code_run, input_1_component, token, url):
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

def test_patch_entry_non_200(url, token):
    with pytest.raises(Exception):
        fdp_utils.patch_entry(
            url = url + '/api/users/1',
            data = {
                'name': 'New Name'
            },
            token = token,
        )

def test_post_storage_root_with_local(url, token):
    storage_root = fdp_utils.post_storage_root(
        token = token,
        url= url,
        data={
            'root': '/test/test',
            'local': True
        }
    )
    assert storage_root['root'] == 'file:///test/test/'