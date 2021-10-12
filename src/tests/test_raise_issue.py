import org.fairdatapipeline.api as pipeline
import os
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils
import pytest
import shutil
test_dir = os.path.join(os.path.dirname(__file__), "ext")

token = fdp_utils.read_token(os.path.join(os.path.expanduser('~'), '.fair/registry/token'))
script = os.path.join(test_dir, 'test_script.sh')
config = os.path.join(test_dir, 'write_csv.yaml')

handle = None

def test_initialise():
    global handle
    handle = pipeline.initialise(token, config, script)
    assert type(handle) == dict

link_write = None

def test_link_write():
    global handle
    global link_write
    link_write = pipeline.link_write(handle, 'test/csv')
    #print(handle)
    assert handle['output']['output_0']['data_product'] == 'test/csv'

def test_raise_issue_by_index():
    global handle
    index = pipeline.get_handle_index_from_path(handle, link_write)
    pipeline.raise_issue_by_index(handle, index, 'Test Issue', 7)
    assert handle['issues']['issue_0']['use_data_product'] == 'test/csv'

def test_raise_issue_with_config():
    global handle
    pipeline.raise_issue_with_config(handle, 'Test Issue with config', 4)
    assert handle['issues']['issue_1']['type'] == 'config'

def test_raise_issue_with_github_repo():
    global handle
    pipeline.raise_issue_with_github_repo(handle, 'Test Issue with github_repo', 4)
    assert handle['issues']['issue_2']['type'] == 'github_repo'

def test_raise_issue_with_script():
    global handle
    pipeline.raise_issue_with_submission_script(handle, 'Test Issue with submission_script', 4)
    #print(handle)
    assert handle['issues']['issue_3']['type'] == 'submission_script'

def test_finalise():
    global handle
    tmp_csv = os.path.join(test_dir, 'test.csv')
    shutil.copy(tmp_csv, link_write)
    pipeline.finalise(token, handle)
    #print(handle)