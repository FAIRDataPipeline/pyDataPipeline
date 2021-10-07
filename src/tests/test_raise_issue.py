import org.fairdatapipeline.api as pipeline
import os
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils
import pytest
test_dir = os.path.join(os.path.dirname(__file__), "ext")

token = os.environ.get('FDP_LOCAL_TOKEN')

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
    print(handle)
    assert handle['output'][0]['data_product'] == 'test/csv'

def test_raise_issue_by_index():
    global handle
    pipeline.raise_issue_by_index(handle, link_write, 'Test Issue', 7)
    assert handle['issues'][0]['use_data_product'] == 'test/csv'