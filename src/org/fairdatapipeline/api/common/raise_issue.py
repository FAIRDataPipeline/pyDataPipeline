from os import write
import org.fairdatapipeline.api.common.fdp_utils as fdp_utils

def raise_issue_by_index(handle: dict, index: str, issue: str, severity):
    """
    Raises an issue for a given index and writes it to the handle

    Args:
        |   handle: the handle containing the index
        |   index: a reference to an input or output in the handle, typically the path returned by link_write or link_read
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue(handle, 'index', issue, severity, index= index)

def raise_issue_by_data_product(handle: dict,
    data_product: str,
    version: str,
    namespace: str,
    issue: str,
    severity
):
    """
    Raises an issue for a given data_product and writes it to the handle

    Args:
        |   data_product: the data_product name as a string
        |   version: version of the data_product as a string
        |   namespace: namespace of the date_product as a string
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    raise_issue(handle,
    'data_product',
    issue,
    severity,
    data_product= data_product,
    namespace = namespace,
    version= version
    )

def raise_issue_with_config(handle: dict, issue: str, severity):
    """
    Raise an issue with config: add an issue for the config to the handle.

    Args:
        |   handle: the handle containing the index
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue_by_type(handle, 'config', issue, severity)

def raise_issue_with_submission_script(handle: dict, issue: str, severity):
    """
    Raise an issue with submission script: add an issue for the submission_script to the handle.

    Args:
        |   handle: the handle containing the index
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue_by_type(handle, 'submission_script', issue, severity)

def raise_issue_with_github_repo(handle: dict, issue: str, severity):
    """
    Raise an issue with config add an issue for the github_repo to the handle.

    Args:
        |   handle: the handle containing the index
        |   issue: what the issue is as a string
        |   severity: How severe the issue is as an interger from 1-10
    """
    return raise_issue_by_type(handle, 'github_repo', issue, severity)

def raise_issue_by_type(handle: dict, type:str, issue: str, severity):
    """
    Raise an issue by type of issue (with config, with submission_script, with github_repo)
    """
    accepted_types = ['config', 'submission_script', 'github_repo']
    if type not in accepted_types:
        raise ValueError('Please supply a valid type. \
        valid types: config, submission_script, github_repo')
    return raise_issue(handle, issue, severity, type=type)

    
def raise_issue(handle: dict, 
    type,
    issue,
    severity,
    index = None,
    data_product = None,
    component= None,
    version = None,
    namespace = None,
):

    if type in ['config', 'submission_script', 'github_repo']:
        print('adding issue ' + issue + ' for ' + type + ' to handle')
    elif index is None:
        data_product_in_config = False
        reads = handle['yaml']['read']
        writes = handle['yaml']['write']

        if reads:
            for i in reads:
                if i['data_product'] == data_product and i['version'] == version:
                    data_product_in_config = True
                    data_product = i['data_product']
        if writes:
            for i in writes:
                if i['data_product'] == data_product and i['version'] == version:
                    data_product_in_config = True
                    data_product = i['data_product']

        if not data_product_in_config:
            raise ValueError('Data product not in config file')

        print('adding issue ' + issue + ' for ' + data_product + '@' + version + ' to handle')

    else:
        tmp = None
        if 'output' in handle.keys():
            for output in handle['output']:
                if index in output.values():
                    tmp = output
        if 'input' in handle.keys():
            for input in handle['input']:
                if index in input.values():
                    tmp = input
        
        if tmp is None:
            raise ValueError('Error: Index not found')

        data_product = tmp['data_product']
        component = tmp['use_component']

        print('adding issue ' + issue + ' for ' + index + ' to handle')
                
    # Write to handle and return path
    issues_dict = {
        'index': index,
        'type': type,
        'use_data_product': data_product,
        'use_component': component,
        'version': version,
        'use_namespace': namespace,
        'issue': issue,
        'severity': severity
    }

    if 'input' in handle.keys():
        handle['issues'].append(issues_dict)
    else:
        handle['issues'] = [issues_dict]
    
