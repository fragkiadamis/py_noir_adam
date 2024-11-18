import json

from py_noir_code.src.API.api_service import post

"""
Define methods for Shanoir datasets MS execution API call
"""

ENDPOINT = '/datasets/vip/execution/'
LOG_FILE = 'datasets_status.csv'


def create_execution(execution: dict):

    path = ENDPOINT
    response = post(path, {}, data=json.dumps(execution), raise_for_status=False)
    return response.json()

