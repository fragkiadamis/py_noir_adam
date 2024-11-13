import json

from py_noir.src.API.api_service import post
from py_noir.src.API.shanoir_context import ShanoirContext

"""
Define methods for Shanoir datasets MS execution API call
"""

ENDPOINT = '/datasets/vip/execution/'
LOG_FILE = 'datasets_status.csv'


def create_execution(context: ShanoirContext, execution: dict):

    path = ENDPOINT
    response = post(context, path, {}, data=json.dumps(execution), raise_for_status=False)
    return response.json()

