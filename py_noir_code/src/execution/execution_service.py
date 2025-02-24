import json

from py_noir_code.src.API.api_service import post, get
from src.utils.log_utils import get_logger

"""
Define methods for Shanoir datasets MS execution API call
"""

logger = get_logger()

def create_execution(execution: dict):
    path = "/datasets/vip/execution/"
    response = post(path, {}, data=json.dumps(execution), raise_for_status=False)
    return response.json()

def get_execution_status(execution_monitoring_id:  str):
    """ Get execution status from [execution_monitoring_id]
    :param execution_monitoring_id:
    :return: json
    """
    path = "/datasets/vip/execution/" + str(execution_monitoring_id) + '/status'
    response = get(path)
    return response.text

def get_execution_monitoring(execution_id: str) -> list:
    """ Get ExecutionMonitoring relative to an execution [execution_id]
    :param execution_id:
    :return: json
    """

    path = '/datasets/execution-monitoring/' + str(execution_id)
    response = get(path)
    return response.json()