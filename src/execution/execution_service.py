import json
import time
from typing import Dict

from src.API.api_service import get, post
from src.utils.log_utils import get_logger

"""
Define methods for Shanoir datasets MS execution API call
"""

logger = get_logger()


def create_execution(execution: Dict):
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


def get_execution_monitoring(execution_id: str) -> Dict | None:
    """ Get ExecutionMonitoring relative to an execution [execution_id]
    :param execution_id:
    :return: json
    """

    path = '/datasets/execution-monitoring/' + str(execution_id)
    response = get(path)

    for attempt in range(3):
        try:
            response = get(path)
            response.raise_for_status()
            return response.json()
        except response.RequestException as e:
            logger.error("Attempt {attempt + 1} failed: {e}")
            if attempt <  2:
                time.sleep(2)
            else:
                raise
    return None