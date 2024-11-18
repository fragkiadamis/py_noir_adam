import json
import os
import string
import sys

from py_noir_code.src.execution.execution_management_service import start_executions
from py_noir_code.src.utils.file_utils import get_project_name
from py_noir_code.src.utils.log_utils import set_logger

logger = set_logger()
sys.path.append("../../")
json_content: list[dict] = []


def init_executions(content_to_process: list[dict]):
    global json_content

    json_content = content_to_process
    for index, item in enumerate(json_content, start=1):
        item['identifier'] = index

    json_file_name = os.path.dirname(os.path.abspath(__file__)) + "/../../resources/WIP_files/" + get_project_name() + ".json"
    resume = True

    if not os.path.exists(json_file_name):
        resume = False
        create_json_file(json_file_name)

    start_executions(json_file_name, resume)


def create_json_file(json_file_name: string):
    global json_content

    if not json_content:
        logger.info("There is nothing to process. Please verify the data transmitted to the init_executions() method.")
        sys.exit(1)

    json_content.insert(0, dict(nb_processed_items=0, processed_item_ids=[]))

    exams_to_exec = open(json_file_name, "w")
    exams_to_exec.write(json.dumps(json_content))
    exams_to_exec.close()
