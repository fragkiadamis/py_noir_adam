import json
import string
import sys

from py_noir_code.src.execution.execution_management_service import start_executions
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()
sys.path.append("../../")
json_content: list[dict] = []

def init_executions(json_file_name: str, content_to_process: list[dict]=None):
    if len(content_to_process) == 0 :
        logger.info("There is nothing to process. Please verify the data transmitted to the init_executions() method.")
        sys.exit(1)
    create_json_file(json_file_name, content_to_process)
    start_executions(json_file_name)

def resume_executions(json_file_name: str):
    start_executions(json_file_name, True)

def create_json_file(json_file_name: string, content_to_process: list[dict]):
    for index, item in enumerate(content_to_process, start=1):
        item['identifier'] = index
    content_to_process.insert(0, dict(nb_processed_items=0, processed_item_ids=[]))

    exams_to_exec = open(json_file_name, "w")
    exams_to_exec.write(json.dumps(content_to_process))
    exams_to_exec.close()
