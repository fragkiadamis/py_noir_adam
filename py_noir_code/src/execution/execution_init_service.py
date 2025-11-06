import json
import shutil
import string
import sys
import re

from py_noir_code.src.execution.execution_management_service import start_executions
from py_noir_code.src.utils.log_utils import get_logger
from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.API.api_service import reset_token

logger = get_logger()
sys.path.append("../../")
json_content: list[dict] = []

def init_executions(working_file: str, content_to_process: list[dict]=None):
    if len(content_to_process) == 0 :
        logger.info("There is nothing to process. Please verify the data transmitted to the init_executions() method.")
        sys.exit(1)
    create_working_file(working_file, content_to_process)
    return start_executions(working_file)

def resume_executions(working_file: str, save_file: str):
    shutil.copy(save_file, working_file)
    update_token(working_file)
    return start_executions(working_file, True)

def create_working_file(working_file: str, content_to_process: list[dict]):
    for index, item in enumerate(content_to_process, start=1):
        item['identifier'] = index
    content_to_process.insert(0, dict(nb_processed_items=0, processed_item_ids=[]))

    exams_to_exec = open(working_file, "w")
    exams_to_exec.write(json.dumps(content_to_process))
    exams_to_exec.close()

def update_token(working_file):
    reset_token()

    try:
        with open(working_file, 'r', encoding='utf-8') as file:
            content = file.read()
            updated_content = re.sub(r'("refreshToken"\s*:\s*")(.+?)(")',
                                     r'\1' + str(APIContext.refresh_token) + r'\3',
                                     content)

            with open(working_file, 'w', encoding='utf-8') as file:
                file.write(updated_content)
        logger.info("Token updated")
    except Exception as e:
        logger.error("Error updating refreshToken:\n" + e)
        sys.exit(1)