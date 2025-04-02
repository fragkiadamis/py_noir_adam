import json
import shutil
import string
import sys
import re

from py_noir_code.src.execution.execution_management_service import start_executions
from py_noir_code.src.utils.log_utils import get_logger
from src.API.api_context import APIContext
from src.API.api_service import reset_token

logger = get_logger()
sys.path.append("../../")
json_content: list[dict] = []

def init_executions(json_file_name: str, content_to_process: list[dict]=None):
    if len(content_to_process) == 0 :
        logger.info("There is nothing to process. Please verify the data transmitted to the init_executions() method.")
        sys.exit(1)
    create_json_file(json_file_name, content_to_process)
    start_executions(json_file_name)

def resume_executions(json_file_path: str, json_save_path: str, json_file_name: str):
    shutil.copy(json_save_path + json_file_name, json_file_path + json_file_name)
    update_token(json_file_path + json_file_name)
    start_executions(json_file_path + json_file_name, True)

def create_json_file(json_file_name: string, content_to_process: list[dict]):
    for index, item in enumerate(content_to_process, start=1):
        item['identifier'] = index
    content_to_process.insert(0, dict(nb_processed_items=0, processed_item_ids=[]))

    exams_to_exec = open(json_file_name, "w")
    exams_to_exec.write(json.dumps(content_to_process))
    exams_to_exec.close()

def update_token(json_file_path):
    reset_token()

    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            updated_content = re.sub(r'("refreshToken"\s*:\s*")(.+?)(")',
                                     r'\1' + str(APIContext.refresh_token) + r'\3',
                                     content)

            with open(json_file_path, 'w', encoding='utf-8') as file:
                file.write(updated_content)
        logger.info("Token updated")
    except Exception as e:
        logger.error("Error updating refreshToken:\n" + e)
        sys.exit(1)