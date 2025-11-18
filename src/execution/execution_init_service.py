import json
import shutil
import sys
import re
from pathlib import Path

from src.API.api_service import reset_token
from src.execution.execution_management_service import start_executions
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.file_writer import FileWriter
from src.utils.log_utils import get_logger

logger = get_logger()
json_content: list[dict] = []

def init_executions(working_file: Path, content_to_process: list[dict]=None):
    if len(content_to_process) == 0 :
        logger.info("There is nothing to process. Please verify the data transmitted to the init_executions() method.")
        sys.exit(1)
    create_working_file(working_file, content_to_process)
    return start_executions(working_file)

def resume_executions(working_file: Path, save_file: Path):
    shutil.copy(save_file, working_file)
    update_token(working_file)
    return start_executions(working_file, True)

def create_working_file(working_file: Path, content_to_process: list[dict]):
    content_to_process.insert(0, dict(nb_processed_items=0, processed_item_ids=[]))
    FileWriter.replace_content(working_file, json.dumps(content_to_process))

def update_token(working_file: Path):
    reset_token()

    try:
        with open(working_file, 'r', encoding='utf-8') as file:
            content = file.read()
            updated_content = re.sub(r'("refreshToken"\s*:\s*")(.+?)(")',
                                     r'\1' + str(APIConfig.refresh_token) + r'\3',
                                     content)

            with open(working_file, 'w', encoding='utf-8') as file:
                file.write(updated_content)
        logger.info("Token updated")
    except Exception as e:
        logger.error("Error updating refreshToken:\n" + e)
        sys.exit(1)