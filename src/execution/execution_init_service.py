import json
import shutil
import sys
import re
from pathlib import Path
from typing import List, Dict

from src.API.api_service import reset_token
from src.execution.execution_management_service import start_executions
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.log_utils import get_logger

logger = get_logger()
json_content: List[Dict] = []


def init_executions(content_to_process: List[Dict]=None):
    if len(content_to_process) == 0:
        logger.info("There is nothing to process. Please verify the data transmitted to the init_executions() method.")
        sys.exit(1)
    create_working_file(content_to_process)
    start_executions()


def resume_executions():
    shutil.copy(ConfigPath.save_file_path, ConfigPath.wip_file_path)
    update_token(ConfigPath.wip_file_path)
    start_executions(resume=True)


def create_working_file(content_to_process: List[Dict]):
    content_to_process.insert(0, dict(nb_processed_items=0, processed_item_ids=[]))
    with open(ConfigPath.wip_file_path, "w", encoding="utf-8") as f:
        json.dump(content_to_process, f, indent=2)


def update_token(working_file: Path):
    reset_token()

    try:
        with open(working_file, 'r', encoding='utf-8') as file:
            content = file.read()
            updated_content = re.sub(r'("refreshToken"\s*:\s*")(.+?)(")', r'\1' + str(APIConfig.refresh_token) + r'\3',content)

            with open(working_file, 'w', encoding='utf-8') as file:
                file.write(updated_content)
        logger.info("Token updated")
    except Exception as e:
        logger.error(f"Error updating refreshToken:\n{e}")
        sys.exit(1)