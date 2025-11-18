from pathlib import Path
from typing import Callable

from src.execution.execution_init_service import init_executions, resume_executions
from src.security.authentication_service import ask_access_token
from src.utils.file_utils import reset_tracking_file
from src.utils.file_writer import FileWriter


def init_serialization(working_file_path: Path, save_file_path: Path, tracking_file_path: Path, generate_json: Callable[[], list[dict]]) -> None:
    FileWriter.open_files(tracking_file_path, working_file_path)

    if save_file_path.stat().st_size == 0:
        reset_tracking_file(tracking_file_path)
        ask_access_token()
        init_executions(working_file_path, generate_json())
    else:
        ask_access_token()
        resume_executions(working_file_path, save_file_path)

    FileWriter.close_all()