from pathlib import Path
from typing import Callable, Optional, List, Dict, Tuple

from src.execution.execution_init_service import init_executions, resume_executions
from src.security.authentication_service import ask_access_token
from src.utils.file_utils import reset_tracking_file
from src.utils.file_writer import FileWriter


def init_serialization(
        working_file_path: Path,
        save_file_path: Path,
        tracking_file_path: Path,
        callback: Callable[[Optional[Path]], Tuple[List[Dict], List[str]] | List[Dict]],
        kwargs: Optional[Dict] = None
) -> None:
    FileWriter.open_files(tracking_file_path, working_file_path)

    if save_file_path.stat().st_size == 0:
        reset_tracking_file(tracking_file_path)
        ask_access_token()
        init_executions(working_file_path, callback(**kwargs))
    else:
        ask_access_token()
        resume_executions(working_file_path, save_file_path)

    FileWriter.close_all()