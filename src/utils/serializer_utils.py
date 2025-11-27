from pathlib import Path
from typing import Callable, Optional, List, Dict

from src.execution.execution_init_service import init_executions, resume_executions
from src.security.authentication_service import ask_access_token
from src.utils.config_utils import ConfigPath
from src.utils.file_utils import reset_tracking_file
from src.utils.file_writer import FileWriter


def init_serialization(callback: Callable[[Optional[Path]], List[Dict]], kwargs: Optional[Dict] = None) -> None:
    if ConfigPath.save_file_path.stat().st_size == 0:
        reset_tracking_file(ConfigPath.tracking_file_path)
        ask_access_token()
        if kwargs:
            init_executions(callback(**kwargs))
        else:
            init_executions(callback(None))
    else:
        ask_access_token()
        resume_executions()

    FileWriter.close_all()