import logging

from src.utils.config_utils import ConfigPath

logger = None

def set_logger(project_name: str):
    global logger
    log_path = ConfigPath.resources_path / "logs"
    file_name = project_name + ".log"
    log_path.mkdir(parents=True, exist_ok=True)

    if logger is None:
        logging.basicConfig(
            filename= str(log_path.joinpath(file_name)),
            filemode='a',
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO
        )
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        stdout_handler = logging.StreamHandler()
        stdout_handler.setFormatter(formatter)
        logging.getLogger().addHandler(stdout_handler)
        logger = logging.getLogger(__name__)

    return logger

def get_logger():
    return logger
