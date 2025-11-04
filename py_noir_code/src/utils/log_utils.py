import logging
import os

from py_noir_code.src.utils.file_utils import get_project_name, create_file_path

logger = None

def set_logger():
    global logger
    log_path = os.path.dirname(os.path.abspath(__file__)) + "/../../resources/logs/"
    file_name = get_project_name() + ".log"
    create_file_path(log_path)

    if logger is None:
        logging.basicConfig(
            filename=log_path + file_name,
            filemode='a',
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO
        )
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        stdout_handler = logging.StreamHandler()
        stdout_handler.setFormatter(formatter)
        logging.getLogger().addHandler(stdout_handler)
        logger = logging.getLogger(__name__)

    return logger

def get_logger():
    return logger or set_logger()
