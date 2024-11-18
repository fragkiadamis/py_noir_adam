import logging
import os

from py_noir_code.src.utils.file_utils import get_project_name

logger = None


def set_logger():
    global logger

    if logger is None:
        logging.basicConfig(
            filename=os.path.dirname(
                os.path.abspath(__file__)) + "/../../resources/logs/" + get_project_name() + ".log",
            filemode='a',
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO)
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        stdout_handler = logging.StreamHandler()
        stdout_handler.setFormatter(formatter)
        logging.getLogger().addHandler(stdout_handler)
        logger = logging.getLogger(__name__)

    return logger
