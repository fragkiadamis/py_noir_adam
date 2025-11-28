import csv
from pathlib import Path
from typing import List, Dict

import pandas as pd

from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger
from src.utils.user_prompt import ask_yes_no

logger = get_logger()


def get_items_from_input_file(file_name: str):
    """ Extract the items in the input file separated by (descending priorities): ";" "," "\n"
    :param file_name: the file name located in py_noir/input
    :return: a list of the extracted items
    """
    file = open(ConfigPath.input_path / file_name, "r")
    content = file.read()
    if ";" in content:
        return content.replace("\n","").split(";")
    elif "," in content:
        return content.replace("\n","").split(",")
    else:
        return content.split("\n")


def create_file_path(file_path):
    """
    Create the directories of the file path and the file if not existing
    :param file_path: the file path to create
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.suffix:
        file_path.touch(exist_ok=True)


def initiate_working_files(project_name: str) -> None:
    """
    Initiate the working and tracking files paths and names and create the files
    """
    ConfigPath.wip_file_path = ConfigPath.wip_file_path / (project_name + ".json")
    ConfigPath.save_file_path = ConfigPath.save_file_path / (project_name + ".json")
    ConfigPath.tracking_file_path = ConfigPath.tracking_file_path / (project_name + ".csv")
    create_file_path(ConfigPath.wip_file_path)
    create_file_path(ConfigPath.save_file_path)
    create_file_path(ConfigPath.tracking_file_path)


def reset_tracking_file(tracking_file_path: Path):
    """
    Reset the tracking file if existing
    :param tracking_file_path:
    """

    if tracking_file_path.stat().st_size != 0:
        if not ask_yes_no("A tracking file of that pipeline is already existing. Do you want to reset its content?"):
            logger.info("Ok, bye.")
            exit()

    df = pd.DataFrame(columns=[
        "identifier", "dataset_id", "examination_id",
        "subject_id", "subject_name", "get_from_shanoir", "executable",
        "execution_requested", "execution_id", "execution_workflow_id",
        "execution_status", "execution_start_time", "execution_end_time"
    ])
    df.to_csv(tracking_file_path, index=False)
