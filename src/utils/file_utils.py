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


def save_values_to_csv(values_list: List[str], column: str, csv_path: Path) -> None:
    csv_path.mkdir(parents=True, exist_ok=True)
    with open(csv_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([column])  # header
        for dataset_id in values_list:
            writer.writerow([dataset_id])


def save_dict_to_csv(dict_list: List[Dict[str, str]], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
        writer.writeheader()
        for row in dict_list:
            writer.writerow(row)


def get_values_from_csv(file_path: Path, column: str) -> List[str] | None:
    if not file_path.exists():
        return None

    values = []
    with open(file_path, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            values.append(row[column])
    return values


def get_dict_from_csv(file_name: Path) -> List[Dict[str, str]] | None:
    if not file_name.exists():
        return None

    values = []
    with open(file_name, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            values.append(row)
    return values


def create_file_path(file_path):
    """
    Create the directories of the file path and the file if not existing
    :param file_path: the file path to create
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.suffix:
        file_path.touch(exist_ok=True)


def get_working_files(project_name: str) -> None:
    """
    Get the working files paths and names and create the files
    """
    ConfigPath.wip_file_path = ConfigPath.wip_file_path / (project_name + ".json")
    ConfigPath.save_file_path = ConfigPath.save_file_path / (project_name + ".json")
    create_file_path(ConfigPath.wip_file_path)
    create_file_path(ConfigPath.save_file_path)


def get_tracking_file(project_name: str) -> None:
    """
    Get the tracking file path and name and create the file
    """
    ConfigPath.tracking_file_path = ConfigPath.tracking_file_path / (project_name + ".csv")
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


def get_working_directory(working_dir: str, project_name: str, sub_dirs: str = None) -> Path:
    if sub_dirs is None:
        download_dir = ConfigPath.resources_path / working_dir / project_name
    else:
        download_dir = ConfigPath.resources_path / working_dir / project_name / sub_dirs
    download_dir.mkdir(parents=True, exist_ok=True)
    return download_dir
