import csv

from pathlib import Path
from typing import List, Dict
from src.utils.config_utils import Config

def get_items_from_input_file(file_name: str):
    """ Extract the items in the input file separated by (descending priorities) : ";" "," "\n"
    :param file_name: the file name located in py_noir/input
    :return: a list of the extracted items
    """
    file = open(Config.inputPath/ file_name, "r")
    content = file.read()
    if ";" in content:
        return content.replace("\n","").split(";")
    elif "," in content:
        return content.replace("\n","").split(",")
    else :
        return content.split("\n")

def save_values_to_csv(values_list: List[str], column: str, csv_path: Path) -> None:
    csv_path.mkdir(parents=True, exist_ok=True)
    with open(csv_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([column])  # header
        for dataset_id in values_list:
            writer.writerow([dataset_id])


def save_dict_to_csv(dict_list: List[Dict[str, str]], csv_path: Path) -> None:
    csv_path.mkdir(parents=True, exist_ok=True)
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
    Create the directories of the file path if not existing
    :param file_path: the directory path to create
    """
    file_path.mkdir(parents=True, exist_ok=True)

def get_working_files(project_name : str):
    """
    Get the working files paths and names, but does not create the files, only the directory paths
    """
    working_file_path, save_file_path = create_working_paths()
    return working_file_path + project_name + ".json", save_file_path + project_name + ".json"

def create_working_paths():
    """
    Create the working paths if not existing
    """
    working_file_path = Config.resourcePath / "WIP_files"
    save_file_path = Config.resourcePath / "save_files"
    create_file_path(working_file_path)
    create_file_path(save_file_path)
    return working_file_path, save_file_path