import os
import sys
from pathlib import Path
import csv
from typing import List, Dict


def remove_file_extension(file_name: str):
    """ Get a file name without its extension [file_full_name]
    :param file_name:
    :return file_name_without_extension:
    """
    pos = file_name.rfind(".")

    if pos != -1:
        return file_name[:pos]
    return file_name

def get_ids_from_file(file_path: Path):
    file = open(file_path, "r")
    content = file.read()
    if ";" in content:
        return content.replace("\n","").split(";")
    else:
        return content.replace("\n","").split(",")

def save_values_to_csv(values_list: List[str], column: str, csv_path: str) -> None:
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([column])  # header
        for dataset_id in values_list:
            writer.writerow([dataset_id])


def save_dict_to_csv(dict_list: List[Dict[str, str]], csv_path: str) -> None:
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
        writer.writeheader()
        for row in dict_list:
            writer.writerow(row)


def get_values_from_csv(file_name: str, column: str) -> List[str] | None:
    if not os.path.exists(file_name):
        return None

    values = []
    with open(file_name, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            values.append(row[column])
    return values


def get_dict_from_csv(file_name: str) -> List[Dict[str, str]] | None:
    if not os.path.exists(file_name):
        return None

    values = []
    with open(file_name, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            values.append(row)
    return values


def get_project_name():
    """ Return the project name (according to the main.py directory name)
    :return project_name:
    """
    return os.path.basename(get_project_path())

def get_project_path():
    """ Return the project name (according to the main.py directory name)
    :return project_name:
    """
    return os.path.dirname(os.path.abspath(sys.argv[0]))

def find_project_root(starting_path, folder_name="py_noir"):
    current_path = Path(starting_path).resolve()
    for parent in current_path.parents:
        if parent.name == folder_name:
            return str(parent)
    raise FileNotFoundError(f"'{folder_name}' folder not found.")

def create_file_path(file_path):
        if not os.path.exists(file_path):
           os.makedirs(file_path)

def get_working_file_paths(project_name : str):
    working_file_path, save_file_path = create_working_paths()
    return working_file_path + project_name + ".json", save_file_path + project_name + ".json"

def create_working_paths():
    working_file_path = find_project_root(__file__) + "/py_noir_code/resources/WIP_files/"
    save_file_path = find_project_root(__file__) + "/py_noir_code/resources/save_files/"
    create_file_path(working_file_path)
    create_file_path(save_file_path)
    return working_file_path, save_file_path