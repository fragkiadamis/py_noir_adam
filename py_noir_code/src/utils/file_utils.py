import os
import string
import sys
from pathlib import Path
import csv


def remove_file_extension(file_name: string):
    """ Get a file name without its extension [file_full_name]
    :param file_name:
    :return file_name_without_extension:
    """
    pos = file_name.rfind(".")

    if pos != -1:
        return file_name[:pos]
    return file_name


def open_project_file(file_name: string, option: string = "r"):
    """ Open a file [file_name] stored at the same location as the executed main.py
    :param file_name:
    :param option:
    :return file_name_without_extension:
    """
    return open(get_project_path() + '/' + file_name, option)

def get_ids_from_file(file_name: string, option: string = "r"):
    file = open_project_file(file_name, option)
    return file.read().replace("\n","").split(",")


def get_values_from_csv(file_name: str, column: str) -> list[str]:
    values = []
    with open(file_name, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            values.append(row[column])
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