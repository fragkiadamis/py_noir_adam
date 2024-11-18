import os
import string
import sys


def remove_file_extension(file_name: string):
    """ Get a file name without its extension [file_full_name]
    :param file_name:
    :return file_name_without_extension:
    """
    pos = file_name.rfind(".")

    if pos != -1:
        return file_name[:pos]
    return file_name


def open_project_file(file_name: string, option: string):
    """ Open a file [file_name] stored at the same location as the executed main.py
    :param file_name:
    :param option:
    :return file_name_without_extension:
    """
    return open(get_project_path() + '/' + file_name, option)

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
