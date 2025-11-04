import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from py_noir_code.projects.Comete_FLAIR.comete_moelle_json_generator import generate_comete_moelle_json
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_project_name, find_project_root, create_file_path

if __name__ == '__main__':
    load_context("context.conf")
    json_file_path = find_project_root(__file__) + "/py_noir_code/resources/WIP_files/"
    json_save_path = find_project_root(__file__) + "/py_noir_code/resources/save_files/"
    json_file_name =  get_project_name() + ".json"
    create_file_path(json_file_path)
    create_file_path(json_save_path)

    if not os.path.exists(json_save_path + json_file_name):
        _ = init_executions(json_file_path + json_file_name, generate_comete_moelle_json())
    else:
        _ = resume_executions(json_file_path, json_save_path, json_file_name)
