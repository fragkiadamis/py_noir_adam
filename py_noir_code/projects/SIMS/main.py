import os

from projects.SIMS.sims_json_generator import generate_sims_json
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_project_name, find_project_root, create_file_path

if __name__ == '__main__':
    load_context("context.conf")
    json_file_path = find_project_root(__file__) + "/py_noir_code/resources/WIP_files/"
    json_file_name =  get_project_name() + ".json"
    create_file_path(json_file_path)

    if not os.path.exists(json_file_path + json_file_name):
        init_executions(json_file_path + json_file_name, generate_sims_json())
    else:
        resume_executions(json_file_path + json_file_name)
