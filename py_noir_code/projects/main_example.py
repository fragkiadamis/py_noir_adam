import os

from comete_moelle_json_generator import generate_comete_moelle_json
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_project_name, find_project_root

if __name__ == '__main__':
    load_context("context.conf")

    #Get theoretical working file path
    json_file_name = find_project_root(__file__) + "/resources/WIP_files/" + get_project_name() + ".json"

    #Check if working file exists, and so initiate or resume executions
    if not os.path.exists(json_file_name):
        init_executions(json_file_name, generate_comete_moelle_json())
    else:
        resume_executions(json_file_name)
