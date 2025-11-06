import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from projects.SIMS.sims_json_generator import generate_sims_json
from src.utils.file_utils import get_project_name, get_working_file_paths
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context

if __name__ == '__main__':

    load_context("context.conf")
    project_name = get_project_name()
    working_file, save_file= get_working_file_paths(project_name)

    if not os.path.exists(working_file):
        _ = init_executions(working_file, generate_sims_json())
    else:
        _ = resume_executions(working_file, save_file)