import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from py_noir_code.projects.RHU_eCAN.ecan_json_generator import generate_rhu_ecan_json
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_project_name, find_project_root, create_file_path
from py_noir_code.projects.RHU_eCAN.ecan_data_transfer import fetch_datasets_from_json, upload_to_orthanc_pacs
from py_noir_code.projects.RHU_eCAN.dicom import inspect_study_tags, c_store

if __name__ == '__main__':
    load_context("context.conf", with_orthanc=True)
    json_file_path = find_project_root(__file__) + "/py_noir_code/resources/WIP_files/"
    json_save_path = find_project_root(__file__) + "/py_noir_code/resources/save_files/"
    json_file_name =  get_project_name() + ".json"
    create_file_path(json_file_path)
    create_file_path(json_save_path)

    if not os.path.exists(json_save_path + json_file_name):
        init_executions(json_file_path + json_file_name, generate_rhu_ecan_json())
    else:
        resume_executions(json_file_path, json_save_path, json_file_name)

    download_dir = fetch_datasets_from_json(Path(f"{json_save_path}initial_{json_file_name}"))
    inspect_study_tags(download_dir)
    c_store(download_dir)
    # upload_to_orthanc_pacs(download_dir)
