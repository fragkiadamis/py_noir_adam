import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from py_noir_code.projects.RHU_eCAN.ecan_json_generator import generate_rhu_ecan_json
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_project_name, find_project_root, create_file_path, save_values_to_csv
from py_noir_code.projects.RHU_eCAN.dicom_dataset_manager import inspect_study_tags, send_dicom_to_pacs_cstore, \
    assign_label_to_study, fetch_datasets_from_json

if __name__ == '__main__':
    load_context("context.conf", with_orthanc=True)
    json_file_path = find_project_root(__file__) + "/py_noir_code/resources/WIP_files/"
    json_save_path = find_project_root(__file__) + "/py_noir_code/resources/save_files/"
    json_file_name =  get_project_name() + ".json"
    create_file_path(json_file_path)
    create_file_path(json_save_path)

    executions_dir = "py_noir_code/resources/executions"
    create_file_path(executions_dir)
    filtered_datasets_csv = executions_dir + "/ecan_datasets.csv"
    executions_csv = executions_dir + "/successful_executions_ids.csv"

    if not os.path.exists(json_save_path + json_file_name):
        executions_dict, filtered_datasets_ids = generate_rhu_ecan_json()
        save_values_to_csv(filtered_datasets_ids, "DatasetId", filtered_datasets_csv)
        successful_executions = init_executions(json_file_path + json_file_name, executions_dict)
    else:
        successful_executions = resume_executions(json_file_path, json_save_path, json_file_name)

    save_values_to_csv(successful_executions, "ExecutionId", executions_csv)
    download_dir = fetch_datasets_from_json(filtered_datasets_csv)
    inspect_study_tags(download_dir)
    send_dicom_to_pacs_cstore(download_dir)
    assign_label_to_study(download_dir)
