import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from py_noir_code.projects.RHU_eCAN.ecan_json_generator import generate_rhu_ecan_json
from py_noir_code.src.execution.execution_init_service import init_executions, resume_executions
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_project_name, find_project_root, create_file_path, save_values_to_csv
from py_noir_code.projects.RHU_eCAN.dicom_dataset_manager import fetch_datasets_from_json, inspect_and_fix_study_tags, \
    upload_to_pacs_rest, assign_label_to_pacs_study, download_from_pacs_rest, upload_processed_dataset, \
    delete_studies_from_pacs, get_patient_ids_from_pacs, purge_pacs_studies, get_orthanc_study_details

if __name__ == '__main__':
    load_context("context.conf", with_orthanc=True)
    json_file_path = find_project_root(__file__) + "/py_noir_code/resources/WIP_files/"
    json_save_path = find_project_root(__file__) + "/py_noir_code/resources/save_files/"
    json_file_name =  get_project_name() + ".json"
    create_file_path(json_file_path)
    create_file_path(json_save_path)

    ecan_tracing_dir = "py_noir_code/resources/ecan_tracing"
    create_file_path(ecan_tracing_dir)
    filtered_datasets_csv = ecan_tracing_dir + "/ecan_filtered_datasets.csv"
    executions_csv = ecan_tracing_dir + "/successful_executions_ids.csv"
    studies_csv = ecan_tracing_dir + "/orthanc_studies.csv"

    download_dir = find_project_root(__file__) + "/py_noir_code/resources/downloads"
    shanoir_output = os.path.join(download_dir, "shanoir_output")
    vip_output = os.path.join(download_dir, "vip_output")
    orthanc_output = os.path.join(download_dir, "orthanc_output")

    if not os.path.exists(json_save_path + json_file_name):
        executions_dict, filtered_datasets_ids = generate_rhu_ecan_json(shanoir_output)
        save_values_to_csv(filtered_datasets_ids, "DatasetId", filtered_datasets_csv)
        successful_executions = init_executions(json_file_path + json_file_name, executions_dict)
    else:
        successful_executions = resume_executions(json_file_path, json_save_path, json_file_name)
    save_values_to_csv(successful_executions, "ExecutionId", executions_csv)
    # Wait for 10 seconds for the data imports to finish in shanoir
    time.sleep(10)

    fetch_datasets_from_json(filtered_datasets_csv, executions_csv, vip_output)
    inspect_and_fix_study_tags(vip_output)
    upload_to_pacs_rest(vip_output, studies_csv)
    assign_label_to_pacs_study(studies_csv)

    # To be used later for the importation of GE AIDream processed output to Shanoir.
    # download_from_pacs_rest(studies_csv, orthanc_output)
    # upload_processed_dataset(orthanc_output)

    # ----- AUXILIARY FUNCTIONS FOR DEBUGGING -----
    # get_patient_ids_from_pacs()
    # get_orthanc_study_details()
    # delete_studies_from_pacs(studies_csv)
    # purge_pacs_studies()
