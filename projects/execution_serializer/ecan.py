import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict
from collections import defaultdict

import pandas as pd
import typer
import pydicom

from src.shanoir_object.dataset.dataset_service import get_examination, download_dataset
from src.shanoir_object.solr_query.solr_query_model import SolrQuery
from src.shanoir_object.solr_query.solr_query_service import solr_search
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.dicom_utils import fetch_datasets_from_json, upload_to_pacs_rest, assign_label_to_pacs_study, \
    inspect_and_fix_study_tags, upload_to_pacs_dicom, get_patient_ids_from_pacs, get_orthanc_study_details, \
    delete_studies_from_pacs, purge_pacs_studies, download_from_pacs_rest, upload_processed_dataset
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.utils.serializer_utils import init_serialization

app = typer.Typer()
logger = get_logger()


def query_datasets(subject_name_list: List) -> defaultdict[Any, defaultdict[Any, List]]:
    logger.info("Searching for subjects' datasets...")
    query = SolrQuery()
    query.size = 100000
    query.expert_mode = True
    query.search_text = f"subjectName: ({subject_name_list[0]}"
    for subject in subject_name_list[1:]:
        query.search_text = query.search_text + " OR " + subject
    query.search_text = query.search_text + ") AND datasetName: *TOF*"
    result = solr_search(query).json()

    subjects_datasets = defaultdict(lambda: defaultdict(list))
    for item in result["content"]:
        subjects_datasets[item.get("subjectName")][str(item.get("examinationId"))].append(item)

    return subjects_datasets


def find_oldest_exams(subjects_datasets: defaultdict[Any, defaultdict[Any, List]]) -> None:
    for subject, exam_items in subjects_datasets.items():
        if len(exam_items.keys()) > 1:
            oldest_exam, oldest_date = None, None
            for exam_id in exam_items.keys():
                exam = get_examination(exam_id)
                date_str = exam["examinationDate"].replace("Z", "").split("+")[0]
                exam_date = datetime.fromisoformat(date_str)

                if not oldest_exam or (exam_date < oldest_date):
                    oldest_exam = exam
                    oldest_date = exam_date

            for exam_id in list(exam_items.keys()):
                if exam_id != str(oldest_exam["id"]):
                    del exam_items[exam_id]


def download_and_filter_datasets(subjects_datasets: defaultdict[Any, defaultdict[Any, List]], download_dir: Path) -> List:
    filtered_datasets = []
    for idx, (subject, exam_items) in enumerate(subjects_datasets.items(), start=1):
        for key in list(exam_items.keys()):
            for ds in exam_items[key][:]:
                dataset_download_path = download_dir / subject / ds["id"]
                dataset_download_path.mkdir(parents=True, exist_ok=True)
                download_dataset(ds["id"], "dcm", dataset_download_path, unzip=True)
                first_file = next(p for p in dataset_download_path.iterdir() if p.is_file())
                slice_thickness = pydicom.dcmread(first_file, stop_before_pixels=True)['SliceThickness'].value
                num_of_slices = sum(1 for p in dataset_download_path.iterdir() if p.is_file() and p.suffix == ".dcm")
                if num_of_slices > 50 and slice_thickness < 10:
                    filtered_datasets.append(ds)
                else:
                    shutil.rmtree(dataset_download_path)

    return filtered_datasets


def generate_json(output_dir: Path) -> List[Dict]:
    ican_list = [*get_items_from_input_file("ican_subset.txt")]
    angptl6_list = [*get_items_from_input_file("angptl6_subset.txt")]
    subject_name_list = [ican_list, angptl6_list]

    executions = []
    subjects_datasets = query_datasets(subject_name_list)
    find_oldest_exams(subjects_datasets)
    filtered_datasets = download_and_filter_datasets(subjects_datasets, output_dir)

    logger.info("Building json content...")
    for idx, dataset in enumerate(filtered_datasets, start=1):
        df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
        values = {
            "identifier": idx,
            "dataset_id": dataset["id"],
            "examination_id": dataset["examinationId"],
            "subject_id": dataset["subjectId"],
            "subject_name": dataset["subjectName"],
            "get_from_shanoir": True,
            "executable": True,
            "label": (
                "ICAN" if dataset["subjectName"] in ican_list else
                "ANGPTL16" if dataset["subjectName"] in angptl6_list else
                None
            )
        }
        for col, val in values.items():
            df.loc[idx - 1, col] = val
        df.to_csv(ConfigPath.tracking_file_path, index=False)

        dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
        executions.append({
            "identifier": idx,
            "name": f"landmarkDetection_0_4_exam_{dataset['examinationId']}_{dt}",
            "pipelineIdentifier": "landmarkDetection/0.4",
            "studyIdentifier": dataset["studyId"],
            "inputParameters": {},
            "outputProcessing": "",
            "processingType": "SEGMENTATION",
            "refreshToken": APIConfig.refresh_token,
            "client": APIConfig.clientId,
            "datasetParameters": [{
                "datasetIds": [dataset["id"]],
                "groupBy": "EXAMINATION",
                "name": "dicom_input_zip",
                "exportFormat": "dcm"
            }],
        })

    return executions


@app.callback()
def explain() -> None:
    """
    eCAN project command-line interface.
    Commands:
    --------
    * `execute-pipeline` — runs the eCAN pipeline for subjects listed in `ecan_subject_id_list.csv` (ignored):
        - Retrieves datasets for each subject ID.
        - Filters the datasets (keep the oldest examination, >=50 slices, )
        - Generates JSON executions for the SIMS/3.0 pipeline.
        - Launches executions or resumes incomplete runs.
        --- Auxiliary debug functions ---
    * `populate-orthanc` — populates the CHU Nantes Orthanc PACS with the processed output and the input datasets
        - Download the processed output alongside the input dataset
        - Inspect DICOM files for inconsistencies and fixes them
        - Upload the processed output along the input dataset to an orthanc instance
        - Assign labels to the orthanc studies
    * `debug-orthanc` — Runs functions for the environment of CHU Nantes to inspect the Orthanc PACS
        - Get and log patients from the Orthanc instance
        - Get and log studies from the Orthanc instance
        - Delete uploaded studies from ecan.csv tracking file
        - Purge Orthanc instance
    * `import-shanoir` — Imports data from Orthanc to shanoir
        - Get further processed outputs from shanoir
        - Upload the processed output to shanoir
    Usage:
    -----
        uv run main.py ecan execute-pipeline
        uv run main.py ecan populate-orthanc
        uv run main.py ecan debug-orthanc
        uv run main.py ecan import-shanoir
    """


@app.command()
def execute_pipeline() -> None:
    """
    Run the eCAN processing pipeline
    """
    initiate_working_files("ecan")
    init_serialization(generate_json, kwargs={"output_dir": ConfigPath.output_path / "ecan" / "shanoir_output"})


@app.command()
def populate_orthanc() -> None:
    initiate_working_files("ecan")
    vip_output = ConfigPath.output_path / "ecan" / "vip_output"
    fetch_datasets_from_json(vip_output)
    inspect_and_fix_study_tags(vip_output)
    upload_to_pacs_rest(vip_output) # RESTAPI or DICOM WEB STORE --> upload_to_pacs_dicom(vip_output)
    assign_label_to_pacs_study()


@app.command()
def debug_orthanc() -> None:
    initiate_working_files("ecan")
    get_patient_ids_from_pacs()
    get_orthanc_study_details()

    # ------------------- DANGER ZONE -------------------
    # delete_studies_from_pacs()
    # purge_pacs_studies()


@app.command()
def import_shanoir() -> None:
    initiate_working_files("ecan")
    orthanc_output = ConfigPath.output_path / "ecan" / "orthanc_output"
    download_from_pacs_rest(orthanc_output)
    upload_processed_dataset(orthanc_output)
