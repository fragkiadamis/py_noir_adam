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
from src.utils.dicom_utils import fetch_processed_datasets, upload_to_pacs_rest, assign_label_to_pacs_study, \
    inspect_and_fix_study_tags, upload_to_pacs_dicom, get_patient_ids_from_pacs, get_orthanc_study_details, \
    delete_studies_from_pacs, purge_pacs_studies, sync_examination_study_instance_uids, download_from_pacs_rest, \
    upload_processed_dataset, create_series_export, check_dicom_consistency, log_mr_series_instance_counts, \
    delete_mip_first_instances
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.utils.serializer_utils import init_serialization
from src.utils.mip_detector import delete_first_slice_if_mip

app = typer.Typer()
logger = get_logger()


def query_datasets(subject_name_list: List) -> defaultdict[Any, defaultdict[Any, List]]:
    logger.info("Searching for subjects' datasets...")
    query = SolrQuery()
    query.size = 100000
    query.expert_mode = True
    query.search_text = f'subjectName: ("{subject_name_list[0]}"'
    for subject in subject_name_list[1:]:
        query.search_text += f' OR "{subject}"'
    query.search_text += ') AND datasetName:*TOF*'
    result = solr_search(query).json()

    subjects_datasets = defaultdict(lambda: defaultdict(list))
    for item in result["content"]:
        subjects_datasets[item.get("subjectName")][str(item.get("examinationId"))].append(item)

    return subjects_datasets


def filter_datasets_by_study(subjects_datasets: defaultdict, study_name: str) -> defaultdict:
    for subject, exam_items in subjects_datasets.items():
        for key in list(exam_items.keys()):
            exam_items[key] = [
                ds for ds in exam_items[key]
                if ds.get("studyName") == study_name
            ]
            if not exam_items[key]:
                del exam_items[key]
    return subjects_datasets


def download_and_filter_datasets(subjects_datasets: defaultdict[Any, defaultdict[Any, List]], download_dir: Path) -> List:
    filtered_datasets = []
    for idx, (subject, exam_items) in enumerate(subjects_datasets.items(), start=1):
        for key in list(exam_items.keys()):
            for ds in exam_items[key][:]:
                dataset_download_path = download_dir / subject / str(ds["examinationId"]) / ds["id"]
                # Use this if no need to download again the files.
                # if dataset_download_path.exists():
                #     filtered_datasets.append(ds)
                dataset_download_path.mkdir(parents=True, exist_ok=True)
                download_dataset(ds["id"], "dcm", dataset_download_path, unzip=True)
                first_file = next(p for p in dataset_download_path.iterdir() if p.is_file())
                slice_thickness = pydicom.dcmread(first_file).get('SliceThickness')
                num_of_slices = sum(1 for p in dataset_download_path.iterdir() if p.is_file() and p.suffix == ".dcm")
                if num_of_slices > 50 and (slice_thickness is not None and slice_thickness < 10):
                    filtered_datasets.append(ds)
                else:
                    shutil.rmtree(dataset_download_path)
                    if not any(dataset_download_path.parent.iterdir()):
                        dataset_download_path.parent.rmdir()

    return filtered_datasets


def keep_one_acquisition(download_dir: Path, filtered_datasets: List) -> List:
    deleted_ids = set()
    for subject_dir in download_dir.iterdir():
        for exam_dir in subject_dir.iterdir():
            dataset_dirs = sorted([d for d in exam_dir.iterdir() if d.is_dir()], key=lambda d: int(d.name))
            if len(dataset_dirs) > 1:
                for d in dataset_dirs[1:]:
                    logger.info(f"Deleting: {d}")
                    deleted_ids.add(d.name)
                    shutil.rmtree(d)

    return [ds for ds in filtered_datasets if str(ds["id"]) not in deleted_ids]


def keep_oldest_examination(download_dir: Path, filtered_datasets: List) -> List:
    deleted_exam_ids = set()
    for subject_dir in download_dir.iterdir():
        exam_dirs = [d for d in subject_dir.iterdir() if d.is_dir()]
        if len(exam_dirs) <= 1:
            continue

        oldest_exam_id, oldest_date = None, None
        for exam_dir in exam_dirs:
            exam = get_examination(exam_dir.name)
            date_str = exam["examinationDate"].replace("Z", "").split("+")[0]
            exam_date = datetime.fromisoformat(date_str)
            if oldest_date is None or exam_date < oldest_date:
                oldest_exam_id = exam_dir.name
                oldest_date = exam_date

        for exam_dir in exam_dirs:
            if exam_dir.name != oldest_exam_id:
                logger.info(f"Deleting examination: {exam_dir}")
                deleted_exam_ids.add(exam_dir.name)
                shutil.rmtree(exam_dir)

    return [ds for ds in filtered_datasets if str(ds["examinationId"]) not in deleted_exam_ids]


def generate_json(output_dir: Path) -> List[Dict]:
    sources = [
        ("ican_subset.txt", "ICAN", "ICAN"),
        ("angptl6_subset.txt", "ICAN", "ANGPTL6"),
        ("ucan_subset.txt", "UCAN", "UCAN"),
    ]

    idx, executions = 0, []
    for filename, study_name, batch_label in sources:
        subject_list = [*get_items_from_input_file(filename)]
        if not subject_list:
            continue

        subjects_datasets = query_datasets(subject_list)
        filtered_datasets = filter_datasets_by_study(subjects_datasets, study_name)
        filtered_datasets = download_and_filter_datasets(filtered_datasets, output_dir)
        filtered_datasets = keep_one_acquisition(output_dir, filtered_datasets)
        filtered_datasets = keep_oldest_examination(output_dir, filtered_datasets)

        logger.info(f"Building json content for label {batch_label}...")
        for dataset in filtered_datasets:
            idx += 1
            df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
            values = {
                "identifier": idx,
                "dataset_id": dataset["id"],
                "examination_id": dataset["examinationId"],
                "subject_id": dataset["subjectId"],
                "subject_name": dataset["subjectName"],
                "get_from_shanoir": True,
                "executable": True,
                "label": batch_label,
            }
            for col, val in values.items():
                df.loc[idx - 1, col] = val
            df.to_csv(ConfigPath.tracking_file_path, index=False)

            dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
            executions.append({
                "identifier": idx,
                "name": f"landmarkDetection_0_7_exam_{dataset['examinationId']}_{dt}",
                "pipelineIdentifier": "landmarkDetection/0.7",
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
    \b
    eCAN project command-line interface.

    Commands:
    --------
    * `execute-pipeline` — runs the eCAN pipeline for subjects listed in `ecan_subject_id_list.csv` (ignored):
        - Retrieves datasets for each subject ID.
        - Filters the datasets (keep the oldest examination, >=50 slices, )
        - Generates JSON executions for the SIMS/3.0 pipeline.
        - Launches executions or resumes incomplete runs.

    Auxiliary debug functions:
    -------------------------
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
        uv run main.py ecan execute
        uv run main.py ecan populate-orthanc
        uv run main.py ecan debug-orthanc
        uv run main.py ecan import-shanoir
    """


@app.command()
def execute() -> None:
    """
    Run the eCAN processing pipeline
    """
    initiate_working_files("ecan")
    init_serialization(generate_json, kwargs={"output_dir": ConfigPath.output_path / "ecan" / "shanoir_output"})


@app.command()
def populate_orthanc() -> None:
    initiate_working_files("ecan")
    vip_output = ConfigPath.output_path / "ecan" / "vip_output"
    fetch_processed_datasets(vip_output)
    delete_first_slice_if_mip(vip_output)
    inspect_and_fix_study_tags(vip_output)
    upload_to_pacs_rest(vip_output) # for REST API
    # upload_to_pacs_dicom(vip_output) # For dicom web store
    assign_label_to_pacs_study()


@app.command()
def import_shanoir() -> None:
    initiate_working_files("ecan")
    orthanc_output = ConfigPath.output_path / "ecan" / "orthanc_output"
    sync_examination_study_instance_uids()
    download_from_pacs_rest(orthanc_output)
    check_dicom_consistency(orthanc_output)
    upload_processed_dataset(orthanc_output)


@app.command()
def delete_mip_orthanc() -> None:
    """
    For each uploaded Orthanc study, delete the first MR instance if it is a MIP.
    """
    initiate_working_files("ecan")
    delete_mip_first_instances()


@app.command()
def debug_orthanc() -> None:
    initiate_working_files("ecan")
    get_patient_ids_from_pacs()
    get_orthanc_study_details()
    log_mr_series_instance_counts()
    # create_series_export()


# ------------------- DANGER ZONE -------------------
# @app.command()
# def delete_studies() -> None:
#     delete_studies_from_pacs()
#     purge_pacs_studies()
# ------------------- DANGER ZONE -------------------
