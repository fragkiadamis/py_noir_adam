from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict, Tuple
from collections import defaultdict

import typer
import pydicom

from src.shanoir_object.dataset.dataset_service import get_examination, download_dataset
from src.shanoir_object.solr_query.solr_query_model import SolrQuery
from src.shanoir_object.solr_query.solr_query_service import solr_search
from src.utils.config_utils import APIConfig
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_working_files, get_tracking_file, get_values_from_csv
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

    subjects_datasets = defaultdict(lambda: defaultdict(List))
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

            for exam_id in List(exam_items.keys()):
                if exam_id != str(oldest_exam["id"]):
                    del exam_items[exam_id]


def download_and_filter_datasets(subjects_datasets: defaultdict[Any, defaultdict[Any, List]], download_dir: Path) -> List:
    filtered_datasets = []
    for idx, (subject, exam_items) in enumerate(subjects_datasets.items(), start=1):
        for key in List(exam_items.keys()):
            for ds in exam_items[key][:]:
                subject_download_subdir = download_dir / subject / ds["id"]
                subject_download_subdir.mkdir(parents=True, exist_ok=True)
                download_dataset(ds["id"], "dcm", subject_download_subdir, unzip=True)
                first_file = next(p for p in subject_download_subdir.iterdir() if p.is_file())
                slice_thickness = pydicom.dcmread(first_file, stop_before_pixels=True)['SliceThickness'].value
                num_of_slices = sum(1 for p in subject_download_subdir.iterdir() if p.is_file() and p.suffix == ".dcm")
                if num_of_slices > 50 and slice_thickness < 10:
                    filtered_datasets.append(ds)
                else:
                    subject_download_subdir.unlink(missing_ok=True)

    return filtered_datasets


def generate_json(download_dir: Path) -> Tuple[List[Dict], List[str]]:
    csv_paths = [
        "py_noir_code/projects/RHU_eCAN/ican_subset.csv",
        "py_noir_code/projects/RHU_eCAN/angptl6_subset.csv"
    ]

    executions, dataset_ids_list, identifier = [], [], 0
    for csv_path in csv_paths:
        csv_path_path = Path(csv_path)
        subject_name_list = get_values_from_csv(csv_path_path, "SubjectName")
        subjects_datasets = query_datasets(subject_name_list)
        find_oldest_exams(subjects_datasets)
        filtered_datasets = download_and_filter_datasets(subjects_datasets, download_dir)

        dataset_ids_list.extend(ds["id"] for ds in filtered_datasets)
        logger.info("Building json content...")
        for dataset in filtered_datasets:
            dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
            executions.append({
                "identifier": identifier,
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

            identifier += 1
        logger.info(f"Finished processing {csv_path}.")
    return executions, dataset_ids_list


@app.callback()
def explain() -> None:
    """
    eCAN project command-line interface.
    Commands:
    --------
    * `execute` — runs the eCAN pipeline for subjects listed in `ecan_subject_id_list.csv` (ignored):
        - Retrieves datasets for each subject ID.
        - Filters the datasets (keep the oldest examination, >=50 slices, )
        - Generates JSON executions for the SIMS/3.0 pipeline.
        - Launches executions or resumes incomplete runs.
        --- Auxiliary debug functions ---
    * `populate` — populates the CHU Nantes Orthanc PACS with the processed output and the input datasets
        - Download the processed output alongside the input dataset
        - Inspect DICOM files for inconsistencies and fixes them
        - Upload the processed output along the input dataset to an orthanc instance
        - Assign labels to the orthanc studies
    * `pacs` — Runs functions for the environment of CHU Nantes to inspect the Orthanc PACS
    Usage:
    -----
        uv run main.py ecan execute
        uv run main.py ecan populate
        uv run main.py ecan pacs
    """


@app.command()
def execute() -> None:
    """
    Run the eCAN processing pipeline
    """
    working_file_path, save_file_path = get_working_files("ecan")
    tracking_file_path = get_tracking_file("ecan")

    init_serialization(working_file_path, save_file_path, tracking_file_path, generate_json, {"download_dir": Path()})


@app.command()
def populate() -> None:
    pass


@app.command()
def pacs() -> None:
    pass
