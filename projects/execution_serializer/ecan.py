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
from src.utils.file_writer import FileWriter
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_working_files, get_tracking_file, get_items_from_input_file, get_working_directory, \
    get_dict_from_csv
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

            for exam_id in List(exam_items.keys()):
                if exam_id != str(oldest_exam["id"]):
                    del exam_items[exam_id]


def download_and_filter_datasets(subjects_datasets: defaultdict[Any, defaultdict[Any, List]], download_dir: Path) -> List:
    filtered_datasets = []
    for idx, (subject, exam_items) in enumerate(subjects_datasets.items(), start=1):
        for key in list(exam_items.keys()):
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


def generate_json(download_dir: Path) -> List[Dict]:
    subject_name_list = [
        *get_items_from_input_file("ican_subset.txt"),
        *get_items_from_input_file("angptl6_subset.txt")
    ]

    executions = []
    subjects_datasets = query_datasets(subject_name_list)
    find_oldest_exams(subjects_datasets)
    filtered_datasets = download_and_filter_datasets(subjects_datasets, download_dir)

    logger.info("Building json content...")
    for idx, dataset in enumerate(filtered_datasets, start=1):
        df = pd.read_csv(ConfigPath.tracking_file_path)
        print(dataset)
        values = {
            "identifier": idx,
            "dataset_id": dataset["id"],
            "examination_id": dataset["examinationId"],
            "subject_id": dataset["subjectId"],
            "subject_name": dataset["subjectName"],
            "get_from_shanoir": True,
            "executable": True
        }
        for col, val in values.items():
            df.loc[0, col] = val
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
    get_working_files("ecan")
    get_tracking_file("ecan")
    download_dir = get_working_directory("downloads", "ecan", "shanoir_output")
    init_serialization(generate_json, kwargs={"download_dir": download_dir})


@app.command()
def populate() -> None:
    pass


@app.command()
def pacs() -> None:
    pass
