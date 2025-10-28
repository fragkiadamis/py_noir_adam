import logging
import shutil
import sys
import os
import tempfile
from typing import List, Any, Tuple

import pydicom

from py_noir_code.src.shanoir_object.dataset.dataset_service import get_dataset_dicom_metadata, download_dataset, \
    get_examination

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from collections import defaultdict
from datetime import datetime

from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.shanoir_object.solr_query.solr_query_model import SolrQuery
from py_noir_code.src.shanoir_object.solr_query.solr_query_service import solr_search
from py_noir_code.src.utils.file_utils import get_values_from_csv
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()


def query_datasets(subject_name_list: List) -> defaultdict[Any, defaultdict[Any, List]]:
    """
    Query SOLR for datasets corresponding to subject IDs from a CSV file.

    Parameters
    ----------
    subject_name_list : List
        Name of the subjects

    Returns
    -------
    defaultdict[Any, List]
        A mapping of examinationId → list of dataset entries.
    """
    # Query the datasets for each subject using SOLR
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
    """
    Keep only the oldest examination for each subject.

    Parameters
    ----------
    subjects_datasets : defaultdict[Any, defaultdict[Any, List]]

    Returns
    -------
    None
    """
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


def download_and_filter_datasets(subjects_datasets: defaultdict[Any, defaultdict[Any, List]]) -> List:
    """
    Download datasets for all subjects and filter based on slice criteria.

    Parameters
    ----------
    subjects_datasets : defaultdict[Any, defaultdict[Any, List]]

    Returns
    -------
    List
        A list of filtered dataset entries that meet the slice count and thickness criteria.
    """
    filtered_datasets = []
    for idx, (subject, exam_items) in enumerate(subjects_datasets.items(), start=1):
        for key in list(exam_items.keys()):
            for ds in exam_items[key][:]:
                # download_dir = tempfile.mkdtemp(prefix=f"{subject}_{ds['id']}")
                download_dir = f"py_noir_code/resources/filtering/{subject}_{ds['id']}"
                os.makedirs(download_dir, exist_ok=True)
                download_dataset(ds["id"], "dcm", download_dir, unzip=True)

                first_file = os.path.join(download_dir, os.listdir(download_dir)[0])
                slice_thickness = pydicom.dcmread(first_file, stop_before_pixels=True)['SliceThickness'].value
                num_of_slices = len(os.listdir(download_dir))
                if num_of_slices > 50 and slice_thickness < 10:
                    filtered_datasets.append(ds)
                else:
                    shutil.rmtree(download_dir)

    return filtered_datasets


def generate_rhu_ecan_json() -> Tuple[List[Any], List[Any]]:
    """
    Generate JSON configurations for RHU eCAN executions.

    The function:
    - Reads subject IDs from predefined CSV files.
    - Queries and filters their datasets.
    - Builds JSON payloads for each selected dataset.

    Returns
    -------
    Tuple[List[Any], List[Any]
        A tuple containing: executions, dataset_ids_list
    """
    csv_paths = [
        "py_noir_code/projects/RHU_eCAN/ican_subset.csv",
        "py_noir_code/projects/RHU_eCAN/angptl6_subset.csv"
    ]

    executions, dataset_ids_list, identifier = [], [], 0
    for csv_path in csv_paths:
        subject_name_list = get_values_from_csv(csv_path, "SubjectName")
        subjects_datasets = query_datasets(subject_name_list)
        find_oldest_exams(subjects_datasets)
        filtered_datasets = download_and_filter_datasets(subjects_datasets)

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
                "refreshToken": APIContext.refresh_token,
                "client": APIContext.clientId,
                "datasetParameters": [{
                    "datasetIds": [dataset["id"]],
                    "groupBy": "EXAMINATION",
                    "name": "dicom_input_zip",
                    "exportFormat": "dcm"
                }],
            })

            identifier += 1
        logging.info(f"Finished processing {csv_path}.")
    return executions, dataset_ids_list
