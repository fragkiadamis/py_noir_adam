import logging
import sys
import os
from typing import List, Dict, Any, Tuple

from py_noir_code.src.shanoir_object.dataset.dataset_service import get_dataset_dicom_metadata

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from collections import defaultdict
from datetime import datetime

from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.shanoir_object.solr_query.solr_query_model import SolrQuery
from py_noir_code.src.shanoir_object.solr_query.solr_query_service import solr_search
from py_noir_code.src.utils.file_utils import get_values_from_csv
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()


def get_number_of_slices(meta: Dict[str, Any]) -> int | None:
    """
    Extract the number of slices from DICOM metadata.

    Parameters
    ----------
    meta : Dict[str, Any]
        DICOM metadata dictionary.

    Returns
    -------
    int | None
        Number of slices derived from tag (0054,0081) 'Number of Slices'.
        Falls back to tag (0020,0013) 'Instance Number' if unavailable.
        Returns None if neither tag is present or valid.
    """
    # Primary: Number of Slices
    tag = meta.get("00540081")
    if tag and "Value" in tag and tag["Value"]:
        return int(tag["Value"][0])

    # Fallback: Instance Number
    instance_tag = meta.get("00200013")
    if instance_tag and "Value" in instance_tag and instance_tag["Value"]:
        return int(instance_tag["Value"][0])

    return None


def get_slice_thickness(meta: Dict[str, Any]) -> float | None:
    """
    Extract the slice thickness from DICOM metadata.

    Parameters
    ----------
    meta : Dict[str, Any]
        DICOM metadata dictionary.

    Returns
    -------
    float | None
        Slice thickness in millimeters from tag (0018,0050) 'Slice Thickness'.
        Returns None if the tag is missing or invalid.
    """
    tag = meta.get("00180050")
    if tag and "Value" in tag and tag["Value"]:
        return float(tag["Value"][0])

    return None


def query_datasets(subject_name_list: List) -> defaultdict[Any, List]:
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

    datasets_by_exam_id = defaultdict(list)
    for item in result["content"]:
        datasets_by_exam_id[item["examinationId"]].append(item)

    return datasets_by_exam_id


def filter_datasets(datasets_by_exam_id: defaultdict[Any, List]) -> Dict[Any, Any]:
    """
    Filter datasets to select a single series per exam based on DICOM metadata.

    Selection Criteria
    ------------------
    1. Select the oldest dataset (based on datasetCreationDate).
    2. Number of slices ≥ 40 (from tag 0054,0081 or fallback 0020,0013).
    3. Slice thickness ≤ 10 mm (from tag 0018,0050).

    If multiple datasets are available for an exam, the oldest one that satisfies
    the criteria is selected. Exams with no dataset meeting the criteria are excluded.

    Parameters
    ----------
    datasets_by_exam_id : defaultdict[Any, List]
        Mapping of examinationId → list of dataset entries.

    Returns
    -------
    Dict[Any, Any]
        Mapping of examinationId → list containing the selected dataset entry.
    """
    filtered_datasets = {}
    for exam_id, dataset_list in datasets_by_exam_id.items():
        # Sort datasets by creation date (oldest first)
        sorted_datasets = sorted(
            dataset_list,
            key=lambda d: datetime.fromisoformat(d['datasetCreationDate'].replace('Z', '').split('+')[0])
        )

        selected_dataset = None
        for ds in sorted_datasets:
            ds["dicom_meta"] = get_dataset_dicom_metadata(ds["id"])[0]
            num_slices = get_number_of_slices(ds["dicom_meta"])
            slice_thickness = get_slice_thickness(ds["dicom_meta"])

            if num_slices >= 40 and slice_thickness <= 10:
                selected_dataset = ds
                break  # found one that fits, stop searching

        if selected_dataset:
            filtered_datasets[exam_id] = selected_dataset

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
        dataset_subset = query_datasets(subject_name_list)
        filtered_subset = filter_datasets(dataset_subset)
        dataset_ids_list.extend(ds["id"] for ds in filtered_subset.values())
        logger.info("Building json content...")
        for exam_id, dataset in filtered_subset.items():
            dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
            executions.append({
                "identifier": identifier,
                "name": f"landmarkDetection_0_4_exam_{exam_id}_{dt}",
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
