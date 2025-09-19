import sys
import os
from typing import List, Dict, Any

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


def get_acquisition_date(meta: Dict[str, Any]) -> datetime:
    """
    Extract the acquisition date from DICOM metadata.

    Parameters
    ----------
    meta : Dict[str, Any]
        DICOM metadata dictionary.

    Returns
    -------
    datetime
        Acquisition date parsed from tag (0008,0012) 'Acquisition Date'.
        Returns datetime.max if missing or invalid.
    """
    tag = meta.get("00080012")
    if tag and "Value" in tag and tag["Value"]:
        return datetime.strptime(tag["Value"][0], "%Y%m%d")
    return datetime.max


def get_number_of_slices(meta: Dict[str, Any]) -> int:
    """
    Extract the number of slices from DICOM metadata.

    Parameters
    ----------
    meta : Dict[str, Any]
        DICOM metadata dictionary.

    Returns
    -------
    int
        Number of slices from tag (0054,0081) 'Number of Slices'.
        Defaults to 1 if missing.
    """
    tag = meta.get("00540081")
    if tag and "Value" in tag and tag["Value"]:
        return int(tag["Value"][0])
    return 1


def get_slice_thickness(meta: Dict[str, Any]) -> float:
    """
    Extract the slice thickness from DICOM metadata.

    Parameters
    ----------
    meta : Dict[str, Any]
        DICOM metadata dictionary.

    Returns
    -------
    float
        Slice thickness in millimeters from tag (0018,0050) 'Slice Thickness'.
        Defaults to 1000.0 if missing.
    """
    tag = meta.get("00180050")
    if tag and "Value" in tag and tag["Value"]:
        return float(tag["Value"][0])
    return 1000.0


def query_datasets(csv_path: str, csv_column: str) -> defaultdict[Any, List]:
    """
    Query SOLR for datasets corresponding to subject IDs from a CSV file.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file containing subject identifiers.
    csv_column : str
        Name of the column containing subject IDs.

    Returns
    -------
    defaultdict[Any, List]
        A mapping of examinationId → list of dataset entries.
    """
    subject_id_list = get_values_from_csv(csv_path, csv_column)
    logger.info("Searching for subjects' datasets...")

    # Query the datasets for each subject using SOLR
    query = SolrQuery()
    query.size = 100000
    query.expert_mode = True
    query.search_text = f"subjectName: ({subject_id_list[0]}"
    for subject in subject_id_list[1:]:
        query.search_text = query.search_text + " OR " + subject
    query.search_text = query.search_text + ") AND datasetName: *TOF*"
    result = solr_search(query).json()

    datasets_by_exam_id = defaultdict(list)
    for item in result["content"]:
        datasets_by_exam_id[item["examinationId"]].append(item)

    return datasets_by_exam_id


def filter_datasets(datasets_by_exam_id: defaultdict[Any, List]) -> Dict[Any, Any]:
    """
    Filter datasets to select a single series per exam.

    Selection criteria
    ------------------
    1. Oldest dataset acquisition (based on Acquisition Date).
    2. Number of slices > 50 (tag 0054,0081).
    3. Optionally, slice thickness < 10 mm (tag 0018,0050).

    Parameters
    ----------
    datasets_by_exam_id : defaultdict[Any, List]
        A mapping of examinationId → list of dataset entries.

    Returns
    -------
    Dict[Any, Any]
        A mapping of examinationId → selected dataset ID.
    """
    filtered = {}
    for exam_id, datasets in datasets_by_exam_id.items():
        if len(datasets) == 1:
            # Only one dataset, no filtering needed
            filtered[exam_id] = datasets[0]
            continue

        enriched = []
        for dataset in datasets:
            meta = get_dataset_dicom_metadata(dataset["id"])[0]
            dataset["dicom_meta"] = meta
            enriched.append(dataset)

        # Sort by acquisition date (oldest first)
        enriched.sort(key=lambda x: get_acquisition_date(x["dicom_meta"]))

        # Apply filters
        valid = []
        for d in enriched:
            meta = d["dicom_meta"]
            if get_number_of_slices(meta) <= 50:
                continue
            if get_slice_thickness(meta) >= 10:  # optional filter
                continue
            valid.append(d)

        if valid:
            filtered[exam_id] = valid[0]  # Oldest valid dataset
        else:
            filtered[exam_id] = enriched[0]  # Fallback: oldest dataset

    return filtered


def generate_rhu_ecan_json() -> List[Any]:
    """
    Generate JSON configurations for RHU eCAN executions.

    The function:
    - Reads subject IDs from predefined CSV files.
    - Queries and filters their datasets.
    - Builds JSON payloads for each selected dataset.

    Returns
    -------
    List[Any]
        A list of execution configurations (dictionaries).
    """
    csv_paths = (
        "py_noir_code/projects/RHU_eCAN/ican_subset_subject_ids.csv",
        "py_noir_code/projects/RHU_eCAN/angptl6_subset_subject_ids.csv"
    )

    executions, identifier = [], 0
    for csv_path in csv_paths:
        dataset_subset = query_datasets(csv_path, "Subject_ID")
        filtered_subset = filter_datasets(dataset_subset)
        logger.info("Building json content...")
        for exam_id, dataset in filtered_subset.items():
            dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
            executions.append({
                "identifier": identifier,
                "name": f"landmarkDetection_04_exam_{exam_id}_{dt}",
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
    return executions
