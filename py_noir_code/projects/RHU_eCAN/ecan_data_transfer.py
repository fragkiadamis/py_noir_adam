import os
import json
import base64
import httplib2
from pathlib import Path
from typing import Dict

from py_noir_code.src.shanoir_object.dataset.dataset_service import download_dataset_processing, \
    find_processed_dataset_ids_by_input_dataset_id, get_dataset
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()


def get_headers(username: str, password: str) -> Dict[str, str]:
    auth_str = f"{username}:{password}"
    auth_bytes = auth_str.encode("utf-8")  # Convert to bytes
    auth_encoded = base64.b64encode(auth_bytes).decode("utf-8")  # Encode and decode back to string

    return {
        'content-type': 'application/dicom',
        'Authorization': f"Basic {auth_encoded}"
    }


def fetch_datasets_from_json(ecan_json_path: Path) -> Path:
    """
    Fetch and download processed datasets from an ECAN JSON export.

    This function performs the following steps:
      1. Reads the provided ECAN JSON file containing dataset information.
      2. Extracts input dataset IDs from each exam entry.
      3. Finds the processing entries that produced output datasets.
      4. Groups processing IDs by subject.
      5. Downloads the corresponding processed datasets for each subject.

    Args:
        ecan_json_path (str): Path to the ECAN JSON file.

    Returns:
        List[Any]: List of results from the download calls, if `download_dataset_processing` returns data.
    """
    # Load JSON content safely
    with open(ecan_json_path, "r") as json_file:
        processed_exam = json.load(json_file)

    # Extract input dataset IDs from each exam
    exam_input_datasets = [
        item["datasetParameters"][0]["datasetIds"]
        for item in processed_exam
        if "identifier" in item
    ]

    # Map each subject ID to its related processed dataset IDs
    subject_datasets = {}
    all_dataset_ids = [dataset_id for exam in exam_input_datasets for dataset_id in exam]
    for dataset_id in all_dataset_ids:
        processing_list = find_processed_dataset_ids_by_input_dataset_id(dataset_id)
        processing_list = [item for item in processing_list if item["outputDatasets"]]
        if len(processing_list) == 0:
            continue

        dataset = get_dataset(dataset_id)
        subject_datasets[dataset['subjectId']] = [proc["id"] for proc in processing_list]

    # Download all processed datasets grouped by subject
    output_dir = Path(f"py_noir_code/resources/downloads")
    for (subject_id, dataset_processing) in subject_datasets.items():
        subject_dir = output_dir.joinpath(str(subject_id))
        os.makedirs(subject_dir, exist_ok=True)
        download_dataset_processing(dataset_processing, str(subject_dir), unzip=True)

    return output_dir


def upload_to_orthanc_pacs(dataset_path: Path) -> None:
    total_file_count, dicom_count = 0, 0

    URL = "http://localhost:8042/instances"
    logger.info(f"PACS URL: {URL}\n")

    # Recursively upload a directory
    dcm_files = sorted(dataset_path.rglob("*.dcm"), key=lambda p: str(p).lower())
    for dcm_file in dcm_files:
        logger.info(f"Importing {dcm_file}")
        with open(dcm_file, "rb") as dcm:
            dcm_content = dcm.read()
        dcm.close()
        total_file_count += 1

        h = httplib2.Http()
        headers = get_headers("orthanc", "orthanc")
        resp, content = h.request(URL, 'POST', body=dcm_content, headers=headers)
        if resp.status == 200:
            logger.info(" => success\n")
            dicom_count += 1

    if dicom_count == total_file_count:
        logger.info(f"SUCCESS: {dicom_count} DICOM file(s) have been successfully imported\n")
    else:
        logger.info(f"WARNING: Only {dicom_count} out of {total_file_count} file(s) have been successfully imported as DICOM instance(s)")
