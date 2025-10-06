import os
import json
from typing import List, Any

from py_noir_code.src.shanoir_object.dataset.dataset_service import download_dataset_processing, \
    find_processed_dataset_ids_by_input_dataset_id, get_dataset
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()


def fetch_datasets_from_json(ecan_json_path) -> List[Any]:
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
    for (subject_id, dataset_processing) in subject_datasets.items():
        output_dir = f"py_noir_code/resources/downloads/{subject_id}"
        os.makedirs(output_dir, exist_ok=True)
        download_dataset_processing(dataset_processing, output_dir, unzip=True)
