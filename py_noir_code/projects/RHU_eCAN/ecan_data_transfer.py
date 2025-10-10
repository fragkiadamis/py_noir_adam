import os
import json
from pathlib import Path
from typing import List, Optional

from py_noir_code.src.orthanc.orthanc_context import OrthancContext
from py_noir_code.src.orthanc.orthanc_service import get_http_headers, upload_dicom_file, assign_label_to_study
from py_noir_code.src.security.authentication_service import load_orthanc_password
from py_noir_code.src.shanoir_object.dataset.dataset_service import download_dataset_processing, \
    find_processed_dataset_ids_by_input_dataset_id, get_dataset
from py_noir_code.src.shanoir_object.subject.subject_service import get_subject_by_id
from py_noir_code.src.utils.file_utils import get_values_from_csv
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()


def get_study_label_for_subject(subject_name: str, csv_paths: List[str]) -> Optional[str]:
    """
    Determine the label to assign based on subject membership in CSV files.
    Args:
        subject_name (str): Subject name to check.
        csv_paths (List[str]): List of CSV files mapping subject names to labels.

    Returns:
        Optional[str]: The label name if the subject is found, otherwise None.
    """
    for csv_path in csv_paths:
        subject_names = get_values_from_csv(csv_path, "SubjectName")
        if subject_name in subject_names:
            return Path(csv_path).stem  # CSV filename (without extension) as label
    return None


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
    """
    Uploads all DICOM studies from a dataset directory to an Orthanc PACS server.
    Assigns study labels based on subject membership in predefined CSV subsets.
    Args:
        dataset_path (Path): Path to the root dataset directory.

    Returns:
        None
    """
    total_file_count, dicom_count = 0, 0
    load_orthanc_password()

    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}\n")

    csv_paths = [
        "py_noir_code/projects/RHU_eCAN/ican_subset.csv",
        "py_noir_code/projects/RHU_eCAN/angptl6_subset.csv",
    ]

    for patient_dir in dataset_path.iterdir():
        if not patient_dir.is_dir():
            continue

        patient = get_subject_by_id(patient_dir.name)
        if not patient:
            logger.warning(f"⚠️ No subject found for folder {patient_dir.name}")
            continue

        label = get_study_label_for_subject(patient["name"], csv_paths)
        if label:
            logger.info(f"Subject {patient['name']} matched label '{label}'")
        else:
            logger.info(f"Subject {patient['name']} has no associated label")

        for study_dir in patient_dir.iterdir():
            if not study_dir.is_dir():
                continue

            logger.info(f"\n📂 Uploading study: {study_dir}")
            study_orthanc_id = None

            for dcm_file in sorted(study_dir.rglob("*.dcm"), key=lambda p: str(p).lower()):
                total_file_count += 1
                study_id = upload_dicom_file(dcm_file, endpoint, headers)
                if study_id:
                    dicom_count += 1
                    study_orthanc_id = study_id

            if label and study_orthanc_id:
                assign_label_to_study(endpoint, headers, study_orthanc_id, label)

    if dicom_count == total_file_count:
        logger.info(f"\n✅ SUCCESS: {dicom_count} DICOM file(s) successfully imported.\n")
    else:
        logger.warning(f"\n⚠️ WARNING: Only {dicom_count}/{total_file_count} files imported successfully.\n")
