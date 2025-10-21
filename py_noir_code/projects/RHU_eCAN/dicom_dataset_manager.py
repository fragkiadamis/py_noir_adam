import os
from typing import Tuple, List

import pydicom
from pydicom.dataset import Dataset
from pydicom.uid import generate_uid
from pynetdicom import AE, StoragePresentationContexts

from py_noir_code.src.security.authentication_service import load_orthanc_password
from py_noir_code.src.orthanc.orthanc_context import OrthancContext
from py_noir_code.src.orthanc.orthanc_service import get_study_orthanc_id_by_uid, get_http_headers, \
    get_orthanc_study_metadata, set_orthanc_study_label, upload_dicom_file_to_orthanc, delete_orthanc_study, \
    get_orthanc_patients, get_orthanc_patient_meta, get_all_orthanc_studies
from py_noir_code.src.shanoir_object.dataset.dataset_service import find_processed_dataset_ids_by_input_dataset_id, \
    download_dataset_processing, get_dataset_processing, get_dataset

from py_noir_code.src.utils.file_utils import get_values_from_csv, save_values_to_csv
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()

TAGS_TO_CHECK = {
    "FrameOfReferenceUID": (0x0020,0x0052),
    "ImageOrientationPatient": (0x0020,0x0037),
    "PixelSpacing": (0x0028,0x0030),
    "SliceThickness": (0x0018,0x0050),
    "Rows": (0x0028,0x0010),
    "Columns": (0x0028,0x0011),
    "NumberOfFrames": (0x0028,0x0008),
    "StudyInstanceUID": (0x0020,0x000D)
}

SEQUENCE_TAG = (0x0040,0x0275)
SEQUENCE_ITEM_TAG = (0x0040,0x0008)


def fetch_datasets_from_json(ecan_json_path: str, executions_csv: str, output_dir: str) -> None:
    """
    Fetch and download processed datasets based on an ECAN JSON export file.

    Args:
        ecan_json_path (str): Path to the ECAN JSON file containing dataset IDs.
        executions_csv (str): Path to the executions csv file containing the successful executions IDs.
        output_dir (str): Output directory path for the downloaded datasets.

    Returns:
        str: Path to the directory where the downloaded datasets are stored.
    """
    # Load JSON content safely
    dataset_ids_list = get_values_from_csv(ecan_json_path, "DatasetId")

    # Map each subject ID to its related processed dataset IDs
    processing_ids_list = []
    execution_ids = get_values_from_csv(executions_csv, "ExecutionId")
    for dataset_id in dataset_ids_list:
        processing_list = find_processed_dataset_ids_by_input_dataset_id(dataset_id)
        processing_ids_list.extend([item["id"] for item in processing_list if str(item["parentId"]) in execution_ids])

    # Download all processed datasets grouped by subject
    os.makedirs(output_dir, exist_ok=True)
    download_dataset_processing(processing_ids_list, output_dir, unzip=True)


def load_first_dicom(dir_path: str) -> Dataset:
    """
    Load the first DICOM file found in the specified directory.

    Args:
        dir_path (str): Path to the directory containing DICOM files.

    Returns:
        Dataset: The first pydicom Dataset object loaded from the directory.
    """
    files = sorted(
        os.path.join(root, f)
        for root, _, files_in_dir in os.walk(dir_path)
        for f in files_in_dir
        if f.endswith(".dcm")
    )
    return pydicom.dcmread(files[0])


def check_series_tag_consistency(series_dir: str) -> Tuple[List, List] | None:
    """
    Check for inconsistencies in key DICOM tags across all instances in a series.

    Args:
        series_dir (str): Path to the DICOM series directory.

    Returns:
        tuple[list, list] | None:
            - A tuple (inconsistent_tags, inconsistent_files) if inconsistencies are found.
            - None if no DICOM files are present.
    """
    logger.info(f"Checking tag consistency for image series: {series_dir}")
    files = [os.path.join(series_dir, f) for f in os.listdir(series_dir) if f.endswith(".dcm")]
    if not files:
        logger.info("No DICOM files found in the series.")
        return None

    # Load first file as reference
    ref_ds = pydicom.dcmread(files[0], stop_before_pixels=False)
    inconsistent_tags, inconsistent_files = [], []
    for tag_name, tag in TAGS_TO_CHECK.items():
        ref_val = getattr(ref_ds, tag_name, None)

        inconsistent_tag = None
        for f in files[1:]:
            ds = pydicom.dcmread(f, stop_before_pixels=False)
            val = getattr(ds, tag_name, None)
            if val != ref_val:
                inconsistent_tag = (tag_name, tag)
                inconsistent_files.append(f)

        if inconsistent_files and inconsistent_tag is not None:
            inconsistent_tags.append(inconsistent_tag)

    return inconsistent_tags, inconsistent_files


def remove_sequence(mr_dir: str) -> None:
    """
    Remove a specific DICOM sequence tag from all instances in a directory.

    Args:
        mr_dir (str): Path to the directory containing DICOM instances.

    Returns:
        None
    """
    for filename in os.listdir(mr_dir):
        path = os.path.join(mr_dir, filename)
        ds = pydicom.dcmread(path, stop_before_pixels=False)

        if SEQUENCE_TAG not in ds:
            ds.save_as(path)
            continue

        cleaned = False
        for item in ds[SEQUENCE_TAG].value:
            if SEQUENCE_ITEM_TAG not in item:
                continue

            found_item = item[(0x0040, 0x0008)]
            if found_item.VR != "SQ":
                continue

            if len(found_item.value) < 2 and len(found_item.value[0]) == 0:
                del item[SEQUENCE_ITEM_TAG]
                cleaned = True

        if cleaned:
            ds.save_as(path)


def fix_inconsistent_tags(mr_dir: str, inconsistent_tags, inconsistent_files) -> None:
    """
    Fix inconsistent DICOM tags by assigning a consistent reference value.

    Args:
        mr_dir (str): Path to the directory containing DICOM instances.
        inconsistent_tags (list): List of (tag_name, tag) pairs found inconsistent.
        inconsistent_files (list): List of file paths that have inconsistent tags.

    Returns:
        None
    """
    for tag_name, _ in inconsistent_tags:
        logger.info(f"Inconsistent tag: {tag_name}")

        if tag_name == "FrameOfReferenceUID":
            new_uid = generate_uid()
            for instance in os.listdir(os.path.join(mr_dir, mr_dir)):
                instance_path = os.path.join(mr_dir, instance)
                ds = pydicom.dcmread(instance_path, stop_before_pixels=False)
                setattr(ds, tag_name, new_uid)
                ds.save_as(instance_path)
            continue

        # Otherwise, copy the reference value from the first file
        ref_ds = load_first_dicom(mr_dir)
        ref_value = getattr(ref_ds, tag_name, None)

        for f in inconsistent_files:
            ds = pydicom.dcmread(f, stop_before_pixels=False)
            setattr(ds, tag_name, ref_value)
            ds.save_as(f)


def inspect_and_fix_study_tags(input_dir: str) -> None:
    """
    Inspect and correct DICOM tag inconsistencies across all studies in a dataset.

    Args:
        input_dir (str): Path to the root folder containing patient subfolders with study data.

    Returns:
        None
    """
    for processing in os.listdir(input_dir):
        processing_dir = os.path.join(input_dir, processing)
        for item in os.listdir(processing_dir):
            if item == "output":
                continue
            mr_dir = os.path.join(processing_dir, item)
            logger.info(f"Checking DICOM tags consistency across the series")
            inconsistent_tags, inconsistent_files = check_series_tag_consistency(mr_dir)

            remove_sequence(mr_dir)

            if not inconsistent_tags:
                logger.info(f"Tags consistent across all slices.")
                continue

            fix_inconsistent_tags(mr_dir, inconsistent_tags, inconsistent_files)


def upload_to_pacs_rest(dataset_path: str, studies_csv: str) -> None:
    """
    Upload all DICOM studies from a dataset directory to an Orthanc PACS server.

    Args:
        dataset_path (str): Path to the root dataset directory containing processing subfolders.
        studies_csv (str): Path to the csv file to save the uploaded study IDs.

    Returns:
        None
    """
    total_file_count, dicom_count = 0, 0
    ican_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/ican_subset.csv", "SubjectName")
    angptl6_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/angptl6_subset.csv", "SubjectName")

    load_orthanc_password()
    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}")

    study_ids = []
    for study in os.listdir(dataset_path):
        parent_study_orthanc_id = None
        study_dir = os.path.join(dataset_path, study)
        logger.info(f"Uploading orthanc study: {study}")
        dcm_files = [
            os.path.join(root, f)
            for root, dirs, files in os.walk(study_dir)
            for f in files
            if f.endswith(".dcm")
        ]

        for dcm in dcm_files:
            total_file_count += 1
            response = upload_dicom_file_to_orthanc(dcm, endpoint, headers)
            if response["Status"] == "Success" or response["Status"] == "AlreadyStored":
                dicom_count += 1
                parent_study_orthanc_id = response["ParentStudy"]

        study_ids.append(parent_study_orthanc_id)
        processing_id = study.split("_")[1]
        processing = get_dataset_processing(processing_id)
        dataset = get_dataset(str(processing["inputDatasets"][0]))
        subject_name = dataset["datasetAcquisition"]["examination"]["subject"]["name"]

        if subject_name in ican_ids:
            set_orthanc_study_label(endpoint, headers, parent_study_orthanc_id, "ICAN_SUBSET")
        elif subject_name in angptl6_ids:
            set_orthanc_study_label(endpoint, headers, parent_study_orthanc_id, "ANGPTL6_SUBSET")

    if len(study_ids) > 0:
        existing_study_ids = get_values_from_csv(studies_csv, "StudyID")
        if existing_study_ids is None:
            save_values_to_csv(study_ids, "StudyID", studies_csv)
        else:
            for study_id in study_ids.copy():
                if study_id in existing_study_ids:
                    study_ids.remove(study_id)
            existing_study_ids.extend(study_ids)
            save_values_to_csv(existing_study_ids, "StudyID", studies_csv)


    if dicom_count == total_file_count:
        logger.info(f"SUCCESS: {dicom_count} DICOM file(s) successfully imported.")
    else:
        logger.warning(f"WARNING: Only {dicom_count}/{total_file_count} files imported successfully.")


def upload_to_pacs_dicom(dataset_dir: str) -> None:
    """
    Sends all DICOM files found recursively in a given directory to a PACS server
    using the DICOM C-STORE service (DICOM network storage).

    Args:
        dataset_dir (str): Path to the root directory containing DICOM files
            to be uploaded. All subdirectories are searched recursively.

    Returns:
        None
    """
    # Initialize AE
    ae = AE(ae_title=OrthancContext.client_ae_title)
    ae.acse_timeout = 30
    ae.network_timeout = 30

    # Add requested presentation contexts for common DICOM storage classes
    for context in StoragePresentationContexts:
        ae.add_requested_context(context.abstract_syntax)

    # Associate with PACS
    assoc = ae.associate(
        OrthancContext.domain,
        int(OrthancContext.dicom_server_port),
        ae_title=OrthancContext.pacs_ae_title
    )

    if not assoc.is_established:
        logger.error("Failed to associate with PACS server.")
        return

    dcm_files = [
        os.path.join(root, f)
        for root, dirs, files in os.walk(dataset_dir)
        for f in files
        if f.endswith(".dcm")
    ]

    logger.info(f"Found {len(dcm_files)} DICOM file(s) to upload.")
    for dcm_file in dcm_files:
        try:
            ds = pydicom.dcmread(str(dcm_file))
            status = assoc.send_c_store(ds)
            if status and status.Status == 0x0000:
                logger.info(f"Successfully sent {dcm_file}")
            else:
                logger.warning(f"Failed to send {dcm_file}, status: {status}")
        except Exception as e:
            logger.error(f"Error sending {dcm_file}: {e}")

    # Release the association
    assoc.release()
    logger.info("C-STORE upload completed.")


def assign_label_to_pacs_study(dataset_path: str) -> None:
    """
    Assign a label to each study in a dataset based on predefined subject subsets.

    Parameters
    ----------
    dataset_path : str

    Returns
    -------
    None
    """
    ican_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/ican_subset.csv", "SubjectName")
    angptl6_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/angptl6_subset.csv", "SubjectName")

    load_orthanc_password()
    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}")

    for idx, process in enumerate(os.listdir(dataset_path), start=1):
        process_dir = os.path.join(dataset_path, process)
        for item in os.listdir(process_dir):
            if item == "output":
                continue

            item_dir = os.path.join(process_dir, item)
            dcm_file = os.path.join(item_dir, os.listdir(item_dir)[0])
            ds = pydicom.dcmread(dcm_file)

            orthanc_study_id = get_study_orthanc_id_by_uid(endpoint, headers, ds.StudyInstanceUID)
            study_meta = get_orthanc_study_metadata(endpoint, headers, orthanc_study_id)

            if study_meta["PatientMainDicomTags"]["PatientName"] in ican_ids:
                set_orthanc_study_label(endpoint, headers, orthanc_study_id, "ICAN_SUBSET")
            elif study_meta["PatientMainDicomTags"]["PatientName"] in angptl6_ids:
                set_orthanc_study_label(endpoint, headers, orthanc_study_id, "ANGPTL6_SUBSET")
            break


def delete_studies_from_pacs(studies_csv: str) -> None:
    """
    Delete all studies IDs of the studies_csv from the Orthanc PACS server.

    Parameters
    ----------
    studies_csv : str

    Returns
    -------
    None
    """
    load_orthanc_password()
    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}")

    orthanc_studies_ids = get_values_from_csv(studies_csv, "StudyID")
    for orthanc_study_id in orthanc_studies_ids:
        delete_orthanc_study(endpoint, headers, orthanc_study_id)


def get_patient_ids_from_pacs() -> None:
    """
    Delete all studies in a dataset from the Orthanc PACS server.

    Returns
    -------
    None
    """

    csv_paths = [
        "py_noir_code/projects/RHU_eCAN/ican_subset.csv",
        "py_noir_code/projects/RHU_eCAN/angptl6_subset.csv"
    ]

    subject_list_names = []
    for csv_path in csv_paths:
        subject_list_names.extend(get_values_from_csv(csv_path, "SubjectName"))

    load_orthanc_password()
    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}")

    patient_list = get_orthanc_patients(endpoint, headers)
    for patient_id in patient_list:
        patient_meta = get_orthanc_patient_meta(endpoint, headers, patient_id)
        logger.info(f"Name: {patient_meta['MainDicomTags']['PatientName']}, ID: {patient_meta['MainDicomTags']['PatientID']}")
    logger.info(f"Total number of patients: {len(patient_list)}")


def purge_pacs_studies() -> None:
    load_orthanc_password()
    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}")

    orthanc_studies_ids = get_all_orthanc_studies(endpoint, headers)
    for orthanc_study_id in orthanc_studies_ids:
        delete_orthanc_study(endpoint, headers, orthanc_study_id)
