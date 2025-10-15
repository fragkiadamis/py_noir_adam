import os
from pathlib import Path

import pydicom
from pydicom.dataset import Dataset
from pynetdicom import AE, StoragePresentationContexts

from py_noir_code.src.security.authentication_service import load_orthanc_password
from py_noir_code.src.orthanc.orthanc_context import OrthancContext
from py_noir_code.src.orthanc.orthanc_service import get_study_orthanc_id_by_uid, get_http_headers, get_study_metadata, \
    set_study_label, upload_dicom_file
from py_noir_code.src.shanoir_object.dataset_processing.dataset_processing_service import \
    find_processed_dataset_ids_by_input_dataset_id, download_dataset_processing
from py_noir_code.src.utils.file_utils import get_values_from_csv
from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()

TAGS_TO_CHECK = {
    "FrameOfReferenceUID": (0x0020, 0x0052),
    "ImageOrientationPatient": (0x0020, 0x0037),
    "PixelSpacing": (0x0028, 0x0030),
    "SliceThickness": (0x0018, 0x0050),
    "Rows": (0x0028, 0x0010),
    "Columns": (0x0028, 0x0011),
    "NumberOfFrames": (0x0028, 0x0008),
    "StudyInstanceUID": (0x0020, 0x000D)
}


def fetch_datasets_from_json(ecan_json_path: str) -> str:
    """
    Fetch and download processed datasets based on an ECAN JSON export file.

    Args:
        ecan_json_path (str): Path to the ECAN JSON file containing dataset IDs.

    Returns:
        str: Path to the directory where the downloaded datasets are stored.
    """
    # Load JSON content safely
    dataset_ids_list = get_values_from_csv(ecan_json_path, "DatasetId")

    # Map each subject ID to its related processed dataset IDs
    processing_ids_list = []
    execution_ids = get_values_from_csv("py_noir_code/resources/datasets/execution_ids.csv", "ExecutionId")
    for dataset_id in dataset_ids_list:
        processing_list = find_processed_dataset_ids_by_input_dataset_id(dataset_id)
        processing_ids_list.extend([item["id"] for item in processing_list if str(item["parentId"]) in execution_ids])

    # Download all processed datasets grouped by subject
    output_dir = "py_noir_code/resources/downloads"
    os.makedirs(output_dir, exist_ok=True)
    download_dataset_processing(processing_ids_list, output_dir, unzip=True)

    return output_dir


def load_first_dicom(dir_path: Path) -> Dataset:
    """
    Load the first DICOM file found in the specified directory.

    Args:
        dir_path (Path): Path to the directory containing DICOM files.

    Returns:
        Dataset: The first pydicom Dataset object loaded from the directory.
    """
    files = sorted(dir_path.rglob("*.dcm"))
    return pydicom.dcmread(files[0])


def check_series_tag_consistency(series_dir: str, fix_files: bool = False) -> bool:
    """
    Check that all DICOM tags in TAGS_TO_CHECK are consistent across all instances in the series.
    If inconsistencies are found, fix them by assigning the reference value to the inconsistent files.

    Args:
        series_dir (str): Path to the DICOM series directory.
        fix_files (bool): Fix inconsistent files with the value of the reference file.

    Returns:
        bool: True if all tags were initially consistent, False otherwise.
    """
    logger.info(f"Checking tag consistency for image series: {series_dir}")
    files = [os.path.join(series_dir, f) for f in os.listdir(series_dir) if f.endswith(".dcm")]
    if not files:
        logger.info("No DICOM files found in the series.")
        return False

    # Load first file as reference
    ref_ds = pydicom.dcmread(files[0], stop_before_pixels=False)
    all_consistent = True

    for name, tag in TAGS_TO_CHECK.items():
        ref_val = getattr(ref_ds, name, None)
        inconsistent_files = []

        for f in files[1:]:
            ds = pydicom.dcmread(f, stop_before_pixels=False)
            val = getattr(ds, name, None)
            if val != ref_val:
                inconsistent_files.append(f)
                setattr(ds, name, ref_val)
                if fix_files:
                    ds.save_as(f)  # overwrite with a corrected value

        if inconsistent_files:
            all_consistent = False
            logger.info(f"Tag '{name}' ({tag}) not consistent across series. Reference value: {ref_val}")
        else:
            logger.info(f"Tag '{name}' consistent across all slices.")

    if all_consistent:
        logger.info("All tags consistent across series.")
    else:
        logger.info("Inconsistent tags were found and fixed using the reference values.")

    return all_consistent


def inspect_study_tags(input_dir: str) -> None:
    """
    Inspect spatial and reference DICOM tags for all studies in the downloads folder.

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
            series_ok = check_series_tag_consistency(mr_dir, fix_files=True)
            if not series_ok:
                logger.warning(f"Series tag consistency check failed for {mr_dir}")


def upload_to_orthanc_pacs(dataset_path: str) -> None:
    """
    Upload all DICOM studies from a dataset directory to an Orthanc PACS server.

    Args:
        dataset_path (str): Path to the root dataset directory containing processing subfolders.

    Returns:
        None
    """
    total_file_count, dicom_count = 0, 0
    load_orthanc_password()

    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}\n")

    for processing in os.listdir(dataset_path):
        processing_dir = os.path.join(dataset_path, processing)
        logger.info(f"Uploading orthanc study: {processing}")
        dcm_files = [
            os.path.join(root, f)
            for root, dirs, files in os.walk(processing_dir)
            for f in files
            if f.endswith(".dcm")
        ]

        for dcm in dcm_files:
            total_file_count += 1
            success = upload_dicom_file(dcm, endpoint, headers)
            if success:
                dicom_count += 1

    if dicom_count == total_file_count:
        logger.info(f"\nSUCCESS: {dicom_count} DICOM file(s) successfully imported.\n")
    else:
        logger.warning(f"\nWARNING: Only {dicom_count}/{total_file_count} files imported successfully.\n")


def send_dicom_to_pacs_cstore(dataset_dir: str) -> None:
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


def assign_label_to_study(dataset_path: str) -> None:
    ican_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/ican_subset.csv", "SubjectName")
    angptl6_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/angptl6_subset.csv", "SubjectName")

    load_orthanc_password()
    endpoint = f"{OrthancContext.scheme}://{OrthancContext.domain}:{OrthancContext.rest_api_port}"
    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    logger.info(f"PACS Endpoint: {endpoint}\n")

    for idx, process in enumerate(os.listdir(dataset_path), start=1):
        process_dir = os.path.join(dataset_path, process)
        for item in os.listdir(process_dir):
            if item == "output":
                continue

            item_dir = os.path.join(process_dir, item)
            dcm_file = os.path.join(item_dir, os.listdir(item_dir)[0])
            ds = pydicom.dcmread(dcm_file)

            orthanc_study_id = get_study_orthanc_id_by_uid(endpoint, headers, ds.StudyInstanceUID)
            study_meta = get_study_metadata(endpoint, headers, orthanc_study_id)

            if study_meta["PatientMainDicomTags"]["PatientName"] in ican_ids:
                set_study_label(endpoint, headers, orthanc_study_id, "ICAN_SUBSET")
            elif study_meta["PatientMainDicomTags"]["PatientName"] in angptl6_ids:
                set_study_label(endpoint, headers, orthanc_study_id, "ANGPTL6_SUBSET")
            break
