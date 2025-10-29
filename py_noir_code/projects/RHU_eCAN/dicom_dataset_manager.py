import os
from typing import Tuple, List

import pydicom
from pydicom.dataset import Dataset
from pydicom.uid import generate_uid
from pynetdicom import AE, AllStoragePresentationContexts, StoragePresentationContexts, evt

from py_noir_code.src.orthanc.orthanc_context import OrthancContext
from py_noir_code.src.orthanc.orthanc_service import set_orthanc_study_label, upload_study_to_orthanc, \
    delete_orthanc_study, get_orthanc_patients, get_orthanc_patient_meta, get_all_orthanc_studies, \
    get_study_orthanc_id_by_uid, download_orthanc_study
from py_noir_code.src.shanoir_object.dataset.dataset_service import find_processed_dataset_ids_by_input_dataset_id, \
    download_dataset_processing, get_dataset_processing, get_dataset, upload_dataset_processing

from py_noir_code.src.utils.file_utils import get_values_from_csv, save_dict_to_csv, get_dict_from_csv
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


def update_studies_registry(studies, studies_csv):
    """
    Updates the studies CSV file with new study entries, avoiding duplicates.

    Args:
        studies (list[dict]): List of new study dictionaries to add.
        studies_csv (str): Path to the CSV file.
    """
    if not studies:
        return

    existing_studies = get_dict_from_csv(studies_csv)

    if existing_studies is None:
        save_dict_to_csv(studies, studies_csv)
        return

    # Filter out duplicates
    new_studies = [s for s in studies if s not in existing_studies]

    if new_studies:
        existing_studies.extend(new_studies)
        save_dict_to_csv(existing_studies, studies_csv)


def fetch_datasets_from_json(ecan_json_path: str, executions_csv: str, output_dir: str) -> None:
    """
    Fetch and download processed datasets based on an ECAN JSON export file.

    Args:
        ecan_json_path (str): Path to the ECAN JSON file containing dataset IDs.
        executions_csv (str): Path to the executions csv file containing the successful executions IDs.
        output_dir (str): Output directory path for the downloaded datasets.
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
        dcm_files = sorted(
            os.path.join(root, f)
            for root, _, files_in_dir in os.walk(mr_dir)
            for f in files_in_dir
            if f.endswith(".dcm")
        )
        ref_ds = pydicom.dcmread(dcm_files[0], stop_before_pixels=False)
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
    """
    total_file_count, dicom_count, studies = 0, 0, []
    for study in os.listdir(dataset_path):
        study_dir = os.path.join(dataset_path, study)
        logger.info(f"Uploading orthanc study: {study}")
        dcm_files = [
            os.path.join(root, f)
            for root, dirs, files in os.walk(study_dir)
            for f in files
            if f.endswith(".dcm")
        ]

        total_files, successful_uploads, response_json = upload_study_to_orthanc(dcm_files)
        total_file_count += total_files
        dicom_count += successful_uploads

        parent_study_orthanc_id = None
        if response_json and "ParentStudy" in response_json:
            parent_study_orthanc_id = response_json["ParentStudy"]

        processing_id = study.split("_")[1]
        processing = get_dataset_processing(processing_id)
        dataset = get_dataset(str(processing["inputDatasets"][0]))
        subject_name = dataset["datasetAcquisition"]["examination"]["subject"]["name"]
        study_instance_uid = pydicom.dcmread(dcm_files[0]).StudyInstanceUID
        studies.append({
            "PatientName": subject_name,
            "StudyID": parent_study_orthanc_id,
            "StudyInstanceUID": study_instance_uid
        })

    update_studies_registry(studies, studies_csv)

    logger.info(f"Total studies uploaded: {len(studies)}")
    if dicom_count == total_file_count:
        logger.info(f"SUCCESS: {dicom_count} DICOM file(s) successfully imported.")
    else:
        logger.warning(f"WARNING: Only {dicom_count}/{total_file_count} files imported successfully.")


def upload_to_pacs_dicom(dataset_path: str, studies_csv: str) -> None:
    """
    Sends all DICOM files found recursively in a given directory to a PACS server
    using the DICOM C-STORE service (DICOM network storage).

    Args:
        dataset_path (str): Path to the root dataset directory containing processing subfolders.
        studies_csv (str): Path to the csv file to save the uploaded study IDs.
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

    studies = []
    for study in os.listdir(dataset_path):
        study_dir = os.path.join(dataset_path, study)
        logger.info(f"Uploading orthanc study: {study}")
        dcm_files = [
            os.path.join(root, f)
            for root, dirs, files in os.walk(study_dir)
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

        processing_id = study.split("_")[1]
        processing = get_dataset_processing(processing_id)
        dataset = get_dataset(str(processing["inputDatasets"][0]))
        subject_name = dataset["datasetAcquisition"]["examination"]["subject"]["name"]
        study_instance_uid = pydicom.dcmread(dcm_files[0]).StudyInstanceUID
        parent_study_orthanc_id = get_study_orthanc_id_by_uid(study_instance_uid)
        studies.append({
            "PatientName": subject_name,
            "StudyID": parent_study_orthanc_id,
            "StudyInstanceUID": study_instance_uid
        })

    update_studies_registry(studies, studies_csv)

    # Release the association
    assoc.release()
    logger.info("C-STORE upload completed.")


def assign_label_to_pacs_study(studies_csv: str) -> None:
    """
    Assign a label to each study in a dataset based on predefined subject subsets.

    Args:
        studies_csv : str
    """
    ican_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/ican_subset.csv", "SubjectName")
    angptl6_ids = get_values_from_csv("py_noir_code/projects/RHU_eCAN/angptl6_subset.csv", "SubjectName")

    uploaded_studies = get_dict_from_csv(studies_csv)
    for study in uploaded_studies:
        subject_name = study["PatientName"]
        if subject_name in ican_ids:
            set_orthanc_study_label(study["StudyID"], "ICAN_SUBSET")
        elif subject_name in angptl6_ids:
            set_orthanc_study_label(study["StudyID"], "ANGPTL6_SUBSET")


def download_from_pacs_rest(studies_csv: str, download_dir: str) -> None:
    """
    Download all instances of a study via REST API.

    Args:
        studies_csv (str): Path to the studies_csv file.
        download_dir: Path to the directory where the downloaded files will be saved.
    """
    studies = get_dict_from_csv(studies_csv)
    os.makedirs(download_dir, exist_ok=True)
    for study in studies:
        download_orthanc_study(study["StudyID"], download_dir)


def start_storage_scp(storage_dir: str) -> None:
    """
    Start a Storage SCP to receive DICOM files sent via C-MOVE.

    Args:
        storage_dir (str): Path to the directory where the received files will be saved.
    """
    os.makedirs(storage_dir, exist_ok=True)

    def handle_store(event):
        ds = event.dataset
        ds.file_meta = event.file_meta
        file_path = os.path.join(storage_dir, f"{ds.SOPInstanceUID}.dcm")
        ds.save_as(file_path)
        logger.info(f"Received {ds.SOPInstanceUID}")
        return 0x0000  # Success status

    ae = AE(ae_title=OrthancContext.client_ae_title)
    # Add all Storage SOP Classes
    for context in AllStoragePresentationContexts:
        ae.add_supported_context(context.abstract_syntax)

    handlers = [(evt.EVT_C_STORE, handle_store)]
    ae.start_server(("", int(OrthancContext.dicom_client_port)), block=False, evt_handlers=handlers)
    return ae


def download_from_pacs_dicom(studies_csv: str, download_dir: str) -> None:
    """
    Download all instances of a study via C-FIND + C-MOVE.

    Args:
        studies_csv (str): Path to the studies_csv file.
        download_dir (str): Path to the directory where the downloaded files will be saved.

    """
    # Start Storage SCP
    storage_ae = start_storage_scp(download_dir)

    # Create the AE for PACS association
    ae = AE(ae_title=OrthancContext.client_ae_title)
    ae.acse_timeout = 30
    ae.network_timeout = 30

    # Request C-FIND and C-MOVE contexts
    ae.add_requested_context("1.2.840.10008.5.1.4.1.2.2.1")
    ae.add_requested_context("1.2.840.10008.5.1.4.1.2.2.2")

    # Add Storage SOP Classes so PACS can push images via C-MOVE
    for context in AllStoragePresentationContexts:
        ae.add_supported_context(context.abstract_syntax)

    # Associate with PACS
    assoc = ae.associate(
        OrthancContext.domain,
        int(OrthancContext.dicom_server_port),
        ae_title=OrthancContext.pacs_ae_title
    )

    if not assoc.is_established:
        logger.error("Failed to associate with PACS.")
        storage_ae.shutdown()
        return

    studies = get_dict_from_csv(studies_csv)
    for study in studies:
        # Create C-FIND dataset to locate the study
        ds = Dataset()
        ds.QueryRetrieveLevel = "STUDY"
        ds.StudyInstanceUID = study["StudyInstanceUID"]

        logger.info(f"Requesting C-MOVE for study: {study['StudyInstanceUID']}")

        # Send C-MOVE to the PACS, telling it to push images to our AE
        for status, _ in assoc.send_c_move(
            ds,
            move_aet=OrthancContext.client_ae_title,  # Local AE receiving the images
            query_model="1.2.840.10008.5.1.4.1.2.2.2"
        ):
            if status:
                logger.info(f"C-MOVE status: 0x{status.Status:04x}")
            else:
                logger.warning("C-MOVE failed or association aborted")

    assoc.release()
    storage_ae.shutdown()


def delete_studies_from_pacs(studies_csv: str) -> None:
    """
    Delete all studies IDs of the studies_csv from the Orthanc PACS server.

    Args:
        studies_csv : str
    """
    orthanc_studies_ids = get_values_from_csv(studies_csv, "StudyID")
    for orthanc_study_id in orthanc_studies_ids:
        delete_orthanc_study(orthanc_study_id)


def upload_processed_dataset(dataset_path: str) -> None:
    """
    Upload DICOM processed data to Shanoir server.

    Args:
        dataset_path : str The path to the dataset directory.
    """
    for study in os.listdir(dataset_path):
        study_dir = os.path.join(dataset_path, study)
        dcm_files = [
            os.path.join(root, f)
            for root, dirs, files in os.walk(study_dir)
            for f in files
            if f.endswith(".dcm") and pydicom.dcmread(os.path.join(root, f)).Modality in ("SR", "SEG")
        ]

        for dcm in dcm_files:
            with open(dcm, "rb") as f:
                dicom_bytes = f.read()
            success = upload_dataset_processing(dicom_bytes)
            if success:
                logger.info(f"Successfully uploaded {dcm} to Shanoir.")
            else:
                logger.warning(f"Failed to upload {dcm} to Shanoir.")


def get_patient_ids_from_pacs() -> None:
    """
    Delete all studies in a dataset from the Orthanc PACS server.
    """
    patient_list = get_orthanc_patients()
    for patient_id in patient_list:
        patient_meta = get_orthanc_patient_meta(patient_id)
        logger.info(f"Name: {patient_meta['MainDicomTags']['PatientName']}, ID: {patient_meta['MainDicomTags']['PatientID']}")
    logger.info(f"Total number of patients: {len(patient_list)}")


def purge_pacs_studies() -> None:
    """
    Purge all studies in a dataset from the Orthanc PACS server.
    """
    orthanc_studies_ids = get_all_orthanc_studies()
    for orthanc_study_id in orthanc_studies_ids:
        delete_orthanc_study(orthanc_study_id)
