import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pydicom
from pynetdicom import AE, StoragePresentationContexts

from src.orthanc.orthanc_service import set_orthanc_study_label, upload_study_to_orthanc, \
    delete_orthanc_study, get_orthanc_patients, get_orthanc_patient_meta, get_all_orthanc_studies, \
    get_study_orthanc_id_by_uid, download_orthanc_study, get_orthanc_study_metadata, get_orthanc_series_metadata, \
    get_orthanc_instance_metadata
from src.shanoir_object.dataset.dataset_service import find_processed_dataset_ids_by_input_dataset_id, \
    download_dataset_processing, get_dataset_processing, get_dataset, upload_dataset_processing

from src.utils.file_utils import get_values_from_csv, save_dict_to_csv, get_dict_from_csv
from src.utils.log_utils import get_logger

logger = get_logger()

SEQUENCE_TAG = (0x0040,0x0275)
SEQUENCE_ITEM_TAG = (0x0040,0x0008)


def update_studies_registry(studies, studies_csv):
    if not studies:
        return

    studies_csv_path = Path(studies_csv)
    existing_studies = get_dict_from_csv(studies_csv)

    if existing_studies is None:
        save_dict_to_csv(studies, studies_csv_path)
        return

    # Filter out duplicates
    new_studies = [s for s in studies if s not in existing_studies]

    if new_studies:
        existing_studies.extend(new_studies)
        save_dict_to_csv(existing_studies, studies_csv_path)


def fetch_datasets_from_json(ecan_json_path: str, executions_csv: str, output_dir: str) -> None:
    # Load JSON content safely
    ecan_json_path_path = Path(ecan_json_path)
    dataset_ids_list = get_values_from_csv(ecan_json_path_path, "DatasetId")

    # Map each subject ID to its related processed dataset IDs
    processing_ids_list = []
    executions_csv_path = Path(executions_csv)
    execution_ids = get_values_from_csv(executions_csv_path, "ExecutionId")
    for dataset_id in dataset_ids_list:
        processing_list = find_processed_dataset_ids_by_input_dataset_id(dataset_id)
        processing_ids_list.extend([item["id"] for item in processing_list if str(item["parentId"]) in execution_ids])

    # Download all processed datasets grouped by subject
    os.makedirs(output_dir, exist_ok=True)
    download_dataset_processing(processing_ids_list, output_dir, unzip=True)


def inspect_and_fix_study_tags(input_dir: str) -> None:
    for processing in os.listdir(input_dir):
        processing_dir = os.path.join(input_dir, processing)
        processing_input_dir = os.path.join(processing_dir, [item for item in os.listdir(processing_dir) if "output" not in item][0])
        processing_output_dir = os.path.join(processing_dir, "output")
        mr_files = [os.path.join(processing_input_dir, f) for f in os.listdir(processing_input_dir) if f.endswith(".dcm")]
        seg_file = os.path.join(processing_output_dir, [f for f in os.listdir(processing_output_dir) if "seg" in f][0])

        is_inconsistent = False
        # Gather all FrameOfReferenceUIDs in your MR instances
        uids = {}
        for file_path in mr_files:
            ds = pydicom.dcmread(file_path, stop_before_pixels=True)
            uid = getattr(ds, "FrameOfReferenceUID", None)
            if uid:
                uids.setdefault(uid, []).append(os.path.basename(file_path).split(".")[0])

        good_uid = None
        if len(uids.keys()) > 1:
            is_inconsistent = True
            logger.info("Found FrameOfReferenceUIDs:")
            for k, v in uids.items():
                logger.info(f"  {k} → {len(v)} instances")

            # Pick the "good" UID (e.g. the most frequent one)
            good_uid = max(uids, key=lambda k: len(uids[k]))
            logger.info(f"\nChosen UID: {good_uid}")

            # For the MR instances with the good UID
            for file_path in mr_files:
                ds = pydicom.dcmread(file_path)
                if getattr(ds, "FrameOfReferenceUID", None) != good_uid:
                    ds.FrameOfReferenceUID = good_uid
                    ds.save_as(file_path)
        else:
            good_uid = List(uids.keys())[0]

        # Fix the SEG as well
        seg = pydicom.dcmread(seg_file)
        if seg.FrameOfReferenceUID != good_uid:
            is_inconsistent = True
            seg.FrameOfReferenceUID = good_uid
            seg.save_as(seg_file)

        if is_inconsistent:
            logger.info(f"Inconsistencies were found in FrameOfReferenceUID.")

        # Remove empty or malformed nested DICOM sequences
        for file_path in mr_files:
            ds = pydicom.dcmread(file_path, stop_before_pixels=False)

            # Skip if the target sequence tag is missing
            if SEQUENCE_TAG not in ds:
                ds.save_as(file_path)
                continue

            cleaned = False
            for item in ds[SEQUENCE_TAG].value:
                # Ensure the sub-sequence exists
                if SEQUENCE_ITEM_TAG not in item:
                    continue
                found_item = item[(0x0040, 0x0008)]

                # Skip if it's not actually a sequence
                if found_item.VR != "SQ":
                    continue

                # Remove if the sequence is empty or malformed
                if len(found_item.value) < 2 and len(found_item.value[0]) == 0:
                    del item[SEQUENCE_ITEM_TAG]
                    cleaned = True
            if cleaned:
                ds.save_as(file_path)


def upload_to_pacs_rest(dataset_path: str, studies_csv: str) -> None:
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
    # Initialize AE
    ae = AE(ae_title=OrthancConfig.client_ae_title)
    ae.acse_timeout = 30
    ae.network_timeout = 30

    # Add requested presentation contexts for common DICOM storage classes
    for context in StoragePresentationContexts:
        ae.add_requested_context(context.abstract_syntax)

    # Associate with PACS
    assoc = ae.associate(
        OrthancConfig.domain,
        int(OrthancConfig.dicom_server_port),
        ae_title=OrthancConfig.pacs_ae_title
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
    ican_ids = get_values_from_csv(Path("py_noir_code/projects/RHU_eCAN/ican_subset.csv"), "SubjectName")
    angptl6_ids = get_values_from_csv(Path("py_noir_code/projects/RHU_eCAN/angptl6_subset.csv"), "SubjectName")

    studies_csv_path = Path(studies_csv)
    uploaded_studies = get_dict_from_csv(studies_csv_path)
    for study in uploaded_studies:
        subject_name = study["PatientName"]
        if subject_name in ican_ids:
            set_orthanc_study_label(study["StudyID"], "ICAN_SUBSET")
        elif subject_name in angptl6_ids:
            set_orthanc_study_label(study["StudyID"], "ANGPTL6_SUBSET")


def download_from_pacs_rest(studies_csv: str, download_dir: str) -> None:
    studies_csv_path = Path(studies_csv)
    studies = get_dict_from_csv(studies_csv_path)
    os.makedirs(download_dir, exist_ok=True)
    for study in studies:
        download_orthanc_study(study["StudyID"], download_dir)


def delete_studies_from_pacs(studies_csv: str) -> None:
    studies_csv_path = Path(studies_csv)
    orthanc_studies_ids = get_values_from_csv(studies_csv_path, "StudyID")
    for orthanc_study_id in orthanc_studies_ids:
        delete_orthanc_study(orthanc_study_id)


def upload_processed_dataset(dataset_path: str) -> None:
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


def get_orthanc_study_details() -> None:
    """
    Retrieve and log Orthanc study details, including FrameOfReferenceUIDs per series.
    """
    studies_ids = get_all_orthanc_studies()
    for study_id in studies_ids:
        study = get_orthanc_study_metadata(study_id)
        orthanc_date = datetime.strptime(study["LastUpdate"], "%Y%m%dT%H%M%S")

        patient_name = study["PatientMainDicomTags"].get("PatientName", "Unknown")
        study_uid = study["MainDicomTags"].get("StudyInstanceUID", "N/A")
        labels = study.get("Labels", [])

        logger.info(f"{orthanc_date} | {patient_name} | {study_uid} | {labels}")

        frame_of_refs: List[Dict[str, str]] = []
        for series_id in study.get("Series", []):
            series = get_orthanc_series_metadata(series_id)
            instance_id = series.get("Instances", [None])[0]

            if not instance_id:
                continue

            instance = get_orthanc_instance_metadata(instance_id)
            series_description = instance.get("SeriesDescription", "Unnamed Series")
            frame_uid = instance.get("FrameOfReferenceUID")

            if frame_uid:
                frame_of_refs.append({series_description: frame_uid})

        for ref in frame_of_refs:
            for series_desc, uid in ref.items():
                logger.info(f"{series_desc}: {uid}")

        logger.info("*" * 90)
