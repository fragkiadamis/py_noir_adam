import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pydicom
from pynetdicom import AE, StoragePresentationContexts

from src.orthanc.orthanc_service import set_orthanc_study_label, upload_study_to_orthanc, \
    delete_orthanc_study, get_orthanc_patients, get_orthanc_patient_meta, get_all_orthanc_studies, \
    get_study_orthanc_id_by_uid, download_orthanc_study, get_orthanc_study_metadata, get_orthanc_series_metadata, \
    get_orthanc_instance_metadata
from src.shanoir_object.dataset.dataset_service import find_processed_dataset_ids_by_input_dataset_id, \
    download_dataset_processing, upload_dataset_processing
from src.utils.config_utils import ConfigPath, OrthancConfig
from src.utils.log_utils import get_logger

logger = get_logger()

SEQUENCE_TAG = (0x0040,0x0275)
SEQUENCE_ITEM_TAG = (0x0040,0x0008)


def fetch_datasets_from_json(output_dir: Path) -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    dataset_pairs_list = [{"input_dataset_id": row["dataset_id"], "execution_id": row["execution_id"]} for _, row in df.iterrows()]

    processing_ids_list = []
    for dataset_pair in dataset_pairs_list:
        processing_list = find_processed_dataset_ids_by_input_dataset_id(dataset_pair["input_dataset_id"])
        processing_id = next(
            (item["id"] for item in processing_list if str(item["parentId"]) == dataset_pair["execution_id"]),
            None  # default if no match is found
        )

        if processing_id is None:
            continue

        processing_ids_list.append(processing_id)
        df["processing_id"] = df["dataset_id"].map({dataset_pair["input_dataset_id"]: processing_id})
        df.to_csv(ConfigPath.tracking_file_path, index=False)

    output_dir.mkdir(parents=True, exist_ok=True)
    download_dataset_processing(processing_ids_list, output_dir, unzip=True)


def inspect_and_fix_study_tags(input_dir: Path) -> None:
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
            good_uid = list(uids.keys())[0]

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


def upload_to_pacs_rest(dataset_path: Path) -> None:
    total_file_count, dicom_count, studies = 0, 0, []
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    for study_path in dataset_path.iterdir():
        logger.info(f"Uploading orthanc study: {study_path.name}")
        dcm_files = list(study_path.rglob("*.dcm"))

        total_files, successful_uploads, response_json = upload_study_to_orthanc(dcm_files)
        total_file_count += total_files
        dicom_count += successful_uploads

        parent_study_orthanc_id = None
        if response_json and "ParentStudy" in response_json:
            parent_study_orthanc_id = response_json["ParentStudy"]

        processing_id = study_path.name.split("_")[1]
        df.loc[df["processing_id"] == processing_id, "orthanc_study_id"] = parent_study_orthanc_id
        df.loc[df["processing_id"] == processing_id, "study_instance_uid"] = pydicom.dcmread(dcm_files[0]).StudyInstanceUID
        df.to_csv(ConfigPath.tracking_file_path, index=False)

    logger.info(f"Total studies uploaded: {len(studies)}")
    if dicom_count == total_file_count:
        logger.info(f"SUCCESS: {dicom_count} DICOM file(s) successfully imported.")
    else:
        logger.warning(f"WARNING: Only {dicom_count}/{total_file_count} files imported successfully.")


def upload_to_pacs_dicom(dataset_path: Path) -> None:
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

    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    for study_path in dataset_path.iterdir():
        logger.info(f"Uploading orthanc study: {study_path.name}")
        dcm_files = list(study_path.rglob("*.dcm"))

        logger.info(f"Found {len(dcm_files)} DICOM file(s) to upload.")
        for dcm_file in dcm_files:
            try:
                ds = pydicom.dcmread(dcm_file)
                status = assoc.send_c_store(ds)
                if status and status.Status == 0x0000:
                    logger.info(f"Successfully sent {dcm_file}")
                else:
                    logger.warning(f"Failed to send {dcm_file}, status: {status}")
            except Exception as e:
                logger.error(f"Error sending {dcm_file}: {e}")

        study_instance_uid = pydicom.dcmread(dcm_files[0]).StudyInstanceUID
        parent_study_orthanc_id = get_study_orthanc_id_by_uid(study_instance_uid)
        processing_id = study_path.name.split("_")[1]
        df.loc[df["processing_id"] == processing_id, "orthanc_study_id"] = parent_study_orthanc_id
        df.loc[df["processing_id"] == processing_id, "study_instance_uid"] = study_instance_uid
        df.to_csv(ConfigPath.tracking_file_path, index=False)

    # Release the association
    assoc.release()
    logger.info("C-STORE upload completed.")


def assign_label_to_pacs_study() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    for _, row in df.iterrows():
        if row["orthanc_study_id"] is None:
            continue
        set_orthanc_study_label(row["orthanc_study_id"], row["label"])


def download_from_pacs_rest(download_dir: Path) -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    download_dir.mkdir(parents=True, exist_ok=True)
    for _, row in df.iterrows():
        download_orthanc_study(row["orthanc_study_id"], download_dir)


def delete_studies_from_pacs() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    orthanc_study_ids = df["orthanc_study_id"]
    for orthanc_study_id in orthanc_study_ids:
        delete_orthanc_study(orthanc_study_id)


def upload_processed_dataset(dataset_path: Path) -> None:
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
    logger.info("------------------------------------ START ------------------------------------")
    for patient_id in patient_list:
        patient_meta = get_orthanc_patient_meta(patient_id)
        logger.info(f"Name: {patient_meta['MainDicomTags']['PatientName']}, ID: {patient_meta['MainDicomTags']['PatientID']}")
        logger.info("*" * 90)
    logger.info(f"Total number of patients: {len(patient_list)}")
    logger.info("------------------------------------ END ------------------------------------")


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
    logger.info("------------------------------------ START ------------------------------------")
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
    logger.info("------------------------------------ END ------------------------------------")
