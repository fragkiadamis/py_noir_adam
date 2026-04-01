from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pydicom
from pynetdicom import AE, StoragePresentationContexts

from src.orthanc.orthanc_service import set_orthanc_study_label, upload_study_to_orthanc, \
    delete_orthanc_study, delete_orthanc_instance, get_orthanc_patients, get_orthanc_patient_meta, \
    get_all_orthanc_studies, get_study_orthanc_id_by_uid, download_orthanc_study, \
    get_orthanc_study_metadata, get_orthanc_series_metadata, get_orthanc_instance_metadata, \
    download_orthanc_series, get_all_orthanc_series, find_orthanc_series_by_uid, \
    find_orthanc_instances_by_image_type, find_orthanc_studies_by_patient_name
from src.utils.config_utils import ConfigPath, OrthancConfig
from src.utils.log_utils import get_logger

logger = get_logger()

_MIP_IMAGE_TYPE_PATTERNS = ["*PROJECTION*", "*MIP*", "*MAXIMUM*", "*MAX_IP*"]


def upload_to_pacs_rest(dataset_path: Path) -> None:
    total_file_count, dicom_count, studies_count = 0, 0, 0
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
            studies_count += 1

        processing_id = study_path.name.split("_")[1]
        df.loc[df["processing_id"] == processing_id, "orthanc_study_id"] = parent_study_orthanc_id
        df.loc[df["processing_id"] == processing_id, "study_instance_uid"] = pydicom.dcmread(dcm_files[0]).StudyInstanceUID
        df.to_csv(ConfigPath.tracking_file_path, index=False)

    logger.info(f"Total studies uploaded: {studies_count}")
    if dicom_count == total_file_count:
        logger.info(f"SUCCESS: {dicom_count} DICOM file(s) successfully imported.")
    else:
        logger.warning(f"WARNING: Only {dicom_count}/{total_file_count} files imported successfully.")


def upload_to_pacs_dicom(dataset_path: Path) -> None:
    ae = AE(ae_title=OrthancConfig.client_ae_title)
    ae.acse_timeout = 30
    ae.network_timeout = 30

    for context in StoragePresentationContexts:
        ae.add_requested_context(context.abstract_syntax)

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

    assoc.release()
    logger.info("C-STORE upload completed.")


def assign_label_to_pacs_study() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    for _, row in df.iterrows():
        if row["orthanc_study_id"] is None:
            continue
        set_orthanc_study_label(row["orthanc_study_id"], row["label"])


def download_from_pacs_rest(download_dir: Path) -> None:
    df = pd.read_csv(ConfigPath.input_path / "series_export.csv", dtype=str, sep=";")
    downloaded_mr_series = set()
    for _, row in df.iterrows():
        series_instance_uid = row["SeriesInstanceUID"]
        patient_name = row["PatientName"]
        study_instance_uid = row["StudyInstanceUID"]
        series_download_path = download_dir / patient_name
        series_download_path.mkdir(parents=True, exist_ok=True)

        series_id = find_orthanc_series_by_uid(series_instance_uid)
        if series_id is None:
            logger.warning(f"Series not found for SeriesInstanceUID {series_instance_uid}, skipping.")
            continue

        logger.info(f"Downloading series {series_instance_uid} for patient {patient_name}...")
        download_orthanc_series(series_id, series_download_path)

        parent_study_id = get_study_orthanc_id_by_uid(study_instance_uid)
        if parent_study_id is None:
            logger.warning(f"Could not resolve study Orthanc ID for StudyInstanceUID {study_instance_uid}, skipping MR input download.")
            continue

        study_meta = get_orthanc_study_metadata(parent_study_id)
        if study_meta is None:
            logger.warning(f"Could not retrieve study metadata for {parent_study_id}, skipping MR input download.")
            continue
        for mr_series_id in study_meta.get("Series", []):
            if mr_series_id in downloaded_mr_series:
                continue
            series_meta = get_orthanc_series_metadata(mr_series_id)
            if series_meta and series_meta.get("MainDicomTags", {}).get("Modality") == "MR":
                logger.info(f"Downloading MR input series {mr_series_id} for patient {patient_name}...")
                download_orthanc_series(mr_series_id, series_download_path)
                downloaded_mr_series.add(mr_series_id)


def delete_studies_from_pacs() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    for orthanc_study_id in df["orthanc_study_id"]:
        delete_orthanc_study(orthanc_study_id)


def purge_pacs_studies() -> None:
    for orthanc_study_id in get_all_orthanc_studies():
        delete_orthanc_study(orthanc_study_id)


def delete_mip_first_instances() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    rows_with_study = df[df["orthanc_study_id"].notna()]

    for _, row in rows_with_study.iterrows():
        orthanc_study_id = str(row["orthanc_study_id"])

        study_meta = get_orthanc_study_metadata(orthanc_study_id)
        if study_meta is None:
            continue

        patient_name = study_meta.get("PatientMainDicomTags", {}).get("PatientName", "Unknown")

        for series_id in study_meta.get("Series", []):
            series_meta = get_orthanc_series_metadata(series_id)
            if series_meta is None:
                continue
            if series_meta.get("MainDicomTags", {}).get("Modality", "") != "MR":
                continue

            series_uid = series_meta.get("MainDicomTags", {}).get("SeriesInstanceUID", "")
            if not series_uid:
                continue

            mip_instance_ids: set[str] = set()
            for pattern in _MIP_IMAGE_TYPE_PATTERNS:
                mip_instance_ids.update(find_orthanc_instances_by_image_type(series_uid, pattern))

            for instance_id in mip_instance_ids:
                logger.info(f"{patient_name} — deleting MIP instance {instance_id} from series {series_uid}.")
                delete_orthanc_instance(instance_id)


def get_patient_ids_from_pacs() -> None:
    patient_list = get_orthanc_patients()
    logger.info("------------------------------------ START ------------------------------------")
    for patient_id in patient_list:
        patient_meta = get_orthanc_patient_meta(patient_id)
        logger.info(f"Name: {patient_meta['MainDicomTags']['PatientName']}, ID: {patient_meta['MainDicomTags']['PatientID']}")
        logger.info("*" * 90)
    logger.info(f"Total number of patients: {len(patient_list)}")
    logger.info("------------------------------------ END ------------------------------------")


def get_orthanc_study_details() -> None:
    studies_ids = get_all_orthanc_studies()
    logger.info("------------------------------------ START ------------------------------------")
    for study_id in studies_ids:
        study = get_orthanc_study_metadata(study_id)
        orthanc_date = datetime.strptime(study["LastUpdate"], "%Y%m%dT%H%M%S")
        patient_name = study["PatientMainDicomTags"].get("PatientName", "Unknown")
        study_uid = study["MainDicomTags"].get("StudyInstanceUID", "N/A")
        labels = study.get("Labels", [])

        logger.info(f"{orthanc_date} | {patient_name} | {study_id} | {study_uid} | {labels}")

        frame_of_refs: List[Dict[str, str]] = []
        for series_id in study.get("Series", []):
            series = get_orthanc_series_metadata(series_id)
            modality = series.get("MainDicomTags", {}).get("Modality", "")
            instance_id = series.get("Instances", [None])[0]

            if not instance_id:
                continue

            instance = get_orthanc_instance_metadata(instance_id)
            series_description = instance.get("SeriesDescription", "Unnamed Series")
            series_uid = series.get("MainDicomTags", {}).get("SeriesInstanceUID", "N/A")
            frame_uid = instance.get("FrameOfReferenceUID")

            if modality in ("SEG", "SR"):
                instance_uid = instance.get("SOPInstanceUID", "N/A")
                logger.info(f"  [{modality}] {series_description} | Series ID: {series_id} | Series UID: {series_uid} | Instance UID: {instance_uid}")
            else:
                logger.info(f"  [{modality}] {series_description} | Series ID: {series_id} | Series UID: {series_uid}")

            if frame_uid:
                frame_of_refs.append({series_description: frame_uid})

        for ref in frame_of_refs:
            for series_desc, uid in ref.items():
                logger.info(f"{series_desc}: {uid}")

        logger.info("*" * 90)
    logger.info("------------------------------------ END ------------------------------------")


def log_mr_series_instance_counts() -> None:
    all_series_ids = get_all_orthanc_series()
    if not all_series_ids:
        logger.error("No series found in Orthanc.")
        return

    study_cache: Dict[str, Dict] = {}
    logger.info("------------------------------------ START ------------------------------------")
    for series_id in all_series_ids:
        series_meta = get_orthanc_series_metadata(series_id)
        if series_meta is None:
            continue

        tags = series_meta.get("MainDicomTags", {})
        if tags.get("Modality", "") != "MR":
            continue

        n_instances = len(series_meta.get("Instances", []))
        series_description = tags.get("SeriesDescription", "N/A")

        parent_study_id = series_meta.get("ParentStudy", "")
        if parent_study_id not in study_cache:
            study_cache[parent_study_id] = get_orthanc_study_metadata(parent_study_id) or {}
        patient_name = study_cache[parent_study_id].get("PatientMainDicomTags", {}).get("PatientName", "Unknown")

        logger.info(f"{patient_name} | {series_description} | instances: {n_instances}")

    logger.info("------------------------------------ END ------------------------------------")


def create_series_export() -> None:
    output_csv = ConfigPath.input_path / "series_export_test.csv"

    all_series_ids = get_all_orthanc_series()
    if not all_series_ids:
        logger.error("No series found in Orthanc.")
        return

    rows = []
    study_cache: Dict[str, Dict] = {}

    for series_id in all_series_ids:
        series_meta = get_orthanc_series_metadata(series_id)
        if series_meta is None:
            continue

        tags = series_meta.get("MainDicomTags", {})
        modality = tags.get("Modality", "")
        if modality not in ("SR", "SEG"):
            continue

        parent_study_id = series_meta.get("ParentStudy", "")
        if parent_study_id not in study_cache:
            study_cache[parent_study_id] = get_orthanc_study_metadata(parent_study_id) or {}
        study_meta = study_cache[parent_study_id]

        study_tags = study_meta.get("MainDicomTags", {})
        patient_tags = study_meta.get("PatientMainDicomTags", {})

        rows.append({
            "ID": series_id,
            "ParentStudy": parent_study_id,
            "Modality": modality,
            "PatientName": patient_tags.get("PatientName", ""),
            "StudyDescription": study_tags.get("StudyDescription", ""),
            "StudyInstanceUID": study_tags.get("StudyInstanceUID", ""),
            "LastUpdate": series_meta.get("LastUpdate", ""),
            "SeriesInstanceUID": tags.get("SeriesInstanceUID", ""),
            "SeriesDescription": tags.get("SeriesDescription", ""),
            "SeriesNumber": tags.get("SeriesNumber", ""),
        })
        logger.info(f"Found {modality} series {series_id} for patient {patient_tags.get('PatientName', '')}")

    df = pd.DataFrame(rows, columns=[
        "ID", "ParentStudy", "Modality", "PatientName", "StudyDescription",
        "StudyInstanceUID", "LastUpdate", "SeriesInstanceUID", "SeriesDescription", "SeriesNumber",
    ])
    df.to_csv(output_csv, index=False)
    logger.info(f"Wrote {len(rows)} series to {output_csv}")


def update_tracking_ids(vip_output: Path) -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)

    for processing_dir in vip_output.iterdir():
        if not processing_dir.is_dir():
            continue

        input_dir = next(d for d in processing_dir.iterdir() if d.is_dir() and "output" not in d.name)
        first_dcm = next(input_dir.glob("*.dcm"), None)
        if first_dcm is None:
            continue

        ds = pydicom.dcmread(first_dcm, stop_before_pixels=True)
        dicom_patient_name = str(ds.PatientName)
        series_instance_uid = str(getattr(ds, "SeriesInstanceUID", ""))
        path_patient_name = "_".join(first_dcm.stem.split("_")[:3])
        if "AIC_01_0002" in path_patient_name or "AIC_01_0002" in dicom_patient_name:
            continue

        study_ids = find_orthanc_studies_by_patient_name(dicom_patient_name)
        if not study_ids:
            logger.warning(f"{path_patient_name} — no Orthanc study found for PatientName '{dicom_patient_name}'")
            continue

        orthanc_study_id = study_ids[0]
        study_meta = get_orthanc_study_metadata(orthanc_study_id)
        if study_meta is None:
            continue

        study_instance_uid = study_meta.get("MainDicomTags", {}).get("StudyInstanceUID", "")
        df.loc[df["subject_name"] == path_patient_name, "orthanc_study_id"] = orthanc_study_id
        df.loc[df["subject_name"] == path_patient_name, "study_instance_uid"] = study_instance_uid
        df.loc[df["subject_name"] == path_patient_name, "series_instance_uid"] = series_instance_uid
        logger.info(f"{path_patient_name} — orthanc_study_id={orthanc_study_id}, study_instance_uid={study_instance_uid}, series_instance_uid={series_instance_uid}")

    df.to_csv(ConfigPath.tracking_file_path, index=False)
