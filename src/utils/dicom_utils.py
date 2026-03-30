import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pydicom
from pynetdicom import AE, StoragePresentationContexts

from src.orthanc.orthanc_service import set_orthanc_study_label, upload_study_to_orthanc, \
    delete_orthanc_study, get_orthanc_patients, get_orthanc_patient_meta, get_all_orthanc_studies, \
    get_study_orthanc_id_by_uid, download_orthanc_study, get_orthanc_study_metadata, get_orthanc_series_metadata, \
    get_orthanc_instance_metadata, download_orthanc_series, get_all_orthanc_series, find_orthanc_series_by_uid
from src.shanoir_object.dataset.dataset_service import find_processed_dataset_ids_by_input_dataset_id, \
    download_dataset_processing, upload_dataset_processing, sync_study_instance_uid
from src.utils.config_utils import ConfigPath, OrthancConfig
from src.utils.log_utils import get_logger

logger = get_logger()

SEQUENCE_TAG = (0x0040,0x0275)
SEQUENCE_ITEM_TAG = (0x0040,0x0008)


def fetch_processed_datasets(output_dir: Path) -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    df["processing_id"] = pd.Series(dtype="Int64")
    dataset_pairs_list = [{
        "input_dataset_id": row["dataset_id"],
        "execution_id": row["execution_id"]
    } for _, row in df.iterrows() if row["execution_status"] == "Finished"]

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
        df.loc[df["dataset_id"] == dataset_pair["input_dataset_id"], "processing_id"] = processing_id
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

        # Gather all FrameOfReferenceUIDs in your MR instances
        uids = {}
        for file_path in mr_files:
            ds = pydicom.dcmread(file_path, stop_before_pixels=True)
            uid = getattr(ds, "FrameOfReferenceUID", None)
            if uid:
                uids.setdefault(uid, []).append(os.path.basename(file_path).split(".")[0])

        good_uid = None
        if len(uids.keys()) > 1:
            subject_name = pydicom.dcmread(mr_files[0]).PatientName
            logger.info(f"{subject_name} --> inconsistencies were found in MR FrameOfReferenceUID.")

            # Pick the "good" UID (e.g. the most frequent one)
            good_uid = max(uids, key=lambda k: len(uids[k]))
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
            subject_name = seg.PatientName
            logger.info(f"{subject_name} --> inconsistencies were found between MR and SEG FrameOfReferenceUID.")
            seg.FrameOfReferenceUID = good_uid
            seg.save_as(seg_file)

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


def sync_examination_study_instance_uids() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    examination_ids = df["examination_id"].dropna().unique().tolist()
    logger.info(f"Syncing StudyInstanceUID for {len(examination_ids)} examination(s)...")
    for examination_id in examination_ids:
        try:
            sync_study_instance_uid(examination_id)
            logger.info(f"Synced StudyInstanceUID for examination {examination_id}")
        except Exception as e:
            logger.error(f"Failed to sync StudyInstanceUID for examination {examination_id}: {e}")


def download_from_pacs_rest(download_dir: Path) -> None:
    df = pd.read_csv(ConfigPath.input_path / "series_export.csv", dtype=str)
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
    orthanc_study_ids = df["orthanc_study_id"]
    for orthanc_study_id in orthanc_study_ids:
        delete_orthanc_study(orthanc_study_id)


def _check_seg_references(ds: pydicom.Dataset, input_sop_uids: set, input_series_uid: str, subject_name: str, out_name: str) -> int:
    """
    Check that a SEG file's ReferencedSeriesSequence points to the correct input series and instances.
    Returns the number of issues found.
    """
    issues = 0
    ref_series_seq = getattr(ds, "ReferencedSeriesSequence", None)
    if ref_series_seq is None:
        logger.warning(f"[{subject_name}] [SEG] {out_name}: missing ReferencedSeriesSequence.")
        return 1

    for series_item in ref_series_seq:
        ref_series_uid = str(getattr(series_item, "SeriesInstanceUID", ""))
        print("SEG", ref_series_uid, input_series_uid)
        if ref_series_uid != input_series_uid:
            logger.warning(
                f"[{subject_name}] [SEG] {out_name}: ReferencedSeriesSequence.SeriesInstanceUID mismatch. "
                f"Input={input_series_uid}, Referenced={ref_series_uid}"
            )
            issues += 1

        # SEG uses ReferencedInstanceSequence (0008,114a), not ReferencedSOPSequence
        ref_instance_seq = getattr(series_item, "ReferencedInstanceSequence", None)
        if ref_instance_seq is None:
            logger.warning(f"[{subject_name}] [SEG] {out_name}: missing ReferencedInstanceSequence inside ReferencedSeriesSequence.")
            issues += 1
            continue

        for sop_item in ref_instance_seq:
            ref_sop = str(getattr(sop_item, "ReferencedSOPInstanceUID", ""))
            if ref_sop not in input_sop_uids:
                logger.warning(
                    f"[{subject_name}] [SEG] {out_name}: ReferencedSOPInstanceUID '{ref_sop}' "
                    f"not found among input SOPInstanceUIDs."
                )
                issues += 1

    return issues


def _check_sr_references(ds: pydicom.Dataset, input_sop_uids: set, input_series_uid: str, subject_name: str, out_name: str) -> int:
    """
    Check that an SR file's CurrentRequestedProcedureEvidenceSequence references the correct input series and instances.
    Mirrors exactly what the server reads: [0] of each sequence level.
    Returns the number of issues found.
    """
    issues = 0
    evidence_seq = getattr(ds, "CurrentRequestedProcedureEvidenceSequence", None)
    if evidence_seq is None:
        logger.warning(f"[{subject_name}] [SR] {out_name}: missing CurrentRequestedProcedureEvidenceSequence.")
        return 1

    evidence_item = evidence_seq[0]
    series_seq = getattr(evidence_item, "ReferencedSeriesSequence", None)
    if series_seq is None:
        logger.warning(f"[{subject_name}] [SR] {out_name}: missing ReferencedSeriesSequence inside CurrentRequestedProcedureEvidenceSequence.")
        return 1

    series_item = series_seq[0]

    # Check SeriesInstanceUID
    ref_series_uid = str(getattr(series_item, "SeriesInstanceUID", ""))
    if ref_series_uid != input_series_uid:
        logger.warning(
            f"[{subject_name}] [SR] {out_name}: ReferencedSeriesSequence.SeriesInstanceUID mismatch. "
            f"Input={input_series_uid}, Referenced={ref_series_uid}"
        )
        issues += 1

    # Check ReferencedSOPInstanceUIDs (SR uses ReferencedSOPSequence, not ReferencedInstanceSequence)
    sop_seq = getattr(series_item, "ReferencedSOPSequence", None)
    if sop_seq is None:
        logger.warning(f"[{subject_name}] [SR] {out_name}: missing ReferencedSOPSequence inside ReferencedSeriesSequence.")
        return issues + 1

    for sop_item in sop_seq:
        ref_sop = str(getattr(sop_item, "ReferencedSOPInstanceUID", ""))
        if ref_sop not in input_sop_uids:
            logger.warning(
                f"[{subject_name}] [SR] {out_name}: ReferencedSOPInstanceUID '{ref_sop}' "
                f"not found among input SOPInstanceUIDs."
            )
            issues += 1

    return issues


def _get_series_modality(series_dir: Path) -> str | None:
    """Read the modality from the first DICOM file in a series directory."""
    for f in series_dir.rglob("*.dcm"):
        ds = pydicom.dcmread(series_dir / f, stop_before_pixels=True)
        return str(getattr(ds, "Modality", None))
    return None


def check_dicom_consistency(input_dir: Path) -> None:
    """
    For each patient directory in orthanc_output, classify downloaded series by modality
    (MR, SEG, SR), then verify DICOM tag consistency:
      - StudyInstanceUID must match between MR and SEG/SR
      - SEG ReferencedSeriesSequence must reference the correct MR SeriesInstanceUID and SOPInstanceUIDs
      - SR CurrentRequestedProcedureEvidenceSequence must reference the correct MR SOPInstanceUIDs

    Expected structure:
        input_dir/
          <patient_name>/
            <series_id>/ *.dcm (modality determined by reading files)
            ...
    """
    total_issues = 0

    for patient_dir in input_dir.iterdir():
        if not os.path.isdir(patient_dir):
            continue

        mr_files: List[Path] = []
        seg_files: List[Path] = []
        sr_files: List[Path] = []

        for series_id in patient_dir.iterdir():
            series_dir = patient_dir / series_id
            if not os.path.isdir(series_dir):
                continue
            modality = _get_series_modality(series_dir)
            if modality == "MR":
                mr_files = [f for f in series_dir.rglob("*.dcm")]
            elif modality == "SEG":
                seg_files = [f for f in series_dir.rglob("*.dcm")]
            elif modality == "SR":
                sr_files = [f for f in series_dir.rglob("*.dcm")]
            else:
                logger.debug(f"[{patient_dir.name}] Series {series_id}: unhandled modality '{modality}', skipping.")

        if not mr_files:
            logger.warning(f"[{patient_dir.name}]: no MR series found, skipping consistency check.")
            continue
        if not seg_files and not sr_files:
            logger.warning(f"[{patient_dir.name}]: no SEG or SR series found, skipping consistency check.")
            continue

        # Collect MR metadata
        input_study_uid = None
        input_series_uid = None
        input_sop_uids: set = set()

        for mr_file in mr_files:
            ds = pydicom.dcmread(mr_file, stop_before_pixels=True)
            sop = getattr(ds, "SOPInstanceUID", None)
            if sop:
                input_sop_uids.add(str(sop))
            if input_study_uid is None:
                input_study_uid = str(getattr(ds, "StudyInstanceUID", ""))
            if input_series_uid is None:
                input_series_uid = str(getattr(ds, "SeriesInstanceUID", ""))

        patient_issues = 0

        for output_file in seg_files + sr_files:
            ds = pydicom.dcmread(output_file, stop_before_pixels=True)
            modality = str(getattr(ds, "Modality", "UNKNOWN"))
            out_name = output_file.name

            out_study_uid = str(getattr(ds, "StudyInstanceUID", ""))
            if out_study_uid != input_study_uid:
                logger.warning(
                    f"[{patient_dir.name}] [{modality}] {out_name}: StudyInstanceUID mismatch. "
                    f"Input={input_study_uid}, Output={out_study_uid}"
                )
                patient_issues += 1

            if modality == "SEG":
                patient_issues += _check_seg_references(ds, input_sop_uids, input_series_uid, patient_dir.name, out_name)
            elif modality == "SR":
                patient_issues += _check_sr_references(ds, input_sop_uids, input_series_uid, patient_dir.name, out_name)

        if patient_issues == 0:
            logger.info(f"[{patient_dir.name}] Consistency check OK.")
        else:
            logger.warning(f"[{patient_dir.name}] Consistency check: {patient_issues} issue(s) found.")
            total_issues += patient_issues

    if total_issues == 0:
        logger.info("All patients passed DICOM consistency check.")
    else:
        logger.warning(f"DICOM consistency check complete: {total_issues} total issue(s) found.")


def upload_processed_dataset(orthanc_output: Path) -> None:
    for dcm_path in orthanc_output.rglob("*.dcm"):
        ds = pydicom.dcmread(dcm_path)

        if ds.Modality not in ("SR", "SEG"):
            continue

        with open(dcm_path, "rb") as f:
            dicom_bytes = f.read()
        success = upload_dataset_processing(dicom_bytes)
        if success:
            logger.info(f"Successfully uploaded {dcm_path.name} to Shanoir.")
        else:
            logger.warning(f"Failed to upload {dcm_path.name} to Shanoir.")


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
            modality = series.get("MainDicomTags", {}).get("Modality", "")
            instance_id = series.get("Instances", [None])[0]

            if not instance_id:
                continue

            instance = get_orthanc_instance_metadata(instance_id)
            series_description = instance.get("SeriesDescription", "Unnamed Series")
            frame_uid = instance.get("FrameOfReferenceUID")

            if modality in ("SEG", "SR"):
                instance_uid = instance.get("SOPInstanceUID", "N/A")
                logger.info(f"  [{modality}] {series_description} | Series ID: {series_id} | Instance UID: {instance_uid}")

            if frame_uid:
                frame_of_refs.append({series_description: frame_uid})

        for ref in frame_of_refs:
            for series_desc, uid in ref.items():
                logger.info(f"{series_desc}: {uid}")

        logger.info("*" * 90)
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
