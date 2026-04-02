import shutil
import subprocess
import sys
from pathlib import Path
from typing import List

import pydicom

from src.utils.log_utils import get_logger

logger = get_logger()

SEQUENCE_TAG = (0x0040, 0x0275)
SEQUENCE_ITEM_TAG = (0x0040, 0x0008)


def inspect_and_fix_study_tags(input_dir: Path) -> None:
    for processing_dir in input_dir.iterdir():
        if not processing_dir.is_dir():
            continue

        processing_input_dir = next(d for d in processing_dir.iterdir() if d.is_dir() and "output" not in d.name)
        processing_output_dir = processing_dir / "output"
        mr_files = list(processing_input_dir.glob("*.dcm"))
        seg_file = next(processing_output_dir.glob("*seg*"))

        uids = {}
        for file_path in mr_files:
            ds = pydicom.dcmread(file_path, stop_before_pixels=True)
            uid = getattr(ds, "FrameOfReferenceUID", None)
            if uid:
                uids.setdefault(uid, []).append(file_path.stem)

        if len(uids.keys()) > 1:
            subject_name = pydicom.dcmread(mr_files[0]).PatientName
            logger.info(f"{subject_name} --> inconsistencies were found in MR FrameOfReferenceUID.")
            good_uid = max(uids, key=lambda k: len(uids[k]))
            for file_path in mr_files:
                ds = pydicom.dcmread(file_path)
                if getattr(ds, "FrameOfReferenceUID", None) != good_uid:
                    ds.FrameOfReferenceUID = good_uid
                    ds.save_as(file_path)
        else:
            good_uid = list(uids.keys())[0]

        seg = pydicom.dcmread(seg_file)
        if seg.FrameOfReferenceUID != good_uid:
            subject_name = seg.PatientName
            logger.info(f"{subject_name} --> inconsistencies were found between MR and SEG FrameOfReferenceUID.")
            seg.FrameOfReferenceUID = good_uid
            seg.save_as(seg_file)

        for file_path in mr_files:
            ds = pydicom.dcmread(file_path, stop_before_pixels=False)

            if SEQUENCE_TAG not in ds:
                ds.save_as(file_path)
                continue

            cleaned = False
            for item in ds[SEQUENCE_TAG].value:
                if SEQUENCE_ITEM_TAG not in item:
                    continue
                found_item = item[(0x0040, 0x0008)]
                if found_item.VR != "SQ":
                    continue
                # Remove empty or malformed nested sequences
                if len(found_item.value) < 2 and len(found_item.value[0]) == 0:
                    del item[SEQUENCE_ITEM_TAG]
                    cleaned = True
            if cleaned:
                ds.save_as(file_path)


def _check_seg_references(ds: pydicom.Dataset, input_sop_uids: set, input_series_uid: str, subject_name: str, out_name: str) -> int:
    issues = 0
    ref_series_seq = getattr(ds, "ReferencedSeriesSequence", None)
    if ref_series_seq is None:
        logger.warning(f"[{subject_name}] [SEG] {out_name}: missing ReferencedSeriesSequence.")
        return 1

    for series_item in ref_series_seq:
        ref_series_uid = str(getattr(series_item, "SeriesInstanceUID", ""))
        if ref_series_uid != input_series_uid:
            logger.warning(
                f"[{subject_name}] [SEG] {out_name}: ReferencedSeriesSequence.SeriesInstanceUID mismatch. "
                f"Input={input_series_uid}, Referenced={ref_series_uid}"
            )
            issues += 1

        # SEG uses ReferencedInstanceSequence (not ReferencedSOPSequence like SR)
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
    issues = 0
    evidence_seq = getattr(ds, "CurrentRequestedProcedureEvidenceSequence", None)
    if evidence_seq is None:
        logger.warning(f"[{subject_name}] [SR] {out_name}: missing CurrentRequestedProcedureEvidenceSequence.")
        return 1

    # SR nests series under evidence[0] → series[0]
    evidence_item = evidence_seq[0]
    series_seq = getattr(evidence_item, "ReferencedSeriesSequence", None)
    if series_seq is None:
        logger.warning(f"[{subject_name}] [SR] {out_name}: missing ReferencedSeriesSequence inside CurrentRequestedProcedureEvidenceSequence.")
        return 1

    series_item = series_seq[0]
    ref_series_uid = str(getattr(series_item, "SeriesInstanceUID", ""))
    if ref_series_uid != input_series_uid:
        logger.warning(
            f"[{subject_name}] [SR] {out_name}: ReferencedSeriesSequence.SeriesInstanceUID mismatch. "
            f"Input={input_series_uid}, Referenced={ref_series_uid}"
        )
        issues += 1

    # SR uses ReferencedSOPSequence (not ReferencedInstanceSequence like SEG)
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
    for f in series_dir.rglob("*.dcm"):
        return str(getattr(pydicom.dcmread(f, stop_before_pixels=True), "Modality", None))
    return None


def check_dicom_consistency(input_dir: Path) -> None:
    total_issues = 0

    for patient_dir in input_dir.iterdir():
        if not patient_dir.is_dir():
            continue

        mr_files: List[Path] = []
        seg_files: List[Path] = []
        sr_files: List[Path] = []

        for series_dir in patient_dir.iterdir():
            if not series_dir.is_dir():
                continue
            modality = _get_series_modality(series_dir)
            if modality == "MR":
                mr_files = list(series_dir.rglob("*.dcm"))
            elif modality == "SEG":
                seg_files = list(series_dir.rglob("*.dcm"))
            elif modality == "SR":
                sr_files = list(series_dir.rglob("*.dcm"))
            else:
                logger.debug(f"[{patient_dir.name}] Series {series_dir.name}: unhandled modality '{modality}', skipping.")

        if not mr_files:
            logger.warning(f"[{patient_dir.name}]: no MR series found, skipping consistency check.")
            continue
        if not seg_files and not sr_files:
            logger.warning(f"[{patient_dir.name}]: no SEG or SR series found, skipping consistency check.")
            continue

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


def run_compliance_fixes(input_dir: Path, input_copy_dir: Path) -> None:
    input_copy_dir.mkdir(exist_ok=True)
    modified_datasets = 0
    for processing_dir in input_dir.iterdir():
        processing_dir_copy = input_copy_dir / processing_dir.name

        input_dataset_dir = next(d for d in processing_dir.iterdir() if d.is_dir() and "output" not in d.name)
        input_dataset_dir_copy = input_copy_dir / processing_dir.name / input_dataset_dir.name

        shutil.copytree(processing_dir / "output", processing_dir_copy / "output", dirs_exist_ok=True)

        compliance_script = Path(__file__).parent / "dicom_compliance.py"
        cmd = [sys.executable, str(compliance_script), str(input_dataset_dir), str(input_dataset_dir_copy)]
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:
            for line in proc.stdout:
                logger.info(line.rstrip())
                if "DICOM files modified" in line:
                    line_parts = line.split(" ")
                    modified_datasets = modified_datasets + 1 if int(line_parts[-1]) > 0 else modified_datasets
                    logger.info(f"Modified DICOM files in {input_dataset_dir_copy}")

    logger.info(f"Modified DICOM datasets: {modified_datasets}")
    shutil.rmtree(input_dir)
    input_copy_dir.rename(input_dir)
