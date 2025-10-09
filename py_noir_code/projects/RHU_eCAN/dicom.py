from pathlib import Path
from typing import Set

import pydicom
from pydicom.dataset import Dataset

from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()

TAGS_TO_CHECK = {
    "FrameOfReferenceUID": (0x0020, 0x0052),
    "ImageOrientationPatient": (0x0020, 0x0037),
    "ImagePositionPatient": (0x0020, 0x0032),
    "PixelSpacing": (0x0028, 0x0030),
    "SliceThickness": (0x0018, 0x0050),
    "Rows": (0x0028, 0x0010),
    "Columns": (0x0028, 0x0011),
    "NumberOfFrames": (0x0028, 0x0008),
    "StudyInstanceUID": (0x0020, 0x000D)
}


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


def check_series_tag_consistency(series_dir: Path, fix_files: bool = False) -> bool:
    """
    Check that all DICOM tags in TAGS_TO_CHECK are consistent across all instances in the series.
    If inconsistencies are found, fix them by assigning the reference value to the inconsistent files.

    Args:
        series_dir (Path): Path to the DICOM series directory.
        fix_files (bool): Fix inconsistent files with the value of the reference file.

    Returns:
        bool: True if all tags were initially consistent, False otherwise.
    """
    logger.info(f"Checking tag consistency for image series: {series_dir}")
    files = sorted(series_dir.glob("*.dcm"))
    if not files:
        logger.info("⚠️ No DICOM files found in the series.")
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
                    ds.save_as(f)  # overwrite with corrected value

        if inconsistent_files:
            all_consistent = False
            logger.info(
                f"❌ Tag '{name}' ({tag}) not consistent across series.\n"
                f"  Reference value: {ref_val}\n"
                f"  Fixed files: {', '.join([f.name for f in inconsistent_files])}"
            )
        else:
            logger.info(f"✅ Tag '{name}' consistent across all slices.")

    if all_consistent:
        logger.info("✅ All tags consistent across series.")
    else:
        logger.info("⚙️ Inconsistent tags were found and fixed using the reference values.")

    return all_consistent


def compare_tags(ds_mr: Dataset, ds_seg: Dataset) -> bool:
    """
    Compare specific DICOM tags between an MR and a SEG dataset.

    Args:
        ds_mr (Dataset): DICOM dataset for the MR series.
        ds_seg (Dataset): DICOM dataset for the SEG series.

    Returns:
        bool: True if all tags match, False otherwise.
    """
    all_match = True
    for name, tag in TAGS_TO_CHECK.items():
        mr_val = getattr(ds_mr, name, None)
        seg_val = getattr(ds_seg, name, None)
        match = mr_val == seg_val
        if not match:
            all_match = False
        logger.info(f"{name} {tag}:\n  MR : {mr_val}\n  SEG: {seg_val}\n  Match: {match}")
    logger.info(f"All tags match: {all_match}")
    return all_match


def collect_mr_sop_uids(mr_dir: Path) -> Set[str]:
    """
    Collect all SOPInstanceUIDs from MR slices in a directory.

    Args:
        mr_dir (Path): Path to the MR series directory.

    Returns:
        Set[str]: Set of SOPInstanceUID strings for all MR slices.
    """
    sop_uids: Set[str] = set()
    for f in sorted(mr_dir.glob("*.dcm")):
        ds = pydicom.dcmread(f, stop_before_pixels=True)
        sop_uids.add(ds.SOPInstanceUID)
    return sop_uids


def check_referenced_sop(ds_seg: Dataset, mr_sop_uids: Set[str]) -> bool:
    """
    Check that all referenced SOPInstanceUIDs in a SEG dataset exist in the MR dataset.

    This function examines the ReferencedSeriesSequence in the SEG dataset. For each referenced
    series, it checks each ReferencedInstanceSequence entry to see if the SOPInstanceUID exists
    in the MR dataset's SOPInstanceUID set. Logs are generated for each reference.

    Args:
        ds_seg (Dataset): DICOM SEG dataset containing references to MR slices.
        mr_sop_uids (Set[str]): Set of SOPInstanceUIDs from the MR dataset.

    Returns:
        bool: True if all referenced SOPInstanceUIDs exist in MR, False otherwise.
    """
    logger.info(f"SEG SOP Instance UID: {ds_seg.SOPInstanceUID}")
    if "ReferencedSeriesSequence" not in ds_seg:
        logger.info("⚠️ No ReferencedSeriesSequence found in SEG.")
        return False

    all_matched = True
    for ref_series in ds_seg.ReferencedSeriesSequence:
        ref_series_uid = ref_series.SeriesInstanceUID
        logger.info(f"Referenced MR SeriesInstanceUID in SEG: {ref_series_uid}")

        if "ReferencedInstanceSequence" not in ref_series:
            logger.info("⚠️ No ReferencedInstanceSequence found!")
            all_matched = False
            continue

        for ref_instance in ref_series.ReferencedInstanceSequence:
            sop_uid = ref_instance.ReferencedSOPInstanceUID
            status = sop_uid in mr_sop_uids
            if not status:
                all_matched = False
            logger.info(f"Referenced SOPInstanceUID: {sop_uid} -> Match MR slice: {status}")
    logger.info(f"All referenced instances in SEG found in MR folder: {all_matched}")
    return all_matched


def inspect_study_tags(input_dir: Path) -> None:
    """
    Inspect spatial and reference DICOM tags for all studies in the downloads folder.

    This function iterates over all subjects and study directories in
    `py_noir_code/resources/downloads/`. For each study, it:
      1. Loads the first MR and SEG DICOM files.
      2. Compares a predefined set of important tags.
      3. If tags do not all match, collects all MR SOPInstanceUIDs and checks that
         referenced SOPInstanceUIDs in the SEG exist in the MR series.
      4. Writes a detailed log for each study to
         `py_noir_code/resources/dicom_logs/{subject_id}/{processing_id}/log.txt`.

    Args:
        input_dir (Path): Path to the root folder containing patient subfolders with study data.

    Returns:
        None
    """
    for subject_dir in input_dir.iterdir():
        for study_dir in subject_dir.iterdir():
            mr_dir = next(d for d in study_dir.iterdir() if d.is_dir() and d.name != "output")
            seg_dir = study_dir / "output"

            ds_mr = load_first_dicom(mr_dir)
            ds_seg = load_first_dicom(seg_dir)

            processing_id = mr_dir.parent.name.split("_")[1]
            subject_id = mr_dir.parent.parent.name

            logger.info(f"Checking DICOM tags consistency across the series")

            series_ok = check_series_tag_consistency(mr_dir, fix_files=True)
            if not series_ok:
                logger.warning(f"Series tag consistency check failed for {study_dir}")

            # Not really necessary for now
            logger.info(
                f"Checking spatial and reference DICOM tags between MR and SEG for patient {subject_id}, exam {processing_id}..."
            )

            # Compare tags
            all_match = compare_tags(ds_mr, ds_seg)

            # Only check SOPs if tags don't all match
            if not all_match:
                logger.warning(f"DICOM tag consistency check between image series and SEG failed for {study_dir}\n"
                               f"Performing further inspections...")
                mr_sop_uids = collect_mr_sop_uids(mr_dir)
                _ = check_referenced_sop(ds_seg, mr_sop_uids)
