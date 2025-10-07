import os
from pathlib import Path
from typing import List, Tuple, Set

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


def compare_tags(ds_mr: Dataset, ds_seg: Dataset) -> Tuple[bool, List[str]]:
    """
    Compare specific DICOM tags between an MR and a SEG dataset.

    Args:
        ds_mr (Dataset): DICOM dataset for the MR series.
        ds_seg (Dataset): DICOM dataset for the SEG series.

    Returns:
        Tuple[bool, List[str]]:
            - bool: True if all tags match, False otherwise.
            - List[str]: List of formatted strings detailing the comparison for each tag.
    """
    all_match = True
    lines: List[str] = []
    for name, tag in TAGS_TO_CHECK.items():
        mr_val = getattr(ds_mr, name, None)
        seg_val = getattr(ds_seg, name, None)
        match = mr_val == seg_val
        if not match:
            all_match = False
        lines.append(f"{name} {tag}:\n  MR : {mr_val}\n  SEG: {seg_val}\n  Match: {match}\n")
    lines.append(f"All tags match: {all_match}\n")
    return all_match, lines


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


def check_referenced_sop(ds_seg: Dataset, mr_sop_uids: Set[str]) -> Tuple[List[str], bool]:
    """
    Check that all referenced SOPInstanceUIDs in a SEG dataset exist in the MR dataset.

    This function examines the ReferencedSeriesSequence in the SEG dataset. For each referenced
    series, it checks each ReferencedInstanceSequence entry to see if the SOPInstanceUID exists
    in the MR dataset's SOPInstanceUID set. Logs are generated for each reference.

    Args:
        ds_seg (Dataset): DICOM SEG dataset containing references to MR slices.
        mr_sop_uids (Set[str]): Set of SOPInstanceUIDs from the MR dataset.

    Returns:
        Tuple[List[str], bool]:
            - List[str]: Log lines detailing the reference checks.
            - bool: True if all referenced SOPInstanceUIDs exist in MR, False otherwise.
    """
    lines: List[str] = [f"SEG SOP Instance UID: {ds_seg.SOPInstanceUID}\n"]
    if "ReferencedSeriesSequence" not in ds_seg:
        lines.append("⚠️ No ReferencedSeriesSequence found in SEG.\n")
        return lines, False

    all_matched = True
    for ref_series in ds_seg.ReferencedSeriesSequence:
        ref_series_uid = ref_series.SeriesInstanceUID
        lines.append(f"Referenced MR SeriesInstanceUID in SEG: {ref_series_uid}\n")

        if "ReferencedInstanceSequence" not in ref_series:
            lines.append("  ⚠️ No ReferencedInstanceSequence found!\n")
            all_matched = False
            continue

        for ref_instance in ref_series.ReferencedInstanceSequence:
            sop_uid = ref_instance.ReferencedSOPInstanceUID
            status = sop_uid in mr_sop_uids
            if not status:
                all_matched = False
            lines.append(f"  Referenced SOPInstanceUID: {sop_uid} -> Match MR slice: {status}\n")
    lines.append(f"All referenced instances in SEG found in MR folder: {all_matched}\n")
    return lines, all_matched


def write_log(log_file: Path, log_lines: List[str]) -> None:
    """
    Write log lines to a specified log file.

    Args:
        log_file (Path): Path to the log file.
        log_lines (List[str]): List of log lines to write.

    Returns:
        None
    """
    os.makedirs(log_file.parent, exist_ok=True)
    with open(log_file, "w") as f:
        f.writelines("\n".join(log_lines))
    logger.info(f"Log written to {log_file}")


def inspect_study_tags() -> None:
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

    Returns:
        None
    """
    input_dir = Path("py_noir_code/resources/downloads/")
    for subject_dir in input_dir.iterdir():
        for study_dir in subject_dir.iterdir():
            mr_dir = next(d for d in study_dir.iterdir() if d.is_dir() and d.name != "output")
            seg_dir = study_dir / "output"

            ds_mr = load_first_dicom(mr_dir)
            ds_seg = load_first_dicom(seg_dir)

            processing_id = mr_dir.parent.name.split("_")[1]
            subject_id = mr_dir.parent.parent.name
            log_file = Path(f"py_noir_code/resources/dicom_logs/{subject_id}/{processing_id}/log.txt")

            log_lines: List[str] = [
                f"Checking spatial and reference DICOM tags between MR and SEG for patient {subject_id}, exam {processing_id}...\n"
            ]

            # Compare tags
            all_match, tag_lines = compare_tags(ds_mr, ds_seg)
            log_lines.extend(tag_lines)

            # Only check SOPs if tags don't all match
            if not all_match:
                mr_sop_uids = collect_mr_sop_uids(mr_dir)
                log_lines.append(f"Number of MR slices found: {len(mr_sop_uids)}\n")
                ref_lines, _ = check_referenced_sop(ds_seg, mr_sop_uids)
                log_lines.extend(ref_lines)

            write_log(log_file, log_lines)
