from pathlib import Path

import pydicom
from pydicom.dataset import Dataset
from pynetdicom import AE
from pynetdicom.sop_class import _STORAGE_CLASSES as STORAGE_CLASSES

from py_noir_code.src.orthanc.orthanc_context import OrthancContext
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
            logger.info(
                f"Tag '{name}' ({tag}) not consistent across series.\n"
                f"Reference value: {ref_val}\n"
                f"Fixed files: {', '.join([f.name for f in inconsistent_files])}"
            )
        else:
            logger.info(f"Tag '{name}' consistent across all slices.")

    if all_consistent:
        logger.info("All tags consistent across series.")
    else:
        logger.info("Inconsistent tags were found and fixed using the reference values.")

    return all_consistent


def inspect_study_tags(input_dir: Path) -> None:
    """
    Inspect spatial and reference DICOM tags for all studies in the downloads folder.

    Args:
        input_dir (Path): Path to the root folder containing patient subfolders with study data.

    Returns:
        None
    """
    for subject_dir in input_dir.iterdir():
        for study_dir in subject_dir.iterdir():
            mr_dir = next(d for d in study_dir.iterdir() if d.is_dir() and d.name != "output")
            logger.info(f"Checking DICOM tags consistency across the series")

            series_ok = check_series_tag_consistency(mr_dir, fix_files=True)
            if not series_ok:
                logger.warning(f"Series tag consistency check failed for {study_dir}")


def c_store(dataset_dir: Path) -> None:
    ae = AE(ae_title=OrthancContext.client_ae_title)
    ae.acse_timeout = 30
    ae.network_timeout = 30
    ae.add_requested_context(STORAGE_CLASSES["MRImageStorage"])
    ae.add_requested_context(STORAGE_CLASSES["SegmentationStorage"])
    ae.add_requested_context(STORAGE_CLASSES["ComprehensiveSRStorage"])
    assoc = ae.associate(OrthancContext.domain, int(OrthancContext.dicom_server_port), ae_title=OrthancContext.pacs_ae_title)

    for dcm_file in sorted(dataset_dir.rglob("*.dcm"), key=lambda p: str(p).lower()):
        max_retries = 3
        retry_delay = 2
        attempts = 0

        ds = pydicom.dcmread(dcm_file)
        status = assoc.send_c_store(ds)

    if assoc.is_established:
        assoc.release()
