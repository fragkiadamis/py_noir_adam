"""
DICOM MIP Detector
==================
Determines whether the first slice of a DICOM series is a Maximum Intensity
Projection (MIP).

Detection strategy (multi-signal approach):
  1. DICOM tag checks — ImageType, SeriesDescription, ProtocolName
  2. Pixel statistics — MIPs have elevated mean intensity, long bright tails
  3. Histogram shape — MIPs show a characteristic bimodal or right-skewed distribution
"""

import os
import re
from pathlib import Path

import numpy as np
import pydicom

from src.utils.log_utils import get_logger

logger = get_logger()

_MIP_KEYWORDS = re.compile(
    r"\b(mip|mips|max[\s_-]?int|maximum[\s_-]?intensity|projection)\b",
    re.IGNORECASE,
)

_IMAGE_TYPE_MIP_VALUES = {"MIP", "MAXIMUM", "MAX_IP"}


def _check_tag_string(value: str | None) -> bool:
    if value is None:
        return False
    return bool(_MIP_KEYWORDS.search(str(value)))


def detect_mip_by_tags_dict(tags: dict) -> tuple[bool, list[str]]:
    """
    Tag-based MIP detection from a plain dict, e.g. from Orthanc's
    /instances/{id}/tags?simplify endpoint.
    ImageType is a backslash-separated string in that context.
    """
    evidence = []

    image_type_raw = tags.get("ImageType", "")
    for val in (image_type_raw.split("\\") if isinstance(image_type_raw, str) else []):
        if val.upper() in _IMAGE_TYPE_MIP_VALUES or _MIP_KEYWORDS.search(val):
            evidence.append(f"ImageType contains '{val}'")

    for attr in ("SeriesDescription", "ProtocolName", "RequestedProcedureDescription", "PerformedProcedureStepDescription"):
        if _check_tag_string(tags.get(attr)):
            evidence.append(f"{attr} = '{tags[attr]}'")

    return bool(evidence), evidence


def detect_mip_by_tags(ds: pydicom.Dataset) -> tuple[bool, list[str]]:
    evidence = []

    image_type = getattr(ds, "ImageType", [])
    print(image_type)
    for val in image_type:
        if str(val).upper() in _IMAGE_TYPE_MIP_VALUES or _MIP_KEYWORDS.search(str(val)):
            evidence.append(f"ImageType contains '{val}'")

    series_desc = getattr(ds, "SeriesDescription", None)
    if _check_tag_string(series_desc):
        evidence.append(f"SeriesDescription = '{series_desc}'")

    protocol = getattr(ds, "ProtocolName", None)
    if _check_tag_string(protocol):
        evidence.append(f"ProtocolName = '{protocol}'")

    for attr in ("RequestedProcedureDescription", "PerformedProcedureStepDescription"):
        val = getattr(ds, attr, None)
        if _check_tag_string(val):
            evidence.append(f"{attr} = '{val}'")

    return bool(evidence), evidence


def _skewness(arr: np.ndarray) -> float:
    mu = np.mean(arr)
    sigma = np.std(arr)
    if sigma == 0:
        return 0.0
    return float(np.mean(((arr - mu) / sigma) ** 3))


def detect_mip_by_pixels(ds: pydicom.Dataset) -> tuple[bool, dict]:
    if not hasattr(ds, "PixelData"):
        return False, {}

    arr = ds.pixel_array.astype(np.float32)
    slope = float(getattr(ds, "RescaleSlope", 1.0))
    intercept = float(getattr(ds, "RescaleIntercept", 0.0))
    arr = arr * slope + intercept
    flat = arr.flatten()

    stats = {
        "min": float(np.min(flat)),
        "max": float(np.max(flat)),
        "mean": float(np.mean(flat)),
        "median": float(np.median(flat)),
        "std": float(np.std(flat)),
        "p99": float(np.percentile(flat, 99)),
        "skewness": _skewness(flat),
    }

    data_range = stats["max"] - stats["min"]
    if data_range == 0:
        return False, stats

    norm_mean = (stats["mean"] - stats["min"]) / data_range
    bright_tail_ratio = (
        (stats["p99"] - stats["median"]) / (stats["max"] - stats["min"])
        if stats["max"] != stats["median"] else 0.0
    )
    stats["norm_mean"] = norm_mean
    stats["bright_tail_ratio"] = bright_tail_ratio

    is_mip = (
        norm_mean > 0.35
        and bright_tail_ratio > 0.15
        and stats["skewness"] > 0.5
    )

    return is_mip, stats


def _load_sorted_dcm_paths(folder: Path) -> list[tuple[str, pydicom.Dataset]]:
    """Return (filepath, dataset) pairs from *folder*, sorted by InstanceNumber."""
    items = []
    for fpath in folder.iterdir():
        if not os.path.isfile(fpath):
            continue
        try:
            ds = pydicom.dcmread(fpath, stop_before_pixels=False)
            items.append((fpath, ds))
        except Exception:
            pass

    def sort_key(item):
        fpath, ds = item
        try:
            return int(ds.InstanceNumber)
        except Exception:
            return fpath

    items.sort(key=sort_key)
    return items


def delete_first_slice_if_mip(vip_output: Path) -> None:
    """
    Load the series in *folder*, check if the first slice is a MIP, and delete
    it if so.  Returns True if a file was deleted.
    """
    for processing in vip_output.iterdir():
        if not processing.is_dir():
            continue
        input_dir = next(d for d in processing.iterdir() if d.is_dir() and "output" not in d.name)
        items = _load_sorted_dcm_paths(input_dir)
        if not items:
            continue

        first_path, first_ds = items[0]
        tag_hit, tag_evidence = detect_mip_by_tags(first_ds)
        print(tag_hit, tag_evidence)
        pixel_hit, _ = detect_mip_by_pixels(first_ds)

        is_mip = tag_hit or pixel_hit
        if not is_mip:
            continue

        confidence = "high" if (tag_hit and pixel_hit) else "medium" if tag_hit else "low"
        patient_name = getattr(first_ds, "PatientName", "unknown")
        logger.info(
            f"{patient_name} — first slice is a MIP ({confidence} confidence, "
            f"evidence: {tag_evidence}).\nDeleting: {first_path}"
        )
        # os.remove(first_path)
