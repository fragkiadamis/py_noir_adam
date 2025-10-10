import base64
from pathlib import Path
from typing import Dict, Optional

import requests

from py_noir_code.src.utils.log_utils import get_logger

logger = get_logger()


def get_http_headers(username: str, password: str) -> Dict[str, str]:
    """
    Generate HTTP headers for authenticated DICOM uploads to Orthanc.
    Args:
        username (str): Orthanc REST API username.
        password (str): Orthanc REST API password.

    Returns:
        Dict[str, str]: Headers containing authentication and content type.
    """
    auth_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    return {
        "Content-Type": "application/dicom",
        "Authorization": f"Basic {auth_token}",
    }


def upload_dicom_file(file_path: Path, endpoint: str, headers: Dict[str, str]) -> Optional[str]:
    """
    Upload a single DICOM file to Orthanc and return its parent Study ID.
    Args:
        file_path (Path): Path to the DICOM file.
        endpoint (str): Base Orthanc API endpoint (e.g., http://localhost:8042).
        headers (Dict[str, str]): HTTP headers for authentication.

    Returns:
        Optional[str]: The Orthanc Study ID if upload succeeds, otherwise None.
    """
    try:
        with open(file_path, "rb") as dcm:
            response = requests.post(f"{endpoint}/instances", headers=headers, data=dcm.read())

        if response.status_code == 200:
            return response.json().get("ParentStudy")
        else:
            logger.warning(f"Upload failed for {file_path.name} (status {response.status_code})")
            return None

    except Exception as e:
        logger.error(f"Error uploading {file_path}: {e}")
        return None


def assign_label_to_study(endpoint: str, headers: Dict[str, str], study_id: str, label: str) -> bool:
    """
    Assign a label to a study in Orthanc.
    Args:
        endpoint (str): Orthanc base URL.
        headers (Dict[str, str]): HTTP authentication headers.
        study_id (str): Orthanc Study ID.
        label (str): Label name to assign.

    Returns:
        bool: True if the label was successfully assigned, False otherwise.
    """
    try:
        response = requests.put(f"{endpoint}/studies/{study_id}/labels/{label}", headers=headers)
        if response.status_code == 200:
            logger.info(f"Assigned label '{label}' to study {study_id}")
            return True
        else:
            logger.warning(f"Failed to assign label '{label}' (status {response.status_code})")
            return False
    except Exception as e:
        logger.error(f"Error assigning label '{label}' to study {study_id}: {e}")
        return False
