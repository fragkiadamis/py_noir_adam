import base64
import os.path
from typing import Dict

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


def upload_dicom_file(file_path: str, endpoint: str, headers: Dict[str, str]) -> bool:
    """
    Upload a single DICOM file to Orthanc and return its parent Study ID.
    Args:
        file_path (str): Path to the DICOM file.
        endpoint (str): Base Orthanc API endpoint (e.g., http://localhost:8042).
        headers (Dict[str, str]): HTTP headers for authentication.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        with open(file_path, "rb") as dcm:
            response = requests.post(f"{endpoint}/instances", headers=headers, data=dcm.read())

        if response.status_code == 200:
            return True
        else:
            logger.warning(f"Upload failed for {os.path.basename(file_path)} (status {response.status_code})")
            return False

    except Exception as e:
        logger.error(f"Error uploading {file_path}: {e}")
        return False


def get_study_orthanc_id_by_uid(endpoint: str, headers: Dict[str, str], study_uid: str) -> str | None:
    """
    Retrieve the Orthanc study ID from a StudyInstanceUID.

    Args:
        endpoint (str): Orthanc base URL.
        headers (Dict[str, str]): HTTP authentication headers.
        study_uid (str): StudyInstanceUID.

    Returns:
        str or None: the Orthanc study ID if found, otherwise None.
    """
    try:
        payload = {"Level": "Study", "Query": {"StudyInstanceUID": study_uid}}
        response = requests.post(f"{endpoint}/tools/find", headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()[0]
        else:
            return None
    except Exception as e:
        logger.error(f"Error getting Orthanc study ID with StudyInstanceUID '{study_uid}'")
        return None


def get_study_metadata(endpoint: str, headers: dict, orthanc_study_id: str) -> Dict[str, str] | None:
    """
    Retrieve metadata (patient info, series, modalities, UIDs, etc.)
    for a study from Orthanc.

    Args:
        endpoint (str): Orthanc base URL.
        headers (dict): HTTP authentication headers.
        orthanc_study_id (str): Orthanc study ID.

    Returns:
        Dict[str, Any] or None: Study metadata dictionary if found, otherwise None.
    """
    try:
        response = requests.get(f"{endpoint}/studies/{orthanc_study_id}", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        logger.error(f"Error getting study meta for study '{orthanc_study_id}'")
        return None


def set_study_label(endpoint: str, headers: Dict[str, str], study_id: str, label: str) -> bool:
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


def delete_orthanc_study(endpoint: str, headers: Dict[str, str], study_id: str) -> bool:
    """
    Delete a study from Orthanc.
    Args:
        endpoint (str): Orthanc base URL.
        headers (Dict[str, str]): HTTP authentication headers.
        study_id (str): Orthanc Study ID.

    Returns:
        bool: True if the study was successfully deleted, False otherwise.
    """
    try:
        response = requests.delete(f"{endpoint}/studies/{study_id}/", headers=headers)
        if response.status_code == 200:
            logger.info(f"Deleted study {study_id}")
            return True
        else:
            logger.warning(f"Failed to delete study '{study_id}' (status {response.status_code})")
            return False
    except Exception as e:
        logger.error(f"Error deleting label 'study {study_id}: {e}")
        return False
