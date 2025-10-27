import base64
import os.path
import zipfile
from typing import Dict, List

import requests

from py_noir_code.src.orthanc.orthanc_context import OrthancContext
from py_noir_code.src.security.authentication_service import load_orthanc_password
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


def orthanc_request(method: str, path: str, raise_for_status: bool = True, **kwargs):
    """ Authenticate / Re-authenticate user [APIContext.username] and execute a [method] HTTP query to [path] endpoint
    :param method:
    :param path:
    :param raise_for_status:
    :param kwargs:
    :return:
    """
    if OrthancContext.password is None:
        load_orthanc_password()

    headers = get_http_headers(OrthancContext.username, OrthancContext.password)
    url = OrthancContext.scheme + "://" + OrthancContext.domain + ":" + OrthancContext.rest_api_port + "/" + path

    response = None
    if method == 'get':
        response = requests.get(url, headers=headers, **kwargs)
    elif method == 'post':
        response = requests.post(url, headers=headers, **kwargs)
    elif method == 'put':
        response = requests.put(url, headers=headers, **kwargs)
    elif method == 'delete':
        response = requests.delete(url, headers=headers, **kwargs)
    else:
        logger.error('Error: unimplemented request type')

    if raise_for_status:
        response.raise_for_status()

    return response


def upload_study_to_orthanc(files: List[str]) -> Dict[str, str] | None:
    """
    Upload a single DICOM file to Orthanc and return its parent Study ID.
    Args:
        files (List[str]): List with pats to the DICOM files.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    response = None
    for file_path in files:
        try:
            with open(file_path, "rb") as dcm:
                response = orthanc_request("post", f"instances", data=dcm.read())

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Upload failed for {os.path.basename(file_path)} (status {response.status_code})")
                return None

        except Exception as e:
            logger.error(f"Error uploading {file_path}: {e}")

    return None


def get_all_orthanc_studies() -> List | None:
    """
        Retrieve all Orthanc studies.

        Returns:
            List or None: the Orthanc study IDs if found, otherwise None.
        """
    try:
        response = orthanc_request("get", f"studies")
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"Failed to get studies (status {response.status_code})")
            return None
    except Exception as e:
        logger.error(f"Error getting Orthanc study IDs: {e}")
        return None


def get_study_orthanc_id_by_uid(study_uid: str) -> str | None:
    """
    Retrieve the Orthanc study ID from a StudyInstanceUID.

    Args:
        study_uid (str): StudyInstanceUID.

    Returns:
        str or None: the Orthanc study ID if found, otherwise None.
    """
    try:
        payload = {"Level": "Study", "Query": {"StudyInstanceUID": study_uid}}
        response = orthanc_request("post", f"/tools/find", json=payload)
        if response.status_code == 200:
            return response.json()[0]
        else:
            logger.warning(f"Failed to get study with StudyInstanceUID '{study_uid}' (status {response.status_code})")
            return None
    except Exception as e:
        logger.error(f"Error getting Orthanc study ID with StudyInstanceUID '{study_uid}': {e}")
        return None


def get_orthanc_study_metadata(orthanc_study_id: str) -> Dict[str, str] | None:
    """
    Retrieve metadata (patient info, series, modalities, UIDs, etc.)
    for a study from Orthanc.

    Args:
        orthanc_study_id (str): Orthanc study ID.

    Returns:
        Dict[str, Any] or None: Study metadata dictionary if found, otherwise None.
    """
    try:
        response = orthanc_request("get", f"/studies/{orthanc_study_id}")
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"Failed to get study metadata with studyID '{orthanc_study_id}' (status {response.status_code})")
            return None
    except Exception as e:
        logger.error(f"Error getting study meta for study '{orthanc_study_id}': {e}")
        return None


def download_orthanc_study(study_id: str, download_path: str, unzip: bool = True):
    try:
        output_file = os.path.join(download_path, f"{study_id}.zip")
        response = orthanc_request("get", f"studies/{study_id}/archive")
        if response.status_code == 200:
            with open(output_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded study {study_id} to {output_file}")

            if unzip:
                extract_dir = os.path.join(download_path, study_id)
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(output_file, "r") as zip_ref:
                    zip_ref.extractall(extract_dir)
                logger.info(f"Extracted study {study_id} to {extract_dir}")

                # Optionally remove ZIP after extraction
                os.remove(output_file)
                logger.debug(f"Removed archive {output_file}")
        else:
            logger.warning(f"Failed to download study {study_id} (status {response.status_code})")
    except Exception as e:
        logger.error(f"Error with download: {e}")
        return None


def set_orthanc_study_label(study_id: str, label: str) -> bool:
    """
    Assign a label to a study in Orthanc.
    Args:
        study_id (str): Orthanc Study ID.
        label (str): Label name to assign.

    Returns:
        bool: True if the label was successfully assigned, False otherwise.
    """
    try:
        response = orthanc_request("put", f"studies/{study_id}/labels/{label}")
        if response.status_code == 200:
            logger.info(f"Assigned label '{label}' to study {study_id}")
            return True
        else:
            logger.warning(f"Failed to assign label '{label}' (status {response.status_code})")
            return False
    except Exception as e:
        logger.error(f"Error assigning label '{label}' to study {study_id}: {e}")
        return False


def delete_orthanc_study(study_id: str) -> bool:
    """
    Delete a study from Orthanc.
    Args:
        study_id (str): Orthanc Study ID.

    Returns:
        bool: True if the study was successfully deleted, False otherwise.
    """
    try:
        response = orthanc_request("delete", f"studies")
        if response.status_code == 200:
            logger.info(f"Deleted study {study_id}")
            return True
        else:
            logger.warning(f"Failed to delete study '{study_id}' (status {response.status_code})")
            return False
    except Exception as e:
        logger.error(f"Error deleting label 'study {study_id}: {e}")
        return False


def get_orthanc_patients() -> List | None:
    """
    Retrieve the list of all patients stored in an Orthanc PACS server.

    Returns:
        list | None:
            - A list of patient identifiers (UUIDs) if the request succeeds.
            - None if the request fails or an error occurs.
    """
    try:
        response = orthanc_request("get", f"patients")
        if response.status_code == 200:
            logger.info(f"Got patients")
            return response.json()
        else:
            logger.warning(f"Failed to get patients (status {response.status_code})")
            return None
    except Exception as e:
        logger.error(f"Error getting patients: {e}")
        return None


def get_orthanc_patient_meta(patient_id: str) -> Dict | None:
    """
    Retrieve metadata for a specific patient stored in Orthanc.

    Args:
        patient_id (str): Orthanc internal patient identifier (UUID).

    Returns:
        dict | None:
            - A dictionary containing the patient's metadata if the request succeeds.
            - None if the request fails or an error occurs.
    """
    try:
        response = orthanc_request("get", f"patients/{patient_id}")
        if response.status_code == 200:
            logger.info(f"Got patient meta for {patient_id}")
            return response.json()
        else:
            logger.warning(f"Failed to get patient meta for {patient_id} (status {response.status_code})")
    except Exception as e:
        logger.error(f"Error getting patient meta: {e}")
        return None