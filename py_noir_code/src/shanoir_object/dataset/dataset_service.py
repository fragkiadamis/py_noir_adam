import requests

from py_noir_code.src.API.api_service import get, download_file, post
from py_noir_code.src.utils.log_utils import get_logger

"""
Define methods for Shanoir datasets MS datasets API call
"""

ENDPOINT = '/datasets/datasets'
ENDPOINT_PROCESSING = '/datasets/datasetProcessing'

logger = get_logger()


def get_dataset(dataset_id: str):
    """ Get dataset [dataset_id]
    :param dataset_id:
    :return: json
    """

    path = ENDPOINT + '/' + dataset_id
    response = get(path)
    return response.json()


def download_dataset(dataset_id, file_format, output_folder, unzip=False, silent=False):
    """ Download dataset [dataset_id] as [file_format] into [output_folder]
    :param dataset_id:
    :param file_format:
    :param output_folder:
    :param silent:
    :param unzip:
    :return:
    """
    if not silent:
        logger.info('Downloading dataset %s' % dataset_id)
    file_format = 'nii' if file_format == 'nifti' else 'dcm'
    path = ENDPOINT + '/download/' + str(dataset_id)
    response = get(path, params={'format': file_format})
    download_file(output_folder, response, unzip)
    return


def download_datasets(dataset_ids, file_format, output_folder, unzip=False):
    """ Download datasets [dataset_ids] as [file_format] into [output_folder]
    :param dataset_ids:
    :param file_format:
    :param output_folder:
    :param unzip:
    :return:
    """
    if len(dataset_ids) > 50:
        logger.error('Cannot download more than 50 datasets at once. Please use the --search_text option instead to download '
              'the datasets one by one.')
        return
    logger.info('Downloading datasets %s' % dataset_ids)
    file_format = 'nii' if file_format == 'nifti' else 'dcm'
    dataset_ids = ','.join([str(dataset_id) for dataset_id in dataset_ids])
    path = ENDPOINT + '/massiveDownload'
    params = dict(datasetIds=dataset_ids, format=file_format)
    response = post(path, params=params, files=params, stream=True)
    download_file(output_folder, response, unzip=unzip)
    return


def download_dataset_processing(dataset_processing_ids, output_folder, result_only=False, unzip=False):
    """ Download datasets [dataset_ids] as [file_format] into [output_folder]
    :param dataset_processing_ids:
    :param output_folder:
    :param result_only:
    :param unzip:
    :return:
    """
    if len(dataset_processing_ids) > 50:
        logger.error('Cannot download more than 50 datasets at once. Please use the --search_text option instead to download '
              'the datasets one by one.')
        return
    logger.info('Downloading dataset processing %s' % dataset_processing_ids)
    path = ENDPOINT_PROCESSING + '/massiveDownloadByProcessingIds'
    params = dict(resultOnly=str(result_only).lower())
    response = post(path, params=params, json=dataset_processing_ids, stream=True)
    download_file(output_folder, response, unzip=unzip)
    return


def download_dataset_by_study(study_id, file_format, output_folder):
    """ Download datasets from study [study_id] as [file_format] into [output_folder]
    :param study_id:
    :param file_format:
    :param output_folder:
    :return:
    """
    logger.info('Downloading datasets from study %s' % study_id)
    file_format = 'nii' if file_format == 'nifti' else 'dcm'
    path = ENDPOINT + '/massiveDownloadByStudy'
    response = get(path, params={'studyId': study_id, 'format': file_format})
    download_file(output_folder, response)
    return


def find_dataset_ids_by_subject_id(subject_id):
    """ Get all datasets from subject [subject_id]
    :param subject_id:
    :return:
    """
    logger.info(f"Getting datasets from subject {subject_id}")
    path = ENDPOINT + '/subject/' + subject_id
    response = get(path)
    return response.json()


def find_datasets_by_examination_id(examination_id, output : bool = False):
    """ Get all datasets from subjet [subject_id]
    :param examination_id:
    :return:
    """
    logger.info(f"Getting datasets from examination {examination_id}")
    path = ENDPOINT + '/examination/' + examination_id

    try:
        response = get(path, params = {"output":output})
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Error for exam %s : %s" %(examination_id, str(e.response.json().get("message"))))
        return {}


def find_dataset_ids_by_subject_id_study_id(subject_id, study_id):
    """ Get all datasets from subject [subject_id] and study [study_id]
    :param subject_id:
    :param study_id:
    :return:
    """
    logger.info(f"Getting datasets from subject {subject_id} and study {study_id}")
    path = ENDPOINT + '/subject/' + subject_id + '/study/' + study_id
    response = get(path)
    return response.json()


def find_processed_dataset_ids_by_input_dataset_id(dataset_id):
    """ Get all processed datasets from dataset [dataset_id]
    :param dataset_id:
    :return:
    """
    logger.info(f"Getting processed datasets from dataset {dataset_id}")
    path = ENDPOINT_PROCESSING + '/inputDataset/' + dataset_id
    response = get(path)
    return response.json()


def get_dataset_dicom_metadata(dataset_id):
    """ Get all dicom metadata from specific dataset [dataset_id]
    :param dataset_id:
    :return:
    """
    path = ENDPOINT + '/dicom-metadata/' + str(dataset_id)
    response = get(path)
    return response.json()


def get_dicom_metadata_by_dataset_id(dataset_id):
    """ Get all dicom metadata from dataset [dataset_id]
    :param dataset_id:
    :return:
    """
    logger.info(f"Getting dicom metadata from dataset {dataset_id}")
    path = ENDPOINT + '/dicom-metadata/' + dataset_id
    response = get(path)
    return response.json()


def download_dataset_by_subject(subject_id, file_format, output_folder):
    """ Download all datasets from subject [subject_id] as [file_format] into [output_folder]
    :param subject_id:
    :param file_format:
    :param output_folder:
    :return:
    """
    dataset_ids = find_dataset_ids_by_subject_id(subject_id)
    download_datasets(dataset_ids, file_format, output_folder)
    return


def download_dataset_by_subject_id_study_id(subject_id, study_id, file_format, output_folder):
    """ Download all datasets from subject [subject_id] and study [study_id] as [file_format] into [output_folder]
    :param subject_id:
    :param study_id:
    :param file_format:
    :param output_folder:
    :return:
    """
    dataset_ids = find_dataset_ids_by_subject_id_study_id(subject_id, study_id)
    download_datasets(dataset_ids, file_format, output_folder)
    return
