import string

from py_noir.src.API.api_service import get, download_file, post
from py_noir.src.API.shanoir_context import ShanoirContext

"""
Define methods for Shanoir datasets MS datasets API call
"""

ENDPOINT = '/datasets/datasets'


def get_dataset(context: ShanoirContext, dataset_id: string):
    """ Get dataset [dataset_id]
    :param dataset_id:
    :param context:
    :return: json
    """

    path = ENDPOINT + '/' + dataset_id
    response = get(context, path)
    return response.json()


def download_dataset(context: ShanoirContext, dataset_id, file_format, output_folder, unzip=False, silent=False):
    """ Download dataset [dataset_id] as [file_format] into [output_folder]
    :param context:
    :param dataset_id:
    :param file_format:
    :param output_folder:
    :param silent:
    :param unzip:
    :return:
    """
    if not silent:
        print('Downloading dataset', dataset_id)
    file_format = 'nii' if file_format == 'nifti' else 'dcm'
    path = ENDPOINT + '/download/' + str(dataset_id)
    response = get(context, path, params={'format': file_format})
    download_file(output_folder, response, unzip)
    return


def download_datasets(context: ShanoirContext, dataset_ids, file_format, output_folder):
    """ Download datasets [dataset_ids] as [file_format] into [output_folder]
    :param context:
    :param dataset_ids:
    :param file_format:
    :param output_folder:
    :return:
    """
    if len(dataset_ids) > 50:
        print('Cannot download more than 50 datasets at once. Please use the --search_text option instead to download '
              'the datasets one by one.')
        return
    print('Downloading datasets', dataset_ids)
    file_format = 'nii' if file_format == 'nifti' else 'dcm'
    dataset_ids = ','.join([str(dataset_id) for dataset_id in dataset_ids])
    path = ENDPOINT + '/massiveDownload'
    params = dict(datasetIds=dataset_ids, format=file_format)
    response = post(context, path, params=params, files=params, stream=True)
    download_file(output_folder, response)
    return


def download_dataset_by_study(context: ShanoirContext, study_id, file_format, output_folder):
    """ Download datasets from study [study_id] as [file_format] into [output_folder]
    :param context:
    :param study_id:
    :param file_format:
    :param output_folder:
    :return:
    """
    print('Downloading datasets from study', study_id)
    file_format = 'nii' if file_format == 'nifti' else 'dcm'
    path = ENDPOINT + '/massiveDownloadByStudy'
    response = get(context, path, params={'studyId': study_id, 'format': file_format})
    download_file(output_folder, response)
    return


def find_dataset_ids_by_subject_id(context: ShanoirContext, subject_id):
    """ Get all datasets from subject [subject_id]
    :param context:
    :param subject_id:
    :return:
    """
    print('Getting datasets from subject', subject_id)
    path = ENDPOINT + '/subject/' + subject_id
    response = get(context, path)
    return response.json()


def find_datasets_by_examination_id(context: ShanoirContext, examination_id):
    """ Get all datasets from subject [subject_id]
    :param context:
    :param examination_id:
    :return:
    """
    print('Getting acquisitions from examination', examination_id)
    path = ENDPOINT + '/examination/' + examination_id
    response = ""
    try:
        response = get(context, path)
        return response.json()
    except:
        print("Error for exam " + examination_id + ": " + str(response))
        return {}


def find_dataset_ids_by_subject_id_study_id(context: ShanoirContext, subject_id, study_id):
    """ Get all datasets from subject [subject_id] and study [study_id]
    :param context:
    :param subject_id:
    :param study_id:
    :return:
    """
    print('Getting datasets from subject', subject_id, 'and study', study_id)
    path = ENDPOINT + '/subject/' + subject_id + '/study/' + study_id
    response = get(context, path)
    return response.json()

def get_dataset_dicom_metadata(context: ShanoirContext, dataset_id):
    """ Get all dicom metadata from specific dataset [dataset_id]
    :param context:
    :param dataset_id:
    :return:
    """
    path = ENDPOINT + '/dicom-metadata/' + str(dataset_id)
    response = get(context, path)
    return response.json()


def get_dicom_metadata_by_dataset_id(context: ShanoirContext, dataset_id):
    """ Get all dicom metadata from dataset [dataset_id]
    :param context:
    :param dataset_id:
    :return:
    """
    print('Getting dicom metadata from dataset', dataset_id)
    path = ENDPOINT + '/dicom-metadata/' + dataset_id
    response = get(context, path)
    return response.json()


def download_dataset_by_subject(context: ShanoirContext, subject_id, file_format, output_folder):
    """ Download all datasets from subject [subject_id] as [file_format] into [output_folder]
    :param context:
    :param subject_id:
    :param file_format:
    :param output_folder:
    :return:
    """
    dataset_ids = find_dataset_ids_by_subject_id(context, subject_id)
    download_datasets(context, dataset_ids, file_format, output_folder)
    return

def download_dataset_by_subject_id_study_id(context: ShanoirContext, subject_id, study_id, file_format, output_folder):
    """ Download all datasets from subject [subject_id] and study [study_id] as [file_format] into [output_folder]
    :param context:
    :param subject_id:
    :param study_id:
    :param file_format:
    :param output_folder:
    :return:
    """
    dataset_ids = find_dataset_ids_by_subject_id_study_id(context, subject_id, study_id)
    download_datasets(context, dataset_ids, file_format, output_folder)
    return
