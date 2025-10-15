from py_noir_code.src.API.api_service import get, download_file, post
from py_noir_code.src.utils.log_utils import get_logger

ENDPOINT = '/datasets/datasetProcessing'

logger = get_logger()


def get_dataset_processing(dataset_processing_id: str):
    """ Get dataset processing [dataset_processing_id]
    :param dataset_processing_id:
    :return: json
    """

    path = ENDPOINT + '/' + dataset_processing_id
    response = get(path)
    return response.json()


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
    path = ENDPOINT + '/massiveDownloadByProcessingIds'
    params = dict(resultOnly=str(result_only).lower())
    response = post(path, params=params, json=dataset_processing_ids, stream=True)
    download_file(output_folder, response, unzip=unzip)
    return


def find_processed_dataset_ids_by_input_dataset_id(dataset_id):
    """ Get all processed datasets from dataset [dataset_id]
    :param dataset_id:
    :return:
    """
    path = ENDPOINT + '/inputDataset/' + dataset_id
    response = get(path)
    return response.json()