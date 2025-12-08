from src.API.api_service import get
from src.utils.log_utils import get_logger

"""
Define methods for Shanoir studies MS subject API call
"""

ENDPOINT = '/studies/subjects'

logger = get_logger()

def get_subject_by_id(subject_id):
    """ Get a subject from its id [subject_id]
    :param subject_id:
    :return:
    """
    path = ENDPOINT + '/' + subject_id
    response = get(path)
    return response.json()



def find_subject_ids_by_study_id(study_id):
    """ Get all subjects from study [study_id]
    :param study_id:
    :return:
    """
    logger.info('Getting subjects from study', study_id)
    path = ENDPOINT + '/' + study_id + '/allSubjects'
    response = get(path)
    return response.json()