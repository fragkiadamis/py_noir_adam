from py_noir.src.API.api_service import get
from py_noir.src.API.shanoir_context import ShanoirContext

"""
Define methods for Shanoir studies MS subject API call
"""

ENDPOINT = '/studies/subjects'


def find_subject_ids_by_study_id(context: ShanoirContext, study_id):
    """ Get all subjects from study [study_id]
    :param context:
    :param study_id:
    :return:
    """
    print('Getting subjects from study', study_id)
    path = ENDPOINT + '/' + study_id + '/allSubjects'
    response = get(context, path)
    return response.json()