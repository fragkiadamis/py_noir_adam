from py_noir_code.src.API.api_service import get

"""
Define methods for Shanoir studies MS subject API call
"""

ENDPOINT = '/studies/subjects'


def find_subject_ids_by_study_id(study_id):
    """ Get all subjects from study [study_id]
    :param study_id:
    :return:
    """
    print('Getting subjects from study', study_id)
    path = ENDPOINT + '/' + study_id + '/allSubjects'
    response = get(path)
    return response.json()