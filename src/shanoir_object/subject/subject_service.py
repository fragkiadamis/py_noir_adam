from src import get

"""
Define methods for Shanoir studies MS subject API call
"""

ENDPOINT = '/studies/subjects'


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
    print('Getting subjects from study', study_id)
    path = ENDPOINT + '/' + study_id + '/allSubjects'
    response = get(path)
    return response.json()