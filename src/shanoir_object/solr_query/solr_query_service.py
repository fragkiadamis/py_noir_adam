import json

from src import post
from src.shanoir_object.solr_query.solr_query_model import SolrQuery

"""
Define methods for Shanoir datasets MS Solr query API call
"""

ENDPOINT = '/datasets/solr'

def solr_search(query: SolrQuery):
    """ Execute a Solr search query
    :param query:
    :return:
    """
    # facet = {
    #   "centerName": {},
    #   "datasetEndDate": {
    #     "month": 0,
    #     "year": 0
    #   },
    #   "datasetName": {},
    #   "datasetNature": {},
    #   "datasetStartDate": {
    #     "month": 0,
    #     "year": 0
    #   },
    #   "datasetType": {},
    #   "examinationComment": {},
    #   "expertMode": True,
    #   "magneticFieldStrength": {
    #     "lowerBound": 0,
    #     "upperBound": 0
    #   },
    #   "pixelBandwidth": {
    #     "lowerBound": 0,
    #     "upperBound": 0
    #   },
    #   "searchText": "string",
    #   "sliceThickness": {
    #     "lowerBound": 0,
    #     "upperBound": 0
    #   },
    #   "studyId": {},
    #   "studyName": {},
    #   "subjectName": {}
    # }

    path = ENDPOINT
    data = {
        # 'subjectName': ['01001'],
        'expertMode': query.expert_mode,
        'searchText': query.search_text,
        'facetPaging': {}
    }

    params = dict(page=query.page, size=query.size, sort=query.sort)
    response = post(path, params=params, data=json.dumps(data))

    return response
