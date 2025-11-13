import os

import requests
import json
import getpass
import sys

from src.API.api_config import APIContext
from src.orthanc.orthanc_config import OrthancConfig
from src.utils.log_utils import get_logger

"""
Define methods for Shanoir authentication and Orthanc password
"""

logger = get_logger()
ENDPOINT = '/auth/realms/shanoir-ng/protocol/openid-connect/token'


def ask_access_token():
    """ Prompt user [APIContext.username] for password
    and set [APIContext.access_token] & [APIContext.refresh_token] from Shanoir auth API
    :return:
    """
    try:
        password = os.environ['shanoir_password'] if 'shanoir_password' in os.environ else getpass.getpass(
            prompt='Password for Shanoir user ' + APIContext.username + ': ', stream=None)
    except Exception as e:
        sys.exit(0)

    url = APIContext.scheme + '://' + APIContext.domain + ENDPOINT

    payload = {
        'client_id': APIContext.clientId,
        'grant_type': 'password',
        'username': APIContext.username,
        'password': password,
        'scope': 'offline_access'
    }
    # curl -d '{"client_id":"shanoir-uploader", "grant_type":"password", "username": "amasson", "password": "", "scope": "offline_access" }' -H "Content-Type: application/json" -X POST

    headers = {'content-type': 'application/x-www-form-urlencoded'}
    logger.info('get keycloak token...')
    response = requests.post(url, data=payload, headers=headers, proxies=APIContext.proxies, verify=APIContext.verify,
                             timeout=APIContext.timeout)

    response_json = json.loads(response.text)
    if not hasattr(response, 'status_code') or response.status_code != 200:
        if 'error_description' in response_json and response_json.get("error_description") == "Invalid user credentials" :
            logger.error(response_json.get("error_description"))
        else :
            logger.error('Failed to connect, make sure you have a certified IP or are connected on a valid VPN.')
        sys.exit(1)

    APIContext.refresh_token = response_json['refresh_token']
    APIContext.access_token = response_json['access_token']


# get a new access token using the refresh token
def refresh_access_token():
    """ Set [APIContext.access_token] from Shanoir auth API using [APIContext.refresh_token]
    """
    url = APIContext.scheme + '://' + APIContext.domain + ENDPOINT
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': APIContext.refresh_token,
        'client_id': APIContext.clientId
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    logger.info('Refreshing keycloak token...')
    response = requests.post(url, data=payload, headers=headers, proxies=APIContext.proxies, verify=APIContext.verify,
                             timeout=APIContext.timeout)
    if response.text.find("No refresh token") != -1 :
        ask_access_token()
    else :
        APIContext.access_token = response.json()['access_token']
        APIContext.refresh_token = response.json()['refresh_token']
    if response.status_code != 200 and APIContext.access_token == "":
        logger.error('Response status :' + str(response.status_code) + "," + response.text)
        exit(1)


def load_orthanc_password():
    """ Prompt the user [OrthancContext.username] for password
    and set [OrthancContext.password]
    :return:
    """
    try:
        password = os.environ['orthanc_password'] if 'orthanc_password' in os.environ else getpass.getpass(
            prompt='Password for Orthanc user ' + OrthancConfig.username + ': ', stream=None)
    except Exception as e:
        sys.exit(0)

    OrthancConfig.password = password
