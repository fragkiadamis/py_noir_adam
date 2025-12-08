import os
import requests
import json
import getpass
import sys

from src.utils.config_utils import OrthancConfig, APIConfig
from src.utils.log_utils import get_logger

"""
Define methods for Shanoir authentication and Orthanc password
"""

logger = get_logger()
ENDPOINT = '/auth/realms/shanoir-ng/protocol/openid-connect/token'

def ask_access_token():
    """ Prompt user [APIConfig.username] for password
    and set [APIConfig.access_token] & [APIConfig.refresh_token] from Shanoir auth API
    :return:
    """
    try:
        password = os.environ['shanoir_password'] if 'shanoir_password' in os.environ else getpass.getpass(
            prompt='Password for Shanoir user ' + APIConfig.username + ': ', stream=None)
    except Exception as e:
        sys.exit(1)

    url = APIConfig.scheme + '://' + APIConfig.domain + ENDPOINT

    payload = {
        'client_id': APIConfig.clientId,
        'grant_type': 'password',
        'username': APIConfig.username,
        'password': password,
        'scope': 'offline_access'
    }
    # curl -d '{"client_id":"shanoir-uploader", "grant_type":"password", "username": "amasson", "password": "", "scope": "offline_access" }' -H "Content-Type: application/json" -X POST

    headers = {'content-type': 'application/x-www-form-urlencoded'}
    logger.info('get keycloak token...')
    response = requests.post(url, data=payload, headers=headers, proxies=APIConfig.proxies, verify=APIConfig.verify,
                             timeout=APIConfig.timeout)

    response_json = json.loads(response.text)
    if not hasattr(response, 'status_code') or response.status_code != 200:
        if 'error_description' in response_json and response_json.get("error_description") == "Invalid user credentials":
            logger.error(response_json.get("error_description"))
        else:
            logger.error('Failed to connect, make sure you have a certified IP or are connected on a valid VPN.')
        sys.exit(1)

    APIConfig.refresh_token = response_json['refresh_token']
    APIConfig.access_token = response_json['access_token']


# get a new access token using the refresh token
def refresh_access_token():
    """ Set [APIConfig.access_token] from Shanoir auth API using [APIConfig.refresh_token]
    """
    url = APIConfig.scheme + '://' + APIConfig.domain + ENDPOINT
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': APIConfig.refresh_token,
        'client_id': APIConfig.clientId
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    logger.info('Refreshing keycloak token...')
    response = requests.post(url, data=payload, headers=headers, proxies=APIConfig.proxies, verify=APIConfig.verify,
                             timeout=APIConfig.timeout)
    if response.text.find("No refresh token") != -1:
        ask_access_token()
    else:
        APIConfig.access_token = response.json()['access_token']
        APIConfig.refresh_token = response.json()['refresh_token']
    if response.status_code != 200 and APIConfig.access_token == "":
        logger.error('Response status:' + str(response.status_code) + "," + response.text)
        exit(1)


def load_orthanc_password():
    """ Prompt the user [OrthancConfig.username] for password
    and set [OrthancConfig.password]
    :return:
    """
    try:
        password = os.environ['orthanc_password'] if 'orthanc_password' in os.environ else getpass.getpass(
            prompt='Password for Orthanc user ' + OrthancConfig.username + ': ', stream=None)
    except Exception as e:
        sys.exit(0)

    OrthancConfig.password = password
