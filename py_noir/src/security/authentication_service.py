import os

import requests
import json
import getpass
import sys
import logging

from py_noir.src.API.shanoir_context import ShanoirContext

"""
Define methods for Shanoir authentication
"""

ENDPOINT = '/auth/realms/shanoir-ng/protocol/openid-connect/token'

def ask_access_token(context: ShanoirContext) -> ShanoirContext:
    """ Prompt user [context.username] for password
    and set [context.access_token] & [context.refresh_token] from Shanoir auth API
    :param context:
    :return:
    """
    try:
        password = os.environ['shanoir_password'] if 'shanoir_password' in os.environ else getpass.getpass(
            prompt='Password for Shanoir user ' + context.username + ': ', stream=None)
    except:
        sys.exit(0)

    url = context.scheme + '://' + context.domain + ENDPOINT

    payload = {
        'client_id': context.clientId,
        'grant_type': 'password',
        'username': context.username,
        'password': password,
        'scope': 'offline_access'
    }
    # curl -d '{"client_id":"shanoir-uploader", "grant_type":"password", "username": "amasson", "password": "", "scope": "offline_access" }' -H "Content-Type: application/json" -X POST

    headers = {'content-type': 'application/x-www-form-urlencoded'}
    print('get keycloak token...')
    response = requests.post(url, data=payload, headers=headers, proxies=context.proxies, verify=context.verify,
                             timeout=context.timeout)
    if not hasattr(response, 'status_code') or response.status_code != 200:
        print('Failed to connect, make sur you have a certified IP or are connected on a valid VPN.')
        raise ConnectionError(response.status_code)

    response_json = json.loads(response.text)
    if 'error_description' in response_json and response_json['error_description'] == 'Invalid user credentials':
        print('bad username or password')
        sys.exit(1)

    context.refresh_token = response_json['refresh_token']
    context.access_token = response_json['access_token']

    return context


# get a new access token using the refresh token
def refresh_access_token(context: ShanoirContext) -> ShanoirContext:
    """ Set [context.access_token] from Shanoir auth API using [context.refresh_token]
    :param context:
    :return:
    """
    url = context.scheme + '://' + context.domain + ENDPOINT
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': context.refresh_token,
        'client_id': context.clientId
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    logging.info('refresh keycloak token...')
    response = requests.post(url, data=payload, headers=headers, proxies=context.proxies, verify=context.verify,
                             timeout=context.timeout)
    if response.status_code != 200:
        logging.error('response status : {response.status_code}, {responses[response.status_code]}')
    response_json = response.json()
    context.access_token = response_json['access_token']
    return context
