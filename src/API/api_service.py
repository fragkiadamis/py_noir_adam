import logging
import os
import getpass
import zipfile
from pathlib import Path
import re
from tqdm import tqdm

import requests

from src.API.api_config import APIContext
from src.security.authentication_service import ask_access_token, refresh_access_token
from src.utils.log_utils import get_logger

logger = get_logger()

"""
Define methods for generic API call
"""


def rest_request(method: str, path, **kwargs):
    """ Execute a [method] HTTP query to [path] endpoint
    :param method:
    :param path:
    :param kwargs:
    :return:
    """
    url = APIContext.scheme + "://" + APIContext.domain + "/shanoir-ng" + path

    response = None
    if method == 'get':
        response = requests.get(url, proxies=APIContext.proxies, verify=APIContext.verify, timeout=APIContext.timeout,
                                **kwargs)
    elif method == 'post':
        response = requests.post(url, proxies=APIContext.proxies, verify=APIContext.verify, timeout=APIContext.timeout,
                                 **kwargs)
    elif method == 'put':
        response = requests.put(url, proxies=APIContext.proxies, verify=APIContext.verify, timeout=APIContext.timeout,
                                **kwargs)
    else:
        logger.error('Error: unimplemented request type')

    return response


# perform a request on the given path, asks for a new access token if the current one is outdated
def request(method, path, raise_for_status=True, content_type=None, **kwargs):
    """ Authenticate / Re-authenticate user [APIContext.username] and execute a [method] HTTP query to [path] endpoint
    :param method:
    :param path:
    :param raise_for_status:
    :param content_type:
    :param kwargs:
    :return:
    """
    if APIContext.access_token is None:
        ask_access_token()

    headers = get_http_headers(content_type)
    response = rest_request(method, path, headers=headers, **kwargs)

    # if the token is outdated, refresh it and try again
    if response.status_code == 401:
        refresh_access_token()
        headers = get_http_headers(content_type)
        response = rest_request(method, path, headers=headers, **kwargs)

    if raise_for_status:
        response.raise_for_status()

    return response


def get_http_headers(content_type=None):
    """ Set HTTP headers with [APIContext.access_token]
    :return:
    """
    headers = {
        'Authorization': 'Bearer ' + APIContext.access_token,
        'content-type': 'application/json' if content_type is None else content_type,
        'charset': 'utf-8'
    }
    return headers


# perform a GET request on the given url, asks for a new access token if the current one is outdated
def get(path: str, params=None, stream=None):
    """ Perform a GET HTTP request on [path] endpoint with given [params]
    :param path: string
    :param params:
    :param stream:
    :return:
    """
    return request('get', path, params=params, stream=stream)


def post(path: str, params=None, files=None, stream=None, json=None, data=None,
         raise_for_status=True, content_type=None):
    """ Perform a POST HTTP request on [path] endpoint with given [params]/[files]/[stream] /[data]
    :param path:
    :param params:
    :param files:
    :param stream:
    :param json:
    :param data:
    :param raise_for_status:
    :param content_type:
    :return:
    """
    return request('post', path, raise_for_status, params=params, files=files, stream=stream, json=json,
                   data=data, content_type=content_type)


def put(path: str, params=None, files=None, stream=None, json=None, data=None,
        raise_for_status=True):
    """ Perform a PUT HTTP request on [path] endpoint with given [params]/[files]/[stream] /[data]
    :param path:
    :param params:
    :param files:
    :param stream:
    :param json:
    :param data:
    :param raise_for_status:
    :return:
    """
    return request('put', path, raise_for_status, params=params, files=files, stream=stream, json=json,
                   data=data)


def download_file(output_folder, response, unzip):
    filename = get_filename_from_response(output_folder, response)
    if not filename:
        return
    total = int(response.headers.get('content-length', 0))
    with open(filename, 'wb') as file, tqdm(
            desc=filename,
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
    if unzip:
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(output_folder)
        os.remove(filename)


def download_files(output_folder, response):
    filename = get_filename_from_response(output_folder, response)
    if not filename:
        return
    open(filename, 'wb').write(response.content)
    return


def get_filename_from_response(output_folder, response):
    """ Build file path with [output_folder] and [response] 'Content-Disposition' header
    :param output_folder:
    :param response:
    :return:
    """
    filename = None
    if response.headers and 'Content-Disposition' in response.headers:
        filenames = re.findall('filename=(.+)', response.headers['Content-Disposition'])
        filename = str(output_folder + '/' + filenames[0]) if len(filenames) > 0 else None
    if filename is None:
        raise Exception('Could not find file name in response header', response.status_code, response.reason,
                        response.error, response.headers, response)
    return filename


def log_response(e):
    logging.error('Response status code: {e.response.status_code}')
    logging.error('		 reason: {e.response.reason}')
    logging.error('		 text: {e.response.text}')
    logging.error('		 headers: {e.response.headers}')
    logging.error(str(e))
    return


def initialize(args):
    APIContext.domain = args.domain
    APIContext.username = args.username

    verify = args.certificate if hasattr(args, 'certificate') and args.certificate != '' else True

    proxy_url = None  # 'user:pass@host:port'

    if hasattr(args, 'proxy_url') and args.proxy_url is not None:
        proxy_a = args.proxy_url.split('@')
        proxy_user = proxy_a[0]
        proxy_host = proxy_a[1]
        proxy_password = getpass.getpass(
            prompt='Proxy password for user ' + proxy_user + ' and host ' + proxy_host + ': ', stream=None)
        proxy_url = proxy_user + ':' + proxy_password + '@' + proxy_host

    else:

        configuration_folder = None

        if hasattr(args, 'configuration_folder') and args.configuration_folder:
            configuration_folder = Path(args.configuration_folder)
        else:
            cfs = sorted(list(Path.home().glob('.su_v*')))
            configuration_folder = cfs[-1] if len(cfs) > 0 else Path().home()

        proxy_settings = configuration_folder / 'proxy.properties'

        proxy_config = {}

        if proxy_settings.exists():
            with open(proxy_settings) as file:
                for line in file:
                    if line.startswith('proxy.'):
                        line_s = line.split('=')
                        proxy_key = line_s[0]
                        proxy_value = line_s[1].strip()
                        proxy_key = proxy_key.split('.')[-1]
                        proxy_config[proxy_key] = proxy_value

                if 'enabled' not in proxy_config or proxy_config['enabled'] == 'true':
                    if 'user' in proxy_config and len(proxy_config['user']) > 0 and 'password' in proxy_config and len(
                            proxy_config['password']) > 0:
                        proxy_url = proxy_config['user'] + ':' + proxy_config['password']
                    proxy_url += '@' + proxy_config['host'] + ':' + proxy_config['port']
        else:
            logger.info("Proxy configuration file not found. Proxy will be ignored.")

    proxies = None

    if proxy_url:
        proxies = {
            'http': 'http://' + proxy_url,
            # 'https': 'https://' + proxy_url,
        }

    APIContext.proxies = proxies
    APIContext.verify = verify
    APIContext.timeout = args.timeout
    APIContext.output_folder = args.output_folder

def reset_token():
    if APIContext.access_token is None:
        refresh_access_token()