import json

from py_noir_code.src.utils.custom_config_parser import CustomConfigParser


class APIContext(object):
    """
    Configuration class for connection and authentication to a Shanoir instance
    """
    scheme: str = None
    domain: str = None
    verify: bool = None
    timeout: float = None
    proxies: dict = None
    username: str = None
    clientId: str = None
    access_token: str = None
    refresh_token: str = None

    @classmethod
    def init(cls, config: CustomConfigParser):
        cls.scheme = config.get('API context', 'scheme')
        cls.domain = config.get('API context', 'domain')
        cls.verify = ("True" == config.get('API context', 'verify'))
        cls.timeout = config.get('API context', 'timeout')
        cls.proxies = json.loads(config.get('API context', 'proxies'))
        cls.username = config.get('API context', 'username')
        cls.clientId = config.get('API context', 'clientId')
        cls.access_token = config.get('API context', 'access_token')
        cls.refresh_token = config.get('API context', 'refresh_token')

    def __init__(self, config: CustomConfigParser):
        self.scheme = config.get('API context', 'scheme')
        self.domain = config.get('API context', 'domain')
        self.verify = ("True" == config.get('API context', 'verify'))
        self.timeout = config.get('API context', 'timeout')
        self.proxies = json.loads(config.get('API context', 'proxies'))
        self.username = config.get('API context', 'username')
        self.clientId = config.get('API context', 'clientId')
        self.access_token = config.get('API context', 'access_token')
        self.refresh_token = config.get('API context', 'refresh_token')
