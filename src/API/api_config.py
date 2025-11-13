import json

from src.utils.custom_config_parser import CustomConfigParser

class APIConfig(object):
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
        cls.scheme = config.get('API config', 'scheme')
        cls.domain = config.get('API config', 'domain')
        cls.verify = ("True" == config.get('API config', 'verify'))
        cls.timeout = config.get('API config', 'timeout')
        cls.proxies = json.loads(config.get('API config', 'proxies'))
        cls.username = config.get('API config', 'username')
        cls.clientId = config.get('API config', 'clientId')
        cls.access_token = config.get('API config', 'access_token')
        cls.refresh_token = config.get('API config', 'refresh_token')

    def __init__(self, config: CustomConfigParser):
        self.scheme = config.get('API config', 'scheme')
        self.domain = config.get('API config', 'domain')
        self.verify = ("True" == config.get('API config', 'verify'))
        self.timeout = config.get('API config', 'timeout')
        self.proxies = json.loads(config.get('API config', 'proxies'))
        self.username = config.get('API config', 'username')
        self.clientId = config.get('API config', 'clientId')
        self.access_token = config.get('API config', 'access_token')
        self.refresh_token = config.get('API config', 'refresh_token')
