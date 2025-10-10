from py_noir_code.src.utils.custom_config_parser import CustomConfigParser

class OrthancContext(object):
    """
    Configuration class for project orthanc
    """
    scheme: str = None
    domain: str = None
    port: str = None
    username: str = None

    @classmethod
    def init(cls, config: CustomConfigParser):
        cls.scheme = config.get('Orthanc context', 'scheme')
        cls.domain = config.get('Orthanc context', 'domain')
        cls.port = config.get('Orthanc context', 'port')
        cls.username = config.get('Orthanc context', 'username')
        cls.password = None

    def __init__(self, config: CustomConfigParser):
        self.scheme = config.get('Orthanc context', 'scheme')
        self.domain = config.get('Orthanc context', 'domain')
        self.port = config.get('Orthanc context', 'port')
        self.username = config.get('Orthanc context', 'username')
        self.password = None
