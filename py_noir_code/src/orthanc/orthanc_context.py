from py_noir_code.src.utils.custom_config_parser import CustomConfigParser

class OrthancContext(object):
    """
    Configuration class for project orthanc
    """
    pacs_ae_title: str = None
    client_ae_title: str = None
    scheme: str = None
    domain: str = None
    rest_api_port: str = None
    dicom_server_port: str = None
    dicom_client_port: str = None
    username: str = None

    @classmethod
    def init(cls, config: CustomConfigParser):
        cls.pacs_ae_title = config.get('Orthanc context', 'pacs_ae_title')
        cls.client_ae_title = config.get('Orthanc context', 'client_ae_title')
        cls.scheme = config.get('Orthanc context', 'scheme')
        cls.domain = config.get('Orthanc context', 'domain')
        cls.rest_api_port = config.get('Orthanc context', 'rest_api_port')
        cls.dicom_server_port = config.get('Orthanc context', 'dicom_server_port')
        cls.dicom_client_port = config.get('Orthanc context', 'dicom_client_port')
        cls.username = config.get('Orthanc context', 'username')
        cls.password = None

    def __init__(self, config: CustomConfigParser):
        self.pacs_ae_title = config.get('Orthanc context', 'pacs_ae_title')
        self.client_ae_title = config.get('Orthanc context', 'client_ae_title')
        self.scheme = config.get('Orthanc context', 'scheme')
        self.domain = config.get('Orthanc context', 'domain')
        self.rest_api_port = config.get('Orthanc context', 'rest_api_port')
        self.dicom_server_port = config.get('Orthanc context', 'dicom_server_port')
        self.dicom_client_port = config.get('Orthanc context', 'dicom_client_port')
        self.username = config.get('Orthanc context', 'username')
        self.password = None
