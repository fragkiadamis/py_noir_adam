import configparser
import json

from pathlib import Path
from src.utils.log_utils import get_logger

logger = get_logger()

def load_config(**kwargs: str) -> None:
    config = CustomConfigParser()
    config.read(Path("config/config.conf"))

    ConfigPath.init(config)
    APIConfig.init(config)
    ExecutionConfig.init(config)
    OrthancConfig.init(config)

class CustomConfigParser(configparser.ConfigParser):
    def get(self, section, option, *, raw=False, vars=None, fallback=None):
        value = super().get(section, option, raw=raw, vars=vars, fallback=fallback)
        if value == "None":
            return None
        return value

class ConfigPath(object):
    """
    Configuration class for project configuration
    """
    rootPath: Path = None

    @classmethod
    def init(cls, config: CustomConfigParser):
        cls.rootPath = Path(config.get('Config', 'rootPath'))
        cls.inputPath = cls.rootPath / "input"
        cls.outputPath = cls.rootPath / "output"
        cls.trackingFilePath = cls.outputPath / "tracking_file"
        cls.resourcePath = cls.rootPath / "resource"
        cls.wipFilePath = cls.resourcePath / "WIP_file"
        cls.saveFilePath = cls.resourcePath / "save_file"

    def __init__(self, config: CustomConfigParser):
        self.rootPath = Path(config.get('Config', 'rootPath'))
        self.inputPath = self.rootPath / "input"
        self.outputPath = self.rootPath / "output"
        self.trackingFilePath = self.outputPath / "tracking_file"
        self.resourcePath = self.rootPath / "resource"
        self.wipFilePath = self.resourcePath / "WIP_file"
        self.saveFilePath = self.resourcePath / "save_file"

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

class ExecutionConfig(object):
    """
    Configuration class for project execution
    """
    max_thread: int = None
    server_reboot_beginning_hour: int = None
    server_reboot_ending_hour: int = None

    @classmethod
    def init(cls, config: CustomConfigParser):
        cls.max_thread = int(config.get('Execution config', 'max_thread'))
        cls.server_reboot_beginning_hour = int(config.get('Execution config', 'server_reboot_beginning_hour'))
        cls.server_reboot_ending_hour = int(config.get('Execution config', 'server_reboot_ending_hour'))

    def __init__(self, config: CustomConfigParser):
        self.max_thread = int(config.get('Execution config', 'max_thread'))
        self.server_reboot_beginning_hour = int(config.get('Execution config', 'server_reboot_beginning_hour'))
        self.server_reboot_ending_hour = int(config.get('Execution config', 'server_reboot_ending_hour'))

class OrthancConfig(object):
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


