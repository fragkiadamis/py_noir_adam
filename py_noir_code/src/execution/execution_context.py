import configparser

from py_noir_code.src.utils.custom_config_parser import CustomConfigParser


class ExecutionContext(object):
    """
    Configuration class for project execution
    """
    max_thread: int = None
    server_reboot_beginning_hour: int = None
    server_reboot_ending_hour: int = None

    @classmethod
    def init(cls, config: CustomConfigParser):
        cls.max_thread = int(config.get('Execution context', 'max_thread'))
        cls.server_reboot_beginning_hour = int(config.get('Execution context', 'server_reboot_beginning_hour'))
        cls.server_reboot_ending_hour = int(config.get('Execution context', 'server_reboot_ending_hour'))

    def __init__(self, config: CustomConfigParser):
        self.max_thread = int(config.get('Execution context', 'max_thread'))
        self.server_reboot_beginning_hour = int(config.get('Execution context', 'server_reboot_beginning_hour'))
        self.server_reboot_ending_hour = int(config.get('Execution context', 'server_reboot_ending_hour'))

