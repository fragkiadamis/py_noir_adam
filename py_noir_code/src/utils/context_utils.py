from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.execution.execution_context import ExecutionContext
from py_noir_code.src.utils.custom_config_parser import CustomConfigParser
from py_noir_code.src.utils.file_utils import get_project_path


def load_context(config_file_name: str, withExec: bool = True):
    config = CustomConfigParser()
    config.read(get_project_path() + "/" + config_file_name)
    APIContext.init(config)
    if withExec :
        ExecutionContext.init(config)
