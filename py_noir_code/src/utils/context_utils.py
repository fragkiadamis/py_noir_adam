from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.execution.execution_context import ExecutionContext
from py_noir_code.src.orthanc.orthanc_context import OrthancContext
from py_noir_code.src.utils.custom_config_parser import CustomConfigParser
from py_noir_code.src.utils.file_utils import get_project_path


def load_context(config_file_name: str, with_exec: bool = True, with_orthanc: bool = False) -> None:
    config = CustomConfigParser()
    config.read(get_project_path() + "/" + config_file_name)
    APIContext.init(config)
    if with_exec:
        ExecutionContext.init(config)
    if with_orthanc:
        OrthancContext.init(config)
