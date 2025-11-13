from pathlib import Path

from src.API.api_config import APIConfig
from src.execution.execution_config import ExecutionConfig
from src.orthanc.orthanc_config import OrthancConfig
from src.utils.custom_config_parser import CustomConfigParser
from src.utils.log_utils import get_logger

logger = get_logger()

def load_config(**kwargs: str) -> None:
    config = CustomConfigParser()
    config.read(Path("../../config/config.conf"))

    APIConfig.init(config)

    for type in kwargs:
        match type:
            case "exec":
                logger.info("Loading execution config.")
                ExecutionConfig.init(config)
            case "orthanc":
                logger.info("Loading orthanc config.")
                OrthancConfig.init(config)
