from requests import Response
from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger

logger = get_logger()

def start_download(response: Response, output_name: str):
    if response.content.__len__() > 100:
        with open(ConfigPath.outputPath / (output_name + ".zip"), "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        logger.info("Download completed (" + output_name + ".zip is in py_noir/output directory) !")
    else:
        logger.info("No data to download for {} run !", output_name)
