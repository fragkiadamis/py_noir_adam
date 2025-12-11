from requests import Response
from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger

logger = get_logger()


def start_download(response: Response, output_name: str) -> None:
    if response.content.__len__() > 10:
        ConfigPath.output_path.mkdir(parents=True, exist_ok=True)
        with open(ConfigPath.output_path / (output_name + ".zip"), "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        logger.info(f"Download completed ({output_name}.zip is in {str(ConfigPath.output_path)} directory) !")
    else:
        logger.info(f"No data to download for {output_name} run !")
