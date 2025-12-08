from requests import Response
from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger

logger = get_logger()


def start_download(response: Response, parent_dir: str, output_name: str) -> None:
    if response.content.__len__() > 100:
        output_dir = (ConfigPath.output_path / parent_dir).mkdir(parents=True, exist_ok=True)
        with open(output_dir / (output_name + ".zip"), "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        logger.info(f"Download completed ({output_name}.zip is in {output_dir} directory) !")
    else:
        logger.info(f"No data to download for {output_name} run !")
