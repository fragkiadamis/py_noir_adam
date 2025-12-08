import typer

from src.API.api_service import post
from src.utils.download_utils import start_download
from src.utils.file_utils import get_items_from_input_file
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()


@app.callback()
def explain() -> None:
    """
    Dicom metadata download project command-line interface.
    Commands:
    --------
    * `execute` — download a csv gathering dicom metadata according to the dataset ids written in `input/inputs.txt` and the metadata keys written in 'input/inputs_bis.txt'
    Usage:
    -----
        uv run main.py dicom_metadata_download execute
    """


@app.command()
def execute() -> None:
    """
    Run the dicom metadata download relatively to the comment value of the executions
    """
    datasets_ids = get_items_from_input_file("inputs.txt")
    metadata_keys = get_items_from_input_file("inputs_bis.txt")

    response = post("/datasets/datasets/dicomMetadataExtraction", data = {"datasetIds":datasets_ids, "metadataKeys":metadata_keys})
    if response.status_code == 200:
        start_download(response, "metadata", "dicom_metadata")
    else:
        logger.error("An error has occured while trying to download the metadata csv.")
