import typer

from src.API.api_service import get
from src.utils.download_utils import start_download
from src.utils.file_utils import get_items_from_input_file
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain():
    """
    Tracking file download project command-line interface.
    Commands:
    --------
    * `execute` — download the tracking files relative to the pipeline names written in `input/inputs.txt`
    Usage:
    -----
        uv run main.py tracking_file_download execute
    """

@app.command()
def execute()-> None:
    """
    Download the tracking files relative to the pipeline names written in `input/inputs.txt`
    """
    pipeline_names = get_items_from_input_file("inputs.txt")

    for pipeline_name in pipeline_names:
        response = get("/datasets/execution-monitoring/tracking-file", params = {"pipelineName":pipeline_name})
        if response.status_code == 200 :
            start_download(response, pipeline_name)
        else :
            logger.error("An error has occured while trying to get {} tracking file.", pipeline_name)

