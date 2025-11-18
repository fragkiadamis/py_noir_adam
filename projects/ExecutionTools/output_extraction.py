import typer

from src.API.api_service import post
from src.utils.config_utils import ConfigPath
from src.utils.download_utils import start_download
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain():
    """
    Output extraction project command-line interface.
    Status:
    ------
    Shanoir output extraction feature is not correctly developed yet, so that execution tool is not available yet.

    Commands:
    --------
    * `execute` — runs the output extraction for processing outputs filtered in `input/inputs.json`:
        - Download the outputs according to the filters written input/input.json.
        - More explanations about the filters are available in input/output_extraction_filter_example.json

    Usage:
    -----
        uv run main.py output_extraction execute
    """

@app.command()
def execute() -> None:
    """
    Run the output extraction
    """
    with open(ConfigPath.inputPath / "input.json", "r") as file:
        response = post("/datasets/datasetProcessing/complexMassiveDownload", data = file, stream=True)
    if response.status_code == 200 :
        start_download(response, "Output_extraction")
    else :
        logger.error("An error has occurred while trying to download processing outputs.")