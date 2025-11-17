import typer

from src.API.api_service import get
from src.utils.download_utils import start_download
from src.utils.file_utils import get_items_from_input_file
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger("carmin")

@app.callback()
def explain():
    """
    CarminAPITest project command-line interface.
    Commands:
    --------
    * `execute` — runs the Carmin API test for shanoir resource_ids listed in `input/inputs.txt`:
        - Download the data relative to the existing resource_ids in the VIP format data reception.
        - Resource ids are like : 1d18478f-9470-4be8-ba4b-21055f3b461b and can be found ine the processing_resource dataset table in ShanoirDB.
    Usage:
    -----
        uv run main.py carmin execute
    """

@app.command()
def execute():
    """
    Run the Carmin API test:
    """
    resource_ids = get_items_from_input_file("inputs.txt")
    for resource_id in resource_ids:
        response = get("/datasets/carmin-data/path/" + resource_id + "'action=content&converterId=5&format=dcm")
        if response.status_code == 200 :
            start_download(response, "CarminAPI_" + resource_id)
        else :
            logger.error("An error has occurred while trying to get resource {} from Shanoir.", resource_id)
