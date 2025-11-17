import typer

from requests import Response
from src.API.api_service import get
from src.utils.config_utils import Config
from src.utils.file_utils import create_file_path, get_items_from_input_file
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger("vip_logs_import")

@app.callback()
def explain():
    """
    Vip log improts project command-line interface.
    Commands:
    --------
    * `execute` — download the VIP logs according to the workflow ids written in `input/inputs.txt`:
    Usage:
    -----
        uv run main.py vip_logs_import execute
    """

@app.command()
def execute() -> None:
    workflow_ids = get_items_from_input_file("inputs.txt")

    for workflow_id in workflow_ids:
        response = get("/datasets/vip/execution/" + workflow_id + "/stdout")
        if response.status_code == 200 :
            log_response(response)
        else :
            logger.error("An error has occurred while trying to download " + workflow_id + " logs.")

def log_response(response : Response):
    if response.content.__len__() > 100 :
        error_file_path = Config.outputPath/ "imported_logs" / "logs.txt"
        create_file_path(error_file_path.parent)

        error_file = open(error_file_path, "a")
        error_file.write("\n\n\n\n" + str(response.text))
        error_file.close()
