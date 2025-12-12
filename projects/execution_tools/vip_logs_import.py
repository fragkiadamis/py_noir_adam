import typer

from requests import Response
from src.API.api_service import get
from src.utils.config_utils import ConfigPath
from src.utils.file_utils import create_file_path, get_items_from_input_file
from src.utils.file_writer import FileWriter
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()


@app.callback()
def explain() -> None:
    """
    \b
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
        if response.status_code == 200:
            log_response(response, workflow_id)
        else:
            logger.error("An error has occurred while trying to download " + workflow_id + " logs.")


def log_response(response: Response, workflow_id: str) -> None:
    if response.content.__len__() > 100:
        error_file_path = ConfigPath.output_path / "imported_logs" / (workflow_id + ".txt")
        create_file_path(error_file_path)

        FileWriter.open_files(error_file_path)
        FileWriter.append_content(error_file_path, "\n\n\n\n" + str(response.text))
        FileWriter.close_all()

        logger.info("VIP logs retrieved for " + workflow_id)

