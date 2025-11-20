import typer

from src.API.api_service import get
from src.utils.file_utils import get_items_from_input_file
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    Post-processing project command-line interface.
    Commands:
    --------
    * `execute` — runs the delayed post processings for VIP outputs in Shanoir according to the pipeline names written in `input/inputs.txt`:
        - Launches delayed post processings to all corresponding execution outputs
        - A post processing is a treatement that should occur on/with execution ouputs once they are received from VIP
        - Care that to have a delayed post processing available, the execution name in Shanoir must end with "_post_processing"
        - To set a delay to a post processing on an execution, you can add "_post_processing" at the end of the name field in the execution serializer json_generator() method.
    Usage:
    -----
        uv run main.py post_processing execute
    """

@app.command()
def execute() -> None:
    """
    Run the post processing relatively to the comment value of the executions
    """
    pipeline_names = get_items_from_input_file("inputs.txt")

    for pipeline_name in pipeline_names:
        response = get("/datasets/vip/postProcessing/", params = {"comment":pipeline_name})
        if response.status_code != 200:
            logger.error("An error has occurred while trying to launch {} delayed post processings.", pipeline_name)