import typer
import re

from src.API.api_service import get
from src.utils.file_utils import get_items_from_input_file
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    \b
    Post-processing project command-line interface.

    Commands:
    --------
    * `execute` — runs the delayed post processings for VIP outputs in Shanoir according to the pipeline names written in `input/inputs.txt`:
        - Launches delayed post processings to all corresponding execution outputs
        - A post processing is a treatement that should occur on/with execution outputs once they are received from VIP
        - Care that to have a delayed post processing available, the execution name in Shanoir must end with "_post_processing"
        - To set a delay to a post processing on an execution, you can add "_post_processing" at the end of the name field in the execution serializer json_generator() method.
        - The input have to be that way : pipe_type1:comment1;pipe_type2:comment2;etc...
    Usage:
    -----
        uv run main.py post_processing execute
    """

@app.command()
def execute() -> None:
    """
    Run the post processing relatively to the comment value of the executions
    """
    pipeline_types_and_comment = get_items_from_input_file("inputs.txt")

    for pipeline_type_and_comment in pipeline_types_and_comment:
        response = get("/datasets/vip/postProcessing/", params = {"comment":re.split(r':', pipeline_type_and_comment)[1], "name":re.split(r':', pipeline_type_and_comment)[0]})
        if response.status_code != 200:
            logger.error("An error has occurred while trying to launch {} delayed post processings.", pipeline_type_and_comment)