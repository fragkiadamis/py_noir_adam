from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import typer

from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from src.utils.serializer_utils import init_serialization

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    \b
    Sienax project command-line interface.

    Commands:
    --------
    * `execute` — runs the Sienax pipeline for examinations listed in `input/comete.txt`:
        - Retrieves datasets for each examination ID.
        - Generates JSON executions for the Siena/1.3 pipeline.
        - Launches executions or resumes incomplete runs.

    Usage:
    -----
        uv run main.py sienax execute
    """

@app.command()
def execute() -> None:
    """
    Run the Sienax processing pipeline
    """
    initiate_working_files("Siena")
    init_serialization(generate_json)

def generate_json(_: Optional[Path] = None) -> List[Dict]:
    examinations = {}
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("comete.txt")

    logger.info("Getting datasets, building json content... ")

    df = pd.read_csv(ConfigPath.tracking_file_path)
    for exam_id in exam_ids_to_exec:
        try:
            datasets = find_datasets_by_examination_id(exam_id, True)
        except:
            logger.error("An error occurred while downloading examination " + exam_id + " from Shanoir")
            values = {
                "identifier": identifier + 1,
                "examination_id": exam_id,
                "get_from_shanoir": False,
            }
            for col, val in values.items():
                df.loc[identifier, col] = val
            df.to_csv(ConfigPath.tracking_file_path, index=False)
            identifier += 1
            continue

        for dataset in datasets:
            ds_id = dataset["id"]
            study_id = dataset["studyId"]

            if exam_id not in examinations:
                values = {
                    "identifier": identifier + 1,
                    "examination_id": exam_id,
                    "get_from_shanoir": True,
                    "executable": False,
                }
                for col, val in values.items():
                    df.loc[identifier, col] = val
                df.to_csv(ConfigPath.tracking_file_path, index=False)
                examinations[exam_id] = {}
                examinations[exam_id]["studyId"] = study_id
                examinations[exam_id]["T1MPRAGE"] = []
                examinations[exam_id]["identifier"] = identifier + 1
                identifier += 1

            if "T1MPRAGE" in dataset["updatedMetadata"]["name"]:
                examinations[exam_id]["T1MPRAGE"].append(ds_id)

    for key, value in examinations.items():
        if value["T1MPRAGE"]:
            values = {"executable": True}
            for col, val in values.items():
                df.loc[value["identifier"], col] = val
            df.to_csv(ConfigPath.tracking_file_path, index=False)

            execution = {
                "identifier":value["identifier"],
                "name": "sienax_1_3_exam_{}_{}".format(key, datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                "pipelineIdentifier": "Sienax/1.3",
                "inputParameters": {},
                "datasetParameters": [
                    {
                        "name": "T1_archive",
                        "groupBy": "DATASET",
                        "exportFormat": "nii",
                        "datasetIds": value["T1MPRAGE"],
                        "converterId": 2
                    },
                ],
                "studyIdentifier": value["studyId"],
                "outputProcessing": "",
                "processingType": "SEGMENTATION",
                "refreshToken": APIConfig.refresh_token,
                "client": APIConfig.clientId,
                "converterId": 2
            }
            executions.append(execution)

    return executions