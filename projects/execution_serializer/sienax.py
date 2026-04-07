from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import typer

from src.execution.execution_init_service import create_working_file
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
    initiate_working_files("sienax")
    init_serialization(generate_json)

def generate_json(_: Optional[Path] = None) -> List[Dict]:
    examinations = {}
    identifier = 0
    executions = []
    executed_dataset = [67835,105988,427660,467687,795568,795591,797784,855309,855317,862696,947053]
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
            if(dataset["id"] not in executed_dataset):
                continue

            ds_id = dataset["id"]
            study_id = dataset["studyId"]


            if exam_id not in examinations:
                examinations[exam_id] = {}
                examinations[exam_id]["studyId"] = study_id
                examinations[exam_id]["T1"] = []
                examinations[exam_id]["identifier"] = []

            if dataset["updatedMetadata"] and dataset["updatedMetadata"]["name"] and (dataset["updatedMetadata"]["name"] == "T3DT1" or dataset["updatedMetadata"]["name"] == "ENC_T1_3D_MPRAGE_MORPHO" or dataset["updatedMetadata"]["name"] == "T3DT1GADO"):
                values = {
                    "identifier": identifier + 1,
                    "examination_id": exam_id,
                    "get_from_shanoir": True,
                    "executable": True,
                }
                for col, val in values.items():
                    df.loc[identifier, col] = val
                df.to_csv(ConfigPath.tracking_file_path, index=False)
                examinations[exam_id]["T1"].append(ds_id)
                examinations[exam_id]["identifier"].append(identifier + 1)
                identifier +=1

        if not examinations.get(exam_id) or not examinations.get(exam_id).get("T1"):
            values = {
                "identifier": identifier + 1,
                "examination_id": exam_id,
                "get_from_shanoir": True,
                "executable": False,
            }
            for col, val in values.items():
                df.loc[identifier, col] = val
            df.to_csv(ConfigPath.tracking_file_path, index=False)
            identifier +=1

    for key, value in examinations.items():
        if value["T1"]:
            for i, t1 in enumerate(value["T1"]):

                execution = {
                    "identifier":value["identifier"][i],
                    "name": "sienax_1_3_exam_{}_{}".format(key, datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                    "pipelineIdentifier": "Sienax/1.3",
                    "inputParameters": {},
                    "datasetParameters": [
                        {
                            "name": "T1_archive",
                            "groupBy": "EXAMINATION",
                            "exportFormat": "nii",
                            "datasetIds": [t1],
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
