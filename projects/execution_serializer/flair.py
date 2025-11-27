from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import typer

from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.serializer_utils import init_serialization

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    Comete_FLAIR project command-line interface.
    Commands:
    --------
    * `execute` — runs the Comete_FLAIR pipeline for examinations listed in `input/inputs.txt`:
        - Retrieves datasets for each examination ID.
        - Generates JSON executions for the Comete_FLAIR/1.3 pipeline.
        - Launches executions or resumes incomplete runs.
    Usage:
    -----
        uv run main.py flair execute
    """

@app.command()
def execute() -> None:
    """
    Run the FLAIR processing pipeline
    """
    initiate_working_files("Comete_FLAIR")
    init_serialization(generate_json)

def generate_json(_: Optional[Path] = None) -> List[Dict]:
    examinations = Dict()
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("inputs.txt")
    logger.info("Getting datasets, building json content... ")

    df = pd.read_csv(ConfigPath.tracking_file_path)
    for exam_id in exam_ids_to_exec:
        try:
            datasets = find_datasets_by_examination_id(exam_id)
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
                examinations[exam_id] = {}
                examinations[exam_id]["studyId"] = study_id
                examinations[exam_id]["FLAIR"] = []
                examinations[exam_id]["identifier"] = []

            if "T3DFLAIR" == dataset["updatedMetadata"]["name"]:
                values = {
                    "identifier": identifier + 1,
                    "examination_id": exam_id,
                    "get_from_shanoir": True,
                    "executable": True,
                }
                for col, val in values.items():
                    df.loc[identifier, col] = val
                df.to_csv(ConfigPath.tracking_file_path, index=False)
                examinations[exam_id]["FLAIR"].append(ds_id)
                examinations[exam_id]["identifier"].append(identifier + 1)
                identifier +=1

        if not examinations[exam_id]["FLAIR"]:
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
        if value["FLAIR"]:
            for i, flair in enumerate(value["FLAIR"]):
                execution = {
                    "identifier":value["identifier"][i],

                    "name": "FLAIR_1_exam_{}_{}".format(key,
                                                        datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                    "pipelineIdentifier": "comete_brain_flair/1.3",
                    "inputParameters": {},
                    "datasetParameters": [
                        {
                            "name": "flair_archive",
                            "groupBy": "EXAMINATION",
                            "exportFormat": "nii",
                            "datasetIds": [flair],
                            "converterId": 2
                        }
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