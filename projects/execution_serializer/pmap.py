from typing import List, Dict

import typer

from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.file_writer import FileWriter
from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, get_working_files, get_tracking_file
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from src.utils.serializer_utils import init_serialization

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    Comete_PMAP project command-line interface.
    Commands:
    --------
    * `execute` — runs the Comete_PMAP pipeline for examinations listed in `input/inputs.txt`:
        - Retrieves datasets for each examination ID.
        - Generates JSON executions for the Comete_PMAP/1.3 pipeline.
        - Launches executions or resumes incomplete runs.
    Usage:
    -----
        uv run main.py pmap execute
    """

@app.command()
def execute() -> None:
    """
    Run the FLAIR processing pipeline
    """
    working_file_path, save_file_path = get_working_files("Comete_PMAP")
    tracking_file_path = get_tracking_file("Comete_PMAP")

    init_serialization(working_file_path, save_file_path, tracking_file_path, generate_json)

def generate_json() -> List[Dict]:
    examinations = Dict()
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("inputs.txt")

    logger.info("Getting datasets, building json content... ")


    for exam_id in exam_ids_to_exec:
        try:
            datasets = find_datasets_by_examination_id(exam_id, True)
        except:
            logger.error("An error occurred while downloading examination " + exam_id + " from Shanoir")
            identifier += 1
            FileWriter.append_content(ConfigPath.trackingFilePath, str(identifier) + "," + str(exam_id) + ",false,,,,,,")
            continue

        for dataset in datasets:
            ds_id = dataset["id"]
            study_id = dataset["studyId"]

            if exam_id not in examinations:
                identifier += 1
                FileWriter.append_content(ConfigPath.trackingFilePath, str(identifier) + "," + str(exam_id) + ",true,false,,,,,")
                examinations[exam_id] = {}
                examinations[exam_id]["studyId"] = study_id
                examinations[exam_id]["T2"] = []
                examinations[exam_id]["PMAP"] = []
                examinations[exam_id]["identifier"] = identifier

            if "pmap.nii.gz" == dataset["updatedMetadata"]["name"]:
                examinations[exam_id]["PMAP"].append(ds_id)
            elif "T2DSAGT2" == dataset["updatedMetadata"]["name"]:
                examinations[exam_id]["T2"].append(ds_id)



    for key, value in examinations.items():
        if value["T2"] and value["PMAP"]:
            FileWriter.update_content_first_matching_line_start(ConfigPath.trackingFilePath, str(value["identifier"]),  ",,,true,,,,,", True)

            execution = {
                "identifier":value["identifier"],

                "name": "comete_pmap_01_exam_{}_{}".format(key,
                                                           datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                "pipelineIdentifier": "comete_sc_pmap_fusion/1.3",
                "inputParameters": {},
                "datasetParameters": [
                    {
                        "name": "t2_archive",
                        "groupBy": "EXAMINATION",
                        "exportFormat": "nii",
                        "datasetIds": value["T2"],
                        "converterId": 2
                    },
                    {
                        "name": "pmap_archive",
                        "groupBy": "EXAMINATION",
                        "datasetIds": value["PMAP"],
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