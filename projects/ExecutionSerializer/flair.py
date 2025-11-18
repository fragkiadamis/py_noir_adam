import typer

from src.execution.execution_init_service import init_executions, resume_executions
from src.utils.file_writer import FileWriter
from src.utils.log_utils import set_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, get_working_files, reset_tracking_file, get_tracking_file
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from src.utils.config_utils import APIConfig, ConfigPath

app = typer.Typer()
logger = set_logger("flair")

@app.callback()
def explain():
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
    working_file_path, save_file_path = get_working_files("Comete_FLAIR")
    tracking_file_path = get_tracking_file("Comete_FLAIR")

    FileWriter.open_files(tracking_file_path)

    if not save_file_path.exists():
        reset_tracking_file(tracking_file_path)
        init_executions(working_file_path, generate_json())
    else:
        resume_executions(working_file_path, save_file_path)

    FileWriter.close_all()

def generate_json():
    examinations = dict()
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("inputs.txt")

    logger.info("Getting datasets, building json content... ")

    for exam_id in exam_ids_to_exec:
        try:
            datasets = find_datasets_by_examination_id(exam_id)
        except:
            logger.error("An error occurred while downloading examination {} from Shanoir", exam_id)
            identifier += 1
            FileWriter.append_content(ConfigPath.trackingFilePath, str(identifier) + "," + str(exam_id) + ",false,,,,")
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
                identifier +=1
                FileWriter.append_content(ConfigPath.trackingFilePath, str(identifier) + "," + str(exam_id) + ",true,,,,")
                examinations[exam_id]["FLAIR"].append(ds_id)
                examinations[exam_id]["identifier"].append(identifier)

    for key, value in examinations.items():
        if value["FLAIR"] :
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