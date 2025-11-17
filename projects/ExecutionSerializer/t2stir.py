import typer

from src.execution.execution_init_service import init_executions, resume_executions
from src.utils.config_utils import APIConfig
from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, get_working_files
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id

app = typer.Typer()
logger = get_logger("t2stir")

@app.callback()
def explain():
    """
    Comete_T2STIR project command-line interface.
    Commands:
    --------
    * `execute` — runs the Comete_T2STIR pipeline for examinations listed in `input/inputs.txt`:
        - Retrieves datasets for each examination ID.
        - Generates JSON executions for the Comete_T2STIR/0.1 pipeline.
        - Launches executions or resumes incomplete runs.
    Usage:
    -----
        uv run main.py t2stir execute
    """

@app.command()
def execute() -> None:
    """
    Run the FLAIR processing pipeline
    """
    working_file_path, save_file_path = get_working_files("Comete_T2STIR")

    if not (save_file_path).exists():
        _ = init_executions(working_file_path, generate_json())
    else:
        _ = resume_executions(working_file_path, save_file_path)

def generate_json():
    examinations = dict()
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("ids_to_exec.txt", "r")

    logger.info("Getting datasets, building json content... ")

    for exam_id in exam_ids_to_exec:
        datasets = find_datasets_by_examination_id(exam_id)

        for dataset in datasets:
            ds_id = dataset["id"]
            study_id = dataset["studyId"]

            if exam_id not in examinations:
                examinations[exam_id] = {}
                examinations[exam_id]["studyId"] = study_id
                examinations[exam_id]["T2"] = []
                examinations[exam_id]["STIR"] = []

            if "T2DSAGSTIR" == dataset["updatedMetadata"]["name"]:
                examinations[exam_id]["STIR"].append(ds_id)
            elif "T2DSAGT2" == dataset["updatedMetadata"]["name"]:
                examinations[exam_id]["T2"].append(ds_id)

    for key, value in examinations.items():
        if value["T2"] and value["STIR"] :
            for t2 in value["T2"]:
                execution = {
                    "identifier":identifier,

                    "name": "comete_moelle_01_exam_{}_{}".format(key,
                                                                 datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                    "pipelineIdentifier": "comete_sc_t2_stir/0.1",
                    "inputParameters": {},
                    "datasetParameters": [
                        {
                            "name": "t2_archive",
                            "groupBy": "DATASET",
                            "exportFormat": "nii",
                            "datasetIds": [t2],
                            "converterId": 2
                        },
                        {
                            "name": "stir_archive",
                            "groupBy": "EXAMINATION",
                            "exportFormat": "nii",
                            "datasetIds": value["STIR"],
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
            identifier = identifier + 1

    return executions