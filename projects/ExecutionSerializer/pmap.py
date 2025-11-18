import typer

from src.execution.execution_init_service import init_executions, resume_executions
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.file_writer import FileWriter
from src.utils.log_utils import set_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, get_working_files, get_tracking_file, reset_tracking_file
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id

app = typer.Typer()
logger = set_logger("pmap")

@app.callback()
def explain():
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
            datasets = find_datasets_by_examination_id(exam_id, True)
        except:
            logger.error("An error occurred while downloading examination {} from Shanoir", exam_id)
            identifier += 1
            FileWriter.append_content(ConfigPath.trackingFilePath, str(identifier) + "," + str(exam_id) + ",false,,,,")
            continue

        for dataset in datasets:
            ds_id = dataset["id"]
            study_id = dataset["studyId"]

            if exam_id not in examinations:
                identifier += 1
                FileWriter.append_content(ConfigPath.trackingFilePath, str(identifier) + "," + str(exam_id) + ",true,,,,")
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
        if value["T2"] and value["PMAP"] :
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