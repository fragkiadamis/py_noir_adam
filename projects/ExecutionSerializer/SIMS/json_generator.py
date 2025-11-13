from config.config import Config
from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.API.api_config import APIConfig
from src.utils.file_utils import get_ids_from_file
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id

logger = get_logger()

def generate_json(project_name: str):
    identifier = 0
    executions = []

    ids_file = Config.rootPath.joinpath("projects", "ExecutionSerializer", project_name, "ids_to_exec.txt")
    exam_ids_to_exec = get_ids_from_file(ids_file)

    logger.info("Getting datasets, building json content... ")

    for exam_id in exam_ids_to_exec:
        datasets = find_datasets_by_examination_id(exam_id)

        execution = {
            "identifier":identifier,
            "name": "SIMS_3_exam_{}_{}_post_processing".format(exam_id, datetime.now(timezone.utc).strftime('%F')),
            "pipelineIdentifier": "SIMS/3",
            "studyIdentifier": datasets[0]["studyId"],
            "inputParameters": {},
            "outputProcessing": "",
            "processingType": "SEGMENTATION",
            "refreshToken": APIConfig.refresh_token,
            "client": APIConfig.clientId,
            "datasetParameters": [
                {
                    "datasetIds": [dataset["id"] for dataset in datasets],
                    "groupBy":"EXAMINATION",
                    "name":"dicom_archive",
                    "exportFormat":"dcm"
                }
            ]
        }
        executions.append(execution)
        identifier = identifier + 1

    return executions