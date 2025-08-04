import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from py_noir_code.src.utils.log_utils import get_logger
from datetime import datetime, timezone
from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.utils.file_utils import get_ids_from_file
from py_noir_code.src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id

logger = get_logger()

def generate_sims_json():
    identifier = 0
    executions = []

    exam_ids_to_exec = get_ids_from_file("ids_to_rename.txt", "r")

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
            "refreshToken": APIContext.refresh_token,
            "client": APIContext.clientId,
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