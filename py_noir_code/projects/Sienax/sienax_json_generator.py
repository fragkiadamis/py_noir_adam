import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from py_noir_code.src.utils.log_utils import get_logger
from datetime import datetime, timezone
from py_noir_code.src.utils.file_utils import get_ids_from_file
from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id

logger = get_logger()

def generate_sienax_json():
    examinations = dict()
    identifier = 0
    executions = []

    exam_ids_to_exec = get_ids_from_file("ids_to_exec.txt", "r")

    logger.info("Getting datasets, building json content... ")

    for exam_id in exam_ids_to_exec:
        datasets = find_datasets_by_examination_id(exam_id, True)

        for dataset in datasets:
            ds_id = dataset["id"]
            study_id = dataset["studyId"]

            if exam_id not in examinations:
                examinations[exam_id] = {}
                examinations[exam_id]["studyId"] = study_id
                examinations[exam_id]["T1MPRAGE"] = []

            if "T1MPRAGE" in dataset["updatedMetadata"]["name"]:
                examinations[exam_id]["T1MPRAGE"].append(ds_id)


    for key, value in examinations.items():
        if value["T1MPRAGE"] :
            execution = {
                "identifier":identifier,

                "name": "sienax_01_exam_{}_{}".format(key,
                                                             datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                "pipelineIdentifier": "sienax/1",
                "inputParameters": {},
                "datasetParameters": [
                    {
                        "name": "t1_archive",
                        "groupBy": "EXAMINATION",
                        "exportFormat": "nii",
                        "datasetIds": value["T1MPRAGE"],
                        "converterId": 2
                    },
                ],
                "studyIdentifier": value["studyId"],
                "outputProcessing": "",
                "processingType": "SEGMENTATION",
                "refreshToken": APIContext.refresh_token,
                "client": APIContext.clientId,
                "converterId": 2
            }
            executions.append(execution)
            identifier = identifier + 1

    return executions
