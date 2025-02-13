import sys

sys.path.append('../')
sys.path.append('../../../../')

from py_noir_code.src.utils.log_utils import set_logger
from datetime import datetime, timezone
from py_noir_code.src.utils.file_utils import open_project_file, get_ids_from_file
from py_noir_code.src.API.api_context import APIContext
from py_noir_code.src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id

logger = set_logger()

def generate_sims_json():
    examinations = dict()
    identifier = 0
    executions = []

    exam_ids_to_exec = get_ids_from_file("ids_to_rename.txt", "r")

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
                    "name": "comete_moelle_01_exam_{}_{}".format(key,
                                                                 datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                    "pipelineIdentifier": "comete_moelle/0.1",
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
                    "refreshToken": APIContext.refresh_token,
                    "client": APIContext.clientId,
                    "converterId": 6
                }
                executions.append(execution)
            identifier = identifier + 1

    return executions
