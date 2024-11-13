from datetime import datetime, timezone
from typing import List

from projects.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from py_noir.src.API.shanoir_context import ShanoirContext


def generate_comete_moelle_json(context: ShanoirContext):
    examinations = dict()
    identifier = 0
    executions = []


    exam_ids_file_name = context.entry_file
    exams_file = open(exam_ids_file_name, "r")
    exam_ids_to_exec = exams_file.read().split(",")

    for exam_id in exam_ids_to_exec:
        datasets = find_datasets_by_examination_id(context, exam_id)

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
        for t2 in value["T2"]:
            execution = {
                "name": "comete_moelle_01_exam_{}_{}".format(key, datetime.now(timezone.utc).strftime('%F_%H%M%S%f')[:-3]),
                "pipelineIdentifier": "comete_moelle/0.1",
                "inputParameters": {},
                "identifier": identifier,
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
                "examinationIdentifier": key,
                "outputProcessing": "",
                "processingType": "SEGMENTATION",
                "refreshToken": context.refresh_token,
                "client": context.clientId,
                "converterId": 6
            }
            executions.append(execution)
        identifier = identifier + 1

    return executions