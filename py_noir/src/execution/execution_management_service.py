import os
import string
import sys
import json
from datetime import datetime

from py_noir.src.API.shanoir_context import ShanoirContext

sys.path.append('../../../')
sys.path.append('../../../projects/shanoir_object/dataset')


def manage_executions(context: ShanoirContext, json_file_name: string, resume: bool):

    acquisitions = read_acquisitions_from_json_file(json_file_name, context, resume)

    nb_processed_exams = int(acquisitions[0]["nb_processed_examinations"])
    processed_exam_ids = list(acquisitions[0]["processed_examination_ids"])
    exams_to_process_ids = [acquisition["examinationIdentifier"] for acquisition in acquisitions[1:]]
    acquisitions_distribution_per_examination = processed_exam_ids.copy()
    acquisitions_distribution_per_examination.extend(exams_to_process_ids)
    total_exams_to_process = len(set(acquisitions_distribution_per_examination))

    with open(json_file_name, "w") as exams_to_processed_file :
        if total_exams_to_process == 1:
            monitoring_sentence = "The examination %s is processed."
        else:
            monitoring_sentence = "Examination %s processed. %s of %s examinations are processed."

        for acquisition in acquisitions[1:]:
            #create_execution(context, acquisition)
            examination_id = acquisition["examinationIdentifier"]
            acquisitions.remove(acquisition)
            exams_to_process_ids.remove(examination_id)

            if examination_id not in exams_to_process_ids:
                nb_processed_exams += 1
                processed_exam_ids.append(examination_id)
                acquisitions[0] = dict(nb_processed_examinations = nb_processed_exams, processed_examination_ids = processed_exam_ids)
                print(str(datetime.now().replace(microsecond=0)) + " : " + monitoring_sentence % (examination_id, nb_processed_exams, total_exams_to_process))

            exams_to_processed_file.truncate(0)
            exams_to_processed_file.seek(0)
            exams_to_processed_file.write(json.dumps(acquisitions))
            exams_to_processed_file.flush()
    os.remove(json_file_name)


def read_acquisitions_from_json_file(json_file_name: string, context: ShanoirContext, resume: bool):

    try:
        exams_to_processed_file = open(json_file_name, "r")
        exams_to_processed = json.load(exams_to_processed_file)
        exams_to_processed_file.close()
        return exams_to_processed

    except:
        if resume:
            print("Resume script is impossible, monitoring file is corrupted.")
            reset = input("Do you want to reset all executions ? (y/n)")

            while reset not in ['n', 'y']:
                reset = input("Please type 'y' or 'n'")
            if reset == 'n':
                sys.exit(1)

        else:
            print("Examinations to process are wrong. Please verify the json file shaping.")
            sys.exit(1)

    from py_noir.src.execution.execution_init_service import create_json_file
    create_json_file(json_file_name, context)
    return read_acquisitions_from_json_file(json_file_name, context, False)