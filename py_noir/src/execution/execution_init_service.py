import json
import os
import string
import sys

from projects.project_switch import generate_json
from py_noir.src.execution.execution_management_service import manage_executions
from py_noir.src.API.shanoir_context import ShanoirContext
from py_noir.src.utils.file_utils import remove_file_extension

def init_executions(context: ShanoirContext):
    exam_ids_file_name = context.entry_file
    json_file_name = "ressources/WIP_files/" + remove_file_extension(os.path.basename(exam_ids_file_name)) + ".json"
    resume = True

    if not os.path.exists(json_file_name):
        resume = False
        create_json_file(json_file_name, context)

    manage_executions(context, json_file_name, resume)

def create_json_file(json_file_name: string, context: ShanoirContext):
    json_content = generate_json(context)

    if not json_content:
        print("There is no exam to process. Please verify the entry file.")
        sys.exit(1)

    json_content.insert(0,dict(nb_processed_examinations = 0, processed_examination_ids = []))

    exams_to_exec = open("ressources/WIP_files/" + os.path.basename(json_file_name), "w")
    exams_to_exec.write(json.dumps(json_content))
    exams_to_exec.close()