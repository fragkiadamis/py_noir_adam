import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from requests import Response
from py_noir_code.src.API.api_service import get
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_ids_from_file, find_project_root, create_file_path


def init_import(ids : []):
    for id in ids:
        response = get("/datasets/vip/execution/" + id + "/stdout")

        if response.status_code == 200 :
            log_response(response)
        else :
            print("An error has occured while trying to download " + id + " logs.")

def log_response(response : Response):
    if response.content.__len__() > 100 :
        error_file_path = find_project_root(__file__) + "/py_noir_code/resources/imported_logs/logs.txt"
        create_file_path(find_project_root(__file__) + "/py_noir_code/resources/imported_logs/")

        error_file = open(error_file_path, "a")
        error_file.write("\n\n\n\n" + str(response.text))
        error_file.close()

if __name__ == '__main__':
    load_context("context.conf", False)
    init_import(get_ids_from_file("workflow_identifier_to_get.txt"))