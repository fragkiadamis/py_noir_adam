import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from requests import Response
from src.API.api_service import get
from src.utils.config_utils import load_config
from src.utils.file_utils import get_ids_from_file, find_project_root, create_file_path


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
    load_config("context.conf", False)
    init_import(get_ids_from_file("workflow_identifier_to_get.txt"))