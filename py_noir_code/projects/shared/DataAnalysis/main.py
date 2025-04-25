import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from requests import Response
from py_noir_code.src.API.api_service import post
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_ids_from_file


def init_analysis(ids : [], dataType: str, pipelineIdentifier: str):
    response = post("/datasets/datasetProcessing/downloadPipelineDatas", params = {"pipelineIdentifier":pipelineIdentifier, "dataType":dataType}, data = json.dumps(ids))

    if response.status_code == 200 :
        start_download(response)
    else :
        print("An error has occured while trying to download analysis.")

def start_download(response : Response):
    if response.content.__len__() > 100 :
        with open("comete_moelle_analysis.zip", "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        print("Download completed (.zip is in your current directory) !")
    else :
        print("No data to download !")

if __name__ == '__main__':
    load_context("context.conf", False)
    #dataType in {"study","subject","examination","acquisition","dataset"}
    init_analysis(get_ids_from_file("processing_ids_to_analyze.txt"), "examination", "comete_moelle/0.1")
