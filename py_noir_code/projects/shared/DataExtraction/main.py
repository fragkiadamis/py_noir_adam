import json
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from requests import Response
from py_noir_code.src.API.api_service import post
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_ids_from_file


def init_extraction(ids : [], resultOnly: bool):
    response = post("/datasets/datasetProcessing/massiveDownloadByProcessingIds", params = {"resultOnly":"true" if resultOnly else "false"}, data = json.dumps(ids))
    #response = post("/datasets/datasetProcessing/massiveDownloadProcessingByExaminationIds", params = {"processingComment":"comete_moelle/0.1", "resultOnly":"true" if resultOnly else "false"}, data = json.dumps(ids))
    if response.status_code == 200 :
        start_download(response)
    else :
        print("An error has occured while trying to download.")

def start_download(response : Response):
    if response.content.__len__() > 100 :
        with open("processing_in_out_extraction.zip", "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        print("Download completed (.zip is in your current directory) !")
    else :
        print("No data to download !")

if __name__ == '__main__':
    load_context("context.conf", False)
    init_extraction(get_ids_from_file("processing_ids_to_extract.txt"), True)
