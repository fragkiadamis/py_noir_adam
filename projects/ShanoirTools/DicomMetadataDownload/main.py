import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from requests import Response
from src.API.api_service import post
from src.utils.config_utils import load_config
from src.utils.file_utils import get_ids_from_file



def init_extraction(datasetsIds: [], metadataKeys: []):
    response = post("/datasets/datasets/dicomMetadataExtraction", data = {"datasetIds":datasetsIds, "metadataKeys":metadataKeys}, jsonBody=False)
    if response.status_code == 200 :
        start_download(response)
    else :
        print("An error has occured while trying to download.")

def start_download(response : Response):
    if response.content.__len__() > 0 :
        with open("metadata_file.csv", "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        print("Download completed (.csv is in your current directory) !")
    else :
        print("No data to download !")

if __name__ == '__main__':
    load_config("context.conf", False)
    init_extraction(get_ids_from_file("ids_to_extract.txt", "r"), get_ids_from_file("metadata_keys_to_extract.txt", "r"))
