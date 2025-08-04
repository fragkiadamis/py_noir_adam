import json
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from requests import Response
from py_noir_code.src.API.api_service import get
from py_noir_code.src.utils.context_utils import load_context


def init_test():

    response = get("/datasets/carmin-data/path/1d18478f-9470-4be8-ba4b-21055f3b461b?action=content&converterId=5&format=dcm")
    if response.status_code == 200 :
        start_download(response)
    else :
        print("An error has occured while trying to download.")

def start_download(response : Response):
    if response.content.__len__() > 100 :
        with open("VIP_data.zip", "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
        print("Download completed (.zip is in your current directory) !")
    else :
        print("No data to download !")

if __name__ == '__main__':
    load_context("context.conf", False)
    init_test()
