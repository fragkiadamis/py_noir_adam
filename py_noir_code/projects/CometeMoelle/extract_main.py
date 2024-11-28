from requests import Response
from py_noir_code.src.API.api_service import get
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_ids_from_file


def init_extraction(ids : [], resultOnly: bool):
    response = get("/datasets/datasetProcessing/massiveDownloadByProcessing", {"processingIds":ids, "resultOnly":"true" if resultOnly else "false"})
    if response.status_code == 200 :
        start_download(response)
    else :
        print("An error has occured while trying to download.")

def start_download(response : Response):
    with open("comete_moelle_in_out_extraction.zip", "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
            file.write(chunk)
    print("Download completed (.zip is in your current directory) !")

if __name__ == '__main__':
    load_context("extract_context.conf", False)
    init_extraction(get_ids_from_file("processing_ids_to_extract.txt"), False)
