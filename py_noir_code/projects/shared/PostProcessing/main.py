import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from py_noir_code.src.API.api_service import get
from py_noir_code.src.utils.context_utils import load_context
from py_noir_code.src.utils.file_utils import get_ids_from_file


def init_post_processing():
    get("/datasets/vip/postProcessing/", params = {"name":"SEGMENTATION", "comment":"testest"})

if __name__ == '__main__':
    load_context("context.conf", False)
    init_post_processing()
