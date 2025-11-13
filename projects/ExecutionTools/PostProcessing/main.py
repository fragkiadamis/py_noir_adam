import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from src.API.api_service import get
from src.utils.config_utils import load_config


def init_post_processing():
    get("/datasets/vip/postProcessing/", params = {"name":"SEGMENTATION", "comment":"SIMS"})

if __name__ == '__main__':
    load_config("context.conf", False)
    init_post_processing()
