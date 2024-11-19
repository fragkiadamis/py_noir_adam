import os
import string
import sys
import json
import threading
import time

from concurrent.futures.thread import ThreadPoolExecutor
from py_noir_code.src.execution.execution_context import ExecutionContext
from py_noir_code.src.execution.execution_service import create_execution
from py_noir_code.src.utils.file_utils import get_project_name
from py_noir_code.src.utils.log_utils import set_logger

sys.path.append('../../../')
sys.path.append('../shanoir_object/dataset')

logger = set_logger()

total_items_to_process = None
items = []
nb_processed_items = 0
processed_item_ids = []


def check_pause_shedule(pause_message_event):
    current_hour = time.localtime(time.time()).tm_hour
    while ExecutionContext.server_reboot_beginning_hour <= current_hour < ExecutionContext.server_reboot_ending_hour:
        if not pause_message_event.is_set():
            logger.info("Current time is between %s and  %s. Pausing..." % (
                ExecutionContext.server_reboot_beginning_hour, ExecutionContext.server_reboot_ending_hour))
            pause_message_event.set()
        time.sleep(60)
    if pause_message_event.is_set():
        pause_message_event.clear()


def manage_threading_execution(working_file):
    global items
    global nb_processed_items
    global processed_item_ids

    monitoring_lock = threading.Lock()
    file_lock = threading.Lock()
    pause_message_event = threading.Event()
    logger.info("Starting new executions...")

    def thread_execution(item: dict):
        global nb_processed_items, processed_item_ids
        check_pause_shedule(pause_message_event)

        try:
            create_execution(item)
            with monitoring_lock:
                manage_execution_succes(item)
        except Exception as e:
            with monitoring_lock:
                manage_execution_failure(e, item)
        with file_lock:
            manage_working_file(working_file)

    with ThreadPoolExecutor(max_workers=ExecutionContext.max_thread) as executor:
        [executor.submit(thread_execution, item) for item in items[1:]]

    logger.info("Executions ended.")


def start_executions(json_file_name: string, resume: bool = False):
    global total_items_to_process
    global nb_processed_items
    global processed_item_ids
    global items

    items = read_items_from_json_file(json_file_name, resume)
    nb_processed_items = int(items[0]["nb_processed_items"])
    processed_item_ids = list(items[0]["processed_item_ids"])
    total_items_to_process = len(processed_item_ids) + len(items) - 1

    with open(json_file_name, "w") as working_file:
        manage_threading_execution(working_file)
    os.remove(json_file_name)


def read_items_from_json_file(json_file_name: string, resume: bool):
    try:
        return get_items_from_json_file(json_file_name)
    except:
        if resume:
            logger.info("Resume script is impossible, monitoring file is corrupted. Deleting monitoring file, please relaunch executions.")
            os.remove(json_file_name)
        else:
            logger.error("Items to process are wrong. Please verify the json file shaping.")
        sys.exit(1)

def manage_execution_succes(item: dict):
    global nb_processed_items
    global processed_item_ids

    item_processed_increment(item)
    logger.info("%s out of %s items processed." % (nb_processed_items, total_items_to_process))


def store_failure_data(item: dict):
    error_file_name = os.path.dirname(os.path.abspath(__file__)) + "/../../resources/errors/" + get_project_name()
    error_file = open(error_file_name, "a")
    error_file.write(json.dumps(item))
    error_file.close()


def manage_execution_failure(e: Exception, item: dict):
    global nb_processed_items
    global processed_item_ids

    item_processed_increment(item)
    store_failure_data(item)
    logger.error(
        "item %s raised an exception. See item and traceback below. You can see item data in py_noir_code/resources/errors \n" + str(
            item) + "\n" + repr(e.__traceback__) %
        item["identifier"])


def item_processed_increment(item: dict):
    global items
    global nb_processed_items
    global processed_item_ids

    item_id = item["identifier"]
    items.remove(item)
    nb_processed_items += 1
    processed_item_ids.append(item_id)
    items[0] = dict(nb_processed_items=nb_processed_items, processed_item_ids=processed_item_ids)


def manage_working_file(working_file):
    global items

    working_file.truncate(0)
    working_file.seek(0)
    working_file.write(json.dumps(items))
    working_file.flush()


def get_items_from_json_file(json_file_name: str):
    items_to_processed_file = open(json_file_name, "r")
    items_to_processed = json.load(items_to_processed_file)
    items_to_processed_file.close()
    return items_to_processed
