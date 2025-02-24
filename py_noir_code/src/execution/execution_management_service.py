import os
import string
import sys
import json
import threading
import time

from concurrent.futures.thread import ThreadPoolExecutor

from py_noir_code.src.execution.execution_context import ExecutionContext
from py_noir_code.src.execution.execution_service import create_execution, get_execution_status, \
    get_execution_monitoring
from py_noir_code.src.utils.file_utils import get_project_name, create_file_path, find_project_root
from py_noir_code.src.utils.log_utils import get_logger

sys.path.append('../../../')
sys.path.append('../shanoir_object/dataset')

logger = get_logger()

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
            execution = create_execution(item)
            monitoring = get_execution_monitoring(execution["id"])
            status = '"Running"'
            countDown = 12
            while status == '"Running"':
                time.sleep(5)
                status = get_execution_status(monitoring['identifier'])
                countDown -= 1
                if countDown == 1 :
                    logger.info("Status for execution " + str(execution["id"]) + " is " + status)
                    countDown = 12

            with monitoring_lock:
                manage_execution_succes(item)
        except:
            with monitoring_lock:
                manage_execution_failure(item, execution["message"] + "\n" if execution != None and "message" in execution.keys() else "", execution["details"] + "\n" if execution != None and "details" in execution.keys() else "")
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


def store_failure_data(item: dict, message: str, detail: str):
    error_file_path = find_project_root(__file__) + "/py_noir_code/resources/errors/"
    error_file_name =  get_project_name() + ".txt"
    create_file_path(error_file_path)

    error_file = open(error_file_path + error_file_name, "a")
    error_file.write("\n\n\n\n" + message + detail +json.dumps(item, indent=4))
    error_file.close()


def manage_execution_failure(item: dict, message: str, detail: str):
    global nb_processed_items
    global processed_item_ids

    item_processed_increment(item)
    store_failure_data(item, message, detail)
    logger.error(
        "item %s raised an exception. You can see the item data in py_noir_code/resources/errors." %
        str(item["identifier"]))


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