import os
import shutil
import sys
import json
import threading
import time

from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from src.execution.execution_service import create_execution, get_execution_status, get_execution_monitoring
from src.utils.config_utils import ExecutionConfig
from src.utils.log_utils import get_logger

logger = get_logger()

total_items_to_process = None
items = []
nb_processed_items = 0
processed_item_ids = []
executions = []
saveFile = ""
start_events = {}

def check_pause_schedule(pause_message_event):
    current_hour = time.localtime(time.time()).tm_hour
    while ExecutionConfig.server_reboot_beginning_hour <= current_hour < ExecutionConfig.server_reboot_ending_hour:
        if not pause_message_event.is_set():
            logger.info("Current time is between %s and  %s. Pausing..." % (
                ExecutionConfig.server_reboot_beginning_hour, ExecutionConfig.server_reboot_ending_hour))
            pause_message_event.set()
        time.sleep(60)
    if pause_message_event.is_set():
        pause_message_event.clear()

def thread_execution_with_start_signal(working_file, item, start_event):
    start_event.set()
    thread_execution(working_file, item)

def manage_threading_execution(working_file):
    global items
    global nb_processed_items
    global processed_item_ids
    global executions

    logger.info("Starting new executions...")

    with ThreadPoolExecutor(max_workers=ExecutionConfig.max_thread) as executor:
        for item in items[1:]:
            start_event = threading.Event()
            start_events[item] = start_event
            executor.submit(thread_execution_with_start_signal, working_file, item, start_event)
            start_event.wait()
            time.sleep(1) # Required, to avoid concurrency issues

    logger.info("Executions ended.")

def thread_execution(working_file, item: dict):
    global nb_processed_items, processed_item_ids
    monitoring_lock = threading.Lock()
    file_lock = threading.Lock()
    pause_message_event = threading.Event()
    check_pause_schedule(pause_message_event)

    try:
        execution = create_execution(item)
        if execution['id'] is not None:
            monitoring = get_execution_monitoring(execution["id"])
            logger.info("Execution " + str(execution['id']) + ", " + str(monitoring['identifier']) + " is created.")
            status = '"Running"'
            count_down = 12

            while status == '"Running"':
                time.sleep(5)

                for attempt in range(5):
                    try:
                        status = get_execution_status(monitoring['identifier'])
                        break  # success, exit retry loop
                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1}/5 failed to get status: {e}")
                        if attempt == 4:
                            raise  # re-raise after 3 failed attempts
                        time.sleep(1)

                count_down -= 1
                if count_down == 1 and status == '"Running"':
                    logger.info("Status for execution " + str(execution["id"]) + " is " + status)
                    count_down = 12

            with monitoring_lock:
                if status == 'Finished' :
                    logger.debug("Success for execution" + str(execution["id"]))
                    executions.append(execution["id"])
                    manage_execution_success(item)
                else :
                    logger.debug("Failure for execution" + str(execution["id"]))

    except:
        logger.debug("Exception for execution " + str(execution["id"]))
        with monitoring_lock:
            manage_execution_failure(item, execution["message"] + "\n" if execution != None and "message" in execution.keys() else "", execution["details"] + "\n" if execution != None and "details" in execution.keys() else "")
    with file_lock:
        manage_working_file(working_file)

def start_executions(working_file: Path, resume: bool = False):
    global total_items_to_process
    global nb_processed_items
    global processed_item_ids
    global items
    global saveFile

    items = read_items_from_json_file(working_file, resume)
    items = items.sort(key=lambda x: x["identifier"])
    nb_processed_items = int(items[0]["nb_processed_items"])
    processed_item_ids = list(items[0]["processed_item_ids"])
    total_items_to_process = len(processed_item_ids) + len(items) - 1

    saveFile = working_file.parent.parent / "save_files" / working_file.name
    shutil.copy(working_file, saveFile)
    initialFile = working_file.parent.parent / "save_files" / ("initial_" + working_file.name)
    shutil.copy(working_file, initialFile)

    with open(working_file, "w") as working_content:
        manage_threading_execution(working_content)
    os.remove(working_file)
    os.remove(saveFile)

    return executions


def read_items_from_json_file(working_file: Path, resume: bool):
    try:
        return get_items_from_json_file(working_file)
    except:
        if resume:
            logger.info("Resume script is impossible, monitoring file is corrupted. Deleting monitoring file, please relaunch executions.")
            os.remove(working_file)
        else:
            logger.error("Items to process are wrong. Please verify the json file shaping.")
        sys.exit(1)

def manage_execution_success(item: dict):
    global nb_processed_items
    global processed_item_ids

    item_processed_increment(item)
    logger.info("%s out of %s items processed." % (nb_processed_items, total_items_to_process))

def manage_execution_failure(item: dict):
    item_processed_increment(item)
    logger.error("item %s raised an exception." % str(item["identifier"]))

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
    global saveFile

    working_file.truncate(0)
    working_file.seek(0)
    working_file.write(json.dumps(items))
    working_file.flush()
    shutil.copy(working_file.name, saveFile)

def get_items_from_json_file(working_file: Path):
    items_to_processed_file = open(working_file, "r")
    items_to_processed = json.load(items_to_processed_file)
    items_to_processed_file.close()
    return items_to_processed