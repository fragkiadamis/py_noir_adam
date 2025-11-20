import os
import shutil
import json
import threading
import time

from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import List

from src.execution.execution_service import create_execution, get_execution_status, get_execution_monitoring
from src.utils.config_utils import ExecutionConfig, ConfigPath
from src.utils.file_writer import FileWriter
from src.utils.log_utils import get_logger

logger = get_logger()

total_items_to_process = None
items = []
nb_processed_items = 0
processed_item_ids = []
saveFile = ""
start_events = {}
monitoring_lock = threading.Lock()
file_lock = threading.Lock()

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

def thread_execution_with_start_signal(working_file: Path, item, start_event):
    start_event.set()
    thread_execution(working_file, item)

def manage_threading_execution(working_file: Path):
    global items
    global nb_processed_items
    global processed_item_ids
    logger.info("Number of planned executions: " + str(len(items) - 1))
    logger.info("Starting new executions...")
    with ThreadPoolExecutor(max_workers=ExecutionConfig.max_thread) as executor:
        for item in items[1:]:
            start_event = threading.Event()
            start_events[item["identifier"]] = start_event
            executor.submit(thread_execution_with_start_signal, working_file, item, start_event)
            start_event.wait()
            time.sleep(1) # Required, to avoid concurrency issues

    logger.info("Executions ended.")

def thread_execution(working_file: Path, item: dict):
    global nb_processed_items, processed_item_ids, monitoring_lock, file_lock
    pause_message_event = threading.Event()
    check_pause_schedule(pause_message_event)

    try:
        execution = create_execution(item)
        if execution['id'] is not None:
            monitoring = get_execution_monitoring(execution["id"])
            with monitoring_lock:
                FileWriter.update_content_first_matching_line_start(ConfigPath.trackingFilePath, str(item["identifier"]), ",,,,true," + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(monitoring['identifier']) + ",Running,", True)
            logger.info("Execution " + str(item["identifier"]) + ", " + str(monitoring['identifier']) + " is created.")
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
                    logger.info("Status for execution " + str(item["identifier"]) + ", " + str(monitoring['identifier']) + " is " + status)
                    count_down = 12

            if status == '"Finished"':
                logger.info("Success for execution " + str(item["identifier"]) + ", " + str(monitoring['identifier']))
            else:
                logger.info("Failure for execution " + str(item["identifier"]) + ", " + str(monitoring['identifier']))
            with monitoring_lock:
                FileWriter.update_content_first_matching_line_start(ConfigPath.trackingFilePath, str(item["identifier"]), ",,,,,,," + status.replace("\"","") + "," + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), True)
            logger.info("%s out of %s items processed." % (nb_processed_items + 1, total_items_to_process))
    except:
        with monitoring_lock:
            logger.error("Exception for execution " + str(item["identifier"] + ", " + str(monitoring['identifier'])))
            FileWriter.update_content_first_matching_line_start(ConfigPath.trackingFilePath, item["identifier"], ",,,,,,," + "PyNoir_exception" + ",", True)
            logger.info("%s out of %s items processed." % (nb_processed_items + 1, total_items_to_process))

    item_processed_increment(item)
    with file_lock:
        FileWriter.replace_content(working_file, json.dumps(items))
        shutil.copy(working_file.name, saveFile)

def start_executions(working_file: Path, resume: bool = False):
    global total_items_to_process
    global nb_processed_items
    global processed_item_ids
    global items
    global saveFile

    items = read_items_from_json_file(working_file, resume)
    nb_processed_items = int(items[0]["nb_processed_items"])
    processed_item_ids = List(items[0]["processed_item_ids"])
    total_items_to_process = len(processed_item_ids) + len(items) - 1

    save_file = ConfigPath.saveFilePath / working_file.name
    shutil.copy(working_file, save_file)
    initial_file = ConfigPath.saveFilePath / ("initial_" + working_file.name)
    shutil.copy(working_file, initial_file)

    manage_threading_execution(working_file)
    os.unlink(working_file)
    os.unlink(save_file)

def read_items_from_json_file(working_file: Path, resume: bool):
    try:
        return get_items_from_json_file(working_file)
    except:
        if resume:
            logger.info("Resume script is impossible, monitoring file is corrupted. Deleting monitoring file, please relaunch executions.")
            Path.unlink(working_file)
        else:
            logger.error("Items to process are wrong. Please verify the json file shaping.")
        exit()

def item_processed_increment(item: dict):
    global items
    global nb_processed_items
    global processed_item_ids

    item_id = item["identifier"]
    items.remove(item)
    nb_processed_items += 1
    processed_item_ids.append(item_id)
    items[0] = dict(nb_processed_items=nb_processed_items, processed_item_ids=processed_item_ids)

def get_items_from_json_file(working_file: Path):
    items_to_processed_file = open(working_file, "r")
    items_to_processed = json.load(items_to_processed_file)
    items_to_processed_file.close()
    return items_to_processed