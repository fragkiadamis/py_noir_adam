import os
import sys
import zipfile
import csv
import json
import argparse
from tqdm import tqdm
import shutil
from itertools import islice
import time
import argparse
import glob

import pydicom
from pydicom.uid import generate_uid
from pydicom import dcmread
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import MRImageStorage, XRayAngiographicImageStorage
from pydicom.dataset import Dataset, FileDataset
from pydicom.sequence import Sequence

sys.path.append( '../../..')
from py_noir_code.src.API import api_service
from py_noir_code.src.shanoir_object.solr_query.solr_query_service import solr_search
from py_noir_code.src.shanoir_object.solr_query.solr_query_model import SolrQuery
from py_noir_code.src.shanoir_object.dataset.dataset_service import get_dataset_dicom_metadata, download_dataset
from py_noir_code.src.shanoir_object.subject.subject_service import find_subject_ids_by_study_id
from py_noir_code.src.utils.context_utils import load_context


def create_arg_parser(description="""Shanoir downloader"""):
  parser = argparse.ArgumentParser(prog=__file__, description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  return parser

def add_username_argument(parser):
  parser.add_argument('-u', '--username', required=True, help='Your shanoir username.')

def add_configuration_arguments(parser):
  parser.add_argument('-c', '--configuration_folder', required=False, help='Path to the configuration folder containing proxy.properties (Tries to use ~/.su_vX.X.X/ by default). You can also use --proxy_url to configure the proxy (in which case the proxy.properties file will be ignored).')
  parser.add_argument('-pu', '--proxy_url', required=False, help='The proxy url in the format "user@host:port". The proxy password will be asked in the terminal. See --configuration_folder.')
  parser.add_argument('-ca', '--certificate', default='', required=False, help='Path to the CA bundle to use.')
  parser.add_argument('-v', '--verbose', default=False, action='store_true', help='Print log messages.')
  parser.add_argument('-t', '--timeout', type=float, default=60*4, help='The request timeout.')
  parser.add_argument('-lf', '--log_file', type=str, help="Path to the log file. Default is output_folder/downloads.log", default=None)
  parser.add_argument('-out', '--output_folder', type=str, help="Path to the result folder.", default=None)
  parser.add_argument('-l', '--limit', type=int, help="Number of datasets to upload.", default=None)
  return parser

def add_subject_entries_argument(parser):
  parser.add_argument('-subjects', '--subjects_csv', required=False, help='Path to the list of subjects in a csv file')
  parser.add_argument('-study', '--shanoir_study', type=str, required=False, default='', help='Shanoir study id to download')
  return parser

### Function to filter datasets based on their metadata
### Check if the metadata contains "tof" or "angio" or "flight" in ProtocolName or SeriesDescription
### Check if the slice thickness is less than 0.5mm or between 100 and 500 (in case unity is not mm but um)
### Check if the number of frames is greater than 50
def checkMetaData(metadata):
  if metadata is None :
    return False
  mri_types = ["tof","angio","angiography","time of flight","mra"]
  slice_thickness = 0.5
  is_tof = False
  thin_enough = False
  #enough_frames = False
  for item in metadata:
    # Check if ProtocolName or SeriesDescription contains "tof" or "angio" or "flight"
    if ('0008103E' in item and any(x in item['0008103E']["Value"][0].lower() for x in mri_types)) or ('00181030' in item and any(x in item['00181030']["Value"][0].lower() for x in mri_types)):
      is_tof = True

    # Check if SliceThickness is less than 0.5mm or between 100 and 500 (in case unity is not mm but um)
    if '00180050' in item and "Value" in item['00180050'] and (float(item['00180050']["Value"][0]) < slice_thickness or (float(item['00180050']["Value"][0]) > 99 and float(item['00180050']["Value"][0]) < slice_thickness*1000)):
      thin_enough = True

    # if ("20011018" in item and item["20011018"]["Value"] != [] and int(item["20011018"]["Value"][0]) > 50) or ("00280008" in item and item["00280008"]["Value"] != [] and int(item["00280008"]["Value"][0]) > 50) or ("07A11002" in item and item["07A11002"]["Value"] != [] and int(item["07A11002"]["Value"][0]) > 50):
    #   enough_frames = True
  return is_tof and thin_enough #and enough_frames

### Function to store trace of already sent datasets in a json file
def update_progress(progress, subject_id, dataset_id, progress_file):
    if subject_id not in progress:
        progress[subject_id] = []
    if dataset_id not in progress[subject_id]:
        progress[subject_id].append(dataset_id)
    with open(progress_file, 'w') as f:
      json.dump(progress, f, indent=2)

### Function to send a DICOM file to a distant PACS
def cStore_dataset(dicom_file_path, assoc):
  max_retries=3
  retry_delay=2
  attempts = 0
  while attempts < max_retries:
    if not assoc.is_established:
      print("🔄 Trying to associate with the PACS...")
      try:
        assoc = ae.associate(pacs_ip, pacs_port, ae_title=pacs_ae_title)
        if not assoc.is_established:
          raise Exception("Association failed")
      except Exception as e:
        print(f"⚠️ Error during association attempt with the PACS : {e}")
        attempts += 1
        time.sleep(retry_delay)
        continue

    print("✅ Association established, sending the file...")
    try:
      ds = dcmread(dicom_file_path)
    except Exception as e:
      print(f"Error reading the DICOM file {dicom_file_path} : {e}")
      return
    status = assoc.send_c_store(ds)

    if status and status.Status == 0x0000:
      print(f"✅ Sending the file {dicom_file_path}, status : {status.Status}")
      os.remove(dicom_file_path)
      return
    else:
      print(f"❌ Error sending the file {dicom_file_path}, status : {status.Status if status else 'Unknown'}")
      attempts += 1
      time.sleep(retry_delay)

  print("❌ Failure after multiple attempts.")

def modifyFieldValue(data, tag, field, newValue):
    if (tag in data):
        data[field] = newValue

def removeField(data, tag):
    if (tag in data):
        del data[tag]

def addField(data, tag, VR, value):
    data.add_new(tag, VR, value)

def retrieveItemsInSequence(data, sequenceTag, itemTag):
    if sequenceTag in data:
        sequence = data[sequenceTag].value

        # search for in the sequence
        for item in sequence:
            # check if the item exists
            if itemTag in item:
                return item

def getModifiedData(inputFileName, initialData):
    isNotModified = True

    if (isNotModified):
        outputFilename = inputFileName
        modified_data = initialData
    else:
        outputFilename = os.path.join(os.path.dirname(inputFileName), "modified", os.path.basename(inputFileName))

        # Create the directory if not existing
        os.makedirs(os.path.dirname(outputFilename), exist_ok=True)

        # create a copied file
        modified_data = Dataset()
        for elem in initialData:
            modified_data.add(elem)

        modified_data.file_meta = initialData.file_meta

    return outputFilename, modified_data

### Function that makes multiple corrections to the DICOM data
### to comply with eCAN requirements
def correcting_data(workingFolder):
    # modify all DICOM files from the dataset folder
    dcm_files = glob.glob(os.path.join(workingFolder, '*.dcm'))
    # we set a common FrameOfReferenceUID metadata for all instances of a serie
    frame_of_reference_uid = generate_uid()
    for dcm_file in dcm_files:
        dcm = pydicom.dcmread(dcm_file)
        dcm.FrameOfReferenceUID = frame_of_reference_uid
        outputFilename, modified_data = getModifiedData(dcm_file, dcm)

        # remove the sequence
        item = retrieveItemsInSequence(modified_data, (0x0040,0x0275), (0x0040,0x0008))
        if (item is not None):
            foundItem = item[(0x0040,0x0008)]
            if ((foundItem.VR == 'SQ') and ((len(foundItem.value) < 2) and (len(foundItem.value[0]) == 0))):
                removeField(item, (0x0040,0x0008))

        # save file
        modified_data.save_as(outputFilename)
        print(f"The DICOM data has been successfully modified in {outputFilename}")

### Function to retrieve the number of instances in a DICOM serie
def count_slices(directory):
    slices = []
    for filename in os.listdir(directory):
        if filename.endswith(".dcm"):
            filepath = os.path.join(directory, filename)
            ds = pydicom.dcmread(filepath)
            if 'InstanceNumber' in ds:
                slices.append(ds.InstanceNumber)
    return len(set(slices))

def downloadDatasets(dataset_ids, assoc, limit):
  # Counter in case of limit argument
  count = 0
  # We store the progress in a json file
  progress = {}
  progress_file = os.path.join(args.output_folder, "progress.json")

  # Load existing progress if the file exists
  if os.path.exists(progress_file):
    with open(progress_file, 'r') as f:
      progress = json.load(f)
      # Remove already processed datasets
      for subject in progress:
        if subject in dataset_ids:
          for dataset_id in progress[subject]:
            if dataset_id in dataset_ids[subject]:
              dataset_ids[subject].remove(dataset_id)
              print(f"Dataset {dataset_id} from subject {subject} has already been processed. Skipping...")
          if not dataset_ids[subject]:
            del dataset_ids[subject]
  # TODO : track progress if limit is not set
  progress_bar = tqdm(total=limit, desc="Downloading and sending datasets")

  for subject in dataset_ids:
    # Check if the limit has been reached with the previous subject
    if limit is not None and count >= limit:
      progress_bar.close()
      return
    subjFolder = args.output_folder + "/" + subject
    for dataset_id in dataset_ids[subject]:
      outFolder = args.output_folder + "/" + subject + "/" + str(dataset_id)
      os.makedirs(outFolder, exist_ok=True)
      download_dataset(dataset_id, 'dcm', outFolder, True)
      # We send the dicom files to the PACS if the number of slices is greater than 50
      if count_slices(outFolder) > 50:
        # Correcting dicom data to comply with eCAN requirements
        correcting_data(outFolder)
        # C-Store the dicom files to the PACS
        print(f"Initiating C-Store of dataset {str(dataset_id)}")
        for file_name in tqdm(os.listdir(outFolder), desc="Sending DICOM files to PACS"):
          if file_name.endswith('.dcm'):
            cStore_dataset(os.path.join(outFolder, file_name), assoc)
        # If the dataset folder is empty it means that all .dcm files have been sent to the PACS
        if not os.listdir(outFolder):
          print(f"Dataset {str(dataset_id)} has been successfully sent to the PACS")
          os.rmdir(outFolder)
          # Update progress
          update_progress(progress, subject, dataset_id, progress_file)
          if limit is not None:
            count += 1
            progress_bar.update(1)
            print(f"{count} dataset(s)/{limit} have been sent to the PACS.")
            if count >= limit:
              print(f"All {count} datasets have been sent to the PACS.")
              break
      else:
        shutil.rmtree(outFolder)
        # Update progress in case dataset not OK but still processed ???
    # We remove the subject folder if it is empty
    if not os.listdir(subjFolder):
      os.rmdir(subjFolder)

def chunk_list(iterable, chunk_size):
    """Chunk a list in sublists of size : `chunk_size`"""
    it = iter(iterable)
    while chunk := list(islice(it, chunk_size)):
        yield chunk

def getDatasets(subjects_entries, shanoir_study, limit, assoc):
  # Get the list of subjects from the csv file if specified or from shanoir study id
  if subjects_entries is not None:
    print(f"Source of data to upload is the following csv file : {str(subjects_entries)}")
    with open(str(subjects_entries), "r") as f:
      reader = csv.reader(f)
      subjects = [row[0].strip() for row in reader if row]
  elif shanoir_study is not None:
    print(f"Source of data to upload is {limit or 'all'} datasets from Shanoir study with id : {shanoir_study}")
    json_subjects_list = find_subject_ids_by_study_id(shanoir_study)
    subjects = [subject['name'] for subject in json_subjects_list]
  else:
    print("No source of data to upload specified.")
    return
  
  # Query SolR to get the datasets
  query = SolrQuery()
  query.size = 100000
  query.expert_mode = True
  full_result = {}
  for batch in chunk_list(subjects, 100):
    query.search_text = ('subjectName:' + str(batch)
                        .replace(',', ' OR')
                        .replace('\'', '')
                        .replace('^', '')
                        .replace("[", "(")
                        .replace("]", ")"))
    query.search_text = query.search_text + " AND datasetName:(*tof* OR *angio* OR *flight* OR *mra* OR *arm*) AND sliceThickness:[* TO 0.5]"
    #print(f"Executing SolR query : {query.search_text}")
    result = solr_search(query)
    batch_result = json.loads(result.content) 

    if isinstance(batch_result, dict) and "content" in batch_result:
        if "content" not in full_result:  
            full_result["content"] = []
        full_result["content"].extend(batch_result["content"])
    else:
        raise ValueError("Batch result format is unexpected")

  dataset_ids = {}
  for dataset in tqdm(full_result["content"], desc="Filtering datasets"):
    # We do not need to check metadata as it is done in the solR query
    # metadata = get_dataset_dicom_metadata(dataset["datasetId"])
    # if checkMetaData(metadata):
    #   subName = dataset["subjectName"]
    #   if (subName not in dataset_ids):
    #     dataset_ids[subName] = []
    #   dataset_ids[dataset["subjectName"]].append(dataset["datasetId"])
    subName = dataset["subjectName"]
    if (subName not in dataset_ids):
      dataset_ids[subName] = []
    dataset_ids[dataset["subjectName"]].append(dataset["datasetId"])
  datasets_nbr = sum(len(d) for d in dataset_ids.values())
  print("Number of datasets available: " + str(datasets_nbr))

  downloadDatasets(dataset_ids, assoc, limit)

if __name__ == '__main__':
  parser = create_arg_parser()
  add_username_argument(parser)
  add_subject_entries_argument(parser)
  add_configuration_arguments(parser)
  args = parser.parse_args()

  if args.subjects_csv and args.shanoir_study:
    print("Error : --subjects_csv and --shanoir_study cannot be used together.")
    sys.exit(1)

  load_context("context.conf", False)

  # Distant PACS parameters
  pacs_ae_title = 'ORTHANC'
  pacs_ip = '127.0.0.1'
  pacs_port = 4242
  client_ae_title = 'ECAN_SCRIPT_AE'

  # Initialize PACS connexion
  ae = AE(ae_title=client_ae_title)
  # Add SOP class service for MR and X-Ray images
  ae.add_requested_context(MRImageStorage)
  ae.add_requested_context(XRayAngiographicImageStorage)

  ae.acse_timeout = 30
  ae.network_timeout = 30

  # Activate pynetdicom additionnal logs
  #debug_logger()
  # Request an association with the PACS
  assoc = ae.associate(pacs_ip, pacs_port, ae_title=pacs_ae_title)

  getDatasets(args.subjects_csv, args.shanoir_study, args.limit, assoc)

  # Release the association with the PACS
  if assoc.is_established:
    assoc.release()
