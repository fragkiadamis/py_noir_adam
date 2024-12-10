import os
import sys
import zipfile
import pydicom
from pydicom.uid import generate_uid
from pydicom import dcmread
from pynetdicom import AE
from pynetdicom.sop_class import MRImageStorage, XRayAngiographicImageStorage
import json
import argparse

sys.path.append( '../../')
sys.path.append( '../../py_noir/dataset')
from py_noir_code.src.API import api_service
from py_noir_code.src.shanoir_object.solr_query.solr_query_service import solr_search
from py_noir_code.src.shanoir_object.solr_query.solr_query_model import SolrQuery
from py_noir_code.src.shanoir_object.dataset.dataset_service import get_dataset_dicom_metadata, download_dataset


def create_arg_parser(description="""Shanoir downloader"""):
  parser = argparse.ArgumentParser(prog=__file__, description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  return parser

def add_username_argument(parser):
  parser.add_argument('-u', '--username', required=True, help='Your shanoir username.')

def add_domain_argument(parser):
  parser.add_argument('-d', '--domain', default='shanoir.irisa.fr', help='The shanoir domain to query.')

def add_common_arguments(parser):
  add_username_argument(parser)
  add_domain_argument(parser)

def add_configuration_arguments(parser):
  parser.add_argument('-c', '--configuration_folder', required=False, help='Path to the configuration folder containing proxy.properties (Tries to use ~/.su_vX.X.X/ by default). You can also use --proxy_url to configure the proxy (in which case the proxy.properties file will be ignored).')
  parser.add_argument('-pu', '--proxy_url', required=False, help='The proxy url in the format "user@host:port". The proxy password will be asked in the terminal. See --configuration_folder.')
  parser.add_argument('-ca', '--certificate', default='', required=False, help='Path to the CA bundle to use.')
  parser.add_argument('-v', '--verbose', default=False, action='store_true', help='Print log messages.')
  parser.add_argument('-t', '--timeout', type=float, default=60*4, help='The request timeout.')
  parser.add_argument('-lf', '--log_file', type=str, help="Path to the log file. Default is output_folder/downloads.log", default=None)
  parser.add_argument('-out', '--output_folder', type=str, help="Path to the result folder.", default=None)
  return parser

def add_subject_entries_argument(parser):
  parser.add_argument('-subjects', '--subjects_json', required=True, default='', help='path to the json structure of the subjects in a csv file')
  return parser

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

    # Check if SliceThickness is less than 0.5mm or between 100 and 500 (in case unity is not mm)
    if "00180050" in item and item["00180050"]["Value"] != [] and (float(item["00180050"]["Value"][0]) < slice_thickness or (float(item["00180050"]["Value"][0]) > 100 and float(item["00180050"]["Value"][0]) < 500)):
      thin_enough = True

    # if ("20011018" in item and item["20011018"]["Value"] != [] and int(item["20011018"]["Value"][0]) > 50) or ("00280008" in item and item["00280008"]["Value"] != [] and int(item["00280008"]["Value"][0]) > 50) or ("07A11002" in item and item["07A11002"]["Value"] != [] and int(item["07A11002"]["Value"][0]) > 50):
    #   enough_frames = True
  return is_tof and thin_enough #and enough_frames

def set_frame_of_reference_UID(workingFolder):
  for dirpath, dirnames, filenames in os.walk(workingFolder):
    if filenames:
      frame_of_reference_uid = generate_uid()
      for dicom_file in os.listdir(dirpath):
        dicom_file_path = os.path.join(dirpath, dicom_file)
        if os.path.isfile(dicom_file_path) and dicom_file.endswith('.dcm'):
          dcm = pydicom.dcmread(dicom_file_path)
          dcm.FrameOfReferenceUID = frame_of_reference_uid
          dcm.save_as(dicom_file_path)

def downloadDatasets(config, dataset_ids):
  for subject in dataset_ids:
    for dataset_id in dataset_ids[subject]:
      outFolder = config.output_folder + "/" + subject + "/" + str(dataset_id)
      os.makedirs(outFolder, exist_ok=True)
      download_dataset(config, dataset_id, 'dcm', outFolder, True)
      # Setting a common FrameOfReferenceUID metadata for all instances of a serie
      set_frame_of_reference_UID(outFolder)
      # C-Store the dicom files to the PACS
      for file_name in os.listdir(outFolder):
        if file_name.endswith('.dcm'):
          cStore_dataset(os.path.join(outFolder, file_name))

def getDatasets(subjects_entries):
  f = open(str(subjects_entries), "r")
  done = f.read()
  subjects = done.split("\n")

  query = SolrQuery()
  query.expert_mode = True
  query.search_text = ('subjectName:' + str(subjects)
                       .replace(',', ' OR ')
                       .replace('\'', '')
                       .replace("[", "(")
                       .replace("]", ")"))
  query.search_text = query.search_text + " AND datasetName:(*tof* OR *angio* OR *flight* OR *mra* OR *arm*)"

  result = solr_search(query)

  jsonresult = json.loads(result.content)

  dataset_ids = {}
  for dataset in jsonresult["content"]:
    metadata = get_dataset_dicom_metadata(dataset["datasetId"])
    if checkMetaData(metadata):
      subName = dataset["subjectName"]
      if (subName not in dataset_ids):
        dataset_ids[subName] = []
      dataset_ids[dataset["subjectName"]].append(dataset["datasetId"])

  print("Datasets to download: " + str(dataset_ids))

  downloadDatasets(config, dataset_ids)

def cStore_dataset(dicom_file_path):
  # Checking if dicom file is valid
  try:
    ds = dcmread(dicom_file_path)
  except Exception as e:
    print(f"Error reading the DICOM file {dicom_file_path} : {e}")
    return

  if assoc.is_established:
    status = assoc.send_c_store(ds)

    # Checking operation status
    if status and status.Status == 0x0000:
      return
    else:
      print(f"Sending the file {dicom_file_path} failed with status : {status.Status if status else 'Unknown'}")
    
  else:
    print("Unable to establish a connection with PACS.")
    try:
      assoc = ae.associate(pacs_ip, pacs_port, ae_title=pacs_ae_title)
    except Exception as e:
      print(f"Error establishing a connection with PACS : {e}")

if __name__ == '__main__':
  parser = create_arg_parser()
  add_common_arguments(parser)
  add_subject_entries_argument(parser)
  add_configuration_arguments(parser)
  args = parser.parse_args()

  config = api_service.initialize(args)

  # Orthanc PACS parameters
  pacs_ae_title = 'DCM4CHEE'
  pacs_ip = '127.0.0.1'
  pacs_port = 11112
  client_ae_title = 'ECAN_SCRIPT_AE'

  # Initialize PACS connexion
  ae = AE(ae_title=client_ae_title)

  # Add SOP class service for MR and X-Ray images
  ae.add_requested_context(MRImageStorage)
  ae.add_requested_context(XRayAngiographicImageStorage)

  assoc = ae.associate(pacs_ip, pacs_port, ae_title=pacs_ae_title)

  getDatasets(config, args.subjects_json)

  # Release the association with the PACS
  assoc.release()

### Fonction pour déduire le nombre d'images d'un dicom

# def count_slices(directory):
#     slices = []
#     for filename in os.listdir(directory):
#         if filename.endswith(".dcm"):
#             filepath = os.path.join(directory, filename)
#             ds = pydicom.dcmread(filepath)
#             if 'InstanceNumber' in ds:
#                 slices.append(ds.InstanceNumber)
#     return len(set(slices))

# dicom_directory = "/path/to/dicom/files"
# num_slices = count_slices(dicom_directory)
# print(f"Number of slices: {num_slices}")
