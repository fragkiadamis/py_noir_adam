import os
import sys

sys.path.append( '../../')
sys.path.append('../shanoir_object/dataset')
import pydicom
from pydicom.uid import generate_uid
import json
import argparse
from py_noir.src.API import api_service
from projects.shanoir_object.solr_query.model import SolrQuery
from projects.shanoir_object.solr_query import service
from projects.shanoir_object.dataset.dataset_service import get_dataset_dicom_metadata, download_dataset


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

    if "00180050" in item and item["00180050"]["Value"] != [] and float(item["00180050"]["Value"][0]) < slice_thickness:
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
      set_frame_of_reference_UID(outFolder)

def getDatasets(config, subjects_entries):
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

  result = service.solr_search(config, query)

  jsonresult = json.loads(result.content)

  dataset_ids = {}
  for dataset in jsonresult["content"]:
    metadata = get_dataset_dicom_metadata(config, dataset["datasetId"])
    if checkMetaData(metadata):
      subName = dataset["subjectName"]
      if (subName not in dataset_ids):
        dataset_ids[subName] = []
      dataset_ids[dataset["subjectName"]].append(dataset["datasetId"])

  print("Datasets to download: " + str(dataset_ids))

  downloadDatasets(config, dataset_ids)

if __name__ == '__main__':
  parser = create_arg_parser()
  add_common_arguments(parser)
  add_subject_entries_argument(parser)
  add_configuration_arguments(parser)
  args = parser.parse_args()

  config = api_service.initialize(args)
  getDatasets(config, args.subjects_json)

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
