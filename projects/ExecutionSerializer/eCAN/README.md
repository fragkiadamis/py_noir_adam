# RHU eCAN Project

## Overview

The RHU eCAN project automates the processing and transfer of medical imaging datasets from Shanoir to an Orthanc PACS server. It handles batch execution of processing pipelines, DICOM dataset management, and study labeling based on patient cohorts.

## Project Structure
RHU_eCAN \
├── main.py # This file Main entry point \
├── ecan_json_generator.py # Generates execution JSON for batch processing \
├── dicom_dataset_manager.py # DICOM dataset operations (inspect, upload, label) \
├── context.conf # Configuration file for Shanoir and Orthanc \
├── ican_subset.csv # ICAN cohort subject names \
├── angptl6_subset.csv # ANGPTL6 cohort subject names \
└── README.md # This file


## Features

### 1. Batch Execution Management
- Generate JSON configurations for processing pipelines
- Initialize and resume execution workflows
- Track execution status and save progress

### 2. DICOM Dataset Management
- **Inspect Study Tags**: Analyze DICOM metadata and fix broken tags
- **C-STORE Upload**: Send DICOM files to PACS using C-STORE protocol
- **Orthanc REST API Upload**: Send DICOM files to PACS using the REST API protocol
- **Study Labeling**: Automatically assign labels based on patient cohorts
- **Auxiliary functions**: Auxiliary functions for Orthanc REST API

### 3. Cohort-Based Labeling
Studies are automatically labeled based on subject membership in CSV files:
- `ican_subset.csv` → Studies labeled as "ican"
- `angptl6_subset.csv` → Studies labeled as "angptl6"

## Configuration

Create a `context.conf` file with Shanoir, Orthanc and VIP connection details:
[API context]

scheme = https
domain = shanoir.irisa.fr
verify = True
timeout = None
proxies = {}
username = <username>
clientId = shanoir-uploader
access_token = None
refresh_token = None

[Orthanc context]

# Orthanc PACS configuration
pacs_ae_title=<PACS_AE_TITLE>
client_ae_title=<CLIENT_AE_TITLE>
dicom_server_port=4242

# REST API (Orthanc web interface)
scheme=http
domain=<localhost>
rest_api_port=8042
username=<username>

[Execution context]

# Do not exceed 3 without inquire, it can exceed allocated VIP resources
max_thread = 3
server_reboot_beginning_hour = 5
server_reboot_ending_hour = 7


## Usage

### Prerequisites
1. Configure `context.conf` with proper credentials
2. Prepare cohort CSV files with subject names
3. Ensure Orthanc PACS server is accessible
4. Passwords for Shanoir and Orthanc are required and demanded during execution

### Running the Script
From the py_noir root directory, run: 
`python py_noir_code/projects/RHU_eCAN/main.py`