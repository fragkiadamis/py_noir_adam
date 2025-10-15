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
- **Study Labeling**: Automatically assign labels based on patient cohorts

### 3. Cohort-Based Labeling
Studies are automatically labeled based on subject membership in CSV files:
- `ican_subset.csv` → Studies labeled as "ican"
- `angptl6_subset.csv` → Studies labeled as "angptl6"

## Configuration

Create a `context.conf` file with Shanoir and Orthanc connection details:


## Usage

### Prerequisites
1. Configure `context.conf` with proper credentials
2. Prepare cohort CSV files with subject names
3. Ensure Orthanc PACS server is accessible

### Running the Script
From the project root directory, run: 
`python py_noir_code/projects/RHU_eCAN/main.py`