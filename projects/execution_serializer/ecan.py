import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict
from collections import defaultdict

import pandas as pd
import typer
import pydicom

from src.shanoir_object.dataset.dataset_service import get_examination, download_dataset, \
    find_processed_dataset_ids_by_input_dataset_id, download_dataset_processing, \
    upload_dataset_processing, sync_study_instance_uid
from src.shanoir_object.solr_query.solr_query_model import SolrQuery
from src.shanoir_object.solr_query.solr_query_service import solr_search
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.dicom_utils import inspect_and_fix_study_tags, check_dicom_consistency
from src.utils.pacs_utils import upload_to_pacs_rest, upload_to_pacs_dicom, assign_label_to_pacs_study, \
    download_from_pacs_rest, delete_studies_from_pacs, purge_pacs_studies, delete_mip_first_instances, \
    get_patient_ids_from_pacs, get_orthanc_study_details, log_mr_series_instance_counts, create_series_export
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.utils.serializer_utils import init_serialization
from src.utils.mip_detector import delete_first_slice_if_mip

app = typer.Typer()
logger = get_logger()


def _fetch_processed_datasets(output_dir: Path) -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    df["processing_id"] = pd.Series(dtype="Int64")
    dataset_pairs_list = [{
        "input_dataset_id": row["dataset_id"],
        "execution_id": row["execution_id"]
    } for _, row in df.iterrows() if row["execution_status"] == "Finished"]

    processing_ids_list = []
    for dataset_pair in dataset_pairs_list:
        processing_list = find_processed_dataset_ids_by_input_dataset_id(dataset_pair["input_dataset_id"])
        processing_id = next(
            (item["id"] for item in processing_list if str(item["parentId"]) == dataset_pair["execution_id"]),
            None
        )
        if processing_id is None:
            continue
        processing_ids_list.append(processing_id)
        df.loc[df["dataset_id"] == dataset_pair["input_dataset_id"], "processing_id"] = processing_id
        df.to_csv(ConfigPath.tracking_file_path, index=False)

    output_dir.mkdir(parents=True, exist_ok=True)
    download_dataset_processing(processing_ids_list, output_dir, unzip=True)


def _sync_examination_study_instance_uids() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    examination_ids = df["examination_id"].dropna().unique().tolist()
    logger.info(f"Syncing StudyInstanceUID for {len(examination_ids)} examination(s)...")
    for examination_id in examination_ids:
        try:
            sync_study_instance_uid(examination_id)
            logger.info(f"Synced StudyInstanceUID for examination {examination_id}")
        except Exception as e:
            logger.error(f"Failed to sync StudyInstanceUID for examination {examination_id}: {e}")


def _upload_processed_dataset(orthanc_output: Path) -> None:
    for dcm_path in orthanc_output.rglob("*.dcm"):
        ds = pydicom.dcmread(dcm_path)
        if ds.Modality not in ("SR", "SEG"):
            continue
        with open(dcm_path, "rb") as f:
            dicom_bytes = f.read()
        success = upload_dataset_processing(dicom_bytes)
        if success:
            logger.info(f"Successfully uploaded {dcm_path.name} to Shanoir.")
        else:
            logger.warning(f"Failed to upload {dcm_path.name} to Shanoir.")


def query_datasets(subject_name_list: List) -> defaultdict[Any, defaultdict[Any, List]]:
    logger.info("Searching for subjects' datasets...")
    query = SolrQuery()
    query.size = 100000
    query.expert_mode = True
    query.search_text = f'subjectName: ("{subject_name_list[0]}"'
    for subject in subject_name_list[1:]:
        query.search_text += f' OR "{subject}"'
    query.search_text += ') AND datasetName:*TOF*'
    result = solr_search(query).json()

    subjects_datasets = defaultdict(lambda: defaultdict(list))
    for item in result["content"]:
        subjects_datasets[item.get("subjectName")][str(item.get("examinationId"))].append(item)

    return subjects_datasets


def filter_datasets_by_study(subjects_datasets: defaultdict, study_name: str) -> defaultdict:
    for subject, exam_items in subjects_datasets.items():
        for key in list(exam_items.keys()):
            exam_items[key] = [
                ds for ds in exam_items[key]
                if ds.get("studyName") == study_name
            ]
            if not exam_items[key]:
                del exam_items[key]
    return subjects_datasets


def download_and_filter_datasets(subjects_datasets: defaultdict[Any, defaultdict[Any, List]], download_dir: Path) -> List:
    filtered_datasets = []
    for idx, (subject, exam_items) in enumerate(subjects_datasets.items(), start=1):
        for key in list(exam_items.keys()):
            for ds in exam_items[key][:]:
                dataset_download_path = download_dir / subject / str(ds["examinationId"]) / ds["id"]
                # Use this if no need to download again the files.
                # if dataset_download_path.exists():
                #     filtered_datasets.append(ds)
                dataset_download_path.mkdir(parents=True, exist_ok=True)
                download_dataset(ds["id"], "dcm", dataset_download_path, unzip=True)
                first_file = next(p for p in dataset_download_path.iterdir() if p.is_file())
                slice_thickness = pydicom.dcmread(first_file).get('SliceThickness')
                num_of_slices = sum(1 for p in dataset_download_path.iterdir() if p.is_file() and p.suffix == ".dcm")
                if num_of_slices > 50 and (slice_thickness is not None and slice_thickness < 10):
                    filtered_datasets.append(ds)
                else:
                    shutil.rmtree(dataset_download_path)
                    if not any(dataset_download_path.parent.iterdir()):
                        dataset_download_path.parent.rmdir()

    return filtered_datasets


def keep_one_acquisition(download_dir: Path, filtered_datasets: List) -> List:
    deleted_ids = set()
    for subject_dir in download_dir.iterdir():
        for exam_dir in subject_dir.iterdir():
            dataset_dirs = sorted([d for d in exam_dir.iterdir() if d.is_dir()], key=lambda d: int(d.name))
            if len(dataset_dirs) > 1:
                for d in dataset_dirs[1:]:
                    logger.info(f"Deleting: {d}")
                    deleted_ids.add(d.name)
                    shutil.rmtree(d)

    return [ds for ds in filtered_datasets if str(ds["id"]) not in deleted_ids]


def keep_oldest_examination(download_dir: Path, filtered_datasets: List) -> List:
    deleted_exam_ids = set()
    for subject_dir in download_dir.iterdir():
        exam_dirs = [d for d in subject_dir.iterdir() if d.is_dir()]
        if len(exam_dirs) <= 1:
            continue

        oldest_exam_id, oldest_date = None, None
        for exam_dir in exam_dirs:
            exam = get_examination(exam_dir.name)
            date_str = exam["examinationDate"].replace("Z", "").split("+")[0]
            exam_date = datetime.fromisoformat(date_str)
            if oldest_date is None or exam_date < oldest_date:
                oldest_exam_id = exam_dir.name
                oldest_date = exam_date

        for exam_dir in exam_dirs:
            if exam_dir.name != oldest_exam_id:
                logger.info(f"Deleting examination: {exam_dir}")
                deleted_exam_ids.add(exam_dir.name)
                shutil.rmtree(exam_dir)

    return [ds for ds in filtered_datasets if str(ds["examinationId"]) not in deleted_exam_ids]


def generate_json(output_dir: Path) -> List[Dict]:
    sources = [
        ("ican_subset.txt", "ICAN", "ICAN"),
        ("angptl6_subset.txt", "ICAN", "ANGPTL6"),
        ("ucan_subset.txt", "UCAN", "UCAN"),
    ]

    idx, executions = 0, []
    for filename, study_name, batch_label in sources:
        subject_list = [*get_items_from_input_file(filename)]
        if not subject_list:
            continue

        subjects_datasets = query_datasets(subject_list)
        filtered_datasets = filter_datasets_by_study(subjects_datasets, study_name)
        filtered_datasets = download_and_filter_datasets(filtered_datasets, output_dir)
        filtered_datasets = keep_one_acquisition(output_dir, filtered_datasets)
        filtered_datasets = keep_oldest_examination(output_dir, filtered_datasets)

        logger.info(f"Building json content for label {batch_label}...")
        for dataset in filtered_datasets:
            idx += 1
            df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
            values = {
                "identifier": idx,
                "dataset_id": dataset["id"],
                "examination_id": dataset["examinationId"],
                "subject_id": dataset["subjectId"],
                "subject_name": dataset["subjectName"],
                "get_from_shanoir": True,
                "executable": True,
                "label": batch_label,
            }
            for col, val in values.items():
                df.loc[idx - 1, col] = val
            df.to_csv(ConfigPath.tracking_file_path, index=False)

            dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
            executions.append({
                "identifier": idx,
                "name": f"landmarkDetection_0_7_exam_{dataset['examinationId']}_{dt}",
                "pipelineIdentifier": "landmarkDetection/0.7",
                "studyIdentifier": dataset["studyId"],
                "inputParameters": {},
                "outputProcessing": "",
                "processingType": "SEGMENTATION",
                "refreshToken": APIConfig.refresh_token,
                "client": APIConfig.clientId,
                "datasetParameters": [{
                    "datasetIds": [dataset["id"]],
                    "groupBy": "EXAMINATION",
                    "name": "dicom_input_zip",
                    "exportFormat": "dcm"
                }],
            })

    return executions


@app.callback()
def explain() -> None:
    """
    \b
    eCAN pipeline CLI.

    Commands:
      execute            — query TOF datasets, filter (oldest exam, oldest acquisition, >=50 slices, <10mm), launch VIP executions
      populate-orthanc   — download VIP output, remove MIP slices, fix DICOM tags, upload to Orthanc, assign Orthanc label
      delete-mip-orthanc — delete MIP instances from already-uploaded Orthanc studies
      import-shanoir     — sync UIDs, download from Orthanc, check DICOM consistency, upload SEG/SR to Shanoir
      debug-orthanc      — log patients, study details, MR series instance counts

    Usage:
      uv run main.py ecan execute
      uv run main.py ecan populate-orthanc
      uv run main.py ecan delete-mip-orthanc
      uv run main.py ecan import-shanoir
      uv run main.py ecan debug-orthanc
    """


@app.command()
def execute() -> None:
    initiate_working_files("ecan")
    init_serialization(generate_json, kwargs={"output_dir": ConfigPath.output_path / "ecan" / "shanoir_output"})


@app.command()
def populate_orthanc() -> None:
    initiate_working_files("ecan")
    vip_output = ConfigPath.output_path / "ecan" / "vip_output"
    _fetch_processed_datasets(vip_output)
    delete_first_slice_if_mip(vip_output)
    inspect_and_fix_study_tags(vip_output)
    upload_to_pacs_rest(vip_output) # for REST API
    # upload_to_pacs_dicom(vip_output) # For dicom web store
    assign_label_to_pacs_study()


@app.command()
def import_shanoir() -> None:
    initiate_working_files("ecan")
    orthanc_output = ConfigPath.output_path / "ecan" / "orthanc_output"
    _sync_examination_study_instance_uids()
    download_from_pacs_rest(orthanc_output)
    check_dicom_consistency(orthanc_output)
    _upload_processed_dataset(orthanc_output)


@app.command()
def delete_mip_orthanc() -> None:
    initiate_working_files("ecan")
    delete_mip_first_instances()


@app.command()
def debug_orthanc() -> None:
    initiate_working_files("ecan")
    get_patient_ids_from_pacs()
    get_orthanc_study_details()
    log_mr_series_instance_counts()
    # create_series_export()


# ------------------- DANGER ZONE -------------------
# @app.command()
# def delete_studies() -> None:
#     initiate_working_files("ecan")
#     delete_studies_from_pacs()
#     purge_pacs_studies()
# ------------------- DANGER ZONE -------------------
