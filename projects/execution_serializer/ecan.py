import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict, Optional
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
from src.utils.dicom_utils import run_compliance_fixes
from src.utils.dicom_utils import inspect_and_fix_study_tags, check_dicom_consistency
from src.utils.pacs_utils import upload_to_pacs_rest, upload_to_pacs_dicom, assign_label_to_pacs_study, \
    download_from_pacs_rest, delete_studies_from_pacs, delete_mip_first_instances, get_orthanc_study_details, \
    log_mr_series_instance_counts, create_series_export, update_tracking_ids
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.utils.serializer_utils import init_serialization
from src.utils.mip_detector import delete_first_slice_if_mip

app = typer.Typer()
logger = get_logger()

SOURCES = [
    {"label": "ICAN", "source": "subject_names", "file": "ican_subset.txt", "study_name": "ICAN"},
    {"label": "ANGPTL6", "source": "subject_names", "file": "angptl6_subset.txt", "study_name": "ICAN"},
    {"label": "UCAN", "source": "subject_names", "file": "ucan_subset.txt", "study_name": "UCAN"},
    {"label": "RCAN", "source": "dataset_csv", "file": "rcan_tof_sans_aic_subset.csv", "study_name": "RCAN"},
]

MANIFEST_COLUMNS = [
    "dataset_id", "examination_id", "subject_id", "subject_name", "study_id",
    "examination_date", "label", "download_path",
    "num_slices", "slice_thickness", "valid", "reason",
]


def resolve_from_subject_names(file: str, study_name: str, label: str) -> List[Dict]:
    subject_list = [*get_items_from_input_file(file)]
    if not subject_list:
        return []

    subjects_datasets = filter_datasets_by_study(query_datasets(subject_list), study_name)
    records = []
    for subject, exam_items in subjects_datasets.items():
        for exam_id, items in exam_items.items():
            for ds in items:
                records.append({
                    "dataset_id": str(ds["id"]),
                    "examination_id": str(ds["examinationId"]),
                    "subject_id": str(ds["subjectId"]),
                    "subject_name": ds["subjectName"],
                    "study_id": str(ds["studyId"]),
                    "examination_date": ds.get("examinationDate", ""),
                    "label": label,
                })

    # Solr docs don't reliably carry the examination date; backfill it once per
    # examination so the validate stage can run without any API calls.
    for exam_id in {r["examination_id"] for r in records if not r["examination_date"]}:
        try:
            date = get_examination(exam_id)["examinationDate"].replace("Z", "").split("+")[0]
        except Exception as e:
            logger.warning(f"Could not fetch examinationDate for examination {exam_id}: {e}")
            date = ""
        for r in records:
            if r["examination_id"] == exam_id:
                r["examination_date"] = date

    return records


def resolve_from_dataset_csv(file: str, label: str) -> List[Dict]:
    df = pd.read_csv(ConfigPath.input_path / file, dtype=str, sep=";")
    return [{
        "dataset_id": row["datasetId"],
        "examination_id": row["examinationId"],
        "subject_id": row["subjectId"],
        "subject_name": row["commonName"],
        "study_id": row["studyId"],
        "examination_date": row.get("examinationDate", ""),
        "label": label,
    } for _, row in df.iterrows()]


def resolve_sources() -> List[Dict]:
    records: List[Dict] = []
    for src in SOURCES:
        if src["source"] == "subject_names":
            resolved = resolve_from_subject_names(src["file"], src["study_name"], src["label"])
        elif src["source"] == "dataset_csv":
            resolved = resolve_from_dataset_csv(src["file"], src["label"])
        else:
            logger.warning(f"Unknown source type '{src['source']}' for label {src['label']}; skipping.")
            continue
        logger.info(f"Resolved {len(resolved)} dataset(s) for label {src['label']}.")
        records += resolved
    return records


def download_records(records: List[Dict], download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for ds in records:
        path = download_dir / str(ds["subject_name"]) / str(ds["examination_id"]) / str(ds["dataset_id"])
        path.mkdir(parents=True, exist_ok=True)
        download_dataset(ds["dataset_id"], "dcm", path, unzip=True)
        rows.append({**ds, "download_path": str(path)})
        logger.info(f"Downloaded dataset {ds['dataset_id']} -> {path}")

    df = pd.DataFrame(rows).reindex(columns=MANIFEST_COLUMNS)
    manifest = ConfigPath.output_path / "ecan" / "download_manifest.csv"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(manifest, index=False)
    logger.info(f"Wrote download manifest with {len(df)} row(s) to {manifest}")


def _check_dicom_qc(path: Path):
    dcm_files = [p for p in path.iterdir() if p.is_file() and p.suffix == ".dcm"] if path.exists() else []
    if not dcm_files:
        return False, "no DICOM files", 0, ""

    num_slices = len(dcm_files)
    thickness = pydicom.dcmread(dcm_files[0]).get("SliceThickness")
    if num_slices > 50 and thickness is not None and float(thickness) < 10:
        return True, "", num_slices, thickness

    reasons = []
    if num_slices <= 50:
        reasons.append(f"slices={num_slices}<=50")
    if thickness is None:
        reasons.append("no SliceThickness")
    elif float(thickness) >= 10:
        reasons.append(f"thickness={thickness}>=10")
    return False, "; ".join(reasons), num_slices, thickness


def _mark_keep_one_acquisition(df: pd.DataFrame) -> None:
    for exam_id, group in df[df["valid"] == "True"].groupby("examination_id"):
        ids = sorted(group["dataset_id"], key=lambda x: int(x))
        for did in ids[1:]:
            df.loc[df["dataset_id"] == did, ["valid", "reason"]] = \
                ["False", f"duplicate acquisition (kept {ids[0]})"]


def _mark_keep_oldest_examination(df: pd.DataFrame) -> None:
    def parse(date_str: str) -> datetime:
        try:
            return datetime.fromisoformat(str(date_str).replace("Z", "").split("+")[0])
        except Exception:
            return datetime.max

    for subject, group in df[df["valid"] == "True"].groupby("subject_name"):
        exam_dates = {r["examination_id"]: parse(r["examination_date"]) for _, r in group.iterrows()}
        if len(exam_dates) <= 1:
            continue
        oldest = min(exam_dates, key=exam_dates.get)
        for did in group.loc[group["examination_id"] != oldest, "dataset_id"]:
            df.loc[df["dataset_id"] == did, ["valid", "reason"]] = \
                ["False", f"not oldest examination (kept {oldest})"]


def validate_manifest() -> None:
    manifest = ConfigPath.output_path / "ecan" / "download_manifest.csv"
    df = pd.read_csv(manifest, dtype=str)
    if df.empty:
        logger.warning(f"Manifest {manifest} is empty; nothing to validate.")
        return

    for idx, row in df.iterrows():
        valid, reason, num_slices, thickness = _check_dicom_qc(Path(row["download_path"]))
        df.loc[idx, ["num_slices", "slice_thickness", "valid", "reason"]] = \
            [num_slices, thickness, str(valid), reason]

    _mark_keep_one_acquisition(df)
    _mark_keep_oldest_examination(df)

    rejected = df[df["valid"] != "True"]
    for _, r in rejected.iterrows():
        logger.info(
            f"Non-conforming: dataset {r['dataset_id']} "
            f"(subject {r['subject_name']}, exam {r['examination_id']}) -- {r['reason']}"
        )

    # Everything downstream only needs the manifest, so clear the environment:
    # drop all downloaded DICOMs and keep only the conforming rows.
    download_dir = ConfigPath.output_path / "ecan" / "shanoir_output"
    if download_dir.exists():
        shutil.rmtree(download_dir)
        logger.info(f"Deleted downloaded data at {download_dir}")

    kept = df[df["valid"] == "True"]
    kept.to_csv(manifest, index=False)
    logger.info(
        f"Validated {len(df)} dataset(s): {len(kept)} conforming kept, "
        f"{len(rejected)} non-conforming dropped. Manifest pruned at {manifest}"
    )


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


def generate_json(_: Optional[Path] = None) -> List[Dict]:
    manifest = ConfigPath.output_path / "ecan" / "download_manifest.csv"
    df = pd.read_csv(manifest, dtype=str)
    valid_datasets = df[df["valid"] == "True"]
    logger.info(f"Building json content for {len(valid_datasets)} validated dataset(s)...")

    idx, executions = 0, []
    for _, dataset in valid_datasets.iterrows():
        idx += 1
        tracking_df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
        values = {
            "identifier": idx,
            "dataset_id": dataset["dataset_id"],
            "examination_id": dataset["examination_id"],
            "subject_id": dataset["subject_id"],
            "subject_name": dataset["subject_name"],
            "get_from_shanoir": True,
            "executable": True,
            "label": dataset["label"],
        }
        for col, val in values.items():
            tracking_df.loc[idx - 1, col] = val
        tracking_df.to_csv(ConfigPath.tracking_file_path, index=False)

        dt = datetime.now().strftime('%F_%H%M%S%f')[:-3]
        executions.append({
            "identifier": idx,
            "name": f"landmarkDetection_0_8_exam_{dataset['examination_id']}_{dt}",
            "pipelineIdentifier": "landmarkDetection/0.8",
            "studyIdentifier": dataset["study_id"],
            "inputParameters": {},
            "outputProcessing": "",
            "processingType": "SEGMENTATION",
            "refreshToken": APIConfig.refresh_token,
            "client": APIConfig.clientId,
            "datasetParameters": [{
                "datasetIds": [dataset["dataset_id"]],
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
      download           — resolve all sources (subject-name lists via Solr, dataset CSVs directly), download DICOMs, write download manifest (no QC)
      validate           — read the manifest, apply DICOM QC + keep-oldest-exam/one-acquisition, flag non-conforming rows (non-destructive)
      execute            — query TOF datasets, filter (oldest exam, oldest acquisition, >=50 slices, <10mm), launch VIP executions
      populate-orthanc   — download VIP output, remove MIP slices, fix DICOM tags, upload to Orthanc, assign Orthanc label
      delete-mip-orthanc — delete MIP instances from already-uploaded Orthanc studies
      import-shanoir     — sync UIDs, download from Orthanc, check DICOM consistency, upload SEG/SR to Shanoir
      debug-orthanc      — log patients, study details, MR series instance counts

    Usage:
      uv run main.py ecan download
      uv run main.py ecan validate
      uv run main.py ecan execute
      uv run main.py ecan populate-orthanc
      uv run main.py ecan delete-mip-orthanc
      uv run main.py ecan import-shanoir
      uv run main.py ecan debug-orthanc
    """


@app.command()
def download() -> None:
    initiate_working_files("ecan")
    download_dir = ConfigPath.output_path / "ecan" / "shanoir_output"
    records = resolve_sources()
    logger.info(f"Resolved {len(records)} dataset(s) across {len(SOURCES)} source(s).")
    download_records(records, download_dir)


@app.command()
def validate() -> None:
    initiate_working_files("ecan")
    validate_manifest()


@app.command()
def execute() -> None:
    initiate_working_files("ecan")
    init_serialization(generate_json)


@app.command()
def dicom_compliance() -> None:
    initiate_working_files("ecan")
    vip_output = ConfigPath.output_path / "ecan" / "vip_output"
    _fetch_processed_datasets(vip_output)
    delete_first_slice_if_mip(vip_output)
    inspect_and_fix_study_tags(vip_output)
    run_compliance_fixes(vip_output, vip_output.parent / "vip_output_corrected")


@app.command()
def populate_orthanc() -> None:
    initiate_working_files("ecan")
    vip_output = ConfigPath.output_path / "ecan" / "vip_output"
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
def sync_tracking_file() -> None:
    initiate_working_files("ecan")
    vip_output = ConfigPath.output_path / "ecan" / "vip_output"
    update_tracking_ids(vip_output)


@app.command()
def orthanc_remove_mips() -> None:
    initiate_working_files("ecan")
    delete_mip_first_instances()


@app.command()
def debug_orthanc() -> None:
    initiate_working_files("ecan")
    get_orthanc_study_details(from_tracking=True)
    log_mr_series_instance_counts()
    # create_series_export()


# ------------------- DANGER ZONE -------------------
# @app.command()
# def delete_studies() -> None:
#     initiate_working_files("ecan")
#     delete_studies_from_pacs(from_tracking=True)
# ------------------- DANGER ZONE -------------------
