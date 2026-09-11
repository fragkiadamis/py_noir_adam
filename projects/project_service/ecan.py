import re
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Optional
from collections import defaultdict

import pandas as pd
import pydicom

from src.execution.execution_service import get_execution_monitoring
from src.shanoir_object.dataset.dataset_service import get_examination, download_dataset, \
    find_processed_dataset_ids_by_input_dataset_id, download_dataset_processing, \
    upload_dataset_processing, sync_study_instance_uid, get_dataset
from src.shanoir_object.solr_query.solr_query_model import SolrQuery
from src.shanoir_object.solr_query.solr_query_service import solr_search
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.log_utils import get_logger
from src.utils.file_utils import get_items_from_input_file

logger = get_logger()

SOURCES = [
    # {"label": "ICAN", "source": "subject_names", "file": "ican_subset.txt", "study_name": "ICAN"},
    # {"label": "ANGPTL6", "source": "subject_names", "file": "angptl6_subset.txt", "study_name": "ICAN"},
    # {"label": "UCAN", "source": "subject_names", "file": "ucan_subset.txt", "study_name": "UCAN"},
    {"label": "RCAN", "source": "dataset_csv", "file": "rcan_tof_sans_aic_subset.csv", "study_name": "RCAN"},
]

MANIFEST_COLUMNS = [
    "dataset_id", "examination_id", "subject_id", "subject_name", "study_id",
    "examination_date", "label", "download_path",
    "num_slices", "slice_thickness", "valid", "reason",
]

TRACKING_COLUMNS = [
    "identifier", "dataset_id", "examination_id", "subject_id", "subject_name",
    "get_from_shanoir", "executable", "execution_requested", "execution_id",
    "execution_workflow_id", "execution_status", "execution_start_time",
    "execution_end_time", "label", "processing_id",
]

_DATE_KEYS = ("startDate", "processingDate", "creationDate", "importDate", "endDate")
_PIPELINE_KEYS = ("pipelineIdentifier", "name", "comment")


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
        if path.is_dir() and any(path.iterdir()):
            logger.info(f"Dataset {ds['dataset_id']} already downloaded at {path}; skipping.")
            rows.append({**ds, "download_path": str(path)})
            continue
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


def _find_acquisition_dir(root: Path, subject_id: str, examination_id: str) -> Optional[Path]:
    """Locate the existing <subject>/<examination>/<acquisition> directory, matched on the ID suffixes."""
    subject_dirs = [d for d in root.iterdir() if d.is_dir() and d.name.endswith(f"_{subject_id}")]
    if len(subject_dirs) != 1:
        logger.warning(f"Expected 1 subject directory ending in '_{subject_id}', found {len(subject_dirs)}.")
        return None

    exam_dirs = [d for d in subject_dirs[0].iterdir() if d.is_dir() and d.name.endswith(f"_{examination_id}")]
    if len(exam_dirs) != 1:
        logger.warning(f"Expected 1 examination directory ending in '_{examination_id}' under {subject_dirs[0].name}, found {len(exam_dirs)}.")
        return None

    acquisition_dirs = [d for d in exam_dirs[0].iterdir() if d.is_dir()]
    if len(acquisition_dirs) != 1:
        logger.warning(f"Expected 1 acquisition directory under {exam_dirs[0]}, found {len(acquisition_dirs)}: {[d.name for d in acquisition_dirs]}.")
        return None

    return acquisition_dirs[0]


def download_campaign_datasets(output_dir: Path, subject_prefix: str = "UCAN") -> None:
    tracking_csv = ConfigPath.resources_path / "ecan.csv"
    df = pd.read_csv(tracking_csv, dtype=str)

    rows = df[df["subject_name"].fillna("").str.upper().str.startswith(subject_prefix.upper())]
    if rows.empty:
        logger.error(f"No {subject_prefix}* subject found in {tracking_csv}.")
        return
    if not output_dir.is_dir():
        logger.error(f"Target directory {output_dir} does not exist; nothing to move the datasets into.")
        return
    logger.info(f"Found {len(rows)} {subject_prefix}* dataset(s) in {tracking_csv}.")

    downloaded, skipped, unmatched, failed = 0, 0, 0, 0
    for _, row in rows.iterrows():
        dataset_id = str(row["dataset_id"])
        acquisition_dir = _find_acquisition_dir(output_dir, str(row["subject_id"]), str(row["examination_id"]))
        if acquisition_dir is None:
            logger.warning(f"No existing directory for dataset {dataset_id} ({row['subject_name']}); skipping.")
            unmatched += 1
            continue

        try:
            dataset_name = get_dataset(dataset_id).get("name") or dataset_id
        except Exception as e:
            logger.error(f"Could not fetch the name of dataset {dataset_id}: {e}")
            failed += 1
            continue

        target = acquisition_dir / (re.sub(r'[/\\]', "_", dataset_name).strip() + ".zip")
        if target.exists():
            logger.info(f"Dataset {dataset_id} already present at {target}; skipping.")
            skipped += 1
            continue

        try:
            with tempfile.TemporaryDirectory() as tmp:
                download_dataset(dataset_id, "dcm", Path(tmp), unzip=False)
                produced = list(Path(tmp).iterdir())
                if len(produced) != 1:
                    raise RuntimeError(f"expected 1 downloaded file, got {[p.name for p in produced]}")
                shutil.move(str(produced[0]), target)
            downloaded += 1
            logger.info(f"Downloaded dataset {dataset_id} -> {target}")
        except Exception as e:
            logger.error(f"Failed to download dataset {dataset_id} ({row['subject_name']}): {e}")
            failed += 1

    logger.info(
        f"{subject_prefix} download complete: {downloaded} downloaded, {skipped} already present, "
        f"{unmatched} without a matching directory, {failed} failed -> {output_dir}"
    )


def fetch_processed_datasets(output_dir: Path) -> None:
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


def sync_examination_study_instance_uids() -> None:
    df = pd.read_csv(ConfigPath.tracking_file_path, dtype=str)
    examination_ids = df["examination_id"].dropna().unique().tolist()
    logger.info(f"Syncing StudyInstanceUID for {len(examination_ids)} examination(s)...")
    for examination_id in examination_ids:
        try:
            sync_study_instance_uid(examination_id)
            logger.info(f"Synced StudyInstanceUID for examination {examination_id}")
        except Exception as e:
            logger.error(f"Failed to sync StudyInstanceUID for examination {examination_id}: {e}")


def upload_processed_dataset(orthanc_output: Path) -> None:
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


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", "null"):
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        ts = float(value)
        if ts > 1e12:  # milliseconds since epoch
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "").split("+")[0])
        except ValueError:
            return None
    return None


def _first_value(obj: Optional[Dict], keys) -> Any:
    for k in keys:
        if isinstance(obj, dict) and obj.get(k) not in (None, ""):
            return obj[k]
    return None


def _processing_datetime(processing: Dict, monitoring: Dict) -> Optional[datetime]:
    for source in (monitoring, processing):
        dt = _coerce_datetime(_first_value(source, _DATE_KEYS))
        if dt is not None:
            return dt
    return None


def _pipeline_text(processing: Dict, monitoring: Dict) -> str:
    parts = [str(_first_value(source, _PIPELINE_KEYS) or "") for source in (monitoring, processing)]
    return " ".join(p for p in parts if p)


def backfill_tracking_from_processings(days: int, pipeline_filter: str) -> None:
    manifest = ConfigPath.output_path / "ecan" / "download_manifest.csv"
    df = pd.read_csv(manifest, dtype=str)
    datasets = df[df["valid"] == "True"] if "valid" in df.columns else df
    cutoff = datetime.now() - timedelta(days=days)
    logger.info(
        f"Backfilling from {len(datasets)} manifest dataset(s); keeping "
        f"'{pipeline_filter}' processings since {cutoff:%Y-%m-%d %H:%M}."
    )

    tracking = pd.read_csv(ConfigPath.tracking_file_path, dtype=str) if \
        ConfigPath.tracking_file_path.stat().st_size else pd.DataFrame(columns=TRACKING_COLUMNS)
    known_proc = set(tracking["processing_id"].dropna()) if "processing_id" in tracking.columns else set()
    ident_col = tracking["identifier"].dropna() if "identifier" in tracking.columns else pd.Series(dtype=str)
    next_id = int(ident_col.astype(int).max()) + 1 if not ident_col.empty else 1

    rows = []
    for _, d in datasets.iterrows():
        dataset_id = d["dataset_id"]
        processings = find_processed_dataset_ids_by_input_dataset_id(dataset_id) or []

        for p in processings:
            pid = str(p.get("id"))
            execution_id = p.get("parentId")
            monitoring = get_execution_monitoring(str(execution_id)) or {}

            if pipeline_filter.lower() not in _pipeline_text(p, monitoring).lower():
                continue
            dt = _processing_datetime(p, monitoring)
            if dt is None:
                logger.warning(f"No date found for processing {pid} (dataset {dataset_id}); skipping.")
                continue
            if dt < cutoff:
                continue
            if pid in known_proc:
                logger.info(f"Processing {pid} already tracked; skipping.")
                continue

            start_dt = _coerce_datetime(_first_value(monitoring, ("startDate",))) or dt
            end_dt = start_dt + timedelta(minutes=3) if start_dt else None
            rows.append({
                "identifier": next_id,
                "dataset_id": dataset_id,
                "examination_id": d.get("examination_id", ""),
                "subject_id": d.get("subject_id", ""),
                "subject_name": d.get("subject_name", ""),
                "get_from_shanoir": True,
                "executable": True,
                "execution_requested": True,
                "execution_id": execution_id,
                "execution_workflow_id": _first_value(monitoring, ("identifier",)),
                "execution_status": _first_value(monitoring, ("status",)),
                "execution_start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S") if start_dt else "",
                "execution_end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else "",
                "label": d.get("label", ""),
                "processing_id": pid,
            })
            known_proc.add(pid)
            next_id += 1
            logger.info(f"Matched processing {pid} (exec {execution_id}) for dataset "
                        f"{dataset_id} @ {dt:%Y-%m-%d}.")

    if not rows:
        logger.warning("No matching processings found; nothing appended.")
        return

    combined = pd.concat([tracking, pd.DataFrame(rows)], ignore_index=True)
    combined = combined.reindex(columns=TRACKING_COLUMNS + [c for c in combined.columns if c not in TRACKING_COLUMNS])
    combined.to_csv(ConfigPath.tracking_file_path, index=False)
    logger.info(f"Appended {len(rows)} row(s) to {ConfigPath.tracking_file_path}.")


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
