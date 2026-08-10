import typer

from src.utils.config_utils import ConfigPath
from src.utils.dicom_utils import run_compliance_fixes, inspect_and_fix_study_tags, check_dicom_consistency
from src.utils.pacs_utils import upload_to_pacs_rest, assign_label_to_pacs_study, \
    download_from_pacs_rest, delete_mip_first_instances, get_orthanc_study_details, \
    log_mr_series_instance_counts, update_tracking_ids, create_series_export
from src.utils.log_utils import get_logger
from src.utils.file_utils import initiate_working_files
from src.utils.serializer_utils import init_serialization
from src.utils.mip_detector import delete_first_slice_if_mip

from projects.project_service.ecan import SOURCES, resolve_sources, \
    download_records, validate_manifest, generate_json, fetch_processed_datasets, \
    sync_examination_study_instance_uids, upload_processed_dataset, \
    backfill_tracking_from_processings, download_campaign_datasets

app = typer.Typer()
logger = get_logger()


@app.callback()
def explain() -> None:
    """
    \b
    eCAN pipeline CLI.

    Commands:
      download                — resolve all sources (subject-name lists via Solr, dataset CSVs directly), download DICOMs, write download manifest (no QC)
      validate                — read the manifest, apply DICOM QC + keep-oldest-exam/one-acquisition, flag non-conforming rows (non-destructive)
      execute                 — query TOF datasets, filter (oldest exam, oldest acquisition, >=50 slices, <10mm), launch VIP executions
      populate-orthanc        — download VIP output, remove MIP slices, fix DICOM tags, upload to Orthanc, assign Orthanc label
      delete-mip-orthanc      — delete MIP instances from already-uploaded Orthanc studies
      import-shanoir          — sync UIDs, download from Orthanc, check DICOM consistency, upload SEG/SR to Shanoir
      debug-orthanc           — log patients, study details, MR series instance counts
      backfill-tracking       — reconcile ecan.csv by registering already-run processings (last N days) from the manifest
      create-campaign-results — export all Orthanc series of UCAN* subjects (excluding MILVUE) to input/result_campaign_ucan.csv
      download-campaign-results — download the UCAN* datasets listed in resources/ecan.csv into output/UCAN_CAMPAIGN_RESULTS/<subject>/<examination>/<dataset>
      create-campaign-results-subjects — same export as create-campaign-results, restricted to an explicit list of subjects

    Usage:
      uv run main.py ecan download
      uv run main.py ecan validate
      uv run main.py ecan execute
      uv run main.py ecan populate-orthanc
      uv run main.py ecan delete-mip-orthanc
      uv run main.py ecan import-shanoir
      uv run main.py ecan debug-orthanc
      uv run main.py ecan backfill-tracking
      uv run main.py ecan create-campaign-results
      uv run main.py ecan download-campaign-results
      uv run main.py ecan create-campaign-results-subjects
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
    fetch_processed_datasets(vip_output)
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
    sync_examination_study_instance_uids()
    download_from_pacs_rest(orthanc_output)
    check_dicom_consistency(orthanc_output)
    upload_processed_dataset(orthanc_output)


@app.command()
def backfill_tracking(
    days: int = typer.Option(14, help="Only keep processings run within the last N days."),
    pipeline: str = typer.Option("landmarkDetection", help="Substring the processing/pipeline name must contain."),
) -> None:
    initiate_working_files("ecan")
    backfill_tracking_from_processings(days=days, pipeline_filter=pipeline)


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
def create_campaign_results(
    modalities: str = typer.Option("SR,SEG", help="Comma-separated modalities to keep, or 'all' for every series."),
) -> None:
    initiate_working_files("ecan")
    kept = () if modalities.lower() == "all" else tuple(m.strip().upper() for m in modalities.split(",") if m.strip())
    create_series_export(
        output_csv=ConfigPath.input_path / "result_campaign_ucan.csv",
        modalities=kept,
        patient_prefix="UCAN",
        excluded_description="MILVUE",
    )


@app.command()
def create_campaign_results_subjects(
    subjects: str = typer.Option("UCAN-16-012,UCAN-16-014", help="Comma-separated subject names; hyphens and underscores are interchangeable."),
    output: str = typer.Option("result_campaign_ucan_missing.csv", help="File name written under input/."),
    modalities: str = typer.Option("SR,SEG", help="Comma-separated modalities to keep, or 'all' for every series."),
) -> None:
    """Export the Orthanc series of the given subjects only, in the 'result Campaign1' format."""
    initiate_working_files("ecan")
    kept = () if modalities.lower() == "all" else tuple(m.strip().upper() for m in modalities.split(",") if m.strip())
    create_series_export(
        output_csv=ConfigPath.input_path / output,
        modalities=kept,
        patient_names=tuple(s.strip() for s in subjects.split(",") if s.strip()),
        excluded_description="MILVUE",
    )


@app.command()
def download_campaign_results(
    subject_prefix: str = typer.Option("UCAN", help="Only download datasets whose subject name starts with this prefix."),
) -> None:
    initiate_working_files("ecan")
    download_campaign_datasets(ConfigPath.output_path / "UCAN_CAMPAIGN_RESULTS", subject_prefix)


@app.command()
def debug_orthanc(
    subjects: str = typer.Option(None, help="Comma-separated subject names to log; defaults to every study in the tracking file."),
) -> None:
    initiate_working_files("ecan")
    names = tuple(s.strip() for s in subjects.split(",") if s.strip()) if subjects else None
    get_orthanc_study_details(from_tracking=names is None, patient_names=names)
    log_mr_series_instance_counts(names)


# ------------------- DANGER ZONE -------------------
# @app.command()
# def delete_studies() -> None:
#     initiate_working_files("ecan")
#     delete_studies_from_pacs(from_tracking=True)
# ------------------- DANGER ZONE -------------------
