import json
import loguru
import pandas as pd
import typer
import re

from pathlib import Path
from typing import Optional, Sequence, Any, List, Dict, Set
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.file_writer import FileWriter
from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, get_working_files, get_tracking_file
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from src.utils.serializer_utils import init_serialization

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    SIMS project command-line interface.
    Commands:
    --------
    * `execute` — runs the SIMS pipeline for examinations listed in `input/inputs.txt`:
        - Retrieves datasets for each examination ID.
        - Generates JSON executions for the SIMS/3.0 pipeline.
        - Launches executions or resumes incomplete runs.
    * `format` - format the SIMS outputs into a .tsv file
        - outputs must be in input/dataset, whatever the subdirectories
    Usage:
    -----
        uv run main.py sims execute
    """

@app.command()
def execute() -> None:
    """
    Run the SIMS processing pipeline
    """
    working_file_path, save_file_path = get_working_files("sims")
    tracking_file_path = get_tracking_file("sims")

    init_serialization(working_file_path, save_file_path, tracking_file_path, generate_json)


def generate_json(_: Optional[Path] = None) -> List[Dict]:
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("inputs.txt")

    logger.info("Getting datasets, building json content... ")

    for exam_id in exam_ids_to_exec:
        identifier += 1
        try:
            datasets = find_datasets_by_examination_id(exam_id)
        except:
            logger.error("An error occurred while downloading examination " + exam_id + " from Shanoir")
            FileWriter.append_content(ConfigPath.tracking_file_path, str(identifier) + "," + str(exam_id) + ",false,,,,,,")
            continue

        FileWriter.append_content(ConfigPath.tracking_file_path, str(identifier) + "," + str(exam_id) + ",true,true,,,,,")

        execution = {
            "identifier":identifier,
            "name": "SIMS_3_exam_{}_{}_post_processing".format(exam_id, datetime.now(timezone.utc).strftime('%F')),
            "pipelineIdentifier": "SIMS/3",
            "studyIdentifier": datasets[0]["studyId"],
            "inputParameters": {},
            "outputProcessing": "",
            "processingType": "SEGMENTATION",
            "refreshToken": APIConfig.refresh_token,
            "client": APIConfig.clientId,
            "datasetParameters": [
                {
                    "datasetIds": [dataset["id"] for dataset in datasets],
                    "groupBy":"EXAMINATION",
                    "name":"dicom_archive",
                    "exportFormat":"dcm"
                }
            ]
        }
        executions.append(execution)

    return executions


@app.command("format")
def format_all_json(input_dir_path: Path) -> None:
    """Format each JSON output and concat them into a single TSV file."""

    input_dir_path = ConfigPath.input_path / "dataset"
    json_paths = list_output_json_available(input_dir_path)
    formatted_dfs = [format_output_to_tsv_by_series(json_path) for json_path in json_paths]

    df = pd.concat(formatted_dfs)
    df.to_csv(ConfigPath.output_path / "formatted_output_SIMS.tsv", sep='\t', index=False)


def list_output_json_available(input_dir_path: Path) -> List[Path]:
    """List available JSON output available in input_dir_path."""
    result = []
    root = Path(input_dir_path)

    for file_path in root.rglob("*"):
        if file_path.is_file():
            if re.search(r"\.json", str(file_path)):
                result.append(file_path)

    return sorted(result)


def format_output_to_tsv_by_series(json_path: Path) -> Optional[pd.DataFrame]:
    """Convert output_json to a pandas dataframe output"""

    with open(json_path, 'r') as file:
        content = json.load(file)

    if len(content['series']) == 0:
        loguru.logger.warning(f"JSON contains no volume: {json_path}")
        return None

    # Split the series list into multiple lines and columns using normalize
    df = pd.json_normalize(content['series'])

    # Split the list of volumes in multiples lines if multiples volume for one serie
    df = df.explode(column="volumes")

    # Split the volumes column directory into multiple columnes
    # print(pd.DataFrame(df['volumes'].tolist()).add_prefix("volumes.", axis=1))
    # print(pd.json_normalize(df['volumes']).add_prefix("volumes.", axis=1))
    volumes_cols = pd.DataFrame(df['volumes'].tolist()).add_prefix("volume.", axis=1)
    df = df.drop(columns=['volumes']).add_prefix("serie.", axis=1).reset_index(drop=True)
    df = pd.concat([df, volumes_cols], axis=1, )

    first_cols = [
        "serie.burnedInAnnotation",
        "serie.coil",
        "serie.deviceConstructor",
        "serie.deviceMagneticField",
        "serie.deviceModel",
        "serie.deviceSerialNumber",
        "serie.modality",
        "serie.name",
        "serie.originalSerieId",
        "serie.protocolValidityStatus",
        "serie.serieNumber",
        "serie.type",
        "serie.contrast",
        "serie.numberOfDirections",
        "serie.champs_DICOM.ImageType",
        "serie.champs_DICOM.AcquisitionContrast",
        "serie.champs_DICOM.StudyDescription",
        "serie.champs_DICOM.SerieDescription",
        "serie.champs_DICOM.SerieNumber",
        "serie.champs_DICOM.AcquisitionNumber",
        "serie.champs_DICOM.InstanceNumber",
        "serie.champs_DICOM.ProtocolName",
        "serie.champs_DICOM.PulseSequenceName",
        "serie.champs_DICOM.RepetitionTime",
        "serie.champs_DICOM.ScanningSequence",
        "serie.champs_DICOM.SequenceVariant",
        "serie.champs_DICOM.ScanOption",
        "serie.champs_DICOM.MRAcquisitionType",
        "serie.champs_DICOM.SequenceName",
        "serie.champs_DICOM.ScanningTechnique",
        "serie.champs_DICOM.PHILIPPS_AcquisitionContrast",
        "serie.champs_DICOM.PHILIPPS_PulseSequenceName",
        "volume.acquisitionNumber",
        "volume.bValue",
        "volume.burnedInAnnotation",
        "volume.contrast",
        "volume.contrastAgent",
        "volume.contrastAgentDICOM",
        "volume.derivedSequence",
        "volume.dimension",
        "volume.extraType",
        "volume.name",
        "volume.organ",
        "volume.organDICOM",
        "volume.sequenceList",
        "volume.sequence",
        "volume.sliceThickness",
        "volume.spacingBetweenSlices",
        "volume.type",
        "volume.serieId",
        "volume.numberOfSlices",
    ]

    df = df.groupby(['volume.serieId']).agg(dict({key: list_unique_str_reduce for key in first_cols}))

    reorder_cols = [col for col in df.columns if col in first_cols]
    df = df[reorder_cols]

    return df


def list_unique_str_reduce(elements: Sequence[Any]) -> Sequence[Any]:
    """
    Convert a list of element to a unique sorted element list of str.
    Ignore empty strings and pd.NA elements.
    If the result is a list of one element, convert it to a string.
    """
    agg_list = sorted(List(Set([str(e) for e in elements if (e != "") and (e is not None)])))
    if len(agg_list) == 0:
        return ""
    elif len(agg_list) == 1:
        return agg_list[0]
    else:
        return agg_list
