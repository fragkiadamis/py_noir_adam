import json
import loguru
import pandas as pd
import typer
import re
import numpy as np

from pathlib import Path
from typing import Optional, Sequence, Any, List, Dict, Set
from src.utils.config_utils import APIConfig, ConfigPath
from src.utils.log_utils import get_logger
from datetime import datetime, timezone
from src.utils.file_utils import get_items_from_input_file, initiate_working_files
from src.shanoir_object.dataset.dataset_service import find_datasets_by_examination_id
from src.utils.serializer_utils import init_serialization

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain() -> None:
    """
    \b
    SIMS project command-line interface.

    Commands:
    --------
    * `execute` — runs the SIMS pipeline for examinations listed in `input/comete.txt`:
        - Retrieves datasets for each examination ID.
        - Generates JSON executions for the SIMS/3.0 pipeline.
        - Launches executions or resumes incomplete runs.
    * `format` - format the SIMS outputs into a .tsv file
        - Outputs must be in input/, whatever the subdirectories
        - All .json files in inputs/ are taken in account

    Usage:
    -----
        uv run main.py sims execute
    """

@app.command()
def execute() -> None:
    """
    Run the SIMS processing pipeline
    """
    initiate_working_files("sims")
    init_serialization(generate_json)


def generate_json(_: Optional[Path] = None) -> List[Dict]:
    identifier = 0
    executions = []

    exam_ids_to_exec = get_items_from_input_file("sims.txt")

    logger.info("Getting datasets, building json content... ")

    df = pd.read_csv(ConfigPath.tracking_file_path)
    for exam_id in exam_ids_to_exec:
        identifier += 1
        try:
            datasets = find_datasets_by_examination_id(exam_id)
        except:
            logger.error("An error occurred while downloading examination " + exam_id + " from Shanoir")
            values = {
                "identifier": identifier,
                "examination_id": exam_id,
                "get_from_shanoir": False,
            }
            for col, val in values.items():
                df.loc[identifier, col] = val
            df.to_csv(ConfigPath.tracking_file_path, index=False)
            continue

        values = {
            "identifier": identifier + 1,
            "examination_id": exam_id,
            "get_from_shanoir": True,
            "executable": True,
        }
        for col, val in values.items():
            df.loc[identifier, col] = val
        df.to_csv(ConfigPath.tracking_file_path, index=False)

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
def format_all_json() -> None:
    """Format each JSON output and concat them into a single TSV file."""

    json_paths = list_output_json_available()
    formatted_dfs = [format_output_to_tsv_by_series(json_path) for json_path in json_paths]

    df = pd.concat(formatted_dfs)
    sims_output_dir = ConfigPath.output_path / "sims"
    sims_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(sims_output_dir / "formatted_output_SIMS.tsv", sep='\t', index=False)


def list_output_json_available() -> List[Path]:
    """List available JSON output available in input_dir_path."""
    result = []

    for file_path in ConfigPath.input_path.rglob("*"):
        if file_path.is_file():
            if re.search(r"\.json", str(file_path)):
                result.append(file_path)

    logger.info(f"Number of listed output.json : {str(len(result))}")
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

    if "volume.status" in df.columns and "volume.type" in df.columns:
        df.loc[df["volume.status"] == "IGNORED", "volume.type"] = df["volume.status"]

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
        "volume.shanoirId"
    ]

    #Remove duplicate volume (bug from SIMS)
    if "volume.shanoirId" in df.columns:
        df["volume.shanoirId"] = df["volume.shanoirId"].apply(lambda x: str(x) if isinstance(x, list) else x)
        df = df.drop_duplicates(subset=['volume.shanoirId', 'volume.serieId'])
    else:
        df = df.drop_duplicates(subset=['volume.serieId'])

    #Group by serieId
    existing_first_cols = [c for c in first_cols if c in df.columns]
    df = df.groupby(['volume.serieId']).agg(dict({key: list_unique_str_reduce for key in existing_first_cols}))

     #Exploding by serieId
    if "volume.shanoirId" in df.columns:
        df["volume.shanoirId"] = df["volume.shanoirId"].str.split(",")
        df = df.explode("volume.shanoirId").reset_index(drop=True)

    reorder_cols = [col for col in df.columns if col in first_cols]
    df = df[reorder_cols]

    return df

def list_unique_str_reduce(elements: Sequence[Any]) -> Sequence[Any]:
    valid_elements = []
    for e in elements:
        # Skip array-like objects first (before any boolean checks)
        if isinstance(e, (np.ndarray, pd.Series, list)):
            continue
        # Now safe to check for NA/None
        if pd.isna(e):
            continue
        # Skip if empty string
        if isinstance(e, str) and e == "":
            continue
        valid_elements.append(e)
    if elements.name == "volume.serieId":
        agg_list = list(set([str(e) for e in valid_elements]))
    else :
        agg_list = list([str(e) for e in valid_elements])

    if len(agg_list) == 0:
        return ""
    elif len(agg_list) == 1:
        return str(agg_list[0])
    else:
        return agg_list
