"""Create a clean TSV from multiple JSON output."""

import json
import os
import re
import time

import loguru
import pandas as pd
from typing import List, Optional, Sequence, Any


def list_unique_str_reduce(elements: Sequence[Any]) -> Sequence[Any]:
    """
    Convert a list of element to a unique sorted element list of str.
    Ignore empty strings and pd.NA elements.
    If the result is a list of one element, convert it to a string.
    """
    agg_list = sorted(list(set([str(e) for e in elements if (e != "") and (e is not None)])))
    if len(agg_list) == 0:
        return ""
    elif len(agg_list) == 1:
        return agg_list[0]
    else:
        return agg_list

def find_pattern(pattern: str, path: str, followlinks: bool = False) -> List[str]:
    """List all files in path directory matchin the regex pattern."""
    result = []
    for root, dirs, files in os.walk(path, followlinks=followlinks):
        for name in files:
            filename = os.path.join(root, name)
            if re.search(pattern, filename):
                result.append(filename)
    print ("ah" + result[0])
    return sorted(result)


def format_all_JSON(input_dir_path: str) -> None:
    """Format each JSON output and concat them into a single TSV file."""

    json_paths = list_output_json_available(input_dir_path)
    formatted_dfs = [format_output_to_tsv_by_serie(json_path) for json_path in json_paths]
    print ("hey" + json_paths[0])
    df = pd.concat(formatted_dfs)

    os.makedirs("../data/output", exist_ok=True)
    df.to_csv('../data/output/formatted_output_SIMS.tsv', sep='\t', index=False)


def list_output_json_available(input_dir_path: str) -> list[str]:
    """List available JSON output available in input_dir_path."""
    return find_pattern(path=input_dir_path, pattern=r"\.json")


def format_output_to_tsv_by_volume(json_path: str) -> Optional[pd.DataFrame]:
    """Convert output_json to a pandas dataframe output"""

    with open(json_path, 'r') as file:
        content = json.load(file)

    if len(content['series']) == 0:
        loguru.logger.warning(f"JSON contains no volume : {json_path}")
        return

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

    reorder_cols = [col for col in df.columns if col in first_cols]
    df = df[reorder_cols]

    return df

def format_output_to_tsv_by_serie(json_path: str) -> Optional[pd.DataFrame]:
    """Convert output_json to a pandas dataframe output"""

    with open(json_path, 'r') as file:
        content = json.load(file)

    if len(content['series']) == 0:
        loguru.logger.warning(f"JSON contains no volume : {json_path}")
        return

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

def check_formatted_output():
    df = pd.read_csv('../data/output/formatted_output_SIMS.tsv', sep='\t')

    print(df.shape)

if __name__ == '__main__':
    format_all_JSON("../data/input")
    check_formatted_output()
