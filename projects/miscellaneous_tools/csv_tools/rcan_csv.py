import pandas as pd

import typer

from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain():
    """
    Filter the RCAN statistics export down to the "TOF SANS AIC" datasets.

    Reads ``rcan_statistics.csv`` from the configured input path and keeps only
    the TOF (time-of-flight angiography) acquisitions: rows whose examination
    comment is "TOF SANS AIC" and whose dataset name contains "TOF", excluding
    exploded multi-view series (MIP/PJN/REC/VUES/SENSE) and AI-reconstructed
    variants (IA/AI/NRI). One dataset is kept per examination, and the result is
    sorted by subject common name.

    Writes two files back to the input path:

    Usage:
      uv run main.py stripe-rcan-csv execute -c "TOF SANS AIC"
    """


@app.command()
def execute(
    examination_comment: str = typer.Option(
        "TOF SANS AIC",
        "--examination-comment",
        "-c",
        help="examinationComment to filter on (case-insensitive).",
    ),
) -> None:
    df = pd.read_csv(ConfigPath.input_path / "rcan_statistics.csv", dtype=str, sep=",")
    dataset_name = df["datasetName"].str.upper()
    filtered_df = df[
        (df["examinationComment"].str.upper() == examination_comment.upper())
        & (dataset_name.str.contains("TOF", na=False))
        # Drop exploded multi-view series and AI-reconstructed variants.
        & (~dataset_name.str.contains("MIP|PJN|REC", na=False, regex=True))
        & (~dataset_name.str.contains("VUES|SENSE", na=False, regex=True))
        & (~dataset_name.str.contains(r"\b(?:IA|AI|NRI)\b", na=False, regex=True))
    ]

    # Keep a single dataset per examination (the first row in file order; every
    # examination has a single examinationDate, so this is the oldest date too).
    deduped_df = filtered_df.drop_duplicates(subset="examinationId", keep="first")

    # Sort the output by subject name.
    deduped_df = deduped_df.sort_values("commonName", kind="stable")

    output_file = ConfigPath.input_path / "rcan_tof_sans_aic.csv"
    deduped_df.to_csv(output_file, sep=";", index=False)

    logger.info(f"Wrote {len(deduped_df)} rows to {output_file}")

    # Write the first 400 datasetIds, one per line, as a subset file.
    subset_ids = deduped_df["datasetId"].head(400)
    subset_file = ConfigPath.input_path / "rcan_tof_sans_aic_subset.txt"
    subset_file.write_text("\n".join(subset_ids) + "\n")

    logger.info(f"Wrote {len(subset_ids)} datasetIds to {subset_file}")
