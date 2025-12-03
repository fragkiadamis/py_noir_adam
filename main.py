import sys
import typer

from src.utils.config_utils import load_config
from src.utils.log_utils import set_logger

load_config()
set_logger(sys.argv[1])

import projects.execution_serializer.sims as sims
import projects.execution_serializer.flair as flair
import projects.execution_serializer.pmap as pmap
import projects.execution_serializer.t2stir as t2stir
import projects.execution_serializer.siena as siena
import projects.execution_serializer.ecan as ecan

import projects.execution_tools.carmin_api_test as carmin
import projects.execution_tools.output_extraction as output_extraction
import projects.execution_tools.post_processing as post_processing
import projects.execution_tools.tracking_file_download as tracking_file_download
import projects.execution_tools.vip_logs_import as vip_logs_import

import projects.shanoir_tools.dicom_metadata_download as dicom_metadata_download

import projects.miscellaneous_tools.csv_tools.sql_select_return_to_csv as sql_to_csv

app = typer.Typer()

# Execution serializers
app.add_typer(sims.app, name="sims")
app.add_typer(flair.app, name="flair")
app.add_typer(pmap.app, name="pmap")
app.add_typer(t2stir.app, name="t2stir")
app.add_typer(siena.app, name="siena")
app.add_typer(ecan.app, name="ecan")

# Execution tools
app.add_typer(carmin.app, name="carmin")
app.add_typer(output_extraction.app, name="output_extraction")
app.add_typer(post_processing.app, name="post_processing")
app.add_typer(tracking_file_download.app, name="tracking_file_download")
app.add_typer(vip_logs_import.app, name="vip_logs_import")

# Execution tools
app.add_typer(dicom_metadata_download.app, name="dicom_metadata_download")

#Miscellaneous tools
app.add_typer(sql_to_csv.app, name="sql_to_csv")

@app.callback()
def explain() -> None:
    """
    \b
    **The py_noir app list (Check `uv run [app_name] --help` for more information):
    *
    * Execution serializers:
    * - sims: runs the SIMS processing pipeline.
    * - flair: runs the Comete_FLAIR processing pipeline.
    * - pmap: runs the Comete_PMAP processing pipeline.
    * - t2stir: runs the Comete_T2STIR processing pipeline.
    * - siena: runs the Siena processing pipeline.
    * - ecan: runs the eCAN pipeline.
    *
    * Execution tools:
    * - carmin: runs the CarminAPITest, to check the format in which the data are received in VIP.
    * - output_extraction: runs the output extraction according to specified filters.
    * - post_processing: runs the delayed post processings according to execution comment value.
    * - tracking_file_download: download the tracking files relatively to pipeline names.
    * - vip_logs_import: download the vip logs relatively to workflow ids.
    *
    * Shanoir tools (not related to executions):
    * - dicom_metadata_download: download dicom metadata gathered into a csv file according to metadata keys and dataset ids.
    *
    * Miscellaneous tools (not related to Shanoir):
    * - sql_to_csv: convert a SQL Select return to a csv file
    ---
    Built for automating dataset execution and processing in Shanoir-NG.
    """


if __name__ == "__main__":
    app()