import re
import csv

import typer

from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger

app = typer.Typer()
logger = get_logger()

@app.callback()
def explain():
    """
    \b
    Line merger for csv project command-line interface.

    Commands:
    --------
    * `execute` — merge lines in a csv,

    Usage:
    -----
        uv run main.py merge_lines_csv execute
    """

@app.command()
def execute() -> None:
    """
    Run the SQL Select return to csv conversion
    """
    file = open(ConfigPath.input_path / "inputs.txt", "r")
    content = file.read()
    lines = content.strip().splitlines()

    rows = []

    for line in lines:
        # Skip border lines like +------+------+
        if re.match(r'^\+\-+', line):
            continue

        # Extract values between | ... |
        parts = [col.strip() for col in line.strip('|').split('|')]
        rows.append(parts)

    # First row is header, remaining are data
    header = rows[0]
    data = rows[1:]

    # Write CSV file
    with open(ConfigPath.output_path / "output.csv", "w", newline="") as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)
        writer.writerows(data)