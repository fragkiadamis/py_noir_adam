from pathlib import Path
import re

from src.utils.config_utils import ConfigPath
from src.utils.log_utils import get_logger

logger = get_logger()

def update_csv_line(line: str, text: str) -> str:
    v1 = line.split(",")
    v2 = text.split(",")

    updated = [
        v2[i] if v2[i] != "" else v1[i]
        for i in range(len(v1))
    ]

    return ",".join(updated)

class FileWriter:
    files = {}

    @classmethod
    def open_files(cls, *file_paths: Path):
        """Open a file in append mode and store it. It does not create the file, it has to exist before. Not optimized for json files."""
        for file_path in file_paths:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if file_path not in cls.files:
                f = file_path.open("r+", encoding="utf-8")
                cls.files[file_path] = f

                # Execution serialization specific code
                type = file_path.parent.name
                match type:
                    case "tracking_file":
                        cls.files[ConfigPath.trackingFilePath] = f
                    case "WIP_file":
                        cls.files[ConfigPath.wipFilePath] = f
                    case "save_file":
                        cls.files[ConfigPath.saveFilePath] = f

    @classmethod
    def replace_content(cls, file_path: Path, text: str):
        """Overwrite the file with text."""
        if file_path in cls.files:
            f = cls.files[file_path]
            f.truncate(0)
            f.seek(0)
            f.write(text)
            f.flush()

        else:
            logger.error(f"File {file_path.name} not opened, can not replace content. Please open it before writing.")

    @classmethod
    def update_content_first_matching_line_start(cls, file_path: Path, line_start: str, text: str, csv: bool = False):
        """Update the first line matching a regex. CSV option to keep value if corresponding value in text arg is empty, only works with, separator."""
        regex = re.compile(rf"^{line_start}")
        start_pos = 0
        if file_path in cls.files:
            f = cls.files[file_path] # file already opened in r+
            f.seek(0)

            while True:
                pos = f.tell()
                line = f.readline().rstrip("\n")

                if not line:
                    break # EOF

                if regex.match(line):
                    start_pos = pos
                    break

            if start_pos != 0:
                rest = f.read()
                f.seek(start_pos)
                f.truncate()
                if csv:
                    text = update_csv_line(line, text)
                f.write(text + (("\n" + rest) if rest != "" else ""))
                f.flush()

        else:
            logger.error(f"File {file_path.name} not opened, can not update content. Please open it before writing.")

    @classmethod
    def append_content(cls, file_path: Path, text: str):
        """Append the given text to the file."""
        if file_path in cls.files:
            f = cls.files[file_path] # file already opened in r+
            f.seek(0,2)
            f.write("\n" + text)
            f.flush()

        else:
            logger.error(f"File {file_path.name} not opened, can not append content. Please open it before writing.")

    @classmethod
    def get_content(cls, file_path: Path) -> str:
        """Return the content of a file."""
        if file_path in cls.files:
            f = cls.files[file_path]
            f.seek(0)
            return f.read()
        else:
            logger.error(f"File {file_path.name} not opened. Please open it before writing. Returned content is null")
            return ""

    @classmethod
    def close_file(cls, file_path: str):
        """Close a specific file."""
        if file_path in cls.files:
            f = cls.files[file_path]
            f.close()
            del cls.files[file_path]

            keys_to_remove = [k for k, v in cls.files.items() if v == f]
            for k in keys_to_remove:
                del cls.files[k]

    @classmethod
    def close_all(cls):
        """Close all open files."""
        for f in cls.files.values():
            f.close()
        cls.files.clear()
