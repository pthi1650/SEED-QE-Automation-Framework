import logging
from pathlib import Path
import pyarrow.csv as pv
import pyarrow.parquet as pq
from typing import Union

LOGGER = logging.getLogger(__name__)


def convert_to_parquet(path: Union[Path, str], file_type: str, delimiter: str = '|') -> None:
    """
    Convert a single file or multiple files in a directory to Parquet format.

    Args:
        path (Union[Path, str]): The path to the file or directory to be converted.
        file_type (str): The type of the file ('csv' currently supported).
        delimiter (str): The delimiter used in the file.

    Raises:
        ValueError: If the file type is not supported or path is invalid.
    """
    try:
        path = Path(path)

        if path.is_file():
            files_to_convert = [path]
        elif path.is_dir():
            files_to_convert = list(path.glob(f'*.{file_type}'))
        else:
            raise ValueError(f"Invalid path: {path}")

        for file_path in files_to_convert:
            if file_type == 'csv':
                read_options = pv.ReadOptions()
                parse_options = pv.ParseOptions(delimiter=delimiter)
                table = pv.read_csv(file_path, read_options=read_options, parse_options=parse_options)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            parquet_path = file_path.with_suffix('.parquet')
            pq.write_table(table, parquet_path)
            LOGGER.info(f"Converted {file_path} to Parquet format at {parquet_path}")

            # Optionally, delete the original file
            file_path.unlink()
            LOGGER.debug(f"Deleted original file at {file_path}")

    except Exception as e:
        LOGGER.error(f"Error converting {path} to Parquet: {e}")
        raise
