from pathlib import Path
from typing import Any, Dict

from utils.commons.file_util import load_toml_file


class LocalSource:
    """
    Source class to load configuration settings from a local TOML file.

    Attributes:
        file_path (Path): The path to the local configuration file.
    """

    def __init__(self, file_path: Path) -> None:
        """
        Initialize the LocalSource with the path to the local configuration file.

        Args:
            file_path (Path): The path to the local configuration file.
        """
        self.file_path = file_path

    def load(self) -> Dict[str, Any]:
        """
        Load the TOML data from the file.

        Returns:
            Dict[str, Any]: A dictionary of settings loaded from the TOML file.

        Raises:
            ValueError: If the file is not a TOML file.
            FileNotFoundError: If the TOML file is not found.
            PermissionError: If there is a permission error when accessing the TOML file.
            toml.TomlDecodeError: If there is an error decoding the TOML file.
        """
        if self.file_path.suffix != '.toml':
            raise ValueError("Only TOML files are supported.")

        return load_toml_file(self.file_path)
