from pathlib import Path
from typing import Dict, Any
import toml
from custom_conf.config_sources.file_source import LocalSource


class FileLoader:
    """
    Loader class to load configuration settings from a local file.

    Attributes:
        file_path (Path): The path to the local configuration file.
    """

    def __init__(self, file_path: Path) -> None:
        """
        Initialize the FileLoader with the path to the local configuration file.

        Args:
            file_path (Path): The path to the local configuration file.
        """
        self.file_path = file_path

    def load(self) -> Dict[str, Any]:
        """
        Load settings using LocalSource.

        Returns:
            Dict[str, Any]: A dictionary of settings loaded from the local file.

        Raises:
            FileNotFoundError: If the local file is not found.
            PermissionError: If there is a permission error when accessing the local file.
            toml.TomlDecodeError: If there is an error decoding the TOML file.
        """
        try:
            local_source = LocalSource(self.file_path)
            return local_source.load()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Failed to load file: {self.file_path}. File not found: {e}")
        except PermissionError as e:
            raise PermissionError(f"Failed to load file: {self.file_path}. Permission denied: {e}")
        except toml.TomlDecodeError as e:
            raise toml.TomlDecodeError(str(e), doc=e.doc, pos=e.pos)
