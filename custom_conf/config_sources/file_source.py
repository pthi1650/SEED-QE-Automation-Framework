from pathlib import Path
from typing import Any, Dict
import toml


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

        return self._load_toml()

    def _load_toml(self) -> Dict[str, Any]:
        """
        Load the TOML data from the file.

        Returns:
            Dict[str, Any]: A dictionary of settings loaded from the TOML file.

        Raises:
            FileNotFoundError: If the TOML file is not found.
            PermissionError: If there is a permission error when accessing the TOML file.
            toml.TomlDecodeError: If there is an error decoding the TOML file.
        """
        try:
            with self.file_path.open('r') as file:
                return toml.load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Failed to load TOML file: {self.file_path}. File not found: {e}")
        except PermissionError as e:
            raise PermissionError(f"Failed to load TOML file: {self.file_path}. Permission denied: {e}")
        except toml.TomlDecodeError as e:
            raise toml.TomlDecodeError(str(e), doc=e.doc, pos=e.pos)
