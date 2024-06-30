from pathlib import Path
from typing import Union, Dict, Any
import toml
import yaml


def file_exists(file_path: Union[str, Path]) -> bool:
    """
    Check if a file exists at the given path.

    Args:
        file_path (Union[str, Path]): The path to the file.

    Returns:
        bool: True if the file exists, False otherwise.

    Raises:
        TypeError: If the file_path is not a string or Path object.
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError(f"file_path {file_path} must be string or Path object")

    path = Path(file_path)
    return path.exists()


def load_toml_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load the TOML data from the file.

    Args:
        file_path (Union[str, Path]): The path to the TOML file.

    Returns:
        Dict[str, Any]: A dictionary of settings loaded from the TOML file.

    Raises:
        TypeError: If the file_path is not a string or Path object.
        FileNotFoundError: If the TOML file is not found.
        PermissionError: If there is a permission error when accessing the TOML file.
        toml.TomlDecodeError: If there is an error decoding the TOML file.
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError(f"file_path {file_path} must be a string or Path object")

    path = Path(file_path)
    try:
        with path.open('r') as file:
            return toml.load(file)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Failed to load TOML file: {file_path}. File not found: {e}")
    except PermissionError as e:
        raise PermissionError(f"Failed to load TOML file: {file_path}. Permission denied: {e}")
    except toml.TomlDecodeError as e:
        raise toml.TomlDecodeError(str(e), doc=e.doc, pos=e.pos)


def load_yaml_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load the YAML data from the file.

    Args:
        file_path (Union[str, Path]): The path to the YAML file.

    Returns:
        Dict[str, Any]: A dictionary of settings loaded from the YAML file.

    Raises:
        TypeError: If the file_path is not a string or Path object.
        FileNotFoundError: If the YAML file is not found.
        PermissionError: If there is a permission error when accessing the YAML file.
        yaml.YAMLError: If there is an error decoding the YAML file.
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError(f"file_path -> {file_path} must be a string or Path object")

    path = Path(file_path)
    try:
        with path.open('r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Failed to load YAML file: {file_path}. File not found: {e}")
    except PermissionError as e:
        raise PermissionError(f"Failed to load YAML file: {file_path}. Permission denied: {e}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(str(e))