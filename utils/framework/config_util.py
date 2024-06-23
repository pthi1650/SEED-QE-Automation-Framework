from pathlib import Path
from typing import Any, Dict

from utils.commons.file_util import load_toml_file


def load_layered_settings(file_path: Path, environment: str) -> Dict[str, Any]:
    """
    Load layered settings from a TOML file and merge with common settings.

    Args:
        file_path (Path): Path to the TOML file.
        environment (str): The environment layer to load (e.g., 'dev', 'stg', 'prod').

    Returns:
        Dict[str, Any]: Merged settings.
    """
    settings = load_toml_file(file_path)

    common_settings = settings.get('team_commons', {})
    env_settings = settings.get(environment, {})

    merged_settings = {**common_settings, **env_settings}
    return merged_settings
