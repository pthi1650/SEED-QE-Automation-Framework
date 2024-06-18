from pathlib import Path
from typing import Any, Dict

from utils.commons.file_util import file_exists, load_toml
from utils.framework.path_util import get_project_root, get_team_folder_path


def load_layered_settings(file_path: Path, environment: str) -> Dict[str, Any]:
    """
    Load layered settings from a TOML file and merge with common settings.

    Args:
        file_path (Path): Path to the TOML file.
        environment (str): The environment layer to load (e.g., 'dev', 'stg', 'prod').

    Returns:
        Dict[str, Any]: Merged settings.
    """
    settings = load_toml(file_path)

    common_settings = settings.get('team_commons', {})
    env_settings = settings.get(environment, {})

    merged_settings = {**common_settings, **env_settings}
    return merged_settings


def get_conf_base_path() -> Path:
    """
    Get the base path for the configuration.

    Returns:
        Path: Base path for the configuration.
    """
    project_root = get_project_root()
    return project_root / 'custom_conf' / 'teams'


def get_conf_secrets_path(team_key: str) -> Path:
    """
    Get the secrets path for a given team.

    Args:
        team_key (str): Team key (e.g., 'seed_intl_pgm').

    Returns:
        Path: Path to the team's secrets file.
    """
    base_path = get_conf_base_path()
    team_path, _ = get_team_folder_path(base_path, team_key)
    return team_path / '.secrets.toml'


def get_settings_file_path(team_path: Path, environment: str) -> Path:
    """
    Get the path to the settings file for a given team path.

    Args:
        team_path (Path): Path to the team's folder.
        environment (str): Environment name.

    Returns:
        Path: Path to the team's settings file.
    """
    return team_path / environment / 'settings.toml'


def validate_settings_file(settings_file: Path) -> None:
    """
    Validate the existence of the settings file.

    Args:
        settings_file (Path): Path to the settings file.

    Raises:
        FileNotFoundError: If the settings file is not found.
    """
    if not file_exists(settings_file):
        raise FileNotFoundError(f"Settings file not found: {settings_file}")
