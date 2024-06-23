import logging
from pathlib import Path
from typing import Tuple

LOGGER = logging.getLogger(__name__)


def get_project_root_path(filename: str = "README.md") -> Path:
    """
    Get the root folder of the project by navigating up the directory tree
    until the specified file is found.

    Args:
        filename (str): The filename to look for to determine the root folder.

    Returns:
        Path: The root folder of the project.
    """
    current_path = Path(__file__).resolve()
    while not (current_path / filename).exists():
        if current_path.parent == current_path:
            LOGGER.error(f"Unable to find the root folder. {filename} not found in any parent directories.")
            raise FileNotFoundError(f"{filename} not found in any parent directories.")
        current_path = current_path.parent

    LOGGER.debug(f"Project root found at: {current_path}")
    return current_path


def get_team_folder_path() -> Path:
    """
    Get the path to the teams folder in the project.

    This function constructs the path to the teams folder by joining the project root path,
    the 'custom_conf' directory, and the 'teams' directory.

    Returns:
        Path: The path to the team's folder.

    Note:
        This function does not take any parameters. It uses the `get_project_root_path` function
        to determine the project root path. The constructed path is then returned.
    """
    team_path = get_project_root_path() / "custom_conf" / "teams"
    return team_path


def get_team_folder_path_with_key(base_path: Path, team_key: str) -> Tuple[Path, str]:
    """
    Get the folder path for a team based on the team key.

    Args:
        base_path (Path): Base path for the teams folder.
        team_key (str): Team key (e.g., 'seed_intl_pgm').

    Returns:
        Tuple[Path, str]: Path to the team's folder and the relative path string.
    """
    team_parts = team_key.split('_')
    team_path = base_path.joinpath(*team_parts)
    return team_path, "/".join(team_parts)


def get_conf_base_path() -> Path:
    """
    Get the base path for the configuration.

    Returns:
        Path: Base path for the configuration.
    """
    project_root = get_project_root_path()
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
    team_path, _ = get_team_folder_path_with_key(base_path, team_key)
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
