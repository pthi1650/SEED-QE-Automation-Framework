from pathlib import Path
from typing import Tuple


def get_project_root() -> Path:
    """
    Get the root folder of the project.

    Returns:
        Path: The root folder of the project.
    """
    return Path(__file__).resolve().parent.parent.parent


def get_team_folder_path(base_path: Path, team_key: str) -> Tuple[Path, str]:
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
