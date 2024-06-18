import toml
from pathlib import Path
from typing import Any
from custom_conf.config_sources import LocalSource
from utils.commons.path_util import construct_path  # Updated import path

class EnvironmentsManager:
    """
    Manage different environments for configuration settings.

    Attributes:
        conf_manager (ConfManager): The configuration manager instance.
    """

    def __init__(self, conf_manager: Any) -> None:
        """
        Initialize the EnvironmentsManager with a configuration manager.

        Args:
            conf_manager (Any): An instance of ConfManager.
        """
        self.conf_manager = conf_manager

    def switch_environment(self, team: str, environment: str) -> None:
        """
        Switch to the specified environment for the given team.

        Args:
            team (str): The team name.
            environment (str): The environment name.

        Raises:
            FileNotFoundError: If the settings file for the specified team and environment does not exist.
        """
        base_path = construct_path('C:/Repos/QA/SEED-QE-Automation-Framework/custom_conf/teams', team, environment, 'settings.json')
        if not base_path.exists():
            raise FileNotFoundError(f"Settings file not found for team: {team}, environment: {environment}")

        local_source = LocalSource(base_path)
        self.conf_manager.load(local_source)

        secrets_path = construct_path('C:/Repos/QA/SEED-QE-Automation-Framework/custom_conf/teams', team, '.secrets.toml')
        if secrets_path.exists():
            with open(secrets_path, 'r') as file:
                secrets = toml.load(file)
                self.conf_manager.settings.update(secrets)
