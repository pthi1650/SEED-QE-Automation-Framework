import logging
from pathlib import Path
import toml
from typing import Optional

from utils.framework import path_util
from .conf_manager import ConfManager
from .environments_manager import EnvironmentsManager
from .loaders.env_var_loader import EnvVarLoader
from .loaders.remote_loader import RemoteLoader
from .config_sources.remote_source import RemoteSource
from utils.framework.config_util import load_layered_settings


LOGGER = logging.getLogger(__name__)


def initialize_config(
        team_key: str, environment: str,
        detect_env_vars: bool = False,
        remote_config_src_type: Optional[str] = None,
        allow_remote_update: bool = False) -> ConfManager:
    """
    Initialize the configuration manager with local, environment, and remote settings.

    Args:
        team_key (str): The team key for loading team-specific configurations.
        environment (str): The environment (e.g., dev, stg, prod) to load configurations for.
        detect_env_vars (bool): Whether to load configurations from environment variables.
        remote_config_src_type (Optional[str]): The type of remote source (e.g., 'vault', 'aws_secrets_manager').
        allow_remote_update (bool): Whether to allow updates to remote secrets.

    Returns:
        ConfManager: The initialized configuration manager.
    """
    conf_manager = ConfManager()
    env_manager = EnvironmentsManager(conf_manager)

    # Get team folder path and relative path for secrets
    team_dir_path = path_util.get_team_folder_path()
    team_path, relative_team_path = path_util.get_team_folder_path_with_key(team_dir_path, team_key)

    LOGGER.info(f"Team path: {team_path}")
    LOGGER.info(f"Relative team path: {relative_team_path}")

    # Load local configuration
    load_local_config(env_manager, team_path, environment)

    # Optionally load environment variables
    if detect_env_vars:
        load_env_vars(conf_manager)

    # Optionally load remote secrets
    if remote_config_src_type:
        load_remote_secrets(conf_manager, team_dir_path, relative_team_path, environment, remote_config_src_type,
                            allow_remote_update)

    return conf_manager


def load_local_config(env_manager: EnvironmentsManager, team_path: Path, environment: str) -> None:
    """
    Load local configuration settings.

    Args:
        env_manager (EnvironmentsManager): The environment manager instance.
        team_path (Path): Path to the team's folder.
        environment (str): The environment (e.g., dev, stg, prod) to load configurations for.
    """
    settings_file = team_path / 'settings.toml'
    if not settings_file.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_file}")

    layered_settings = load_layered_settings(settings_file, environment)
    env_manager.conf_manager.settings.update(layered_settings)


def load_env_vars(conf_manager: ConfManager) -> None:
    """
    Load configuration settings from environment variables.

    Args:
        conf_manager (ConfManager): The configuration manager instance.
    """
    env_loader = EnvVarLoader()
    conf_manager.load(env_loader)


def load_remote_secrets(conf_manager: ConfManager, base_path: Path, relative_team_path: str, environment: str,
                        remote_config_src_type: str, allow_remote_update: bool) -> None:
    """
    Load configuration settings from a remote source.

    Args:
        conf_manager (ConfManager): The configuration manager instance.
        base_path (Path): Base path to the team's folders.
        relative_team_path (str): Relative path to the team's folder.
        environment (str): The environment (e.g., dev, stg, prod) to load configurations for.
        remote_config_src_type (str): The type of remote source (e.g., 'vault', 'aws_secrets_manager').
        allow_remote_update (bool): Whether to allow updates to remote secrets.
    """
    secrets_file = base_path / relative_team_path / '.secrets.toml'
    remote_params = {
        'vault_url': conf_manager.settings.get('SEED_CONF_VAULT_HOST_IP'),
        'vault_token': conf_manager.settings.get('SEED_CONF_VAULT_KEY'),
        'env_secret_path': f'{relative_team_path}/{environment}/secrets'
    }
    LOGGER.info(f"Using environment secret path: {remote_params['env_secret_path']}")
    remote_source = RemoteSource(remote_config_src_type, remote_params)
    remote_source.read_and_store_secrets(secrets_file, allow_remote_update, environment)

    # Load the secrets for the specified environment into the configuration manager
    with secrets_file.open('r') as file:
        existing_secrets = toml.load(file)

    env_secrets = existing_secrets.get(environment, {})
    conf_manager.settings.update(env_secrets)
    remote_loader = RemoteLoader(source=remote_config_src_type, parameters=remote_params)
    conf_manager.load(remote_loader)
