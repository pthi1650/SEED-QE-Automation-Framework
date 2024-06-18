from pathlib import Path
from typing import Dict, Any, Optional
import json
import boto3
import toml
from hvac import Client as VaultClient
from custom_conf.config_sources.remote_source import RemoteSource

class RemoteLoader:
    """
    Loader class to load configuration settings from a remote source.

    Attributes:
        source (RemoteSource): The remote source instance for loading settings.
    """

    def __init__(self, source: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the RemoteLoader with a remote source and optional parameters.

        Args:
            source (str): The type of remote source (e.g., 'vault', 'aws_secrets_manager').
            parameters (Optional[Dict[str, Any]]): Optional parameters for the remote source.
        """
        self.source = RemoteSource(source, parameters)

    def load(self) -> Dict[str, Any]:
        """
        Load settings from the remote source.

        Returns:
            Dict[str, Any]: A dictionary of settings loaded from the remote source.

        Raises:
            FileNotFoundError: If the remote file is not found.
            PermissionError: If there is a permission error when accessing the remote file.
            json.JSONDecodeError: If there is an error decoding the JSON data.
        """
        try:
            return self.source.load()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Failed to load settings from remote source. File not found: {e}")
        except PermissionError as e:
            raise PermissionError(f"Failed to load settings from remote source. Permission denied: {e}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Failed to load settings from remote source. JSON decode error: {e.msg}",
                                       e.doc, e.pos)
