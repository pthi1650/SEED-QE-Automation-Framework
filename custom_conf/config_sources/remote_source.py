import json
import logging
from pathlib import Path

import boto3
import toml
from hvac import Client as VaultClient
from typing import Any, Dict, Optional

LOGGER = logging.getLogger(__name__)


class RemoteSource:
    """
    A class to handle loading secrets from various remote sources such as AWS Secrets Manager,
    AWS Parameter Store, and HashiCorp Vault.
    """

    def __init__(self, source: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the RemoteSource with the given source type and parameters.

        Parameters:
            source (str): The type of remote source (e.g., 'vault', 'aws_secrets_manager').
            parameters (Optional[Dict[str, Any]]): Additional parameters for the remote source.
        """
        self.source = source
        self.parameters = parameters or {}

    def load(self) -> Dict[str, Any]:
        """
        Load the secrets from the specified remote source.

        Returns:
            Dict[str, Any]: The secrets retrieved from the remote source.
        """
        if self.source == 'aws_secrets_manager':
            return self._load_from_aws_secrets_manager()
        elif self.source == 'aws_parameter_store':
            return self._load_from_aws_parameter_store()
        elif self.source == 'vault':
            return self._load_from_vault()
        else:
            raise ValueError('Unsupported remote source')

    def _load_from_aws_secrets_manager(self) -> Dict[str, Any]:
        """
        (RR:Load secrets from AWS Secrets Manager.)

        Returns:
            Dict[str, Any]: The secrets retrieved from AWS Secrets Manager.
        """
        client = boto3.client('secretsmanager')
        secret_name = self.parameters.get('secret_name')
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])

    def _load_from_aws_parameter_store(self) -> Dict[str, str]:
        """
        (RR:Load secrets from AWS Parameter Store.)

        Returns:
            Dict[str, str]: The secrets retrieved from AWS Parameter Store.
        """
        client = boto3.client('ssm')
        parameter_name = self.parameters.get('parameter_name')
        response = client.get_parameter(Name=parameter_name, WithDecryption=True)
        return {parameter_name: response['Parameter']['Value']}

    def _load_from_vault(self) -> Dict[str, Any]:
        """
        Load secrets from HashiCorp Vault.

        Returns:
            Dict[str, Any]: The secrets retrieved from HashiCorp Vault.
        """
        client = VaultClient(url=self.parameters.get('vault_url'))
        client.token = self.parameters.get('vault_token')
        env_secret_path = self.parameters.get('env_secret_path')

        if env_secret_path:
            env_secret_path = env_secret_path.lstrip('/')

        LOGGER.info(f"Reading secrets from: {env_secret_path}")

        if not env_secret_path:
            raise ValueError("Environment secret path is None or invalid.")

        try:
            secrets = client.secrets.kv.read_secret_version(
                path=env_secret_path, raise_on_deleted_version=True
            )['data']['data']
            return secrets
        except Exception as e:
            LOGGER.info(f"Failed to read secrets from Vault: {e}")
            return {}

    def store_in_vault(self, data: Dict[str, Any], env_secret_path: str) -> None:
        """
        Store the given data in HashiCorp Vault at the specified path.

        Parameters:
            data (Dict[str, Any]): The data to store in HashiCorp Vault.
            env_secret_path (str): The path to store the data in HashiCorp Vault.
        """
        client = VaultClient(url=self.parameters.get('vault_url'))
        client.token = self.parameters.get('vault_token')
        env_secret_path = env_secret_path.lstrip('/')
        LOGGER.info(f"Storing secrets to: {env_secret_path}")
        client.secrets.kv.v2.create_or_update_secret(path=env_secret_path, secret=data)

    def read_and_store_secrets(self, secrets_path: Path, allow_vault_secret_update: bool, environment: str) -> None:
        """
        Read secrets from a file & store them in the remote source, or read from the remote source and write to a file.

        Parameters:
            secrets_path (str): The path to the secrets file.
            allow_vault_secret_update (bool): Whether to allow updates to the remote source.
            environment (str): The environment for which to read/store secrets (e.g. 'dev', 'stg', 'prod').
        """
        if allow_vault_secret_update:
            with open(secrets_path, 'r') as file:
                data = toml.load(file)
                if environment in data:
                    self.store_in_vault(data[environment], self.parameters.get('env_secret_path'))
                else:
                    LOGGER.info(f"No secrets found for environment: {environment}")
        else:
            secrets = self.load()
            with open(secrets_path, 'w') as file:
                toml.dump({environment: secrets}, file)

    def read_secrets(self) -> Dict[str, Any]:
        """
        Load the secrets from the remote source.

        Returns:
            Dict[str, Any]: The secrets retrieved from the remote source.
        """
        return self.load()
