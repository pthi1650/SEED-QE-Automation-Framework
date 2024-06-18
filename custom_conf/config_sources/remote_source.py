import json
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
import toml
from hvac import Client as VaultClient


class RemoteSource:
    """
    Source class to load configuration settings from remote sources such as AWS Secrets Manager,
    AWS Parameter Store, or Vault.

    Attributes:
        source (str): The type of remote source.
        parameters (Dict[str, Any]): Parameters for the remote source.
    """

    def __init__(self, source: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the RemoteSource with a remote source type and optional parameters.

        Args:
            source (str): The type of remote source (e.g., 'aws_secrets_manager', 'aws_parameter_store', 'vault').
            parameters (Optional[Dict[str, Any]]): Optional parameters for the remote source.
        """
        self.source = source
        self.parameters = parameters or {}

    def load(self) -> Dict[str, Any]:
        """
        Load settings from the remote source.

        Returns:
            Dict[str, Any]: A dictionary of settings loaded from the remote source.

        Raises:
            ValueError: If the source is unsupported or an error occurs while loading from the remote source.
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
        client = boto3.client('secretsmanager')
        secret_name = self.parameters.get('secret_name')
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])

    def _load_from_aws_parameter_store(self) -> Dict[str, Any]:
        client = boto3.client('ssm')
        parameter_name = self.parameters.get('parameter_name')
        response = client.get_parameter(Name=parameter_name, WithDecryption=True)
        return {parameter_name: response['Parameter']['Value']}

    def _load_from_vault(self) -> Dict[str, Any]:
        client = VaultClient(url=self.parameters.get('vault_url'))
        client.token = self.parameters.get('vault_token')
        env_secret_path = self.parameters.get('env_secret_path', '').lstrip('/')

        print(f"Reading secrets from: {env_secret_path}")

        if not env_secret_path:
            raise ValueError("Environment secret path is None or invalid.")

        try:
            secrets = client.secrets.kv.read_secret_version(path=env_secret_path)['data']['data']
            return secrets
        except Exception as e:
            raise ValueError(f"Failed to read secrets from Vault: {e}")

    def store_in_vault(self, data: Dict[str, Any], env_secret_path: str) -> None:
        """
        Store secrets in Vault.

        Args:
            data (Dict[str, Any]): The secrets data to store.
            env_secret_path (str): The path to store the secrets in Vault.
        """
        client = VaultClient(url=self.parameters.get('vault_url'))
        client.token = self.parameters.get('vault_token')
        env_secret_path = env_secret_path.lstrip('/')
        print(f"Storing secrets to: {env_secret_path}")
        client.secrets.kv.v2.create_or_update_secret(path=env_secret_path, secret=data)

    def read_and_store_secrets(self, secrets_path: Path, allow_vault_secret_update: bool, environment: str) -> None:
        """
        Read and optionally store secrets in Vault.

        Args:
            secrets_path (Path): The path to the local secrets file.
            allow_vault_secret_update (bool): Whether to allow updates to Vault secrets.
            environment (str): The environment for which to load the secrets.
        """
        if allow_vault_secret_update:
            with secrets_path.open('r') as file:
                data = toml.load(file)
                if environment in data:
                    self.store_in_vault(data[environment], self.parameters.get('env_secret_path', ''))
                else:
                    print(f"No secrets found for environment: {environment}")
        else:
            secrets = self.load()
            with secrets_path.open('w') as file:
                toml.dump({environment: secrets}, file)

    def read_secrets(self) -> Dict[str, Any]:
        """
        Read secrets from the remote source.

        Returns:
            Dict[str, Any]: A dictionary of secrets loaded from the remote source.
        """
        return self.load()
