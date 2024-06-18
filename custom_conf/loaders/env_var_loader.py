import os
from typing import Dict


class EnvVarLoader:
    """
    Loader class to load configuration settings from environment variables prefixed with 'CONF_'.
    """

    @staticmethod
    def load() -> Dict[str, str]:
        """
        Load settings from environment variables prefixed with 'CONF_'.

        Returns:
            Dict[str, str]: A dictionary of settings loaded from environment variables.
        """
        return {key: value for key, value in os.environ.items() if key.startswith('CONF_')}
