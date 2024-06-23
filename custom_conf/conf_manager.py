from typing import Any, Dict, Optional
from threading import Lock


class ConfManager:
    """
    Configuration Manager class to manage application settings.

    Attributes:
        settings (Dict[str, Any]): Dictionary to store configuration settings.
        _lock (Lock): Thread lock for thread-safe operations.
    """

    def __init__(self) -> None:
        """
        Initialize the ConfManager with an empty settings dictionary and a thread lock.
        """
        self.settings: Dict[str, Any] = {}
        self._lock = Lock()

    def load(self, loader: Any) -> None:
        """
        Load settings using the provided loader and update the settings dictionary.

        Args:
            loader (Any): Loader instance with a `load` method that returns a dictionary of settings.
        """
        with self._lock:
            self.settings.update(loader.load())

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve a setting value by key with an optional default.

        Args:
            key (str): The key of the setting to retrieve.
            default (Optional[Any], optional): The default value to return if the key is not found. Defaults to None.

        Returns:
            Any: The value of the setting if found, otherwise the default value.
        """
        with self._lock:
            return self.settings.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a setting value by key.

        Args:
            key (str): The key of the setting to set.
            value (Any): The value of the setting to set.
        """
        with self._lock:
            self.settings[key] = value

    def clear(self) -> None:
        """
        Clear all settings.
        """
        with self._lock:
            self.settings.clear()
