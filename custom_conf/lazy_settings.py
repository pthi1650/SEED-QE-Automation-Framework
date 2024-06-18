from typing import Any
from .conf_manager import ConfManager


class LazySettings:
    """
    LazySettings class to lazily initialize and manage settings using ConfManager.

    Attributes:
        _conf_manager (Optional[ConfManager]): The ConfManager instance for managing settings.
    """

    def __init__(self) -> None:
        """
        Initialize the LazySettings with no initial ConfManager.
        """
        self._conf_manager: ConfManager | None = None

    def _setup(self) -> None:
        """
        Initialize the ConfManager if it hasn't been already.
        """
        if self._conf_manager is None:
            self._conf_manager = ConfManager()
            # Load default settings here, if any

    def __getattr__(self, item: str) -> Any:
        """
        Get an attribute from the ConfManager.

        Args:
            item (str): The name of the attribute to get.

        Returns:
            Any: The value of the attribute from the ConfManager.
        """
        self._setup()
        return getattr(self._conf_manager, item)

    def __setattr__(self, key: str, value: Any) -> None:
        """
        Set an attribute in the ConfManager.

        Args:
            key (str): The name of the attribute to set.
            value (Any): The value of the attribute to set.
        """
        if key == "_conf_manager":
            super().__setattr__(key, value)
        else:
            self._setup()
            setattr(self._conf_manager, key, value)
