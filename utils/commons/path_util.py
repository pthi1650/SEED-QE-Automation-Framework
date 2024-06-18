from pathlib import Path
from typing import Union


def construct_path(*args: Union[str, Path]) -> Path:
    """
    Construct a Path object from the given arguments.

    Args:
        *args (Union[str, Path]): Parts of the path to join.

    Returns:
        Path: The constructed Path object.
    """
    return Path(*args)
