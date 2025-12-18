import os
import traceback
from pathlib import Path
from typing import Union

from CQmanager.core.logging import logger


def check_if_idat_pair_exists(
    sentrix_id: str, idat_directory: Union[Path, str]
) -> bool:
    """
    Check if both Red and Green IDAT files for a given Sentrix ID exist in a directory.

    Args:
        sentrix_id (str): The Sentrix ID prefix for the IDAT files (e.g., "12345").
        idat_directory (Union[Path, str]): The directory path to check for IDAT files.
            Can be a string or a pathlib.Path object.

    Returns:
        bool: True if both "{sentrix_id}_Red.idat" and "{sentrix_id}_Grn.idat" exist
            in the directory, False otherwise.

    Raises:
        FileNotFoundError: If the directory does not exist.
        PermissionError: If access to the directory is denied.
        TypeError: If sentrix_id is not a string or idat_directory is an invalid type.
    """
    if not isinstance(sentrix_id, str):
        raise TypeError("sentrix_id must be a string")

    # Convert to Path object for consistency
    dir_path = Path(idat_directory)

    # Get directory contents as a set for O(1) lookups
    try:
        dir_contents = set(os.listdir(path=dir_path))
    except FileNotFoundError:
        error: str = traceback.format_exc()
        logger.error(msg=error)
        raise FileNotFoundError(
            f"Directory not found: {idat_directory}.\nError:\n{error}"
        )
    except PermissionError:
        error: str = traceback.format_exc()
        logger.error(msg=error)
        raise PermissionError(
            f"Permission denied for directory: {idat_directory}.\nError:\n{error}"
        )

    # Define the expected file names
    red_idat = f"{sentrix_id}_Red.idat"
    green_idat = f"{sentrix_id}_Grn.idat"

    # Check if both files exist
    return all(file in dir_contents for file in (red_idat, green_idat))
