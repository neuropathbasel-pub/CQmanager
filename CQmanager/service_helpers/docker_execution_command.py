from typing import Optional, Union

import orjson


def make_execution_command(dictionary: dict[str, Union[str, int, list[str]]]) -> str:
    """
    Serializes a dictionary to a JSON string wrapped in single quotes.

    Args:
        dictionary (dict[str, Union[str, int, list[str]]]): The dictionary to serialize, containing strings, ints, or lists of strings.

    Returns:
        str: The JSON-encoded string wrapped in single quotes (e.g., "'{"key": "value"}'").
    """
    return f"'{orjson.dumps(dictionary).decode(encoding='utf-8')}'"


def make_an_execution_command(
    batch: Optional[dict[tuple[int, int, str, str], list[str]]],
) -> str:
    """
    Generates an execution command string (JSON wrapped in quotes) from a batch dict.

    Args:
        batch: A dict with exactly one key-value pair (key: tuple of params, value: list of sentrix_ids).
               Returns empty string if None or invalid.

    Returns:
        str: The JSON command string wrapped in single quotes, or empty string if invalid.
    """
    if not batch or len(batch) != 1:
        return ""

    key, sentrix_ids = next(iter(batch.items()))
    if not sentrix_ids:
        return ""

    bin_size, min_probes_per_bin, preprocessing_method, downsize_to = key
    execution_dictionary = {
        "sentrix_ids": ",".join(sentrix_ids),
        "preprocessing_method": preprocessing_method,
        "bin_size": bin_size,
        "min_probes_per_bin": min_probes_per_bin,
        "downsize_to": downsize_to,
    }
    return make_execution_command(execution_dictionary)
