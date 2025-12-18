from typing import Optional

from cnquant_dependencies.CommonArrayType import CommonArrayType


def add_unique_pair(
    dictionary: dict,
    outer_key: str,
    inner_key: str,
    pair: list[int],
) -> None:
    """
    Adds a unique pair of integers to a nested dictionary if both elements are non-None and the keys exist.

    Checks if the provided pair contains non-None integers and if the outer and inner keys exist in the dictionary.
    If the pair is not already in the list at the specified nested location, it is appended.

    Args:
        dictionary (dict): The nested dictionary to modify.
        outer_key (str): The key for the outer dictionary level.
        inner_key (str): The key for the inner dictionary level.
        pair (list[int]): A list of two integers to add to the inner dictionary's list.

    Returns:
        None
    """
    if (pair[0] is not None and pair[1] is not None) and (
        outer_key in dictionary and inner_key in dictionary[outer_key]
    ):
        pair_list = dictionary[outer_key][inner_key]
        if pair not in pair_list:
            pair_list.append(pair)


def prepare_sentrix_ids_to_process_dictionary(
    batch_to_process: list[dict[str, str | int]],
) -> dict[str, dict[str, list[list[int]]]]:
    """
    Prepares a nested dictionary of Sentrix IDs to process, organized by preprocessing method.

    Processes a list of dictionaries containing batch processing details, extracting Sentrix IDs, preprocessing methods,
    bin sizes, and minimum probes per bin. Constructs a nested dictionary where each preprocessing method maps to
    Sentrix IDs, which in turn map to lists of unique [bin_size, min_probes_per_bin] pairs. Skips invalid or missing data.

    Args:
        batch_to_process (list[dict[str, str | int]]): List of dictionaries containing processing details with keys
            'sentrix_id', 'preprocessing_method', 'bin_size', and 'min_probes_per_bin'.

    Returns:
        dict[str, dict[str, list[list[int]]]]: A nested dictionary where:
            - Outer keys are preprocessing methods (str).
            - Inner keys are Sentrix IDs (str).
            - Values are lists of [bin_size, min_probes_per_bin] pairs (list[int]).
    """
    sentrix_ids_to_process_dictionary = {
        preprocessing_method: {
            sentrix_id: []
            for sentrix_id in [
                element.get("sentrix_id", None) for element in batch_to_process
            ]
            if (sentrix_id is not None and not isinstance(sentrix_id, int))
        }
        for preprocessing_method in [
            element.get("preprocessing_method", None) for element in batch_to_process
        ]
        if (
            preprocessing_method is not None
            and not isinstance(preprocessing_method, int)
        )
    }
    ###################################################################################
    for element in batch_to_process:
        sentrix_id = element.get("sentrix_id", None)
        if element.get("preprocessing_method", None) is not None:
            preprocessing_method: Optional[str] = str(
                element.get("preprocessing_method")
            )
        else:
            continue
        bin_size = element.get("bin_size", None)
        min_probes_per_bin = element.get("min_probes_per_bin", None)
        # downsize_to = element.get("downsize_to", CommonArrayType.NO_DOWNSIZING.value)

        if (
            sentrix_id is not None
            and preprocessing_method is not None
            and bin_size is not None
            and min_probes_per_bin is not None
        ):
            add_unique_pair(
                dictionary=sentrix_ids_to_process_dictionary,
                outer_key=preprocessing_method,
                inner_key=str(sentrix_id),
                pair=[int(bin_size), int(min_probes_per_bin)],
            )

    return sentrix_ids_to_process_dictionary
