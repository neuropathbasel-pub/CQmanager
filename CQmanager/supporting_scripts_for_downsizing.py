import logging
import traceback
from pathlib import Path
from typing import Optional, cast

from cnquant_dependencies.enums.ArrayType import ArrayType
from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from cnquant_dependencies.models.RawData import RawData
from cnquant_dependencies.models.StatusJson import (
    load_analysis_status_json,
)


def get_successfully_analyzed_sentrix_ids_dictionary(
    analyzed_sentrix_ids_directory: Path,
    get_only_this_downsizing_target: Optional[CommonArrayType] = None,
    list_of_sentrix_ids_to_consider: Optional[list[str]] = None,
) -> dict[CommonArrayType, dict[ArrayType, set[str]]]:
    """
    Retrieves successfully analyzed Sentrix IDs from status JSON files in subdirectories.

    Scans the given directory for subdirectories containing .json status files. For each
    status file where analysis completed successfully, extracts the Sentrix ID and groups
    it by the downsizing target (CommonArrayType) and array type (ArrayType).

    Args:
        analyzed_sentrix_ids_directory (Path): The root directory containing subdirectories
            with status JSON files.
        list_of_sentrix_ids_to_consider (Optional[list[str]], optional): If provided, only
            consider subdirectories whose names are in this list. Defaults to None.

    Returns:
        dict[CommonArrayType, dict[ArrayType, set[str]]]: A nested dictionary where the
        outer keys are CommonArrayType (downsizing targets), inner keys are ArrayType,
        and values are sets of successfully analyzed Sentrix IDs.
    """
    if get_only_this_downsizing_target is not None:
        downsizing_targets: list[CommonArrayType] = [get_only_this_downsizing_target]
    else:
        downsizing_targets: list[CommonArrayType] = [
            array_type for array_type in CommonArrayType.get_members()
        ]

    available_results_per_downsizing_target: dict[
        CommonArrayType, dict[ArrayType, set[str]]
    ] = {
        downsizing_target: dict(
            (array_type, set()) for array_type in ArrayType.valid_array_types()
        )
        for downsizing_target in downsizing_targets
    }
    sentrix_ids_directories: list[Path] = [
        directory
        for directory in analyzed_sentrix_ids_directory.iterdir()
        if directory.is_dir()
    ]

    if list_of_sentrix_ids_to_consider is not None:
        sentrix_ids_directories = [
            directory
            for directory in sentrix_ids_directories
            if directory.name in list_of_sentrix_ids_to_consider
        ]

    valid_array_types: list[str] = [
        array_type.value for array_type in ArrayType.valid_array_types()
    ]

    for directory in sentrix_ids_directories:
        status_files_paths: list[Path] = [
            file for file in directory.iterdir() if file.name.endswith(".json")
        ]
        for status_file_path in status_files_paths:
            status_data: dict = load_analysis_status_json(
                status_json_path=status_file_path
            )
            if (
                status_data.get("analysis_completed_successfully", "False").lower()
                != "true"
            ):
                continue
            analysis_settings: Optional[dict] = status_data.get(
                "analysis_settings", None
            )

            if analysis_settings is not None:
                downsizing_target_str = analysis_settings.get("downsized_to", None)
                valid_array_type_str: Optional[str] = status_data.get(
                    "array_type", None
                )
                if (
                    valid_array_type_str in valid_array_types
                    and downsizing_target_str is not None
                    and downsizing_target_str
                    in [
                        downsizing_target.value
                        for downsizing_target in downsizing_targets
                    ]
                ):
                    downsizing_target: CommonArrayType = (
                        CommonArrayType.get_member_from_string(
                            value=downsizing_target_str
                        )
                    )  # type: ignore[assignment]
                    array_type: ArrayType = ArrayType.get_member_from_string(
                        value=valid_array_type_str
                    )
                    sentrix_id: Optional[str] = status_data.get("sentrix_id", None)
                    if sentrix_id is not None:
                        available_results_per_downsizing_target[downsizing_target][
                            array_type
                        ].add(sentrix_id)

    return available_results_per_downsizing_target


def select_best_downsizing_target(
    downsizing_target_members: dict[CommonArrayType, set[ArrayType]],
    all_array_types_for_the_checked_list: set[ArrayType],
) -> Optional[CommonArrayType]:
    """
    Selects the best CommonArrayType downsizing target that can handle all provided array types.

    Finds CommonArrayType keys whose associated set of ArrayType values contains all elements
    of all_array_types_for_the_checked_list (i.e., the set is a superset). Among matching keys,
    prioritizes the one with the fewest ArrayType elements.

    Args:
        downsizing_target_members (dict[CommonArrayType, set[ArrayType]]): Dictionary mapping
            CommonArrayType to sets of ArrayType it can convert from.
        all_array_types_for_the_checked_list (set[ArrayType]): Set of ArrayType to check against.

    Returns:
        Optional[CommonArrayType]: The matching CommonArrayType with the smallest set, or None
        if no matches are found.
    """
    matching_targets = [
        (key, value_set)
        for key, value_set in downsizing_target_members.items()
        if all_array_types_for_the_checked_list.issubset(value_set)
    ]
    if not matching_targets:
        return None

    matching_targets.sort(key=lambda x: len(x[1]))
    matching_target = matching_targets[0][0]

    return matching_target


def get_best_downsizing_match_for_given_sentrix_ids(
    sentrix_ids_to_check: list[str] | set[str],
    successfully_analyzed_sentrix_ids_dictionary: dict[
        CommonArrayType, dict[ArrayType, set[str]]
    ],
) -> Optional[CommonArrayType]:
    """
    Selects the best CommonArrayType downsizing target that can handle all provided array types.

    Finds CommonArrayType keys whose associated set of ArrayType values contains all elements
    of all_array_types_for_the_checked_list (i.e., the set is a superset). Among matching keys,
    prioritizes the one with the fewest ArrayType elements.

    Args:
        downsizing_target_members (dict[CommonArrayType, set[ArrayType]]): Dictionary mapping
            CommonArrayType to sets of ArrayType it can convert from.
        all_array_types_for_the_checked_list (set[ArrayType]): Set of ArrayType to check against.

    Returns:
        Optional[CommonArrayType]: The matching CommonArrayType with the smallest set, or None
        if no matches are found.
    """
    all_existing_downsizing_targets: set[CommonArrayType] = set(
        [
            target
            for target in CommonArrayType.get_members()
            if target != CommonArrayType.NO_DOWNSIZING
        ]
    )

    downsizing_target_members: dict[CommonArrayType, set[ArrayType]] = {
        target: cast(
            set[ArrayType], set(CommonArrayType.get_array_types(convert_from_to=target))
        )
        for target in all_existing_downsizing_targets
    }
    all_array_types_for_the_checked_list: set[ArrayType] = set()
    for array_type_dict in successfully_analyzed_sentrix_ids_dictionary.values():
        for array_type, available_sentrix_ids in array_type_dict.items():
            matching_sentrix_ids: set[str] = available_sentrix_ids.intersection(
                set(sentrix_ids_to_check)
            )
            if matching_sentrix_ids:
                all_array_types_for_the_checked_list.add(array_type)

    best_match = select_best_downsizing_target(
        downsizing_target_members=downsizing_target_members,
        all_array_types_for_the_checked_list=all_array_types_for_the_checked_list,
    )

    return best_match


def get_sentrix_ids_per_downsizing_target(
    all_downsizing_targets_for_the_given_sentrix_ids: list[CommonArrayType],
    available_not_downsized_annotated_samples_results: dict[ArrayType, set[str]],
) -> dict[CommonArrayType, set[str]]:
    """
    Groups sentrix IDs by downsizing targets based on available array types.

    For each provided downsizing target, collects sentrix IDs from the available results
    that correspond to array types valid for that target.

    Args:
        all_downsizing_targets_for_the_given_sentrix_ids (list[CommonArrayType]): List of
            CommonArrayType targets to group by.
        available_not_downsized_annotated_samples_results (dict[ArrayType, set[str]]):
            Dictionary mapping ArrayType to sets of sentrix IDs from non-downsized results.

    Returns:
        dict[CommonArrayType, set[str]]: Dictionary where keys are CommonArrayType targets
        and values are sets of sentrix IDs that can be downsized using that target.
    """
    samples_to_downsize_per_target: dict[CommonArrayType, set[str]] = {
        target: set() for target in all_downsizing_targets_for_the_given_sentrix_ids
    }
    for target in all_downsizing_targets_for_the_given_sentrix_ids:
        valid_array_types_for_the_target = CommonArrayType.get_array_types(
            convert_from_to=target
        )
        for array_type in available_not_downsized_annotated_samples_results.keys():
            if array_type in valid_array_types_for_the_target:
                samples_to_downsize_per_target[target].update(
                    available_not_downsized_annotated_samples_results.get(
                        array_type, set()
                    )
                )
    return samples_to_downsize_per_target


def get_analyzed_sentrix_ids_dictionary(
    analyzed_sentrix_ids_directory: Path,
    list_of_sentrix_ids_to_consider: Optional[list[str]] = None,
    logger: logging.Logger = logging.getLogger(__name__),
) -> dict[str, dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]]:
    """
    Retrieves analyzed sentrix IDs and their details from status JSON files.

    Scans subdirectories in the given directory for .json status files. For each,
    extracts array type and downsizing targets if analysis completed successfully.
    Optionally filters to specific sentrix IDs.

    Args:
        analyzed_sentrix_ids_directory (Path): Root directory containing sentrix ID subdirs.
        list_of_sentrix_ids_to_consider (Optional[list[str]], optional): Sentrix IDs to include.
            If None, includes all found. Defaults to None.

    Returns:
        dict[str, dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]]: Nested dict
        with sentrix_id keys, each containing 'array_type' (str) and 'downsizing_targets'
        (dict[CommonArrayType, bool] for success status).
    """
    analyzed_sentrix_ids_dictionary: dict[
        str, dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]
    ] = dict()

    if not analyzed_sentrix_ids_directory.exists():
        return analyzed_sentrix_ids_dictionary

    sentrix_ids_directories: list[Path] = [
        directory
        for directory in analyzed_sentrix_ids_directory.iterdir()
        if directory.is_dir()
    ]

    if list_of_sentrix_ids_to_consider is not None:
        sentrix_ids_directories = [
            directory
            for directory in sentrix_ids_directories
            if directory.name in list_of_sentrix_ids_to_consider
        ]

    for directory in sentrix_ids_directories:
        status_files_paths: list[Path] = [
            file for file in directory.iterdir() if "_status" in file.name
        ]
        analyzed_sentrix_ids_dictionary[directory.name] = {
            "downsizing_targets": dict(),
        }
        for status_file_path in status_files_paths:
            status_data: dict = load_analysis_status_json(
                status_json_path=status_file_path
            )
            array_type: Optional[str] = status_data.get("array_type", None)
            if array_type is None:
                message: str = (
                    f"Array type missing in status file '{status_file_path}'."
                )
                logger.error(msg=message)
                raise ValueError(message)

            analyzed_sentrix_ids_dictionary[directory.name]["array_type"] = array_type
            analysis_settings: Optional[dict] = status_data.get(
                "analysis_settings", None
            )
            if analysis_settings is not None:
                downsizing_target_str = analysis_settings.get("downsized_to", "None")
            else:
                downsizing_target_str = "None"

            downsizing_target: Optional[CommonArrayType] = (
                CommonArrayType.get_member_from_string(downsizing_target_str)
            )  # type: ignore[assignment]

            if (
                status_data.get("analysis_completed_successfully", "False").lower()
                == "true"
            ):
                status: bool = True
            else:
                status = False
            if downsizing_target is not None:
                analyzed_sentrix_ids_dictionary[directory.name]["downsizing_targets"][
                    downsizing_target
                ] = status  # pyright: ignore[reportArgumentType]
            else:
                message: str = f"Invalid downsizing target string '{downsizing_target_str}' in status file '{status_file_path}'."
                logger.error(msg=message)
                raise ValueError(message)

    return analyzed_sentrix_ids_dictionary


def append_missing_info_to_the_analyzed_sentrix_ids_dictionary(
    missing_sentrix_ids: set[str],
    analyzed_sentrix_ids_dictionary: dict[
        str, dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]
    ],
    idat_directory: Path,
    logger: logging.Logger = logging.getLogger(__name__),
):
    """
    Appends missing sentrix IDs to the analyzed dictionary with array type info.

    For each missing sentrix ID, adds an entry with array type from RawData and
    an empty downsizing_targets dict.

    Args:
        missing_sentrix_ids (set[str]): Set of sentrix IDs not in the dictionary.
        analyzed_sentrix_ids_dictionary (dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]):
            The dictionary to update.
        idat_directory (Path): Directory containing IDAT files for array type detection.

    Returns:
        dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]: Updated dictionary.
    """
    for sentrix_id in missing_sentrix_ids:
        try:
            analyzed_sentrix_ids_dictionary[sentrix_id] = {  # pyright: ignore[reportArgumentType]
                "array_type": RawData(basenames=idat_directory / sentrix_id).array_type,
                "downsizing_targets": {},
            }
        except FileNotFoundError:
            pass
            # traceback.print_exc()
            # logger.error(
            #     msg=f"IDAT files for Sentrix ID '{sentrix_id}' not found in '{idat_directory}'. Skipping."
            # )
        except Exception:
            traceback.print_exc()
            logger.error(msg=f"Error processing Sentrix ID '{sentrix_id}'. Skipping.")
    return analyzed_sentrix_ids_dictionary


higher_probes_downsizing_targets: dict[CommonArrayType, list[CommonArrayType]] = {
    CommonArrayType.EPIC_v2_EPIC_v1_to_HM450K: [
        CommonArrayType.EPIC_v2_EPIC_v1_to_HM450K,
    ],
    CommonArrayType.EPIC_v2_EPIC_v1_HM450_to_MSA48: [
        CommonArrayType.EPIC_v2_EPIC_v1_to_HM450K,
        CommonArrayType.EPIC_v2_EPIC_v1_HM450_to_MSA48,
    ],
}
