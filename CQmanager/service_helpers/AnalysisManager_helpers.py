import logging
from typing import TYPE_CHECKING

import polars as pl
from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from cnquant_dependencies.paths_functions import (
    get_sentrix_ids,
    sentrix_ids_to_process,
)

from CQmanager.models.AnalysisTaskData import AnalysisTaskData

if TYPE_CHECKING:
    from CQmanager.core.config import AppConfig


def get_annotated_sentrix_ids(config: "AppConfig") -> set[str]:
    """
    Retrieves the set of annotated Sentrix IDs from the annotation file and filtered by available IDAT files.

    Args:
        config: The application configuration object containing paths and column names.

    Returns:
        set[str]: A set of Sentrix IDs that are annotated with a methylation class and present in the IDAT directory.
    """

    return set(
        (
            pl.read_csv(source=config.annotation_file_path)
            .select(config.sentrix_ids_column_in_annotation_file)
            .drop_nans()
            .to_series()
            .to_list()
        )
    ).intersection(get_sentrix_ids(idat_directory=config.idat_directory))


def get_reference_sentrix_ids(config: "AppConfig") -> set[str]:
    """
    Retrieves the set of reference Sentrix IDs from the reference annotation file

    Args:
        config: The application configuration object containing paths and column names.

    Returns:
        set[str]: A set of Sentrix IDs that are annotated as reference and present in the IDAT directory.
    """

    return set(
        pl.read_csv(source=config.reference_annotation_file_path)
        .select("Sentrix_id")
        .to_series()
        .to_list()
    ).intersection(get_sentrix_ids(idat_directory=config.idat_directory))


async def analyze_single_sentrix_id(task_data: dict):
    list_of_analysis_tasks: list[AnalysisTaskData] = [
        AnalysisTaskData(
            task_data={
                "sentrix_id": task_data["sentrix_id"],
                "preprocessing_method": task_data["preprocessing_method"],
                "bin_size": task_data["bin_size"],
                "min_probes_per_bin": task_data["min_probes_per_bin"],
                "downsize_to": task_data["downsize_to"],
            }
        )
    ]
    return list_of_analysis_tasks


async def get_missing_sentrix_ids_to_analyze(
    task_data: dict,
    config,
    downsize_to: str = CommonArrayType.NO_DOWNSIZING.value,
    logger: logging.Logger = logging.getLogger(name=__name__),
) -> list[AnalysisTaskData]:
    preprocessing_method = task_data["preprocessing_method"]
    bin_size = task_data["bin_size"]
    min_probes_per_bin = task_data["min_probes_per_bin"]

    reference_sentrix_ids = get_reference_sentrix_ids(config=config)

    set_of_sentrix_ids_to_process: set[str] = sentrix_ids_to_process(
        idat_directory=config.idat_directory,
        preprocessing_method=preprocessing_method,
        reference_sentrix_ids=reference_sentrix_ids,
        CNV_base_output_directory=config.results_directory,
        bin_size=bin_size,
        min_probes_per_bin=min_probes_per_bin,
        rerun_sentrix_ids=config.rerun_failed_analyses,
        downsize_to=downsize_to,
    )
    if downsize_to not in [target.value for target in CommonArrayType.get_members()]:
        downsize_to = CommonArrayType.NO_DOWNSIZING.value
        logger.error(
            msg=f"Invalid downsizing type: {downsize_to}. Using NO_DOWNSIZING instead."
        )
    list_of_analysis_tasks: list[AnalysisTaskData] = [
        AnalysisTaskData(
            task_data={
                "sentrix_id": sentrix_id,
                "preprocessing_method": preprocessing_method,
                "bin_size": bin_size,
                "min_probes_per_bin": min_probes_per_bin,
                "downsize_to": downsize_to,
            }
        )
        for sentrix_id in set_of_sentrix_ids_to_process
    ]
    return list_of_analysis_tasks


async def get_non_reduced_and_all_annotated_sentrix_ids_to_analyze(
    task_data: dict, config
) -> tuple[list[AnalysisTaskData], list[AnalysisTaskData]]:
    preprocessing_method = task_data["preprocessing_method"]
    bin_size = task_data["bin_size"]
    min_probes_per_bin = task_data["min_probes_per_bin"]

    annotated_sentrix_ids = get_annotated_sentrix_ids(config=config)
    reference_sentrix_ids = get_reference_sentrix_ids(config=config)

    annotated_sentrix_ids = annotated_sentrix_ids - reference_sentrix_ids
    # Get non-processed and not reduced sentrix IDs to process
    set_of_sentrix_ids_to_process: set[str] = sentrix_ids_to_process(
        sentrix_ids_to_process=annotated_sentrix_ids,
        idat_directory=config.idat_directory,
        preprocessing_method=preprocessing_method,
        reference_sentrix_ids=reference_sentrix_ids,
        CNV_base_output_directory=config.results_directory,
        bin_size=bin_size,
        min_probes_per_bin=min_probes_per_bin,
        rerun_sentrix_ids=config.rerun_failed_analyses,
        downsize_to=CommonArrayType.NO_DOWNSIZING.value,
    )
    list_of_analysis_tasks: list[AnalysisTaskData] = [
        AnalysisTaskData(
            task_data={
                "sentrix_id": sentrix_id,
                "preprocessing_method": preprocessing_method,
                "bin_size": bin_size,
                "min_probes_per_bin": min_probes_per_bin,
                "downsize_to": CommonArrayType.NO_DOWNSIZING.value,
            }
        )
        for sentrix_id in set_of_sentrix_ids_to_process
    ]

    list_of_annotated_sentrix_ids: list[AnalysisTaskData] = [
        AnalysisTaskData(
            task_data={
                "sentrix_id": sentrix_id,
                "preprocessing_method": preprocessing_method,
                "bin_size": bin_size,
                "min_probes_per_bin": min_probes_per_bin,
                "downsize_to": CommonArrayType.NO_DOWNSIZING.value,
            }
        )
        for sentrix_id in annotated_sentrix_ids
    ]

    return list_of_analysis_tasks, list_of_annotated_sentrix_ids


async def get_missing_sentrix_ids_to_reduce_and_analyze(
    sentrix_ids: set[str],
    preprocessing_method: str,
    bin_size: int,
    min_probes_per_bin: int,
    config: "AppConfig",
) -> list[AnalysisTaskData]:
    reference_sentrix_ids: set[str] = get_reference_sentrix_ids(config=config)

    set_of_sentrix_ids_to_process: set[str] = sentrix_ids_to_process(
        idat_directory=config.idat_directory,
        preprocessing_method=preprocessing_method,
        reference_sentrix_ids=reference_sentrix_ids,
        CNV_base_output_directory=config.results_directory,
        bin_size=bin_size,
        min_probes_per_bin=min_probes_per_bin,
        rerun_sentrix_ids=config.rerun_failed_analyses,
        downsize_to=CommonArrayType.NO_DOWNSIZING.value,
    )

    list_of_analysis_tasks: list[AnalysisTaskData] = [
        AnalysisTaskData(
            task_data={
                "sentrix_id": sentrix_id,
                "preprocessing_method": preprocessing_method,
                "bin_size": bin_size,
                "min_probes_per_bin": min_probes_per_bin,
                "downsize_to": CommonArrayType.NO_DOWNSIZING.value,
            }
        )
        for sentrix_id in set_of_sentrix_ids_to_process
    ]
    return list_of_analysis_tasks


async def get_missing_annotated_sentrix_ids_to_analyze(
    task_data: dict,
    config: "AppConfig",
    downsize_to: str = CommonArrayType.NO_DOWNSIZING.value,
) -> list[AnalysisTaskData]:
    preprocessing_method: str = task_data["preprocessing_method"]
    bin_size: int = task_data["bin_size"]
    min_probes_per_bin: int = task_data["min_probes_per_bin"]

    annotated_sentrix_ids: set[str] = get_annotated_sentrix_ids(config=config)
    reference_sentrix_ids: set[str] = get_reference_sentrix_ids(config=config)
    annotated_sentrix_ids = annotated_sentrix_ids - reference_sentrix_ids

    # Get missing non-downsized sentrix IDs to process
    set_of_sentrix_ids_to_process: set[str] = sentrix_ids_to_process(
        sentrix_ids_to_process=annotated_sentrix_ids,
        idat_directory=config.idat_directory,
        preprocessing_method=preprocessing_method,
        reference_sentrix_ids=reference_sentrix_ids,
        CNV_base_output_directory=config.results_directory,
        bin_size=bin_size,
        min_probes_per_bin=min_probes_per_bin,
        rerun_sentrix_ids=config.rerun_failed_analyses,
        downsize_to=downsize_to,
    )

    list_of_analysis_tasks: list[AnalysisTaskData] = [
        AnalysisTaskData(
            task_data={
                "sentrix_id": sentrix_id,
                "preprocessing_method": preprocessing_method,
                "bin_size": bin_size,
                "min_probes_per_bin": min_probes_per_bin,
                "downsize_to": downsize_to,
            }
        )
        for sentrix_id in set_of_sentrix_ids_to_process
    ]

    return list_of_analysis_tasks
