import asyncio
from typing import Union

import polars as pl
# from cnquant_dependencies.CommonArrayType import CommonArrayType

from CQmanager.core.config import config
from CQmanager.endpoint_models.CQdownsizeAnnotatedSamples import (
    CQdownsizeAnnotatedSamples,
)
from CQmanager.endpoint_models.CQmissingSettings import CQmissingSettings
from CQmanager.endpoint_models.CQsettings import CQsettings
from CQmanager.services.tasks import analysis_manager
from CQmanager.services.TaskType import TaskType
# from CQmanager.utilities.utilities import get_sentrix_ids, sentrix_ids_to_process


async def perform_downsize_annotated_samples(request: CQdownsizeAnnotatedSamples):
    """Perform the downsizing logic for annotated samples."""
    # Read data annotation file
    annotated_samples = pl.read_csv(source=config.annotation_file_path)
    # Check if the Sentrix IDs have corresponding idat files
    sentrix_ids = list(
        set(
            (
                annotated_samples.select(config.sentrix_ids_column_in_annotation_file)
                .drop_nans()
                .to_series()
                .to_list()
            )
        ).intersection(get_sentrix_ids(idat_directory=config.idat_directory))
    )
    # Check if the Sentrix IDs were analyzed in the first place
    reference_sentrix_ids: set[str] = set(
        pl.read_csv(source=config.reference_annotation_file_path)
        .select("Sentrix_id")
        .to_series()
        .to_list()
    )
    set_of_sentrix_ids_to_process = sentrix_ids_to_process(
        sentrix_ids_to_process=sentrix_ids,
        idat_directory=config.idat_directory,
        preprocessing_method=request.preprocessing_method,
        reference_sentrix_ids=reference_sentrix_ids,
        CNV_base_output_directory=config.results_directory,
        bin_size=request.bin_size,
        min_probes_per_bin=request.min_probes_per_bin,
        rerun_sentrix_ids=config.rerun_failed_analyses,
        downsize_to=CommonArrayType.NO_DOWNSIZING.value,
    )

    # Send for analysis without downsizing
    await send_sentrix_ids_for_analysis_without_downsizing(
        request=request,
        set_of_sentrix_ids_to_process=set_of_sentrix_ids_to_process,
        downsize_to=CommonArrayType.NO_DOWNSIZING.value,
        task_type=TaskType.ANALYSIS,
    )

    # Send for downsizing
    await send_sentrix_ids_for_downsizing(
        request=request,
        set_of_sentrix_ids_to_process=set(sentrix_ids),
        task_type=TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
    )

    # Start the not-ready data process if not already started
    if not analysis_manager.check_for_not_ready_data:
        asyncio.create_task(analysis_manager.process_not_ready_data())
        analysis_manager.check_for_not_ready_data = True


# Helper functions (move from router_analyse.py if not already shared)
async def send_sentrix_ids_for_analysis_without_downsizing(
    request: Union[CQsettings, CQmissingSettings, CQdownsizeAnnotatedSamples],
    set_of_sentrix_ids_to_process: set[str],
    downsize_to: str,
    task_type: str = TaskType.ANALYSIS,
):
    # ... (copy the function from router_analyse.py)
    pass


async def send_sentrix_ids_for_downsizing(
    request: Union[CQsettings, CQmissingSettings, CQdownsizeAnnotatedSamples],
    set_of_sentrix_ids_to_process: set[str],
    task_type: str = TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
):
    # ... (copy the function from router_analyse.py)
    pass
