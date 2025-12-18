from typing import Union

from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from CQmanager.endpoint_models.CQdownsizeAnnotatedSamples import (
    CQdownsizeAnnotatedSamples,
)
from CQmanager.endpoint_models.CQmissingSettings import CQmissingSettings
from CQmanager.endpoint_models.CQsettings import CQsettings
from CQmanager.services.tasks import analysis_manager, task_queuer
from CQmanager.services.TaskType import TaskType

router = APIRouter(
    prefix="/CQmanager",
)


# TODO: Move this logic inside of AnalysisManager
@router.post(path="/analyse/")
async def analyse(request: CQsettings):
    new_task: dict[str, Union[str, CQsettings]] = {
        "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
        "data": request,
    }

    await task_queuer.task_queue.put(item=new_task)
    downsizing_to = (
        f"', and downsizing to ' {request.downsize_to}"
        if request.downsize_to != CommonArrayType.NO_DOWNSIZING.value
        else ""
    )
    return {
        "message": f"{request.sentrix_id} will be processed shortly with following settings: min_probes_per_bin {request.min_probes_per_bin}, bin_size {request.bin_size}, preprocessing_method {request.preprocessing_method}{downsizing_to}.",
    }


@router.post(path="/analyse_missing/")
async def analyse_missing(request: CQmissingSettings):
    """Add missing Sentrix IDs to analysis task queue in chunks.

    Args:
        request (CQmissingSettings): Settings for preprocessing method, bin size, and minimum probes per bin.

    Returns:
        dict: Confirmation message with count of Sentrix IDs and analysis settings.

    """
    # TODO: First check if the AnalysisManager already has a task with these settings in the queue
    new_task: dict[str, Union[str, CQmissingSettings]] = {
        "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
        "data": request,
    }

    await task_queuer.task_queue.put(item=new_task)
    return {
        "message": f"Missing data will be processed shortly with following settings: min_probes_per_bin {request.min_probes_per_bin}, bin_size {request.bin_size}, preprocessing_method {request.preprocessing_method}"
    }


@router.post(path="/downsize_annotated_samples_for_summary_plots/")
async def downsize_annotated_samples_for_summary_plots(
    request: CQdownsizeAnnotatedSamples,
):
    # This prevents crashes after submitting multiple downsizing tasks
    # FIXME: This needs to be improved, as it is not user friendly
    current_key = tuple(
        [
            str(request.preprocessing_method),
            int(request.bin_size),
            int(request.min_probes_per_bin),
        ]
    )

    if (
        len(
            analysis_manager.unique_sentrix_ids_to_analyze_with_downsizing.get(
                current_key,  # type: ignore
                set(),  # type: ignore
            )
        )
        == 0
    ):
        new_task: dict[str, Union[str, CQdownsizeAnnotatedSamples]] = {
            "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
            "data": request,
        }

        await task_queuer.task_queue.put(item=new_task)
        return JSONResponse(
            status_code=400,
            content={
                "message": "Missing data for summary plots will be processed shortly"
            },
        )
    else:
        return JSONResponse(
            status_code=400,
            content={
                "message": "There are still downsizing tasks being processed. Please wait until these are finished before submitting new downsizing tasks."
            },
        )


# TODO: FIXME: It seems that this queue length is not as you wish to see
@router.get(path="/view_analysis_queue/")
async def view_analysis_queue():
    """Add an analysis task to the task queue and register the request.

    Args:
        request (CQsettings): Analysis task data.

    Returns:
        dict: Confirmation message with Sentrix ID and timestamp.

    Raises:
        Exception: Logs error if appending to analysis requests register fails.
    """
    return {
        "analysis_queue_length": f"{analysis_manager.batch_processor.queue_length()}"
    }


@router.post(path="/empty_analysis_queue/")
async def empty_analysis_queue(request: CQsettings):
    """Add an analysis task to the task queue and register the request.

    Args:
        request (CQsettings): Analysis task data.

    Returns:
        dict: Confirmation message with Sentrix ID and timestamp.

    Raises:
        Exception: Logs error if appending to analysis requests register fails.
    """
    analysis_manager.batch_processor.empty_queue()
    return {"message": "Analysis queue emptied"}
