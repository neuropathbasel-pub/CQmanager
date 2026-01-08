from typing import Optional, Union

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.endpoint_models.CQdownsizeAnnotatedSamples import (
    CQdownsizeAnnotatedSamples,
)
from CQmanager.endpoint_models.CQmissingSettings import CQmissingSettings
from CQmanager.endpoint_models.CQsettings import CQsettings
from CQmanager.services.tasks import analysis_manager, task_queuer
from CQmanager.services.TaskType import TaskType
from CQmanager.utilities.endpoint_utilities import detect_cli_client

router = APIRouter(
    prefix="/CQmanager",
)


# TODO: Move this logic inside of AnalysisManager
@router.post(path="/analyse/")
async def analyse(
    request: CQsettings,
    req: Request,
    format: Optional[str] = None,
):
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    new_task: dict[str, Union[str, CQsettings]] = {
        "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
        "data": request,
    }

    await task_queuer.task_queue.put(item=new_task)

    if is_cli_client:
        message: str = f"Sentrix ID {request.sentrix_id} will be processed shortly with following settings:\n - min_probes_per_bin: {request.min_probes_per_bin},\n - bin_size: {request.bin_size},\n - preprocessing_method: {request.preprocessing_method},\n - downsize_to: {request.downsize_to}.\n"
        return PlainTextResponse(
            content=message,
            status_code=200,
        )
    else:
        message: str = "A new single analysis task has been added to the queue."
        return JSONResponse(
            content={
                "message": message,
                "sentrix_id": request.sentrix_id,
                "min_probes_per_bin": request.min_probes_per_bin,
                "bin_size": request.bin_size,
                "preprocessing_method": request.preprocessing_method,
                "downsize_to": request.downsize_to,
                "timestamp": request.timestamp,
            },
            status_code=200,
        )


@router.post(path="/analyse_missing/")
async def analyse_missing(
    request: CQmissingSettings,
    req: Request,
    format: Optional[str] = None,
):
    """Add missing Sentrix IDs to analysis task queue in chunks.

    Args:
        request (CQmissingSettings): Settings for preprocessing method, bin size, and minimum probes per bin.

    Returns:
        dict: Confirmation message with count of Sentrix IDs and analysis settings.

    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    # TODO: First check if the AnalysisManager already has a task with these settings in the queue
    new_task: dict[str, Union[str, CQmissingSettings]] = {
        "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
        "data": request,
    }

    await task_queuer.task_queue.put(item=new_task)
    if is_cli_client:
        message: str = f"Missing data will be processed shortly with following settings:\n - min_probes_per_bin: {request.min_probes_per_bin},\n - bin_size: {request.bin_size},\n - preprocessing_method: {request.preprocessing_method},\n - downsize_to: {request.downsize_to}.\n"
        return PlainTextResponse(
            content=message,
            status_code=200,
        )
    else:
        message: str = "Missing data has been added to the analysis queue."
        return JSONResponse(
            content={
                "message": message,
                "min_probes_per_bin": request.min_probes_per_bin,
                "bin_size": request.bin_size,
                "preprocessing_method": request.preprocessing_method,
                "downsize_to": request.downsize_to,
                "timestamp": request.timestamp,
            },
            status_code=200,
        )


# TODO: FIXME: There is a duplication of the endpoints. Decide which stays
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
async def view_analysis_queue(
    req: Request,
    format: Optional[str] = None,
):
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


# FIXME
@router.post(path="/empty_analysis_queue/")
async def empty_analysis_queue(
    request: CQsettings,
    req: Request,
    format: Optional[str] = None,
):
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
