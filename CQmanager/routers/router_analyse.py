from typing import Optional, Union

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.endpoint_models.CQdownsizeAnnotatedSamples import (
    CQdownsizeAnnotatedSamples,
)
from CQmanager.endpoint_models.CQmissingSettings import CQmissingSettings
from CQmanager.endpoint_models.CQsettings import CQsettings
from CQmanager.services.tasks import analysis_manager, cooldown_manager, task_queuer
from CQmanager.services.TaskType import TaskType
from CQmanager.utilities.endpoint_utilities import detect_cli_client

router = APIRouter(
    prefix="/CQmanager",
)


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
        message: str = f"\nSentrix ID {request.sentrix_id} will be processed shortly with following settings:\n - min_probes_per_bin: {request.min_probes_per_bin},\n - bin_size: {request.bin_size},\n - preprocessing_method: {request.preprocessing_method},\n - downsize_to: {request.downsize_to}.\n"
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
    if cooldown_manager.is_on_cooldown(endpoint_name="analyse_missing"):
        if is_cli_client:
            message: str = f"\nThe endpoint 'analyse_missing' is on cooldown.\nPlease wait {cooldown_manager.return_remaining_time(endpoint_name='analyse_missing')} seconds before submitting a new request\n"
            return PlainTextResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content=message,
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "message": f"This endpoint is on cooldown. Please wait a moment before submitting a new request.\nRemaining cooldown time: {cooldown_manager.return_remaining_time(endpoint_name='analyse_missing')} seconds."
                },
            )
    else:
        cooldown_manager.update_last_request_time(endpoint_name="analyse_missing")

        new_task: dict[str, Union[str, CQmissingSettings]] = {
            "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
            "data": request,
        }

        await task_queuer.task_queue.put(item=new_task)
        if is_cli_client:
            message: str = f"\nMissing data will be processed shortly with following settings:\n - min_probes_per_bin: {request.min_probes_per_bin},\n - bin_size: {request.bin_size},\n - preprocessing_method: {request.preprocessing_method},\n - downsize_to: {request.downsize_to}.\n"
            return PlainTextResponse(
                content=message,
                status_code=status.HTTP_200_OK,
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
                status_code=status.HTTP_200_OK,
            )


@router.post(path="/downsize_annotated_samples_for_summary_plots/")
async def downsize_annotated_samples_for_summary_plots(
    request: CQdownsizeAnnotatedSamples,
    req: Request,
    format: Optional[str] = None,
):
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)

    if cooldown_manager.is_on_cooldown(
        endpoint_name="downsize_annotated_samples_for_summary_plots_cooldown"
    ):
        if is_cli_client:
            message: str = f"\nThe endpoint 'downsize_annotated_samples_for_summary_plots' is on cooldown.\nPlease wait {cooldown_manager.return_remaining_time(endpoint_name='downsize_annotated_samples_for_summary_plots_cooldown')} seconds before submitting a new request\n"
            return PlainTextResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content=message,
            )
        else:
            message: str = f"This endpoint is on cooldown. Please wait a moment before submitting a new request.\nRemaining cooldown time: {cooldown_manager.return_remaining_time(endpoint_name='downsize_annotated_samples_for_summary_plots_cooldown')} seconds."
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"message": message},
            )
    else:
        new_task: dict[str, Union[str, CQdownsizeAnnotatedSamples]] = {
            "type": TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS,
            "data": request,
        }
        cooldown_manager.update_last_request_time(
            endpoint_name="downsize_annotated_samples_for_summary_plots_cooldown"
        )
        await task_queuer.task_queue.put(item=new_task)
        if is_cli_client:
            message: str = f"\nMissing data for summary plots will be processed shortly with following settings:\n - min_probes_per_bin: {request.min_probes_per_bin},\n - bin_size: {request.bin_size},\n - preprocessing_method: {request.preprocessing_method}.\n"
            return PlainTextResponse(
                content=message,
                status_code=status.HTTP_200_OK,
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "message": "Missing data for summary plots will be processed shortly. If there was no non-downsized data, this request will need to be repeated in order to analyse the downsized data."
                },
            )


# FIXME
@router.post(path="/empty_analysis_queue/")
async def empty_analysis_queue(
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
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    analysis_manager.batch_processor.empty_queue()
    if is_cli_client:
        return PlainTextResponse(
            content="\nThe queue for CQcalc jobs has been emptied.\n",
            status_code=status.HTTP_200_OK,
        )
    else:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "The queue for CQcalc jobs has been emptied."},
        )
