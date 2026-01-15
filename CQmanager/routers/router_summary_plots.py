from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.endpoint_models.SummaryPlottingEndpointValidator import (
    SummaryPlottingEndpointValidator,
)
from CQmanager.services.docker_runners import docker_runner
from CQmanager.services.tasks import task_queuer
from CQmanager.services.TaskType import TaskType
from CQmanager.utilities.endpoint_utilities import detect_cli_client
from CQmanager.utilities.lock_thread import lock

router = APIRouter(
    prefix="/CQmanager",
)


@router.post(path="/make_summary_plots/")
async def make_summary_plots(
    request: SummaryPlottingEndpointValidator,
    req: Request,
    format: Optional[str] = None,
):
    """Add a summary plotting task to the task queue.

    Args:
        request (SummaryPlotting): Summary plotting task data.

    Returns:
        JSONResponse: Confirmation message with HTTP 200 status code.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    is_CQall_plotter_running: bool = (
        False
        if docker_runner.check_running_CNV_containers(
            name_prefix=docker_runner.cqall_plotter_container_name_prefix
        )
        == 0
        else True
    )

    if is_CQall_plotter_running:
        message: str = "\nA CQall_plotter container is already running.\nThe new summary plotting task will be added to the queue and processed once the current container has finished.\n"
        if is_cli_client:
            return PlainTextResponse(content=message, status_code=200)
        else:
            return JSONResponse(
                content={"message": message, "timestamp": request.timestamp},
                status_code=200,
            )

    with lock:
        task = {"type": TaskType.SUMMARY_PLOT, "data": request.model_dump()}
        await task_queuer.task_queue.put(item=task)

    if is_cli_client:
        message: str = f"""
The make_summary_plots endpoint received a request on {request.timestamp} with the following settings:

Preprocessing method: {request.preprocessing_method}
Methylation classes: {request.methylation_classes}
Bin size: {request.bin_size}
Min probes per bin: {request.min_probes_per_bin}
Downsize to: {request.downsize_to}\n"""
        return PlainTextResponse(content=message)
    else:
        message: str = f"The make_summary_plots endpoint received a request with the following settings: preprocessing method: {request.preprocessing_method}, methylation classes: {request.methylation_classes}, bin size: {request.bin_size}, min probes per bin: {request.min_probes_per_bin}, downsize to: {request.downsize_to}."
        return JSONResponse(
            content={"message": message, "timestamp": request.timestamp},
            status_code=200,
        )


@router.get(path="/stop_summary_plotting_container/")
async def stop_summary_plotting_container(
    req: Request,
    format: Optional[str] = None,
):
    """Stop summary plotting container and return status.

    Returns:
        PlainTextResponse: Status message with HTTP 200 status code.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    status_code, return_message = docker_runner.stop_summary_plotting_container()
    timestamp: str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if is_cli_client:
        return PlainTextResponse(content=return_message, status_code=status_code)
    else:
        return JSONResponse(
            content={
                "message": return_message,
                "timestamp": timestamp,
            },
            status_code=status_code,
        )
