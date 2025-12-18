from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from CQmanager.endpoint_models.SummaryPlotting import SummaryPlotting
from CQmanager.services.docker_runners import docker_runner
from CQmanager.services.tasks import task_queuer
from CQmanager.services.TaskType import TaskType
from CQmanager.utilities.lock_thread import lock

router = APIRouter(
    prefix="/CQmanager",
)


@router.post(path="/make_summary_plots/")
async def make_summary_plots(request: SummaryPlotting):
    """Add a summary plotting task to the task queue.

    Args:
        request (SummaryPlotting): Summary plotting task data.

    Returns:
        JSONResponse: Confirmation message with HTTP 200 status code.
    """
    with lock:
        task = {"type": TaskType.SUMMARY_PLOT, "data": request.model_dump()}
        await task_queuer.task_queue.put(item=task)
    return JSONResponse(
        content=f"Task added to batch on {request.timestamp}",
        status_code=status.HTTP_200_OK,
    )


@router.get(path="/stop_summary_plotting_container/")
async def stop_summary_plotting_container():
    """Stop summary plotting containers and return status.

    Returns:
        JSONResponse: Status message with HTTP 200 status code.
    """
    return_status = docker_runner.stop_summary_plotting_container()
    return JSONResponse(content=return_status, status_code=status.HTTP_200_OK)
