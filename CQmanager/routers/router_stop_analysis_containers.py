import asyncio

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from CQmanager.services.docker_runners import docker_runner

router = APIRouter(
    prefix="/CQmanager",
)


@router.get(path="/stop_all_cqmanager_analysis_and_plotting_containers/")
async def stop_all_containers():
    """Stop all CQmanager analysis containers asynchronously.

    Returns:
        JSONResponse: HTTP 200 response with the count of containers to be stopped.
    """
    response = JSONResponse(
        content="Stopping all running CQcalc and CQall_plotter containers.",
        status_code=status.HTTP_200_OK,
    )

    asyncio.create_task(asyncio.to_thread(docker_runner.stop_analysis_containers))

    return response
