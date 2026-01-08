import asyncio
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.services.docker_runners import docker_runner
from CQmanager.utilities.endpoint_utilities import detect_cli_client

router = APIRouter(
    prefix="/CQmanager",
)


@router.get(path="/stop_all_cqmanager_analysis_and_plotting_containers/")
async def stop_all_containers(
    req: Request,
    format: Optional[str] = None,
):
    """Stop all CQmanager analysis containers asynchronously.

    Returns:
        JSONResponse: HTTP 200 response with the count of containers to be stopped.
    """

    asyncio.create_task(coro=asyncio.to_thread(docker_runner.stop_analysis_containers))

    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)

    if is_cli_client:
        message: str = "Stopping all running CQcalc and CQall_plotter containers.\n"
        response = PlainTextResponse(content=message)
    else:
        message: str = "Stopping all running CQcalc and CQall_plotter containers."
        response = JSONResponse(
            content=message,
            status_code=200,
        )

    return response
