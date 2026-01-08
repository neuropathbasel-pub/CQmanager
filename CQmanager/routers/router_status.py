from typing import Optional

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.services.tasks import analysis_manager
from CQmanager.utilities.endpoint_utilities import detect_cli_client

router = APIRouter(
    prefix="/CQmanager",
)


@router.get(path="/queue_status/")
async def batch_status(
    req: Request,
    format: Optional[str] = None,
):
    """Get the current size of the task queue.

    Returns:
        JSONResponse: HTTP 200 response with the queue size.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    number_of_sentrix_ids_in_queue = (
        analysis_manager.batch_processor.get_total_number_of_sentrix_ids()
        + sum(
            len(item)
            for item in analysis_manager.unique_sentrix_ids_to_analyze_with_downsizing.values()
        )
    )
    message: str = f"Analysis manager queue_size: {number_of_sentrix_ids_in_queue}"
    if is_cli_client:
        return PlainTextResponse(content=f"{message}\n", status_code=status.HTTP_200_OK)
    else:
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"message": message}
        )


@router.get(path="/app_status/")
async def app_status(
    req: Request,
    format: Optional[str] = None,
):
    """Check the status of the CQmanager application.

    Returns:
        JSONResponse: HTTP 200 response indicating CQmanager is running.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    message: str = "CQmanager is running."
    if is_cli_client:
        return PlainTextResponse(content=f"{message}\n", status_code=status.HTTP_200_OK)
    else:
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"message": message}
        )
