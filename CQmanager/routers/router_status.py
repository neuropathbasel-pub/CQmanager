from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from CQmanager.services.tasks import analysis_manager

router = APIRouter(
    prefix="/CQmanager",
)


@router.get(path="/queue_status/")
async def batch_status():
    """Get the current size of the task queue.

    Returns:
        JSONResponse: HTTP 200 response with the queue size.
    """
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=f"Analysis manager queue_size: {analysis_manager.batch_processor.get_total_number_of_sentrix_ids() + sum(len(item) for item in analysis_manager.unique_sentrix_ids_to_analyze_with_downsizing.values())}",
    )


@router.get(path="/app_status/")
async def app_status():
    """Check the status of the CQmanager application.

    Returns:
        JSONResponse: HTTP 200 response indicating CQmanager is running.
    """
    return JSONResponse(status_code=status.HTTP_200_OK, content="CQmanager is running.")
