from typing import Optional

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.core.logging import logger
from CQmanager.services.tasks import file_cleaner
from CQmanager.utilities.endpoint_utilities import detect_cli_client

router = APIRouter(
    prefix="/CQmanager",
)

# TODO: add endpoints for other cleanup tasks


@router.post(path="/remove_permission_denied_analyses/")
async def remove_permission_denied_analyses(
    req: Request,
    format: Optional[str] = None,
):
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    status_code: int = status.HTTP_501_NOT_IMPLEMENTED
    removed_results: int = 0
    message: str = "This endpoint is not yet implemented."

    if is_cli_client:
        return PlainTextResponse(content=message, status_code=status_code)
    else:
        return JSONResponse(
            content={
                "message": message,
                "removed_results_count": removed_results,
            },
            status_code=status_code,
        )


# TODO: add stopping all containers running locally and restarting them when it is done. Move this to a background task
@router.get(path="/remove_temporary_files/")
async def remove_temporary_files(
    req: Request,
    format: Optional[str] = None,
):
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)

    success, removed_files_count = file_cleaner.remove_temporary_files()

    status_code: int = (
        status.HTTP_200_OK if success else status.HTTP_500_INTERNAL_SERVER_ERROR
    )

    if is_cli_client:
        message: str = (
            f"Removed {removed_files_count} temporary files or directories.\n"
        )
        return PlainTextResponse(
            content=message,
            status_code=status_code,
        )
    else:
        return JSONResponse(
            content={
                "message": "Removed temporary files or directories.",
                "removed_files_count": removed_files_count,
            },
            status_code=status_code,
        )
