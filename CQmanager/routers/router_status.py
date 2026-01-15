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

    def get_queue_status_message(
        batch_processor_queue: dict, message_for_cli_client: bool = True
    ) -> str:
        analysis_groups = analysis_manager.batch_processor.queue.keys()
        if message_for_cli_client:
            line_break: str = "\n"
        else:
            line_break = ""

        if analysis_groups:
            message: str = f"{line_break}There are following analysis groups in the analysis queue for CQcalc:{line_break}{line_break}"
            for group in analysis_groups:
                num_sentrix_ids = len(batch_processor_queue[group])
                group_descriptor: str = f"(bin size: {group[0]}, min probes per bin: {group[1]}, preprocessing method: {group[2]}, downsize to: {group[3]})"
                message += f" - Analysis group {group_descriptor} has {num_sentrix_ids} sentrix IDs in the queue.{line_break}"
            return message
        else:
            message: str = (
                f"{line_break}The analysis queue is currently empty.{line_break}"
            )
        return message

    queue_status_message = get_queue_status_message(
        batch_processor_queue=analysis_manager.batch_processor.queue,
        message_for_cli_client=is_cli_client,
    )

    if is_cli_client:
        return PlainTextResponse(
            content=f"{queue_status_message}\n", status_code=status.HTTP_200_OK
        )
    else:
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"message": queue_status_message}
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
