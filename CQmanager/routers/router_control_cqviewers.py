from typing import Optional

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.services.docker_runners import cq_viewers_runner
from CQmanager.utilities.endpoint_utilities import detect_cli_client

router = APIRouter(
    prefix="/CQmanager",
)


# TODO: FIXME: Change the http status codes from docker class to bool and handle the response here
@router.get(path="/check_cqviewers_containers/")
async def check_cqviewers_containers(
    req: Request,
    format: Optional[str] = None,
):
    """Check status of CQcase and CQall containers.

    Returns:
        JSONResponse: Status message indicating running containers, no containers, or error, with appropriate HTTP status code (200 or 500).
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    running_containers, docker_operation_successful = (
        cq_viewers_runner.check_if_cqcase_and_cqall_are_running()
    )
    if docker_operation_successful == 200 and running_containers:
        status_code = status.HTTP_200_OK
        message: str = "Running CQcase or CQall containers have been found"

    elif docker_operation_successful == 200 and not running_containers:
        status_code = status.HTTP_200_OK
        message: str = "No running CQcase or CQall containers have been found"
    else:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        message: str = "Error by checking running cnviewers containers"

    if is_cli_client:
        if running_containers:
            content: str = f"{message}:\n{running_containers}"
        else:
            content: str = message
        PlainTextResponse(content=content, status_code=status_code)
    else:
        JSONResponse(
            content={"message": message, "running_containers": running_containers},
            status_code=status_code,
        )


@router.get(path="/start_cqviewers/")
async def start_cqviewers(
    req: Request,
    format: Optional[str] = None,
):
    """Start CQcase and CQall containers after cleaning non-running ones.

    Returns:
        JSONResponse: Status message with started container names and HTTP 200, or error message with HTTP 500 if checking running containers fails.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    # Clean up possible non-running containers
    cq_viewers_runner.remove_non_running_containers()
    # Check if there are containers running
    # TODO: Change the http status codes from docker class to bool and handle the response here
    running_containers, docker_operation_successful = (
        cq_viewers_runner.check_if_cqcase_and_cqall_are_running()
    )
    if docker_operation_successful == 200:
        message: str = "Started containers"
        started_containers = ", ".join(cq_viewers_runner.start_cqcase_and_cqall())
        status_code: int = status.HTTP_200_OK

    else:
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
        started_containers = ""
        message: str = (
            "An error has occurred while trying to start CQcase and CQall containers"
        )

    if is_cli_client:
        content: str = (
            f"{message}:\n{started_containers}" if started_containers else message
        )
        return PlainTextResponse(content=content, status_code=status_code)
    else:
        return JSONResponse(
            content={"message": message, "started_containers": started_containers},
            status_code=status_code,
        )


@router.get(path="/stop_cqviewers/")
async def stop_cqviewers(
    req: Request,
    format: Optional[str] = None,
):
    """Stop CQviewers containers and return their names.

    Returns:
        JSONResponse: Message with stopped container names and HTTP 200 status code.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    stopped_containers, docker_operation_successful = (
        cq_viewers_runner.stop_cqviewers_containers()
    )
    # TODO: Change the http status codes from docker class to bool and handle the response here
    status_code: int = (
        status.HTTP_200_OK
        if docker_operation_successful == 200
        else status.HTTP_500_INTERNAL_SERVER_ERROR
    )
    if is_cli_client:
        no_containers_message: str = (
            "There were no containers to stop"
            if status_code == 200
            else "An error occurred while trying to stop containers"
        )
        content: str = (
            f"Stopped following containers: {', '.join(stopped_containers)}"
            if stopped_containers
            else no_containers_message
        )
        return PlainTextResponse(content=content, status_code=status_code)
    else:
        message: str = (
            "There were no containers to stop"
            if status_code == 200 and not stopped_containers
            else "An error occurred while trying to stop containers"
            if status_code != 200
            else ""
        )
        return JSONResponse(
            content={
                "message": message,
                "stopped_containers": stopped_containers,
            },
            status_code=status_code,
        )


@router.get(path="/containers_cleanup/")
async def containers_cleanup(
    req: Request,
    format: Optional[str] = None,
):
    """
    Remove all non-running Docker containers and report the results.

    Calls the CQviewersRunner to clean up stopped containers, then returns a response
    indicating success or failure. For CLI clients, returns plain text; for GUI clients,
    returns JSON with details.

    Args:
        req (Request): The FastAPI request object.
        format (Optional[str]): Explicit response format ('json' or 'text'). If None, auto-detects based on client.

    Returns:
        PlainTextResponse or JSONResponse:
            - PlainTextResponse (for CLI): Plain text message with status code 200 on success or 500 on failure.
            - JSONResponse (for GUI): JSON object with 'message' and 'removed_count' keys, with status code 200 or 500.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)
    container_cleanup_successful, removed_count = (
        cq_viewers_runner.remove_non_running_containers()
    )

    if container_cleanup_successful:
        message: str = "Container cleanup has been successfully performed."
    else:
        if removed_count == -1:
            message: str = "An error occurred while trying to connect to Docker"
        else:
            message: str = (
                "An error occurred while trying to remove non-running containers"
            )

    status_code: int = (
        status.HTTP_200_OK
        if container_cleanup_successful
        else status.HTTP_500_INTERNAL_SERVER_ERROR
    )

    if is_cli_client:
        return PlainTextResponse(content=message, status_code=status_code)
    else:
        return JSONResponse(
            content={"message": message, "removed_count": removed_count},
            status_code=status_code,
        )
