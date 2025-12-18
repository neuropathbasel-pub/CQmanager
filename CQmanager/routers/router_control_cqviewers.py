from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from CQmanager.services.docker_runners import cq_viewers_runner

router = APIRouter(
    prefix="/CQmanager",
)


# Warning: JSONResponse(status_code=status.HTTP_204_NO_CONTENT, content=None) crashes the code. Issue with status.HTTP_204_NO_CONTENT handling
@router.get(path="/check_cqviewers_containers/")
async def check_cqviewers_containers():
    """Check status of CQcase and CQall containers.

    Returns:
        JSONResponse: Status message indicating running containers, no containers, or error, with appropriate HTTP status code (200 or 500).
    """
    running_containers, status_code = (
        cq_viewers_runner.check_if_cqcase_and_cqall_are_running()
    )
    if status_code == 200 and running_containers:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=f"Following running CnQuant viewer containers have been found: {running_containers}",
        )
    elif status_code == 200 and not running_containers:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content="No running containers have been found",
        )
    else:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content="Error by checking running cnviewers containers",
        )


@router.get(path="/start_cqviewers/")
async def start_cqviewers():
    """Start CQcase and CQall containers after cleaning non-running ones.

    Returns:
        JSONResponse: Status message with started container names and HTTP 200, or error message with HTTP 500 if checking running containers fails.
    """
    # Clean up possible non-running containers
    cq_viewers_runner.remove_non_running_containers()
    # Check if there are containers running
    running_containers, status_code = (
        cq_viewers_runner.check_if_cqcase_and_cqall_are_running()
    )
    if status_code == 200:
        started_containers = ", ".join(cq_viewers_runner.start_cqcase_and_cqall())
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=f"Started following containers: {started_containers}",
        )
    else:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content="An error occured while checking running cqviewers' containers",
        )


@router.get(path="/stop_cqviewers/")
async def stop_cqviewers():
    """Stop CQviewers containers and return their names.

    Returns:
        JSONResponse: Message with stopped container names and HTTP 200 status code.
    """
    stopped_containers, status_code = cq_viewers_runner.stop_cqviewers_containers()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=f"Stopped following containers: {', '.join(stopped_containers)}",
    )


@router.get(path="/containers_cleanup/")
async def containers_cleanup():
    """Remove non-running Docker containers.

    Returns:
        Dictionary containing a message about the cleanup operation.
    """
    message = cq_viewers_runner.remove_non_running_containers()
    return {"message": message}
