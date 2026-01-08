import os
import traceback
from typing import Optional

import polars as pl
import requests
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from CQmanager.core.config import config
from CQmanager.core.logging import logger
from CQmanager.utilities.endpoint_utilities import detect_cli_client, scp_file

router = APIRouter(
    prefix="/CQmanager",
)


@router.get(path="/update_sample_annotations/")
async def update_sample_annotations(
    req: Request,
    format: Optional[str] = None,
):
    """
    Updates the local sample annotation file by comparing it with an online version.

    Downloads annotation data from a predefined URL and compares it with the local file.
    If differences exist, replaces the local file and attempts to update the remote server via SCP.
    Returns a response formatted based on the client type (CLI: plain text, GUI: JSON).

    Args:
        req (Request): The FastAPI request object.
        format (Optional[str]): Explicit response format ('json' or 'text'). If None, auto-detects based on client.

    Returns:
        PlainTextResponse or JSONResponse:
            - PlainTextResponse (for CLI clients): Plain text with status messages.
            - JSONResponse (for GUI clients): JSON object with 'message' and 'remote_annotation_update_status' keys.
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)

    online_data_annotation = download_annotations(
        annotation_url=config.DATA_ANNOTATION_SHEET
    )

    if os.path.exists(path=config.annotation_file_path):
        local_data_annotation = pl.read_csv(source=config.annotation_file_path)
        if online_data_annotation is not None and not online_data_annotation.equals(
            other=local_data_annotation
        ):
            online_data_annotation.write_csv(file=config.annotation_file_path)
            try:
                scp_file(
                    host=config.CQviewers_host,
                    username=config.CQviewers_user,
                    local_path=str(config.annotation_file_path),
                    remote_path=str(config.remote_annotation_file_path),
                )
                remote_annotation_update_status: str = (
                    "Remote sample annotation has been updated."
                )
            except Exception:
                remote_annotation_update_status: str = (
                    "Remote sample annotation could not be updated."
                )

                logger.error(
                    msg=f"Error while trying to send annotation file to the remote server {config.CQviewers_host}:\n{traceback.format_exc()}"
                )

            message: str = "Local data annotation has been replaced with the online data annotation."
            status_code: int = status.HTTP_200_OK

        else:
            message: str = "Your local data annotation was the same as online. csv file has not been replaced"
            status_code: int = status.HTTP_200_OK
            remote_annotation_update_status: str = ""
    else:
        if online_data_annotation is not None:
            online_data_annotation.write_csv(file=config.annotation_file_path)
            try:
                scp_file(
                    host=config.CQviewers_host,
                    username=config.CQviewers_user,
                    local_path=str(config.annotation_file_path),
                    remote_path=str(config.remote_annotation_file_path),
                )
                remote_annotation_update_status: str = (
                    "Remote sample annotation has been updated."
                )
                message: str = "Data annotation file has not been found locally and therefore has been downloaded."
                status_code: int = status.HTTP_200_OK
            except Exception:
                remote_annotation_update_status: str = (
                    "Remote sample annotation could not be updated."
                )
                message: str = "Data annotation file has not been found locally and therefore has been downloaded, but remote update failed."
                status_code: int = status.HTTP_200_OK
                logger.error(
                    msg=f"{message}\nRemote server:\n{config.CQviewers_host}.\nError:\n{traceback.format_exc()}"
                )

        else:
            message: str = "Data annotation has not been found locally and could not have been downloaded."
            status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
            remote_annotation_update_status: str = ""

    if is_cli_client:
        return_message: str = f"{message}\n{remote_annotation_update_status}"
        return PlainTextResponse(content=return_message, status_code=200)
    else:
        return JSONResponse(
            content={
                "message": message,
                "remote_annotation_update_status": remote_annotation_update_status,
            },
            status_code=status_code,
        )


@router.get(path="/update_reference_annotations/")
async def update_reference_annotations(
    req: Request,
    format: Optional[str] = None,
):
    """
    Updates the local reference annotation file by comparing it with an online version.

    This endpoint downloads annotation data from a predefined URL (REFERENCE_DATA_ANNOTATION_SHEET)
    using the `download_annotations` function. It then checks if a local annotation file exists at
    `reference_annotation_file_path`. If the local file exists, it compares it with the online data:
    - If the online data is different, the local file is replaced with the online version.
    - If the online and local data are identical, no changes are made.
    If the local file does not exist, the online data is downloaded and saved locally if available.
    The function returns a JSON response indicating the outcome of the operation.

    Returns:
        JSONResponse: A JSON response with a message describing the result of the operation
                      and an HTTP status code of 200.

    Examples:
        - If the local file is updated:
            {"content": "Local reference annotation has been replaced with the online data annotation.", "status_code": 200}
        - If the local file is identical to the online data:
            {"content": "Your local reference was the same as online. csv file has not been replaced", "status_code": 200}
        - If the local file is missing and downloaded:
            {"content": "Data reference annotation file has not been found locally and therefore has been downloaded", "status_code": 200}
        - If the local file is missing and download fails:
            {"content": "Data reference annotation has not been found locally and could not have been downloaded.", "status_code": 200}
    """
    is_cli_client: bool = detect_cli_client(req=req, specified_format=format)

    online_reference_annotation = download_annotations(
        annotation_url=config.REFERENCE_DATA_ANNOTATION_SHEET
    )
    remote_reference_update_status: str = ""

    if os.path.exists(path=config.annotation_file_path):
        local_reference_annotation = pl.read_csv(
            source=config.reference_annotation_file_path
        )
        if (
            online_reference_annotation is not None
            and not online_reference_annotation.equals(other=local_reference_annotation)
        ):
            online_reference_annotation.write_csv(config.reference_annotation_file_path)

            try:
                scp_file(
                    host=config.CQviewers_host,
                    username=config.CQviewers_user,
                    local_path=str(config.reference_annotation_file_path),
                    remote_path=str(config.remote_reference_annotation_file_path),
                )
                remote_reference_update_status = (
                    "Remote reference annotation has been updated."
                )
            except Exception:
                remote_reference_update_status = (
                    "Remote reference annotation could not be updated."
                )
                logger.error(
                    msg=f"Error while trying to send reference file to the remote server {config.CQviewers_host}:\n{traceback.format_exc()}"
                )

            message: str = "Local reference annotation has been replaced with the online data annotation."
            status_code: int = status.HTTP_200_OK

        else:
            message: str = "Your local reference was the same as online. csv file has not been replaced"
            status_code: int = status.HTTP_200_OK

    else:
        if online_reference_annotation is not None:
            online_reference_annotation.write_csv(
                file=config.reference_annotation_file_path
            )

            try:
                scp_file(
                    host=config.CQviewers_host,
                    username=config.CQviewers_user,
                    local_path=str(config.reference_annotation_file_path),
                    remote_path=str(config.remote_reference_annotation_file_path),
                )
                remote_reference_update_status: str = (
                    "Remote reference annotation has been updated."
                )
            except Exception:
                remote_reference_update_status = (
                    "Remote reference annotation could not be updated."
                )
                logger.error(
                    msg=f"Error while trying to send reference file to the remote server {config.CQviewers_host}:\n{traceback.format_exc()}"
                )
            message: str = "Data reference annotation file has not been found locally and therefore has been downloaded."
            status_code: int = status.HTTP_200_OK

        else:
            message: str = "Data reference annotation has not been found locally and could not have been downloaded."
            status_code: int = status.HTTP_200_OK

    if is_cli_client:
        return PlainTextResponse(
            content=f"{message}\n{remote_reference_update_status}", status_code=200
        )
    else:
        return JSONResponse(
            content={
                "message": message,
                "remote_reference_update_status": remote_reference_update_status,
            },
            status_code=status_code,
        )


def download_annotations(annotation_url: str) -> pl.DataFrame | None:
    """
    Downloads annotation data from a specified URL and returns it as a Polars DataFrame.

    This function sends a GET request to the provided URL to retrieve a CSV file containing
    annotation data. If the request is successful (status code 200), the CSV content is parsed
    into a Polars DataFrame and returned. If the request fails, an error is logged, and None is returned.

    Args:
        annotation_url (str): The URL pointing to the CSV file containing annotation data.

    Returns:
        pl.DataFrame | None: A Polars DataFrame containing the annotation data if the request is successful,
                             or None if the request fails.
    """

    response = requests.get(url=annotation_url)

    if response.status_code == 200:
        data_annotation = pl.read_csv(source=response.content)

        return data_annotation
    else:
        logger.error(
            msg=f"Failed to fetch the sheet. Status code: {response.status_code}"
        )
        return None
