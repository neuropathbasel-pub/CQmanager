import traceback

from CnQuant_utilities.crash_report import send_crash_email
from fastapi import Request
from fastapi.responses import JSONResponse

from CQmanager.core.config import config


async def global_exception_handler(request: Request, exc: Exception):
    """
    Handles global exceptions in a FastAPI application, sending crash reports via email if enabled.

    This async exception handler captures any unhandled exceptions, formats the stack trace, and conditionally
    sends an email notification to specified receivers with error details. It returns a JSON response to the client
    indicating whether the admin was notified or if email notifications are disabled.

    Args:
        request (Request): The incoming HTTP request that triggered the exception.
        exc (Exception): The exception that was raised.

    Returns:
        JSONResponse: A response with a 500 status code and a message indicating whether the admin was notified
                    or if email notifications are disabled.
    """
    if config.send_crash_reports:
        error_details = traceback.format_exc()
        send_crash_email(
            error_message=error_details,
            sender=config.crash_email_sender,
            receivers=config.crash_email_receivers.split(sep=","),
            password=config.crash_email_sender_password,
            app_name="CQmanager",
        )

        return JSONResponse(
            status_code=500,
            content={
                "message": "An unexpected error occurred. The admin has been notified."
            },
        )
    else:
        return JSONResponse(
            status_code=500,
            content={"message": "Email notifications have been switched off."},
        )
