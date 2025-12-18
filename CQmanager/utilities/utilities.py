import asyncio
from datetime import datetime, timedelta

import requests
from CnQuant_utilities.crash_report import send_crash_email

from CQmanager.core.config import config
from CQmanager.core.logging import logger


def slice_set_into_parts(input_set: set, num_parts: int):
    """
    Divides a set into a specified number of parts.

    Converts the input set to a list and splits it into approximately equal parts. Distributes any remainder across the
    initial parts to ensure balanced chunks.

    Args:
        input_set: The set to be divided.
        num_parts: The number of parts to divide the set into.

    Returns:
        list: A list of lists, where each inner list is a chunk of the original set.
    """
    input_list: list = list(input_set)
    total_length: int = len(input_list)

    part_size: int = total_length // num_parts
    remainder: int = total_length % num_parts

    chunks = []
    start = 0
    for i in range(num_parts):
        extra = 1 if i < remainder else 0
        end = start + part_size + extra
        chunk = input_list[start:end]
        chunks.append(chunk)
        start = end

    return chunks


def check_if_app_is_running(url: str) -> bool:
    """Checks if a get request sent to a given url returns response code 200
    Args:
        url (str): url to which get request will be sent
    Returns:
        bool
    """
    response = requests.get(url=url)
    if response.status_code == 200:
        return True
    else:
        return False


def has_24_hours_passed(last_time: datetime, min_hours_of_difference: int = 24) -> bool:
    """Check if at least the specified number of hours have passed since the given time.

    Args:
        last_time (datetime): The reference datetime to compare against.
        min_hours_of_difference (int, optional): Minimum hours required to have passed. Defaults to 24.

    Returns:
        bool: True if the specified hours have passed, False otherwise.
    """
    current_time = datetime.now()
    return current_time - last_time >= timedelta(hours=min_hours_of_difference)


async def check_CQviewers_status(
    base_url: str,
    server_name: str,
    delay: int = 120,
    checkup_intervals: int = config.intervals_for_checking_CQcase_and_CQall_status,
) -> None:
    """Periodically check the status of CQcase and CQall applications and send email notifications if both are down.

    Args:
        base_url (str): Base URL for checking CQcase and CQall status.
        server_name (str): Name of the server hosting the applications.
        delay (int, optional): Initial delay in seconds before starting checks. Defaults to 120.
        checkup_intervals (int, optional): Interval in seconds between status checks. Defaults to intervals_for_checking_CQcase_and_CQall_status.

    Notes:
        - Sends an email if both CQcase and CQall are not running and 24 hours have passed since the last email.
        - Logs an error message when the applications are down.
    """
    if config.notify_if_CQcase_and_CQall_are_not_running:
        await asyncio.sleep(delay=delay)
        last_time = datetime.strptime("2000-01-01_00-00-00", "%Y-%m-%d_%H-%M-%S")
        while True:
            cqcase_is_running: bool = check_if_app_is_running(
                url=f"{base_url}/cqcase/status_check/"
            )
            cqall_is_running: bool = check_if_app_is_running(
                url=f"{base_url}/cqall/status_check/"
            )

            if not any([cqcase_is_running, cqall_is_running]) and has_24_hours_passed(
                last_time=last_time
            ):
                current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

                message: str = (
                    f"{current_time}\nCQcase or CQall is not running on {server_name}."
                )

                logger.error(msg=message)

                send_crash_email(
                    error_message=message,
                    sender=config.crash_email_sender,
                    receivers=config.crash_email_receivers.split(sep=","),
                    password=config.crash_email_sender_password,
                    app_name="CQcase or CQall",
                )
                last_time = datetime.now()

            await asyncio.sleep(delay=checkup_intervals)
