from shutil import rmtree

import orjson
from fastapi import APIRouter

from CQmanager.core.config import config
from CQmanager.core.logging import logger

router = APIRouter(
    prefix="/CQmanager",
)


@router.post(path="/remove_permission_denied_analyses/")
async def remove_permission_denied_analyses():
    removed_results: list[str] = []
    for status_file in config.results_directory.rglob(pattern="*status*.json"):
        try:
            with open(file=status_file, mode="rb") as f:
                status_data = orjson.loads(f.read())
            sentrix_id = status_data.get("sentrix_id", "missing_sentrix_id")
            failure_reason = status_data.get("failure_reason", "")
            if (
                "Permission error" in failure_reason
                or "Permission denied" in failure_reason
            ):
                removed_results.append(sentrix_id)
                rmtree(status_file.parent)
        except PermissionError as pe:
            logger.critical(
                msg=f"Permission error when trying to read {status_file}: {pe}"
            )
        except Exception as e:
            rmtree(status_file.parent)
            logger.error(msg=f"Error reading {status_file}: {e}")

    return {
        "removed_results": f"Removed {len(removed_results)} directories with 'permission denied' errors."
    }
