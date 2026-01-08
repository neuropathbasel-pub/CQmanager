import logging
from pathlib import Path
from shutil import rmtree

import orjson
from icecream import ic


class FileCleaner:
    def __init__(
        self,
        results_directory: Path,
        temp_directory: Path,
        logger: logging.Logger = logging.getLogger(name=__name__),
    ):
        self.results_directory = results_directory
        self.temp_directory = temp_directory
        self.logger = logger

    def remove_temporary_files(self) -> tuple[bool, int]:
        removed_files_count = 0
        success: bool = False
        if self.temp_directory.exists() and self.temp_directory.is_dir():
            for temp_file in self.temp_directory.iterdir():
                try:
                    if temp_file.is_file():
                        temp_file.unlink()
                        removed_files_count += 1
                    elif temp_file.is_dir():
                        rmtree(temp_file)
                        removed_files_count += 1
                except Exception as e:
                    self.logger.error(
                        msg=f"Error removing temporary file {temp_file}: {e}"
                    )
            else:
                success = True
        return success, removed_files_count

    def remove_failed_results_directories_due_to_permission_errors(
        self,
    ) -> tuple[bool, int]:
        success: int = False
        removed_results_count: int = 0
        removed_results: list[str] = []
        for status_file in self.results_directory.rglob(pattern="*status*.json"):
            try:
                with open(file=status_file, mode="rb") as f:
                    status_data = orjson.loads(f.read())
                sentrix_id = status_data.get("sentrix_id", "missing_sentrix_id")
                # failure_reason = status_data.get("failure_reason", "")
                status = status_data.get("analysis_completed_successfully", "False")
                # print(status)
                # print(status_file, status)
                if status.lower() != "true":
                    # TODO: Implement the logic for checking permission errors in status
                    removed_results.append(sentrix_id)
                    rmtree(status_file.parent)
                    removed_results_count += 1
            except PermissionError as pe:
                self.logger.critical(
                    msg=f"Permission error when trying to read {status_file}: {pe}"
                )
            except Exception as e:
                self.logger.error(msg=f"Error processing {status_file}: {e}")
                try:
                    rmtree(status_file.parent)
                    removed_results_count += 1
                except Exception as re:
                    print(f"Also failed to remove directory {status_file.parent}: {re}")

        results_dir = self.temp_directory.parent / "results"
        if results_dir.exists() and results_dir.is_dir():
            for result_dir in results_dir.iterdir():
                status_file = result_dir / "status.txt"
                if status_file.exists() and status_file.is_file():
                    try:
                        with open(file=status_file, mode="r") as f:
                            status_content = f.read()
                        if "permission denied" in status_content.lower():
                            rmtree(result_dir)
                            removed_results_count += 1
                    except Exception as e:
                        self.logger.error(msg=f"Error reading {status_file}: {e}")
            else:
                success = True
        return success, removed_results_count
