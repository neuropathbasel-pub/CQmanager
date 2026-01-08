import asyncio
import logging
import traceback
from typing import Any, Optional

from CQmanager.core.config import config
from CQmanager.core.logging import logger
from CQmanager.services.tasks import (
    analysis_manager,
    summary_plotter,
    task_queuer,
)
from CQmanager.services.TaskType import TaskType
from CQmanager.utilities.utilities import check_CQviewers_status


class TaskManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        value=None,
        logger: logging.Logger = logging.getLogger(name=__name__),
    ):
        # Initialize only once
        if not hasattr(self, "initialized"):
            self.value: Optional[Any] = value
            self.logger: logging.Logger = logger
            self.initialized: bool = True
            self.process_task: Optional[asyncio.Task] = None
            self.check_CQviewers_status_task: Optional[asyncio.Task] = None

    async def start_initial_tasks(self) -> None:
        await analysis_manager.check_and_generate_missing_manifest_files()

        return None

    async def manage_docker_tasks(self, start: bool) -> None:
        """
        Start or stop the analysis manager and summary plotter tasks.

        Args:
            start (bool): If True, start the tasks; if False, stop them.
        """
        if start:
            await analysis_manager.start()
            await summary_plotter.start()
        else:
            await analysis_manager.stop()
            await summary_plotter.stop()

        return None

    async def start_process_task(self) -> None:
        """Start the process_tasks coroutine as an asyncio task."""
        if self.process_task is None or self.process_task.done():
            self.process_task = asyncio.create_task(coro=self.process_tasks())
        else:
            self.logger.warning(msg="Process task is already running.")

    async def stop_process_task(self) -> None:
        """Cancel and clean up the process task."""
        if self.process_task and not self.process_task.done():
            self.process_task.cancel()
            try:
                await self.process_task
            except asyncio.CancelledError:
                self.logger.info(msg="Process task cancelled.")
        self.process_task = None

    async def cleanup_tasks(self, *tasks: Optional[asyncio.Task]) -> None:
        """
        Cancel and gather the provided tasks, handling exceptions.

        Cancels each task if not done, then awaits them with gather to ensure completion.

        Args:
            *tasks: Variable number of asyncio.Task objects to cleanup.
        """
        valid_tasks: list[asyncio.Task] = [task for task in tasks if task is not None]

        for task in valid_tasks:
            if not task.done():
                task.cancel()

        if valid_tasks:
            results = await asyncio.gather(*valid_tasks, return_exceptions=True)
            for i, result in enumerate(iterable=results):
                if isinstance(result, Exception):
                    self.logger.error(msg=f"Task {i} failed during cleanup: {result}")

    async def manage_check_CQviewers_status_task(self, start: bool) -> None:
        if start:
            if (
                self.check_CQviewers_status_task is None
                or self.check_CQviewers_status_task.done()
            ):
                self.check_CQviewers_status_task = asyncio.create_task(
                    coro=check_CQviewers_status(
                        base_url=config.base_url_CQviewers,
                        server_name=config.server_name,
                    )
                )
            else:
                self.logger.warning(
                    msg="Check CQviewers status task is already running."
                )
        elif not start:
            if (
                self.check_CQviewers_status_task
                and not self.check_CQviewers_status_task.done()
            ):
                self.check_CQviewers_status_task.cancel()
                try:
                    await self.check_CQviewers_status_task
                except asyncio.CancelledError:
                    self.logger.info(msg="Check CQviewers status task cancelled.")
            self.check_CQviewers_status_task = None

    async def process_tasks(self) -> None:
        """
        Asynchronously processes tasks from a task queue, delegating them to appropriate handlers based on task type.

        This function runs an infinite loop, retrieving tasks from `task_queuer.task_queue` and processing them according
        to their `type`. Tasks are dispatched to specific managers (e.g., `analysis_manager`, `summary_plotter`) for
        analysis, plotting, or other operations. Errors during task processing are logged, and the task is marked as done
        to ensure queue integrity.

        Tasks:
            - `TaskType.ANALYSIS`: Adds a single analysis task to the batch queue via `analysis_manager`.
            - `TaskType.ANALYSE_MISSING`: Adds multiple analysis tasks to the batch queue.
            - `TaskType.SUMMARY_PLOT`: Initiates plot generation with `summary_plotter` and logs settings.
            - `TaskType.CQVIEWERS`: Initiates CQviewers tasks and logs task data.
            - Unknown task types: Logs a warning with the unrecognized type.

        Exceptions:
            Any exceptions during task processing are caught, logged using `logger.error`, and the task is marked as done.

        Dependencies:
            - `task_queuer`: An object with a `task_queue` (asyncio.Queue) and `task_done` method.
            - `analysis_manager`: An object with `add_to_batch_queue` and `add_many_to_batch_queue` methods.
            - `summary_plotter`: An object with an `order_plots` method.
            - `logger`: A logging object for error reporting.
            - `TaskType`: An enum defining task types (e.g., ANALYSIS, ANALYSE_MISSING).
        """
        while True:
            task = await task_queuer.task_queue.get()
            try:
                task_type, task_data = task["type"], task["data"]
                if task_type == TaskType.ANALYSIS:
                    await analysis_manager.put_task(task_data=task_data)
                elif task_type == TaskType.ANALYSE_SENTRIX_IDS_FOR_SUMMARY_PLOTS:
                    await analysis_manager.put_task(task_data=task_data)
                elif task_type == TaskType.SUMMARY_PLOT:
                    summary_plotter.order_plots(task_data=task_data)
                elif task_type == TaskType.CQVIEWERS:
                    logger.warning(msg=f"Initiating CQviewers tasks: {task_data}")
                else:
                    logger.error(msg=f"Unknown task type: {task_type}")
            except Exception:
                logger.error(
                    msg=f"Processing tasks returned following exception:\n{traceback.format_exc()}"
                )
            finally:
                task_queuer.task_queue.task_done()
