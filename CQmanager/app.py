import argparse
import asyncio
import signal
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI

from CQmanager.core.config import config
from CQmanager.core.logging import logger
from CQmanager.handlers.handle_shutdown import handle_shutdown
from CQmanager.handlers.handlers import global_exception_handler
from CQmanager.routers.router_analyse import router as analyse_router
from CQmanager.routers.router_control_cqviewers import (
    router as control_cqviewers_router,
)
from CQmanager.routers.router_crash_simulation import router as crash_simulation_router
from CQmanager.routers.router_status import router as status_router
from CQmanager.routers.router_stop_analysis_containers import (
    router as stop_all_cqmanager_analysis_and_plotting_containers_router,
)
from CQmanager.routers.router_summary_plots import router as summary_plots_router
from CQmanager.routers.router_update_data_annotation import (
    router as update_data_annotation_router,
)
from CQmanager.services.tasks import (
    analysis_manager,
    summary_plotter,
    task_queuer,
)
from CQmanager.services.TaskType import TaskType
from CQmanager.utilities.utilities import check_CQviewers_status


async def process_tasks():
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages what to start before the application and what to do after closing it"""
    loop = asyncio.get_event_loop()

    # Register signal handlers
    def signal_handler():
        handle_shutdown(loop=loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig=sig, callback=signal_handler)

    await analysis_manager.check_and_generate_missing_manifest_files()
    await analysis_manager.start()
    await summary_plotter.start()

    process_task = asyncio.create_task(coro=process_tasks())
    check_CQviewers_status_task = asyncio.create_task(
        coro=check_CQviewers_status(
            base_url=config.base_url_CQviewers, server_name=config.server_name
        )
    )

    try:
        yield
    except asyncio.CancelledError:
        # Handle task cancellation
        process_task.cancel()
        check_CQviewers_status_task.cancel()
        await asyncio.gather(
            process_task, check_CQviewers_status_task, return_exceptions=True
        )
        raise
    finally:
        # Cleanup
        process_task.cancel()
        check_CQviewers_status_task.cancel()
        await asyncio.gather(
            process_task, check_CQviewers_status_task, return_exceptions=True
        )
        await analysis_manager.stop()
        await summary_plotter.stop()
        handle_shutdown(loop=loop)


app = FastAPI(lifespan=lifespan)
app.exception_handler(exc_class_or_status_code=Exception)(global_exception_handler)
app.include_router(router=analyse_router)
app.include_router(router=status_router)
app.include_router(router=summary_plots_router)
app.include_router(router=update_data_annotation_router)
app.include_router(router=stop_all_cqmanager_analysis_and_plotting_containers_router)
app.include_router(router=control_cqviewers_router)
app.include_router(router=crash_simulation_router)


def run():
    import uvicorn

    parser = argparse.ArgumentParser(description="Run FastAPI app")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8002, help="Port to bind")
    parser.add_argument("--reload", action="store_false", help="Enable auto-reload")
    args = parser.parse_args()

    uvicorn.run(
        app="CQmanager.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        timeout_keep_alive=30,
        timeout_graceful_shutdown=60,
    )
