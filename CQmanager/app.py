import argparse
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from CQmanager.handlers.handlers import global_exception_handler
from CQmanager.routers.router_analyse import router as analyse_router
from CQmanager.routers.router_cleanups import router as cleanups_router
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
from CQmanager.services.TaskManager import (
    TaskManager,
)

task_manager = TaskManager()


@asynccontextmanager
async def lifespan(app: FastAPI, task_manager: TaskManager = task_manager):
    """Manages what to start before the application and what to do after closing it"""

    await task_manager.start_initial_tasks()
    await task_manager.manage_docker_tasks(start=True)
    await task_manager.start_process_task()
    await task_manager.manage_check_CQviewers_status_task(start=True)

    try:
        yield
    except asyncio.CancelledError:
        # Handle task cancellation
        await task_manager.stop_process_task()
        await task_manager.manage_check_CQviewers_status_task(start=False)
        await task_manager.cleanup_tasks(
            task_manager.process_task, task_manager.check_CQviewers_status_task
        )
        raise
    finally:
        # Cleanup
        try:
            await task_manager.stop_process_task()
        except asyncio.CancelledError:
            pass
        await task_manager.manage_check_CQviewers_status_task(start=False)
        try:
            await task_manager.cleanup_tasks(
                task_manager.process_task, task_manager.check_CQviewers_status_task
            )
        except asyncio.CancelledError:
            pass
        try:
            await task_manager.manage_docker_tasks(start=False)
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan, task_manager=task_manager)  # type: ignore
app.exception_handler(exc_class_or_status_code=Exception)(global_exception_handler)
app.include_router(router=analyse_router)
app.include_router(router=status_router)
app.include_router(router=summary_plots_router)
app.include_router(router=update_data_annotation_router)
app.include_router(router=stop_all_cqmanager_analysis_and_plotting_containers_router)
app.include_router(router=control_cqviewers_router)
app.include_router(router=crash_simulation_router)
app.include_router(router=cleanups_router)


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
