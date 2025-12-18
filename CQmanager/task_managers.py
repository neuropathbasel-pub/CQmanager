from contextlib import asynccontextmanager

from CQmanager.services.tasks import analysis_manager, summary_plotter


@asynccontextmanager
async def manage_AnalyseSentrixID():
    """
    Async context manager for handling the lifecycle of an analysis manager.

    Starts the analysis manager upon entering the context and ensures it is stopped when exiting, even if an error occurs.

    Yields:
        The started analysis manager instance.

    Returns:
        None
    """
    try:
        yield await analysis_manager.start()
    finally:
        await analysis_manager.stop()


@asynccontextmanager
async def manage_SummaryPlotter():
    """
    Async context manager for handling the lifecycle of a summary plotter.

    Starts the summary plotter upon entering the context and ensures it is stopped when exiting, even if an error occurs.

    Yields:
        The started summary plotter instance.

    Returns:
        None
    """
    try:
        yield await summary_plotter.start()
    finally:
        await summary_plotter.stop()
