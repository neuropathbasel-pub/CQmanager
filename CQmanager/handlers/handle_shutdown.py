import asyncio

def handle_shutdown(loop):
    """Gracefully shut down an asyncio event loop by cancelling tasks and closing resources.
    
    Args:
        loop: The asyncio event loop to shut down.
    """
    tasks = [task for task in asyncio.all_tasks(loop=loop) if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    loop.stop()
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()