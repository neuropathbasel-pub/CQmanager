from asyncio import Queue


class TaskQueue:
    task_queue = Queue()

    def __str__(self):
        return "task_queue()"

    def __repr__(self):
        return "task_queue()"


# FIXME
# class TaskQueue:
#     def __init__(self):
#         self.task_queue = asyncio.Queue()
#         self._unique_tasks = set()  # Track unique tasks

#     async def put_unique(self, item):
#         if item not in self._unique_tasks:
#             await self.task_queue.put(item)
#             self._unique_tasks.add(item)

#     async def get(self):
#         item = await self.task_queue.get()
#         self._unique_tasks.discard(item)  # Remove when dequeued
#         return item

#     def __str__(self):
#         return f"task_queue(size={self.task_queue.qsize()})"

#     def __repr__(self):
#         return (
#             f"task_queue(queue={self.task_queue.qsize()}, unique={self._unique_tasks})"
#         )
