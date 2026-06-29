import asyncio
from dataclasses import dataclass, field
from datetime import datetime

from harness.types import Task


@dataclass(order=True)
class _Entry:
    priority:   int       # negated task.priority for min-heap ascending order
    created_at: datetime
    task: Task = field(compare=False)


class PriorityTaskQueue:
    """
    Async priority queue wrapping asyncio.PriorityQueue.
    Higher task.priority values are dequeued first.
    Tasks at the same priority are served FIFO by created_at.
    """

    def __init__(self) -> None:
        self._q: asyncio.PriorityQueue[_Entry] = asyncio.PriorityQueue()

    async def put(self, task: Task) -> None:
        await self._q.put(_Entry(
            priority   = -task.priority,
            created_at = task.created_at,
            task       = task,
        ))

    async def get(self) -> Task:
        entry = await self._q.get()
        return entry.task

    def task_done(self) -> None:
        self._q.task_done()

    async def join(self) -> None:
        await self._q.join()

    def qsize(self) -> int:
        return self._q.qsize()
