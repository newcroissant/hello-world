import asyncio
from collections import defaultdict

from harness.types import TaskResult, TaskStatus


class ResultAggregator:
    """
    Thread-safe result collector.
    Supports awaiting a specific task's result via wait_for().
    """

    def __init__(self) -> None:
        self._results: list[TaskResult]          = []
        self._waiters: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def collect(self, result: TaskResult) -> None:
        async with self._lock:
            self._results.append(result)
            fut = self._waiters.pop(result.task_id, None)
        if fut and not fut.done():
            fut.set_result(result)

    async def wait_for(
        self, task_id: str, timeout: float | None = None
    ) -> TaskResult:
        """
        Await a specific task result. Safe to call before the task completes.
        Raises asyncio.TimeoutError if timeout is exceeded.
        """
        async with self._lock:
            for r in self._results:
                if r.task_id == task_id:
                    return r
            loop = asyncio.get_event_loop()
            fut: asyncio.Future = loop.create_future()
            self._waiters[task_id] = fut

        return await asyncio.wait_for(fut, timeout=timeout)

    async def flush(self) -> list[TaskResult]:
        """Atomically return and clear all collected results."""
        async with self._lock:
            out, self._results = self._results, []
        return out

    def summary(self) -> dict[str, int]:
        by_status: dict[str, int] = defaultdict(int)
        for r in self._results:
            by_status[r.status.name] += 1
        return dict(by_status)

    def results(self) -> list[TaskResult]:
        return list(self._results)
