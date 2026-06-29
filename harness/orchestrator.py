import asyncio
import logging
import time
from datetime import datetime

from harness.aggregator import ResultAggregator
from harness.bus import MessageBus
from harness.errors import NoAgentAvailableError, TaskExpiredError
from harness.protocols import Worker
from harness.queue import PriorityTaskQueue
from harness.registry import AgentRegistry
from harness.types import Message, Task, TaskResult, TaskStatus

log = logging.getLogger(__name__)

_BROADCAST = "__all__"


class Orchestrator:
    """
    Central coordinator: dequeues tasks, routes to healthy agents,
    retries on failure, and streams results to the aggregator.
    """

    def __init__(
        self,
        registry:    AgentRegistry,
        bus:         MessageBus,
        aggregator:  ResultAggregator,
        concurrency: int = 10,
    ) -> None:
        self._registry   = registry
        self._bus        = bus
        self._aggregator = aggregator
        self._queue      = PriorityTaskQueue()
        self._sem        = asyncio.Semaphore(concurrency)
        self._running    = False
        self._inflight:  set[asyncio.Task] = set()

    async def submit(self, task: Task) -> None:
        await self._queue.put(task)
        log.debug("Submitted task %s (type=%s priority=%d)",
                  task.task_id, task.task_type, task.priority)

    async def broadcast(self, topic: str, body: object, sender: str = "orchestrator") -> None:
        await self._bus.send(Message(sender=sender, recipient=_BROADCAST, topic=topic, body=body))

    async def run(self) -> None:
        self._running = True
        log.info("Orchestrator started")
        while self._running:
            try:
                task = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue
            t = asyncio.create_task(self._dispatch(task))
            self._inflight.add(t)
            t.add_done_callback(self._inflight.discard)

    async def shutdown(self) -> None:
        self._running = False
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
        await self._queue.join()
        log.info("Orchestrator shut down")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _dispatch(self, task: Task) -> None:
        async with self._sem:
            try:
                if task.deadline and datetime.utcnow() > task.deadline:
                    raise TaskExpiredError(f"Task {task.task_id} expired")

                agents = self._registry.agents_for_type(task.task_type)
                if not agents:
                    raise NoAgentAvailableError(
                        f"No healthy agent for task type '{task.task_type}'"
                    )

                agent  = self._select(agents, task)
                result = await self._execute_with_retry(agent, task)

            except (TaskExpiredError, NoAgentAvailableError) as exc:
                log.error("Task %s failed permanently: %s", task.task_id, exc)
                result = TaskResult(
                    task_id  = task.task_id,
                    agent_id = "orchestrator",
                    status   = TaskStatus.FAILED,
                    output   = None,
                    error    = str(exc),
                )
            finally:
                self._queue.task_done()

        await self._aggregator.collect(result)

    def _select(self, agents: list[Worker], task: Task) -> Worker:
        """Round-robin selection by task_id hash — deterministic per task."""
        return agents[hash(task.task_id) % len(agents)]

    async def _execute_with_retry(self, agent: Worker, task: Task) -> TaskResult:
        attempt = 0
        result: TaskResult | None = None

        while True:
            t0 = time.monotonic()
            try:
                result = await agent.execute(task)
                result.duration_s = time.monotonic() - t0
                if result.status == TaskStatus.SUCCEEDED:
                    log.debug("Task %s succeeded on agent %s (attempt %d)",
                              task.task_id, agent.agent_id, attempt + 1)
                    return result
            except Exception as exc:
                duration = time.monotonic() - t0
                result = TaskResult(
                    task_id    = task.task_id,
                    agent_id   = agent.agent_id,
                    status     = TaskStatus.FAILED,
                    output     = None,
                    error      = repr(exc),
                    duration_s = duration,
                )

            attempt += 1
            if attempt > task.max_retries:
                log.warning("Task %s exhausted retries (%d)", task.task_id, task.max_retries)
                return result  # type: ignore[return-value]

            task.retries = attempt
            backoff = 2 ** attempt
            log.warning("Task %s failed, retry %d/%d in %ds",
                        task.task_id, attempt, task.max_retries, backoff)
            await asyncio.sleep(backoff)

        return result  # type: ignore[return-value]  # unreachable; satisfies type checkers
