import asyncio
import logging
from typing import Any

from harness.bus import MessageBus
from harness.types import AgentStatus, HealthReport, Message, Task, TaskResult, TaskStatus

log = logging.getLogger(__name__)


class BaseWorker:
    """
    Concrete base satisfying the Worker protocol.
    Subclasses override _run(task) to implement their logic,
    and optionally override on_message(msg) to handle bus messages.
    """

    task_types: frozenset[str] = frozenset()

    def __init__(self, agent_id: str, bus: MessageBus) -> None:
        self.agent_id    = agent_id
        self._bus        = bus
        self._inbox      = bus.subscribe(agent_id)
        self._tasks_done = 0
        self._errors     = 0
        self._listener:  asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._listener = asyncio.create_task(
            self._listen(), name=f"listener-{self.agent_id}"
        )

    async def stop(self) -> None:
        if self._listener and not self._listener.done():
            self._listener.cancel()
            try:
                await self._listener
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    # Task execution
    # ------------------------------------------------------------------

    async def execute(self, task: Task) -> TaskResult:
        try:
            output = await self._run(task)
            self._tasks_done += 1
            return TaskResult(
                task_id  = task.task_id,
                agent_id = self.agent_id,
                status   = TaskStatus.SUCCEEDED,
                output   = output,
            )
        except Exception as exc:
            self._errors += 1
            raise

    async def _run(self, task: Task) -> Any:
        """Override in subclasses to implement task handling."""
        raise NotImplementedError(f"{type(self).__name__} must implement _run()")

    # ------------------------------------------------------------------
    # Health & messaging
    # ------------------------------------------------------------------

    async def health(self) -> HealthReport:
        total = self._tasks_done + self._errors
        rate  = self._errors / max(1, total)
        status = AgentStatus.DEGRADED if rate > 0.5 else AgentStatus.HEALTHY
        return HealthReport(
            agent_id    = self.agent_id,
            status      = status,
            queue_depth = self._inbox.qsize(),
            tasks_done  = self._tasks_done,
            error_rate  = rate,
        )

    async def on_message(self, msg: Message) -> None:
        log.debug("Agent %s received msg topic=%s from %s", self.agent_id, msg.topic, msg.sender)

    async def send(self, recipient: str, topic: str, body: Any) -> None:
        await self._bus.send(Message(sender=self.agent_id, recipient=recipient, topic=topic, body=body))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _listen(self) -> None:
        while True:
            msg = await self._inbox.get()
            try:
                await self.on_message(msg)
            except Exception:
                log.exception("Agent %s error handling message topic=%s", self.agent_id, msg.topic)
