import asyncio
import logging

from harness.errors import AgentRegistrationError
from harness.protocols import Worker
from harness.types import AgentStatus, HealthReport

log = logging.getLogger(__name__)


class AgentRegistry:
    """Lifecycle manager and routing index for all registered workers."""

    def __init__(self) -> None:
        self._agents:  dict[str, Worker]      = {}
        self._status:  dict[str, AgentStatus] = {}
        self._by_type: dict[str, set[str]]    = {}  # task_type -> {agent_id}

    def register(self, agent: Worker) -> None:
        if not isinstance(agent, Worker):
            raise AgentRegistrationError(
                f"{agent!r} does not implement the Worker protocol"
            )
        self._agents[agent.agent_id] = agent
        self._status[agent.agent_id] = AgentStatus.REGISTERED
        for t in agent.task_types:
            self._by_type.setdefault(t, set()).add(agent.agent_id)
        log.info("Registered agent %s for types %s", agent.agent_id, agent.task_types)

    async def start_all(self) -> None:
        await asyncio.gather(*(self._start(a) for a in self._agents.values()))

    async def stop_all(self) -> None:
        await asyncio.gather(*(self._stop(a) for a in self._agents.values()))

    async def _start(self, agent: Worker) -> None:
        self._status[agent.agent_id] = AgentStatus.STARTING
        await agent.start()
        self._status[agent.agent_id] = AgentStatus.HEALTHY
        log.info("Agent %s started", agent.agent_id)

    async def _stop(self, agent: Worker) -> None:
        await agent.stop()
        self._status[agent.agent_id] = AgentStatus.STOPPED
        log.info("Agent %s stopped", agent.agent_id)

    def agents_for_type(self, task_type: str) -> list[Worker]:
        """Return all HEALTHY agents capable of handling task_type."""
        ids = self._by_type.get(task_type, set())
        return [
            self._agents[i] for i in ids
            if self._status.get(i) == AgentStatus.HEALTHY
        ]

    async def health_check_all(self) -> list[HealthReport]:
        reports: list[HealthReport] = await asyncio.gather(
            *(a.health() for a in self._agents.values())
        )
        for r in reports:
            self._status[r.agent_id] = r.status
        return list(reports)

    @property
    def agent_ids(self) -> list[str]:
        return list(self._agents)

    def status(self, agent_id: str) -> AgentStatus | None:
        return self._status.get(agent_id)
