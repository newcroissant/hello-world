import asyncio
from collections import defaultdict

from harness.types import Message


class MessageBus:
    """
    In-process async pub/sub bus.
    Each agent gets its own queue; broadcast recipient "__all__" fans out to all.
    """

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[Message]] = defaultdict(asyncio.Queue)

    def subscribe(self, agent_id: str) -> asyncio.Queue[Message]:
        return self._queues[agent_id]

    async def send(self, msg: Message) -> None:
        if msg.recipient == "__all__":
            for q in self._queues.values():
                await q.put(msg)
        else:
            await self._queues[msg.recipient].put(msg)

    async def drain(self, agent_id: str) -> list[Message]:
        """Return all pending messages for an agent without blocking."""
        q = self._queues[agent_id]
        msgs: list[Message] = []
        while not q.empty():
            msgs.append(q.get_nowait())
        return msgs
