import pytest
from harness.bus   import MessageBus
from harness.types import Message


@pytest.mark.asyncio
async def test_send_to_specific_recipient():
    bus = MessageBus()
    q   = bus.subscribe("agent-a")

    msg = Message(sender="agent-b", recipient="agent-a", topic="ping", body=None)
    await bus.send(msg)

    assert q.qsize() == 1
    received = q.get_nowait()
    assert received.topic == "ping"


@pytest.mark.asyncio
async def test_broadcast_reaches_all_subscribers():
    bus = MessageBus()
    qa  = bus.subscribe("agent-a")
    qb  = bus.subscribe("agent-b")

    msg = Message(sender="orch", recipient="__all__", topic="shutdown", body=None)
    await bus.send(msg)

    assert qa.qsize() == 1
    assert qb.qsize() == 1


@pytest.mark.asyncio
async def test_drain_clears_queue():
    bus = MessageBus()
    bus.subscribe("agent-a")

    for i in range(3):
        await bus.send(Message(sender="s", recipient="agent-a", topic=f"t{i}", body=i))

    msgs = await bus.drain("agent-a")
    assert len(msgs) == 3

    remaining = await bus.drain("agent-a")
    assert remaining == []
