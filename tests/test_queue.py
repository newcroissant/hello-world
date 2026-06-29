import pytest
from harness.queue import PriorityTaskQueue
from harness.types import Task


@pytest.mark.asyncio
async def test_higher_priority_dequeued_first():
    q = PriorityTaskQueue()
    low  = Task(task_type="echo", payload=None, priority=1)
    high = Task(task_type="echo", payload=None, priority=10)

    await q.put(low)
    await q.put(high)

    first = await q.get()
    assert first.task_id == high.task_id


@pytest.mark.asyncio
async def test_fifo_within_same_priority():
    q = PriorityTaskQueue()
    t1 = Task(task_type="echo", payload="first",  priority=5)
    t2 = Task(task_type="echo", payload="second", priority=5)

    await q.put(t1)
    await q.put(t2)

    assert (await q.get()).payload == "first"
    assert (await q.get()).payload == "second"


@pytest.mark.asyncio
async def test_qsize():
    q = PriorityTaskQueue()
    assert q.qsize() == 0
    await q.put(Task(task_type="echo", payload=None))
    assert q.qsize() == 1
